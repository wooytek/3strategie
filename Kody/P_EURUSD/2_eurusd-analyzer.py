import os, json, math, psycopg2, statistics, traceback, boto3, logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# -----------------------------------------------------------------------------
# Konfiguracja
# -----------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Klient S3 i globalne stałe
s3_client = boto3.client("s3")
KEY_EURUSD_MAIN_DASHBOARD_HTML = "eurusd_dashboard_index.html"
KEY_EURUSD_PNL_CHART_ONLY_HTML = "eurusd_pnl_chart_only.html"
BUCKET_TARGET = "3strategie"

# Parametry strategii i wskaźników
SL1,TP1 = 20,30
SL2,TP2 = 15,25
SL3,TP3 = 12,24
Z_TH=2.5
RSI_LEN=14
SMA_LEN=50
EPS=1e-5

# -----------------------------------------------------------------------------
# Funkcje pomocnicze do przetwarzania danych
# -----------------------------------------------------------------------------

def to_float(rowlist):
    """
    Konwertuje listę wierszy transakcji z bazy danych na listę krotek (data, pnl),
    obsługując różne formaty daty i zapewniając, że PnL jest typu float.
    """
    processed_data = []
    for r_item in rowlist:
        current_close_time = r_item[6]
        current_open_time = r_item[1]
        parsed_close_time = None
        if current_close_time is not None:
            if isinstance(current_close_time, datetime):
                parsed_close_time = current_close_time
            else:
                try:
                    parsed_close_time = datetime.fromisoformat(str(current_close_time))
                except (ValueError, TypeError):
                    logger.error(f"Failed to parse close_time from type {type(current_close_time)}: {current_close_time}", exc_info=True)
                    raise AttributeError(f"Could not convert close_time '{current_close_time}' (type: {type(current_close_time)}) to datetime. Check DB schema and data.") [cite: 3, 4]

        parsed_open_time = None
        if isinstance(current_open_time, datetime):
            parsed_open_time = current_open_time
        else:
            try:
                parsed_open_time = datetime.fromisoformat(str(current_open_time))
            except (ValueError, TypeError):
                logger.error(f"Unexpected type for open_time (r[1]): {type(r_item[1])} with value {r_item[1]}. Expected datetime. Please check DB schema.", exc_info=True) [cite: 6]
                raise AttributeError(f"Could not convert open_time '{current_open_time}' (type: {type(current_open_time)}) to datetime. Check DB schema and data.")
        
        # Użyj daty zamknięcia, jeśli jest dostępna (w przeciwnym razie daty otwarcia)
        date_to_use = parsed_close_time.date() if parsed_close_time else parsed_open_time.date()
        processed_data.append((date_to_use, float(r_item[7] or 0)))
    return processed_data

def cumulative_by_day(data_list):
    """
    Agreguje PnL (Profit/Loss) per dzień i oblicza skumulowaną sumę.
    Wypełnia brakujące dni, aby zapewnić ciągłość danych na wykresie.
    """
    daily_pnl = defaultdict(float)
    for trade_date, pnl in data_list:
        daily_pnl[trade_date] += pnl [cite: 7]
    
    if not daily_pnl: return [], []

    sorted_dates = sorted(daily_pnl.keys())
    start_date, end_date = sorted_dates[0], sorted_dates[-1]

    all_days = []
    current_date = start_date
    while current_date <= end_date:
        all_days.append(current_date)
        current_date += timedelta(days=1)

    cumulative_results = []
    current_cumulative_pnl = 0.0
    for d in all_days:
        current_cumulative_pnl += daily_pnl[d] [cite: 8]
        cumulative_results.append(round(current_cumulative_pnl, 1))
        
    return [d.strftime('%Y-%m-%d') for d in all_days], cumulative_results

def align_data_to_labels(original_labels, original_data, common_labels):
    """
    Wyrównuje dane szeregów czasowych do wspólnej osi etykiet (dat).
    Wypełnia brakujące punkty danych, aby umożliwić rysowanie wielu serii na jednym wykresie.
    """
    aligned_data, original_map, current_val = [], dict(zip(original_labels, original_data)), 0.0
    if not common_labels: return []

    if original_labels and original_data:
        # Znalezienie pierwszej znanej wartości, aby wypełnić początkowe brakujące dane
        first_known_label_in_common = next((lbl for lbl in common_labels if lbl in original_map), None)
        if first_known_label_in_common:
            initial_fill_value = original_map[first_known_label_in_common]
            found_first_known = False
            temp_aligned_data = []
            for label_date_str in common_labels:
                if label_date_str in original_map:
                    current_val = original_map[label_date_str]
                    found_first_known = True
                elif not found_first_known:
                    current_val = initial_fill_value # Wypełnij wartością początkową
                temp_aligned_data.append(current_val) [cite: 10]
            return temp_aligned_data
        else:
            # Jeśli żadna z etykiet nie pasuje, zwracanie tablicy zer
            return [current_val] * len(common_labels)

    # Wyrównanie danych, przenoszenie ostatniej znanej wartości
    for label_date_str in common_labels:
        if label_date_str in original_map:
            current_val = original_map[label_date_str]
        aligned_data.append(current_val)
    return aligned_data

# -----------------------------------------------------------------------------
# Funkcje generujące HTML
# -----------------------------------------------------------------------------

def rows_to_html(rows_for_table):
    """Konwertuje wiersze z bazy na wiersze tabeli HTML."""
    return "\n".join(
        f"<tr><td>{r[1].strftime('%Y-%m-%d %H:%M')}</td><td>{r[2]:.5f}</td><td>{r[3]}</td>"
        f"<td>{r[4]:.5f}</td><td>{r[5]:.5f}</td><td>{'-' if r[8] is None else f'{r[8]:.5f}'}</td>"
        f"<td data-pips='{r[7] or 0:+.1f}'>{r[7] or 0:+.1f}</td></tr>"
        for r in rows_for_table
    )

def to_html_table(title, rows_from_fetch):
    """Tworzy kompletną tabelę HTML dla danej strategii."""
    total_pips = sum(float(r[7] or 0) for r in rows_from_fetch)
    return f"""
<div class="tbl">
  <h2>{title} (Σ {total_pips:+.1f} pips)</h2>
  <div class="tbl-inner">
    <table>
      <thead>
        <tr><th>Open time</th><th>Open price</th><th>Dir</th><th>SL</th><th>TP</th><th>Close Price</th><th>Res Pips</th></tr>
      </thead>
      <tbody>{rows_to_html(rows_from_fetch)}</tbody>
    </table>
  </div>
</div>"""

def prepare_pnl_chart_data(s1_trades, s2_trades, s3_trades, logger_instance):
    """
    Przygotowuje dane do wykresu PnL. Przetwarza transakcje dla każdej strategii,
    oblicza skumulowany PnL i wyrównuje wszystkie serie danych do wspólnej osi czasu.
    """
    logger_instance.info("prepare_pnl_chart_data: Rozpoczynam przetwarzanie danych dla wykresów P/L.")
    pnl_labels1, cum1 = cumulative_by_day(to_float(s1_trades))
    pnl_labels2, cum2 = cumulative_by_day(to_float(s2_trades))
    pnl_labels3, cum3 = cumulative_by_day(to_float(s3_trades))

    # wspólna, posortowana lista dat dla osi X wykresu
    all_dates_set = set(pnl_labels1) | set(pnl_labels2) | set(pnl_labels3)
    all_dates_common = sorted(list(all_dates_set)) [cite: 12]
    
    min_date_val, max_date_val = (all_dates_common[0], all_dates_common[-1]) if all_dates_common else (None, None)

    # Wyrównnie danych PnL do wspólnej osi dat
    cum1_aligned = align_data_to_labels(pnl_labels1, cum1, all_dates_common)
    cum2_aligned = align_data_to_labels(pnl_labels2, cum2, all_dates_common) [cite: 13]
    cum3_aligned = align_data_to_labels(pnl_labels3, cum3, all_dates_common)
    
    logger_instance.info(f"prepare_pnl_chart_data: Dane wyrównane. Min date: {min_date_val}, Max date: {max_date_val}") [cite: 14]
    
    return {
        "all_dates": all_dates_common,
        "cum1_aligned": cum1_aligned, "cum2_aligned": cum2_aligned, "cum3_aligned": cum3_aligned,
        "min_date_val": min_date_val,
        "max_date_val": max_date_val
    }

def render_main_eurusd_dashboard_html(rate_chart_labels, rate_chart_values, 
                                      s1_table_data, s2_table_data, s3_table_data,
                                      pnl_prepared_data, logger_instance):
    """
    Generuje pełny kod HTML dla głównego dashboardu, zawierający wykresy
    kursu i PnL oraz tabele transakcji.
    """
    s1_html_table = to_html_table("Strategia 1 – Klasyczna", s1_table_data) [cite: 16]
    s2_html_table = to_html_table("Strategia 2 – Anomalie", s2_table_data) [cite: 16]
    s3_html_table = to_html_table("Strategia 3 – Fraktal + SMA", s3_table_data) [cite: 16]
    
    # Konwersja danych do formatu JSON dla JavaScript
    formatted_rate_labels_str = json.dumps(rate_chart_labels)
    formatted_rate_values_str = json.dumps(rate_chart_values)
    formatted_all_dates_pnl_str = json.dumps(pnl_prepared_data["all_dates"])
    formatted_cum1_aligned_pnl_str = json.dumps(pnl_prepared_data["cum1_aligned"])
    formatted_cum2_aligned_pnl_str = json.dumps(pnl_prepared_data["cum2_aligned"])
    formatted_cum3_aligned_pnl_str = json.dumps(pnl_prepared_data["cum3_aligned"])
    min_date_pnl_for_js = json.dumps(pnl_prepared_data["min_date_val"])
    max_date_pnl_for_js = json.dumps(pnl_prepared_data["max_date_val"])

    HOME_ICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" height="45px" viewBox="0 0 24 24" width="45px" fill="currentColor"><path d="M0 0h24v24H0V0z" fill="none"/><path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8z"/></svg>""" [cite: 15]

    html = """<!doctype html><html lang="pl"><head><meta charset=utf-8>
    <title>Dashboard EUR/USD</title> 
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/luxon@3.x/build/global/luxon.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon"></script>
    <style>
        body { font-family: sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; display: flex; justify-content: center; align-items: flex-start; min-height: 100vh;  }
        .main-content-wrapper { width: 90%%; max-width: 1200px; background-color: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,.1); box-sizing: border-box; position: relative;  }
        h1 { text-align: center; color: #333; margin-bottom: 30px;  }
        h2 { color: #333; text-align: left; margin-top: 30px; margin-bottom: 15px;  }
        .chart-box { width: 100%%; height: 350px; margin: 20px auto; display: flex; justify-content: center; align-items: center;  }
        canvas { max-width: 100%%; height: 100%%;  }
        .tbl { width: 100%%; margin: 20px auto; text-align: left;  }
        .tbl-inner { max-height: 300px; overflow-y: auto; border: 1px solid #e0e0e0; border-radius: 5px; box-shadow: inset 0 0 5px rgba(0,0,0,.05);  }
        table { width: 100%%; border-collapse: collapse; margin: 0; font-size: 0.9em; min-width: 600px;  }
        th, td { padding: 12px 15px; border-bottom: 1px solid #f0f0f0; text-align: left;  }
        th { background-color: #e9ecef; color: #495057; font-weight: 600; position: sticky; top: 0; z-index: 2;  }
        tbody tr:nth-child(even) { background-color: #f8f9fa;  }
        tbody tr:hover { background-color: #e2e6ea;  }
        td:last-child { text-align: left; font-weight: bold;  }
        td[data-pips^="+"] { color: #28a745;  }
        td[data-pips^="-"] { color: #dc3545;  }
        .home-link { position: absolute; top: 15px; right: 15px; text-decoration: none; color: #555; z-index: 1000;  }
        .home-link:hover { color: #007bff;  }
    </style>
    </head>
    <body>
        <div class="main-content-wrapper">
            <a href="https://3strategie.s3.eu-central-1.amazonaws.com/summary_dashboard.html" class="home-link" title="Strona główna podsumowania">%s</a>
            <h1>EUR/USD – Dashboard strategii</h1>
            <div class="chart-box"><canvas id="rateChart"></canvas></div>
            <div class="chart-box"><canvas id="pnlChart"></canvas></div>
            %s %s %s </div> <script>
        new Chart(document.getElementById('rateChart'),{type:'line',data:{labels:%s,datasets:[{label:'EUR/USD Rate',data:%s,borderColor:'#2563eb',tension:0.1}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{ticks:{callback:function(value){return value.toFixed(5);}}},x:{}},layout:{padding:{right:20}}}});
        new Chart(document.getElementById('pnlChart'),{type:'line',data:{labels:%s,datasets:[{label:'Strategia 1 - PnL',data:%s,borderColor:'rgba(255, 99, 132, 1)',tension:0.1,fill:false},{label:'Strategia 2 - PnL',data:%s,borderColor:'rgba(54, 162, 235, 1)',tension:0.1,fill:false},{label:'Strategia 3 - PnL',data:%s,borderColor:'rgba(75, 192, 192, 1)',tension:0.1,fill:false}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{beginAtZero:true,ticks:{callback:function(value){let f=value.toFixed(1);const l=7;if(value>0){f=' +'+f}f=' '+f;return f.padStart(l);}}},x:{type:'time',time:{unit:'day',tooltipFormat:'dd-MM-yyyy',displayFormats:{day:'dd-MM-yyyy'}},min:%s,max:%s}},layout:{padding:{right:20}}}});
    </script></body></html>""" % (
            HOME_ICON_SVG,
            s1_html_table, s2_html_table, s3_html_table,
            formatted_rate_labels_str, formatted_rate_values_str,
            formatted_all_dates_pnl_str,
            formatted_cum1_aligned_pnl_str, formatted_cum2_aligned_pnl_str, formatted_cum3_aligned_pnl_str,
            min_date_pnl_for_js, max_date_pnl_for_js
        )
    logger_instance.info("render_main_eurusd_dashboard_html: Zakończono generowanie HTML.") [cite: 52]
    return html

def render_eurusd_pnl_chart_only_html(pnl_prepared_data):
    """
    Generuje kod HTML zawierający wyłącznie wykres PnL. Przeznaczony do osadzania
    w innych panelach (np. w głównym dashboardzie).
    """
    datasets_python_structure = [
        {"label":'Strategia 1 - PnL',"data":pnl_prepared_data["cum1_aligned"],"borderColor":'rgba(255, 99, 132, 1)',"tension":0.1,"fill":False}, [cite: 53]
        {"label":'Strategia 2 - PnL',"data":pnl_prepared_data["cum2_aligned"],"borderColor":'rgba(54, 162, 235, 1)',"tension":0.1,"fill":False}, [cite: 54]
        {"label":'Strategia 3 - PnL',"data":pnl_prepared_data["cum3_aligned"],"borderColor":'rgba(75, 192, 192, 1)',"tension":0.1,"fill":False}
    ]
    datasets_json_str = json.dumps(datasets_python_structure)
    
    formatted_x_labels_pnl = json.dumps(pnl_prepared_data["all_dates"])
    min_date_for_js = json.dumps(pnl_prepared_data["min_date_val"])
    max_date_for_js = json.dumps(pnl_prepared_data["max_date_val"])

    html_content = """<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <title>Wykres PnL EUR/USD</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/luxon@3.x/build/global/luxon.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon"></script>
    <style>
        html, body { margin: 0; padding: 0; width: 100%%; height: 100%%; overflow: hidden; background-color: transparent;  }
        canvas#pnlChartOnly { display: block; width: 100%% !important; height: 100%% !important;  }
    </style>
</head>
<body>
    <canvas id="pnlChartOnly"></canvas>
    <script>
        document.addEventListener('DOMContentLoaded', function () {
            try {
                const ctx = document.getElementById('pnlChartOnly').getContext('2d');
                if (!ctx) { console.error('Nie udało się pobrać kontekstu 2D dla canvas.'); return;  }
                new Chart(ctx, {
                    type: 'line',
                    data: { labels: %s, datasets: %s },
                    options: { 
                        responsive: true, maintainAspectRatio: false,
                        plugins: { tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.raw.toFixed(1) + ' pips' } } },
                        scales: {
                            y: { beginAtZero: true, ticks: { callback: function(v) { let f = v.toFixed(1); if (v > 0) f = '+' + f; return (' ' + f).padStart(7); } } },
                            x: { type: 'time', time: { unit: 'day', tooltipFormat: 'dd-MM-yyyy', displayFormats: { day: 'dd-MM-yyyy' } }, min: %s, max: %s }
                        },
                        layout: { padding: 5 }
                    }
                });
            } catch (e) { console.error('Błąd podczas inicjalizacji wykresu Chart.js:', e);  }
        });
    </script>
</body>
</html>""" % (formatted_x_labels_pnl, datasets_json_str, min_date_for_js, max_date_for_js)
    return html_content

# -----------------------------------------------------------------------------
# Połączenie z bazą danych i logika strategii
# -----------------------------------------------------------------------------

def db():
    """Nawiązuje połączenie z bazą danych PostgreSQL."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"), 
        dbname=os.getenv("DB_NAME"), 
        user=os.getenv("DB_USER"), 
        password=os.getenv("DB_PASSWORD"), 
        port=os.getenv("DB_PORT", "5432") [cite: 76]
    )
    conn.autocommit = True 
    return conn

def safe_rsi(vals, n=14):
    """
    Oblicza wskaźnik RSI (Relative Strength Index) w sposób bezpieczny,
    sprawdzając, czy dostępna jest wystarczająca ilość danych.
    """
    if len(vals) < n + 1:
        logger.warning(f"safe_rsi: Za mało danych ({len(vals)}) do obliczenia RSI({n}). Wymagane {n+1}.") [cite: 77]
        return None
    
    deltas = [vals[i] - vals[i-1] for i in range(1, len(vals))]
    gains = [d for d in deltas if d > 0]
    losses = [abs(d) for d in deltas if d <= 0]
    
    avg_gain = sum(gains[-n:]) / n if len(gains) >= n else 0.0
    avg_loss = sum(losses[-n:]) / n if len(losses) >= n else 0.0

    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def handle_strategy(cur, table, open_cond, sl, tp, price, time, eps, strategy_name, extra_cols=None):
    """
    Centralna funkcja do zarządzania logiką pojedynczej strategii.
    Sprawdza i zamyka otwarte pozycje (TP/SL), a jeśli nie ma otwartych pozycji,
    sprawdza warunki i otwiera nową.
    """
    cur.execute(f"SELECT trade_id, direction, sl_price, tp_price, open_price FROM {table} WHERE close_time IS NULL")
    open_trades = cur.fetchall()

    # Sprawdzenie czy zamknąć istniejące transakcje
    for trade_id, direction, sl_px, tp_px, open_px in open_trades:
        sl_px, tp_px, open_px = map(float, (sl_px, tp_px, open_px))
        hit_tp = (direction == 'LONG' and price >= tp_px - eps) or (direction == 'SHORT' and price <= tp_px + eps) [cite: 81]
        hit_sl = (direction == 'LONG' and price <= sl_px + eps) or (direction == 'SHORT' and price >= sl_px - eps) [cite: 81]

        if hit_tp or hit_sl:
            pnl = (tp_px - open_px if hit_tp else sl_px - open_px) * (10000 if direction == 'LONG' else -10000)
            logger.info(f"handle_strategy '{strategy_name}': Zamykanie transakcji {trade_id}. Cena: {price}, Wynik: {'TP' if hit_tp else 'SL'}.") [cite: 82]
            cur.execute(f"UPDATE {table} SET close_time=%s, close_price=%s, result_pips=%s WHERE trade_id=%s", (time, price, round(pnl, 1), trade_id))
    
    # Jeśli istnieje już otwarta transakcja, nie otwieraj nowej
    cur.execute(f"SELECT 1 FROM {table} WHERE close_time IS NULL LIMIT 1")
    if cur.fetchone(): [cite: 83]
        return

    # Warunki otwarcia nowej transakcji
    long_c, short_c = open_cond
    if not (long_c or short_c):
        return
    
    dir_action = 'LONG' if long_c else 'SHORT'
    sl_px_val = round(price - 0.0001 * sl, 6) if dir_action == 'LONG' else round(price + 0.0001 * sl, 6)
    tp_px_val = round(price + 0.0001 * tp, 6) if dir_action == 'LONG' else round(price - 0.0001 * tp, 6) [cite: 84]
    
    logger.info(f"handle_strategy '{strategy_name}': Otwieranie nowej transakcji {dir_action} @ {price}") [cite: 85]
    
    # Wstawienie nowej transakcji do bazy danych
    if extra_cols:
        col, val = extra_cols
        cur.execute(f"INSERT INTO {table}(open_time, open_price, direction, {col}, sl_price, tp_price) VALUES (%s, %s, %s, %s, %s, %s)", (time, price, dir_action, val, sl_px_val, tp_px_val))
    else:
        cur.execute(f"INSERT INTO {table}(open_time, open_price, direction, sl_price, tp_price) VALUES (%s, %s, %s, %s, %s)", (time, price, dir_action, sl_px_val, tp_px_val)) [cite: 86]

def fetch(cur, table):
    """Pobiera ostatnie 100 transakcji z podanej tabeli."""
    cur.execute(f"""SELECT trade_id, open_time, open_price, direction, sl_price, tp_price,
                           close_time, result_pips, close_price 
                   FROM {table} ORDER BY trade_id DESC LIMIT 100""") [cite: 87]
    return cur.fetchall()

# -----------------------------------------------------------------------------
# Główna funkcja Lambda
# -----------------------------------------------------------------------------
def lambda_handler(event, context):
    logger.info(f"lambda_handler: Rozpoczęto wykonanie funkcji.") [cite: 88]

    try:
        # Najnowsze dane o kursach
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT timestamp, rate FROM eurusd_rates ORDER BY timestamp DESC LIMIT 300")
            rows = cur.fetchall()[::-1]
        
        if len(rows) < 1: [cite: 89]
            raise ValueError("Brak danych w tabeli eurusd_rates do analizy.")

        times = [r[0].astimezone(timezone.utc) for r in rows]
        prices = [float(r[1]) for r in rows]
        t_now, p_now = times[-1], prices[-1]
        logger.info(f"lambda_handler: Ostatni kurs: {p_now:.5f} z czasu {t_now}") [cite: 90]

        # Obliczanie wskaźników i  logika strategii
        rsi14 = safe_rsi(prices, RSI_LEN)
        if rsi14 is None:
            logger.warning("lambda_handler: Za mało danych do obliczenia RSI. Pomijam logikę strategii.")
        else:
            logger.info(f"lambda_handler: Obliczone RSI14 = {rsi14}. Przetwarzanie strategii.") [cite: 91]
            with db() as conn, conn.cursor() as cur:
                # Strategia 1: Klasyczna (RSI)
                handle_strategy(cur, 'eurusd_trades', (rsi14 < 30, rsi14 > 70), SL1, TP1, p_now, t_now, EPS, 'Klasyczna')

                # Strategia 2: Anomalie (Z-Score)
                if len(prices) >= 51: [cite: 92]
                    log_returns = [math.log(prices[i] / prices[i - 1]) for i in range(len(prices) - 50, len(prices))]
                    ret = math.log(prices[-1] / prices[-2]) if len(prices) >= 2 else 0.0
                    mean = statistics.fmean(log_returns) [cite: 94]
                    std = statistics.stdev(log_returns) if len(log_returns) > 1 else 0.0
                    z = (ret - mean) / std if std != 0 else 0.0
                    logger.info(f"lambda_handler: Strategia Anomalii - z_score={z:.2f}") [cite: 95]
                    handle_strategy(cur, 'eurusd_anom_trades', (z <= -Z_TH and rsi14 < 40, z >= Z_TH and rsi14 > 60), SL2, TP2, p_now, t_now, EPS, 'Anomalia', extra_cols=('z_score', round(z, 2))) [cite: 96]
                
                # Strategia 3: Fraktale + SMA
                if len(prices) >= SMA_LEN: [cite: 97]
                    sma50 = statistics.fmean(prices[-SMA_LEN:])
                    is_high = prices[-3] == max(prices[-5:]) and prices[-3] > prices[-4] and prices[-3] > prices[-2] [cite: 98]
                    is_low = prices[-3] == min(prices[-5:]) and prices[-3] < prices[-4] and prices[-3] < prices[-2] [cite: 98]
                    handle_strategy(cur, 'eurusd_frac_trades', (is_low and p_now > sma50, is_high and p_now < sma50), SL3, TP3, p_now, t_now, EPS, 'Fraktal+SMA') [cite: 99]
        
        # Pobranie zaktualizowanych danych o transakcjach
        with db() as conn, conn.cursor() as cur:
            s1_trades = fetch(cur, 'eurusd_trades')
            s2_trades = fetch(cur, 'eurusd_anom_trades') [cite: 101]
            s3_trades = fetch(cur, 'eurusd_frac_trades') [cite: 101]

        # Przygotowanie danych i wygenerowanie raportu HTML
        pnl_prepared_data = prepare_pnl_chart_data(s1_trades, s2_trades, s3_trades, logger)
        rate_chart_labels = [t.strftime('%H:%M') for t in times[-15:]]
        rate_chart_values = prices[-15:]

        logger.info("lambda_handler: Generuję HTML dla głównego dashboardu EUR/USD.") [cite: 102]
        html_content_main_dashboard = render_main_eurusd_dashboard_html(
            rate_chart_labels, rate_chart_values,
            s1_trades, s2_trades, s3_trades,
            pnl_prepared_data, logger
        )

        logger.info("lambda_handler: Generuję HTML dla wykresu PnL EUR/USD (tylko wykres).") [cite: 105]
        pnl_chart_only_html_eurusd = render_eurusd_pnl_chart_only_html(pnl_prepared_data)

        # Zapis wygenerowanych plików HTML w S3
        s3_client.put_object(
            Bucket=BUCKET_TARGET, 
            Key=KEY_EURUSD_MAIN_DASHBOARD_HTML,
            Body=html_content_main_dashboard.encode("utf-8"),
            ContentType="text/html; charset=utf-8", [cite: 104]
            CacheControl="no-cache" [cite: 104]
        )
        logger.info(f"📈 Główny dashboard EUR/USD zaktualizowany w S3.")

        s3_client.put_object(
            Bucket=BUCKET_TARGET, 
            Key=KEY_EURUSD_PNL_CHART_ONLY_HTML, [cite: 106]
            Body=pnl_chart_only_html_eurusd.encode("utf-8"),
            ContentType="text/html; charset=utf-8", [cite: 107]
            CacheControl="no-cache"
        )
        logger.info(f"📈 Wykres PnL EUR/USD zaktualizowany w S3.")

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "text/html; charset=utf-8"}, [cite: 109]
            "body": html_content_main_dashboard
        }

    except Exception as e:
        logger.error(f"lambda_handler: KRYTYCZNY BŁĄD: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}