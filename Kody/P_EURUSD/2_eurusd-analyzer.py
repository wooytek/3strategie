import os, json, math, psycopg2, statistics, traceback, boto3, logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ───────── konfiguracja loggera ─────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ───────── Global S3 Client and Constants ─────────
s3_client = boto3.client("s3")
KEY_EURUSD_MAIN_DASHBOARD_HTML = "eurusd_dashboard_index.html"
KEY_EURUSD_PNL_CHART_ONLY_HTML = "eurusd_pnl_chart_only.html"
BUCKET_TARGET = "3strategie"

# ───────── HTML helpers (istniejące) ─────────
def rows_to_html(rows_for_table):
    return "\n".join(
        f"<tr><td>{r[1].strftime('%Y-%m-%d %H:%M')}</td><td>{r[2]:.5f}</td><td>{r[3]}</td>"
        f"<td>{r[4]:.5f}</td><td>{r[5]:.5f}</td><td>{'-' if r[8] is None else f'{r[8]:.5f}'}</td>"
        f"<td data-pips='{r[7] or 0:+.1f}'>{r[7] or 0:+.1f}</td></tr>"
        for r in rows_for_table
    )

def to_float(rowlist):
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
                    raise AttributeError(f"Could not convert close_time '{current_close_time}' (type: {type(current_close_time)}) to datetime. Check DB schema and data.")
        parsed_open_time = None
        if isinstance(current_open_time, datetime):
            parsed_open_time = current_open_time
        else:
            try:
                parsed_open_time = datetime.fromisoformat(str(current_open_time))
            except (ValueError, TypeError):
                logger.error(f"Unexpected type for open_time (r[1]): {type(r_item[1])} with value {r_item[1]}. Expected datetime. Please check DB schema.", exc_info=True)
                raise AttributeError(f"Could not convert open_time '{current_open_time}' (type: {type(current_open_time)}) to datetime. Check DB schema and data.")
        date_to_use = parsed_close_time.date() if parsed_close_time else parsed_open_time.date()
        processed_data.append((date_to_use, float(r_item[7] or 0)))
    return processed_data

def cumulative_by_day(data_list):
    daily_pnl = defaultdict(float)
    for trade_date, pnl in data_list:
        daily_pnl[trade_date] += pnl
    if not daily_pnl: return [], []
    sorted_dates = sorted(daily_pnl.keys())
    if not sorted_dates: return [], []
    start_date, end_date = sorted_dates[0], sorted_dates[-1]
    all_days = []
    current_date = start_date
    while current_date <= end_date:
        all_days.append(current_date)
        current_date += timedelta(days=1)
    cumulative_results = []
    current_cumulative_pnl = 0.0
    for d in all_days:
        current_cumulative_pnl += daily_pnl[d]
        cumulative_results.append(round(current_cumulative_pnl, 1))
    return [d.strftime('%Y-%m-%d') for d in all_days], cumulative_results

def align_data_to_labels(original_labels, original_data, common_labels):
    aligned_data, original_map, current_val = [], dict(zip(original_labels, original_data)), 0.0
    if not common_labels: return []
    if original_labels and original_data:
        first_known_label_in_common = next((lbl for lbl in common_labels if lbl in original_map), None)
        if first_known_label_in_common:
            initial_fill_value, found_first_known, temp_aligned_data = original_map[first_known_label_in_common], False, []
            for label_date_str in common_labels:
                if label_date_str in original_map:
                    current_val, found_first_known = original_map[label_date_str], True
                elif not found_first_known:
                    current_val = initial_fill_value
                temp_aligned_data.append(current_val)
            return temp_aligned_data
        else: return [current_val] * len(common_labels)
    for label_date_str in common_labels:
        if label_date_str in original_map: current_val = original_map[label_date_str]
        aligned_data.append(current_val)
    return aligned_data

def to_html_table(title, rows_from_fetch):
    tot = sum(float(r[7] or 0) for r in rows_from_fetch)
    return f"""
<div class="tbl">
  <h2>{title} (Σ {tot:+.1f} pips)</h2>
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
    logger_instance.info("prepare_pnl_chart_data: Rozpoczynam przetwarzanie danych dla wykresów P/L.")
    pnl_labels1, cum1 = cumulative_by_day(to_float(s1_trades))
    pnl_labels2, cum2 = cumulative_by_day(to_float(s2_trades))
    pnl_labels3, cum3 = cumulative_by_day(to_float(s3_trades))
    all_dates_set = set()
    if pnl_labels1: all_dates_set.update(pnl_labels1)
    if pnl_labels2: all_dates_set.update(pnl_labels2)
    if pnl_labels3: all_dates_set.update(pnl_labels3)
    all_dates_common = sorted(list(all_dates_set))
    logger_instance.info(f"prepare_pnl_chart_data: Wszystkie unikalne daty posortowane: {len(all_dates_common)}")
    
    min_date_val, max_date_val = None, None
    cum1_aligned, cum2_aligned, cum3_aligned = [], [], []

    if not all_dates_common:
        logger_instance.warning("prepare_pnl_chart_data: Brak danych 'all_dates_common' do generowania wykresu PNL.")
    else:
        min_date_val = all_dates_common[0]
        max_date_val = all_dates_common[-1]
        cum1_aligned = align_data_to_labels(pnl_labels1, cum1, all_dates_common)
        cum2_aligned = align_data_to_labels(pnl_labels2, cum2, all_dates_common)
        cum3_aligned = align_data_to_labels(pnl_labels3, cum3, all_dates_common)
    
    logger_instance.info(f"prepare_pnl_chart_data: Dane wyrównane. Min date: {min_date_val}, Max date: {max_date_val}")
    
    return {
        "all_dates": all_dates_common,
        "cum1_aligned": cum1_aligned, "cum2_aligned": cum2_aligned, "cum3_aligned": cum3_aligned,
        "min_date_val": min_date_val, # Zwracamy bezpośrednio string daty lub None
        "max_date_val": max_date_val  # Zwracamy bezpośrednio string daty lub None
    }

# ───────── Modified render_html (Main EURUSD Dashboard) Function ─────────
HOME_ICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" height="45px" viewBox="0 0 24 24" width="45px" fill="currentColor">
  <path d="M0 0h24v24H0V0z" fill="none"/>
  <path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8z"/>
</svg>"""

# ───────── Modified render_html (Main EURUSD Dashboard) Function ─────────
def render_main_eurusd_dashboard_html(rate_chart_labels, rate_chart_values, 
                                      s1_table_data, s2_table_data, s3_table_data,
                                      pnl_prepared_data, logger_instance):
    
    s1_html_table = to_html_table("Strategia 1 – Klasyczna", s1_table_data)
    s2_html_table = to_html_table("Strategia 2 – Anomalie", s2_table_data)
    s3_html_table = to_html_table("Strategia 3 – Fraktal + SMA", s3_table_data)
    
    formatted_rate_labels_str = json.dumps(rate_chart_labels)
    formatted_rate_values_str = json.dumps(rate_chart_values)
    
    formatted_all_dates_pnl_str = json.dumps(pnl_prepared_data["all_dates"])
    formatted_cum1_aligned_pnl_str = json.dumps(pnl_prepared_data["cum1_aligned"])
    formatted_cum2_aligned_pnl_str = json.dumps(pnl_prepared_data["cum2_aligned"])
    formatted_cum3_aligned_pnl_str = json.dumps(pnl_prepared_data["cum3_aligned"])
    
    min_date_pnl_for_js = json.dumps(pnl_prepared_data["min_date_val"])
    max_date_pnl_for_js = json.dumps(pnl_prepared_data["max_date_val"])

    html = """<!doctype html><html lang="pl"><head><meta charset=utf-8>
    <title>Dashboard EUR/USD</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/luxon@3.x/build/global/luxon.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon"></script>
    <style>
        body { font-family: sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; display: flex; justify-content: center; align-items: flex-start; min-height: 100vh; }
        .main-content-wrapper { 
            width: 90%%; 
            max-width: 1200px; 
            background-color: #fff; 
            padding: 20px; 
            border-radius: 8px; 
            box-shadow: 0 2px 4px rgba(0,0,0,.1); 
            box-sizing: border-box; 
            position: relative; /* Kontekst dla pozycjonowania absolutnego linku "home" */
        }
        h1 { text-align: center; color: #333; margin-bottom: 30px; }
        h2 { color: #333; text-align: left; margin-top: 30px; margin-bottom: 15px; }
        .chart-box { width: 100%%; height: 350px; margin: 20px auto; display: flex; justify-content: center; align-items: center; }
        canvas { max-width: 100%%; height: 100%%; }
        .tbl { width: 100%%; margin: 20px auto; text-align: left; }
        .tbl-inner { max-height: 300px; overflow-y: auto; border: 1px solid #e0e0e0; border-radius: 5px; box-shadow: inset 0 0 5px rgba(0,0,0,.05); }
        table { width: 100%%; border-collapse: collapse; margin: 0; font-size: 0.9em; min-width: 600px; }
        th, td { padding: 12px 15px; border-bottom: 1px solid #f0f0f0; text-align: left; }
        th { background-color: #e9ecef; color: #495057; font-weight: 600; position: sticky; top: 0; z-index: 2; }
        tbody tr:nth-child(even) { background-color: #f8f9fa; }
        tbody tr:hover { background-color: #e2e6ea; }
        td:last-child { text-align: left; font-weight: bold; }
        td[data-pips^="+"] { color: #28a745; }
        td[data-pips^="-"] { color: #dc3545; }
        .home-link {
            position: absolute; 
            top: 15px;      /* Dopasuj odstęp od góry kontenera */
            right: 15px;     /* Dopasuj odstęp od prawej strony kontenera */
            text-decoration: none;
            color: #555;    /* Kolor SVG (dziedziczony przez fill="currentColor") */
            z-index: 1000; 
            transition: color 0.2s ease-in-out;
            display: inline-block; /* Dla poprawnego działania paddingu i rozmiaru */
            line-height: 0; /* Usuwa dodatkową przestrzeń pod SVG */
        }
        .home-link svg { /* Bezpośrednie stylowanie SVG jeśli potrzebne */
            display: block; /* Lub inline, w zależności od preferencji */
        }
        .home-link:hover {
            color: #007bff; 
        }
    </style>
    </head>
    <body>
        <div class="main-content-wrapper">
            <a href="https://3strategie.s3.eu-central-1.amazonaws.com/summary_dashboard.html" class="home-link" title="Strona główna podsumowania">
                %s </a>
            <h1>EUR/USD – Dashboard strategii</h1>
            <div class="chart-box"><canvas id="rateChart"></canvas></div>
            <div class="chart-box"><canvas id="pnlChart"></canvas></div>
            %s %s %s </div> <script>
        let maxGlobalYAxisWidth = 0; let chartInstances = [];
        const yAxisSyncPlugin = { id: 'yAxisSync', beforeLayout: (chart) => { if (chart.canvas.id === 'rateChart' && chartInstances.length === 0) { maxGlobalYAxisWidth = 0; } }, afterFit: (chart) => { if (chart.scales.y && chart.scales.y.id === 'y') { maxGlobalYAxisWidth = Math.max(maxGlobalYAxisWidth, chart.scales.y.width); } }, afterDraw: (chart) => { if (chart.scales.y && chart.scales.y.id === 'y') { if (chart.scales.y.width < maxGlobalYAxisWidth) { chart.scales.y.width = maxGlobalYAxisWidth; chart.update('none'); } } }, afterInit: (chart) => { chartInstances.push(chart); if (chartInstances.length === 2) { chartInstances.forEach(inst => { if (inst.scales.y && inst.scales.y.id === 'y') { inst.scales.y.width = maxGlobalYAxisWidth; } inst.update('none'); }); } } };
        Chart.register(yAxisSyncPlugin);
        new Chart(document.getElementById('rateChart'), { type: 'line', data: { labels: %s, datasets: [{ label: 'EUR/USD Rate', data: %s, borderColor: '#2563eb', tension: 0.1 }] }, options: { responsive: true, maintainAspectRatio: false, scales: { y: { id: 'y', ticks: { callback: function(value) { return value.toFixed(5); } } }, x: {} }, layout: { padding: { right: 20 } } } });
        new Chart(document.getElementById('pnlChart'), { type: 'line', data: { labels: %s, datasets: [ { label: 'Strategia 1 - PnL', data: %s, borderColor: 'rgba(255, 99, 132, 1)', tension: 0.1, fill: false }, { label: 'Strategia 2 - PnL', data: %s, borderColor: 'rgba(54, 162, 235, 1)', tension: 0.1, fill: false }, { label: 'Strategia 3 - PnL', data: %s, borderColor: 'rgba(75, 192, 192, 1)', tension: 0.1, fill: false } ] }, options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true, id: 'y', ticks: { callback: function(value) { let formattedValue = value.toFixed(1); const desiredLength = 7; if (value > 0) { formattedValue = ' +' + formattedValue; } formattedValue = ' ' + formattedValue; return formattedValue.padStart(desiredLength); } } }, x: { type: 'time', time: { unit: 'day', tooltipFormat: 'dd-MM-yyyy', displayFormats: { day: 'dd-MM-yyyy' } }, min: %s, max: %s } }, layout: { padding: { right: 20 } } } });
        </script></body></html>""" % (
            HOME_ICON_SVG, # Dodano ikonę SVG jako pierwszy argument
            s1_html_table, s2_html_table, s3_html_table,
            formatted_rate_labels_str, formatted_rate_values_str,
            formatted_all_dates_pnl_str,
            formatted_cum1_aligned_pnl_str, formatted_cum2_aligned_pnl_str, formatted_cum3_aligned_pnl_str,
            min_date_pnl_for_js, max_date_pnl_for_js
        )
    logger_instance.info("render_main_eurusd_dashboard_html: Zakończono generowanie HTML.")
    return html

def render_eurusd_pnl_chart_only_html(pnl_prepared_data):
    # Zbuduj listę datasetów jako strukturę Pythona
    datasets_python_structure = [
        {
            "label": 'Strategia 1 - PnL',
            "data": pnl_prepared_data["cum1_aligned"], # Bezpośrednio lista danych
            "borderColor": 'rgba(255, 99, 132, 1)',
            "tension": 0.1,
            "fill": False
        },
        {
            "label": 'Strategia 2 - PnL',
            "data": pnl_prepared_data["cum2_aligned"],
            "borderColor": 'rgba(54, 162, 235, 1)',
            "tension": 0.1,
            "fill": False
        },
        {
            "label": 'Strategia 3 - PnL',
            "data": pnl_prepared_data["cum3_aligned"],
            "borderColor": 'rgba(75, 192, 192, 1)',
            "tension": 0.1,
            "fill": False
        }
    ]
    # Skonwertuj całą strukturę datasetów do stringa JSON
    # Ten string będzie wyglądał np. tak: "[{\"label\": \"Strategia 1...\", ...}, {...}]"
    datasets_json_str = json.dumps(datasets_python_structure)
    
    formatted_x_labels_pnl = json.dumps(pnl_prepared_data["all_dates"])
    min_date_for_js = json.dumps(pnl_prepared_data["min_date_val"])
    max_date_for_js = json.dumps(pnl_prepared_data["max_date_val"])

    html_content = """<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Wykres PnL EUR/USD</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/luxon@3.x/build/global/luxon.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon"></script>
    <style>
        html, body {
            margin: 0;
            padding: 0;
            width: 100%%; 
            height: 100%%; 
            overflow: hidden; 
            background-color: transparent;
        }
        canvas#pnlChartOnly {
            display: block;
            width: 100%% !important;
            height: 100%% !important;
        }
    </style>
</head>
<body>
    <canvas id="pnlChartOnly"></canvas>
    <script>
        document.addEventListener('DOMContentLoaded', function () {
            try {
                const ctx = document.getElementById('pnlChartOnly').getContext('2d');
                if (!ctx) {
                    console.error('Nie udało się pobrać kontekstu 2D dla canvas.');
                    return;
                }

                new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: %s,      /* formatted_x_labels_pnl */
                        datasets: %s     /* datasets_json_str */
                    },
                    options: { 
                        responsive: true, 
                        maintainAspectRatio: false,
                        plugins: {
                            tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.raw.toFixed(1) + ' pips' } }
                        },
                        scales: {
                            y: { 
                                beginAtZero: true, 
                                ticks: { 
                                    callback: function(value) {
                                        let formattedValue = value.toFixed(1);
                                        const desiredLength = 7; 
                                        if (value > 0) {
                                            formattedValue = '+' + formattedValue; 
                                        }
                                        formattedValue = ' ' + formattedValue;
                                        return formattedValue.padStart(desiredLength); 
                                    }
                                }
                            },
                            x: { 
                                type: 'time', 
                                time: { 
                                    unit: 'day', 
                                    tooltipFormat: 'dd-MM-yyyy', 
                                    displayFormats: { day: 'dd-MM-yyyy' } 
                                }, 
                                min: %s, /* min_date_for_js */
                                max: %s  /* max_date_for_js */
                            }
                        },
                        layout: { 
                            padding: 5
                        }
                    }
                });
            } catch (e) {
                console.error('Błąd podczas inicjalizacji wykresu Chart.js:', e);
            }
        });
    </script>
</body>
</html>""" % (formatted_x_labels_pnl, datasets_json_str, min_date_for_js, max_date_for_js)
    return html_content

# Reszta kodu (DB helper, PARAMS, safe_rsi, handle_strategy, fetch, lambda_handler)
# pozostaje taka sama jak w ostatnio przesłanym pliku (eurusd.txt),
# ponieważ logi wskazują, że te części działają poprawnie aż do momentu renderowania HTML.

# ───────── DB helper (istniejące) ─────────
def db(): #
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"), 
        dbname=os.getenv("DB_NAME"), 
        user=os.getenv("DB_USER"), 
        password=os.getenv("DB_PASSWORD"), 
        port=os.getenv("DB_PORT", "5432") 
    )
    conn.autocommit = True 
    return conn

# ───────── PARAMS & INDICATORS (istniejące) ─────────
SL1,TP1 = 20,30; SL2,TP2 = 15,25; SL3,TP3 = 12,24 #
Z_TH=2.5; RSI_LEN=14; SMA_LEN=50; EPS=1e-5 #

def safe_rsi(vals,n=14): #
    if len(vals) < n+1: #
        logger.warning(f"safe_rsi: Za mało danych ({len(vals)}) do obliczenia RSI({n}). Wymagane {n+1}.") #
        return None
    gains, losses = [], [] #
    
    relevant_vals = vals[-(n + 1):] if len(vals) > (n + 1) else vals #

    if len(relevant_vals) < 2: #
        logger.warning(f"safe_rsi: Za mało wartości w relevant_vals ({len(relevant_vals)}) do obliczenia zmian.") #
        return None #

    for i in range(1, len(relevant_vals)):  #
        delta = relevant_vals[i] - relevant_vals[i-1] #
        if delta > 0: #
            gains.append(delta) #
            losses.append(0.0) #
        else:
            losses.append(abs(delta)) #
            gains.append(0.0) #
    
    if len(gains) < n :  #
        logger.warning(f"safe_rsi: Niewystarczająca liczba okresów zmian ({len(gains)}) do obliczenia RSI({n}).") #
        return None #

    avg_gain = sum(gains) / n #
    avg_loss = sum(losses) / n #

    if avg_loss == 0: #
        return 100.0 if avg_gain > 0 else 50.0  #
    
    rs = avg_gain / avg_loss #
    rsi = 100.0 - (100.0 / (1.0 + rs)) #
    return rsi #

# ───────── helpers (istniejące) ─────────
def handle_strategy(cur, table, open_cond, sl, tp, price, time, eps,
                    strategy_name, extra_cols=None): #
    logger.info(f"handle_strategy: Przetwarzanie strategii '{strategy_name}' dla tabeli '{table}'.") #
    cur.execute(f"""SELECT trade_id,direction,sl_price,tp_price,open_price
                   FROM {table} WHERE close_time IS NULL""") #
    open_trades = cur.fetchall() #
    logger.debug(f"handle_strategy '{strategy_name}': Znaleziono {len(open_trades)} otwartych transakcji.") #

    for trade_id,direction,sl_px,tp_px,open_px in open_trades: #
        sl_px,tp_px,open_px = map(float,(sl_px,tp_px,open_px)) #
        hit_tp = price>=tp_px-eps if direction=='LONG' else price<=tp_px+eps #
        hit_sl = price<=sl_px+eps if direction=='LONG' else price>=sl_px-eps #
        if hit_tp or hit_sl: #
            pnl=(tp_px-open_px if hit_tp else sl_px-open_px)*(10000 if direction=='LONG' else -10000) #
            logger.info(f"handle_strategy '{strategy_name}': Zamykanie transakcji {trade_id} (kierunek: {direction}). Cena zamknięcia: {price}, {'TP' if hit_tp else 'SL'}. PnL: {pnl:.1f} pips.") #
            cur.execute(f"""UPDATE {table}
                           SET close_time=%s,close_price=%s,result_pips=%s
                           WHERE trade_id=%s""",(time,price,round(pnl,1),trade_id)) #
    
    cur.execute(f"SELECT 1 FROM {table} WHERE close_time IS NULL LIMIT 1") #
    if cur.fetchone(): #
        logger.info(f"handle_strategy '{strategy_name}': Istnieje już otwarta transakcja. Pomijam otwieranie nowej.") #
        return #

    long_c, short_c = open_cond #
    if not (long_c or short_c): #
        logger.debug(f"handle_strategy '{strategy_name}': Brak sygnału do otwarcia nowej transakcji.") #
        return #
    
    dir_action='LONG' if long_c else 'SHORT' #
    sl_px_val=round(price-0.0001*sl,6) if dir_action=='LONG' else round(price+0.0001*sl,6) #
    tp_px_val=round(price+0.0001*tp,6) if dir_action=='LONG' else round(price-0.0001*tp,6) #
    
    logger.info(f"handle_strategy '{strategy_name}': Otwieranie nowej transakcji. Kierunek: {dir_action}, Cena: {price}, SL: {sl_px_val}, TP: {tp_px_val}") #
    
    if extra_cols: #
        col,val=extra_cols #
        cur.execute( #
            f"""INSERT INTO {table}(open_time,open_price,direction,{col},sl_price,tp_price)
                VALUES (%s,%s,%s,%s,%s,%s)""",(time,price,dir_action,val,sl_px_val,tp_px_val))
    else:
        cur.execute( #
            f"""INSERT INTO {table}(open_time,open_price,direction,sl_price,tp_price)
                VALUES (%s,%s,%s,%s,%s)""",(time,price,dir_action,sl_px_val,tp_px_val)) #
    logger.info(f"handle_strategy '{strategy_name}': Zakończono przetwarzanie.") #


def fetch(cur, table): #
    cur.execute(f"""SELECT trade_id, open_time, open_price, direction, sl_price, tp_price,
                            close_time, result_pips, close_price 
                   FROM {table}
                   ORDER BY trade_id DESC 
                   LIMIT 100""") #
    fetched_rows = cur.fetchall() #
    return fetched_rows #

# ───────── MAIN ─────────
def lambda_handler(event, context): #
    log_list_main = [] #
    logger.info(f"lambda_handler: Rozpoczęto wykonanie funkcji. RequestId: {context.aws_request_id if context else 'N/A'}") #

    try:
        logger.info("lambda_handler: Pobieram początkowe kursy z bazy danych.") #
        with db() as conn, conn.cursor() as cur: #
            cur.execute("SELECT timestamp,rate FROM eurusd_rates ORDER BY timestamp DESC LIMIT 300") #
            rows = cur.fetchall()[::-1]  #
        logger.info(f"lambda_handler: Pobranych {len(rows)} wierszy z tabeli eurusd_rates.") #

        if len(rows) == 0: #
            logger.error("lambda_handler: Brak danych w tabeli eurusd_rates. Przerywam wykonanie.") #
            raise ValueError("Brak danych w tabeli eurusd_rates") #

        times  = [r[0].astimezone(timezone.utc) for r in rows] #
        prices = [float(r[1]) for r in rows] #
        t_now, p_now = times[-1], prices[-1] #
        logger.info(f"lambda_handler: Ostatni kurs: Cena={p_now:.5f} o czasie t_now={t_now} (minuta={t_now.minute})") #

        rsi14 = safe_rsi(prices, RSI_LEN) #
        logger.info(f"lambda_handler: Obliczone RSI14 = {rsi14}") #

        if rsi14 is None: #
            logger.warning("lambda_handler: Za mało świeżych danych do obliczenia RSI – pomijam logikę strategii.") #
            log_list_main.append("Za mało świeżych danych – pomijam logikę strategii") #
        else:
            logger.info("lambda_handler: Rozpoczynam przetwarzanie strategii (RSI14 dostępne).") #
            with db() as conn, conn.cursor() as cur:  #
                handle_strategy(cur,'eurusd_trades', #
                    (rsi14<30, rsi14>70), SL1,TP1, p_now,t_now,EPS,'Klasyczna') #

                if len(prices) >= 51: #
                    logger.info("lambda_handler: Wystarczająco danych dla strategii Anomalii.") #
                    log_returns = [math.log(prices[i]/prices[i-1]) for i in range(len(prices)-50, len(prices))] #
                    if not log_returns:  #
                        logger.warning("lambda_handler: Pusta lista log_returns dla strategii Anomalii.") #
                        z = 0.0 #
                    else: #
                        ret  = math.log(prices[-1]/prices[-2]) if len(prices) >= 2 else 0.0 #
                        mean = statistics.fmean(log_returns) #
                        std  = statistics.stdev(log_returns) if len(log_returns) > 1 else 0.0 #
                        z    = (ret-mean)/std if std != 0 else 0.0 #
                    logger.info(f"lambda_handler: Strategia Anomalii - z_score={z:.2f}") #
                    handle_strategy( #
                        cur,'eurusd_anom_trades', #
                        (z<=-Z_TH and rsi14<40, z>=Z_TH and rsi14>60), #
                        SL2,TP2,p_now,t_now,EPS,'Anomalia', #
                        extra_cols=('z_score',round(z,2)) #
                    ) #
                else:
                    logger.warning("lambda_handler: Za mało danych dla strategii Anomalii (potrzebne >=51).") #

                if len(prices) >= SMA_LEN and len(prices) >= 5: #
                    logger.info("lambda_handler: Wystarczająco danych dla strategii Fraktal+SMA.") #
                    sma50 = statistics.fmean(prices[-SMA_LEN:]) #
                    is_high = prices[-3] == max(prices[-5:]) and prices[-3] > prices[-4] and prices[-3] > prices[-2] #
                    is_low  = prices[-3] == min(prices[-5:]) and prices[-3] < prices[-4] and prices[-3] < prices[-2] #

                    logger.info(f"lambda_handler: Strategia Fraktal+SMA - SMA50={sma50:.5f}, is_high={is_high}, is_low={is_low}") #
                    handle_strategy( #
                        cur,'eurusd_frac_trades', #
                        (is_low and p_now>sma50, is_high and p_now<sma50), #
                        SL3,TP3,p_now,t_now,EPS,'Fraktal+SMA' #
                    )
                else: #
                    logger.warning(f"lambda_handler: Za mało danych dla strategii Fraktal+SMA (potrzebne >= {max(SMA_LEN, 5)}).") #
            logger.info("lambda_handler: Zakończono przetwarzanie strategii.") #

        logger.info("lambda_handler: Pobieram dane do raportu HTML PO przetworzeniu strategii.") #
        with db() as conn, conn.cursor() as cur: #
            s1_trades = fetch(cur,'eurusd_trades') #
            s2_trades = fetch(cur,'eurusd_anom_trades') #
            s3_trades = fetch(cur,'eurusd_frac_trades') #
        logger.info(f"lambda_handler: Dane do tabel: s1={len(s1_trades)} wierszy, s2={len(s2_trades)} wierszy, s3={len(s3_trades)} wierszy.") #

        # Przygotowanie danych do wykresów
        pnl_prepared_data = prepare_pnl_chart_data(s1_trades, s2_trades, s3_trades, logger)

        rate_chart_labels = [t.strftime('%H:%M') for t in times[-15:]] #
        rate_chart_values = prices[-15:] #

        logger.info("lambda_handler: Generuję HTML dla głównego dashboardu EUR/USD.") #
        html_content_main_dashboard = render_main_eurusd_dashboard_html(
            rate_chart_labels, rate_chart_values,
            s1_trades, s2_trades, s3_trades,
            pnl_prepared_data, logger
        )
        logger.info(f"lambda_handler: Wygenerowano HTML dla głównego dashboardu EUR/USD (długość: {len(html_content_main_dashboard)} znaków).") #

        # Zapis głównego dashboardu EUR/USD do S3
        try: #
            s3_client.put_object(
                Bucket=BUCKET_TARGET, 
                Key=KEY_EURUSD_MAIN_DASHBOARD_HTML,
                Body=html_content_main_dashboard.encode("utf-8"),
                ContentType="text/html; charset=utf-8",
                CacheControl="no-cache" #
            )
            logger.info(f"📈 Główny dashboard EUR/USD zaktualizowany → s3://{BUCKET_TARGET}/{KEY_EURUSD_MAIN_DASHBOARD_HTML}")
        except Exception as e:
            logger.error(f"lambda_handler: Nie udało się zapisać głównego dashboardu EUR/USD do S3: {str(e)}", exc_info=True)


        # Generowanie HTML tylko dla wykresu PnL EUR/USD
        logger.info("lambda_handler: Generuję HTML dla wykresu PnL EUR/USD (tylko wykres).") #
        pnl_chart_only_html_eurusd = render_eurusd_pnl_chart_only_html(pnl_prepared_data)
        logger.info(f"lambda_handler: Wygenerowano HTML dla wykresu PnL EUR/USD (długość: {len(pnl_chart_only_html_eurusd)} znaków).")

        # Zapis samego wykresu PnL EUR/USD do S3
        try:
            s3_client.put_object(
                Bucket=BUCKET_TARGET, 
                Key=KEY_EURUSD_PNL_CHART_ONLY_HTML, #
                Body=pnl_chart_only_html_eurusd.encode("utf-8"),
                ContentType="text/html; charset=utf-8", #
                CacheControl="no-cache"
            )
            logger.info(f"📈 EUR/USD PnL chart only HTML updated → s3://{BUCKET_TARGET}/{KEY_EURUSD_PNL_CHART_ONLY_HTML}")
        except Exception as e:
            logger.error(f"lambda_handler: Nie udało się zapisać wykresu PnL EUR/USD (tylko wykres) do S3: {str(e)}", exc_info=True)
            # Jeśli ten zapis jest krytyczny, można tu rzucić błąd lub odpowiednio zareagować #

        logger.info(f"lambda_handler: Końcowa zawartość listy log_list_main: {log_list_main}") #
        logger.info("lambda_handler: Funkcja zakończona pomyślnie.") #
        return {
            "statusCode":200,
            "headers":{"Content-Type":"text/html; charset=utf-8"}, #
            "body":html_content_main_dashboard #
        } 

    except Exception as e: #
        logger.error(f"lambda_handler: KRYTYCZNY BŁĄD w głównej obsłudze: {str(e)}", exc_info=True) #
        log_list_main.append(f"ERROR main: {str(e)} - traceback: {traceback.format_exc()}") #
        logger.info(f"lambda_handler: Końcowa zawartość listy log_list_main przy błędzie: {log_list_main}") #
        
        return {"statusCode":500,"body":json.dumps({"error":str(e),"log":log_list_main})} #
