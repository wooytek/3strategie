# lambda_function.py — dashboard-usdjpy-lambda
import os, json, boto3, logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone, tzinfo
from botocore.config import Config
from botocore.exceptions import ClientError

# Inicjalizacja loggera do zapisywania informacji o działaniu funkcji.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Nazwa bucketu S3, z której odczytywany jest główny plik HTML dashboardu. Wartość pobierana jest ze zmiennych środowiskowych Lambda.
BUCKET_MAIN_DASHBOARD = os.environ["S3BUCKET_DASH"] 
# Nazwa bucketu S3, w którym przechowywane są wykresy PnL w formacie "chart_only". Jest to stała wartość.
BUCKET_PNL_CHARTS = "3strategie"

# Prefiksy (foldery) w buckecie S3 do organizacji plików z notowaniami (ticks) i transakcjami (trades).
PREFIX_TICKS = "ticks/"
PREFIX_TRD = "trades/"
# Nazwa (klucz) docelowego pliku HTML dla głównego dashboardu USD/JPY.
KEY_HTML_MAIN_USDJPY = "index.html"
# Nazwa (klucz) docelowego pliku HTML dla samego wykresu PnL strategii USD/JPY.
KEY_PNL_CHART_ONLY_HTML_USDJPY = "usdjpy_pnl_chart_only.html" 

# Adresy e-mail do wysyłania alertów, pobierane ze zmiennych środowiskowych.
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO = os.getenv("EMAIL_TO")

# Inicjalizacja klientów AWS SDK (boto3) do interakcji z usługami S3 i SES (Simple Email Service).
s3 = boto3.client("s3")
ses = boto3.client("ses", config=Config(connect_timeout=5, read_timeout=10))

# Globalna zmienna przechowująca znacznik czasu ostatniej modyfikacji, używana do optymalizacji (unikanie niepotrzebnych uruchomień).
_last_ts: str | None = None

# Definicja kodu SVG dla ikony "domku", która służy jako link do strony głównej w głównym dashboardzie.
HOME_ICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" height="45px" viewBox="0 0 24 24" width="45px" fill="currentColor">
  <path d="M0 0h24v24H0V0z" fill="none"/>
  <path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8z"/>
</svg>"""

# Funkcja pobierająca listę najnowszych obiektów z bucketu S3 pasujących do danego prefiksu.
def list_latest(prefix: str, wanted: int = 300):
    objs, token = [], None
    # Pętla obsługująca paginację wyników z S3 (list_objects_v2 zwraca max 1000 obiektów na raz).
    while True:
        kw = dict(Bucket=BUCKET_MAIN_DASHBOARD, Prefix=prefix, MaxKeys=1000)
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        objs.extend(resp.get("Contents", [])) 
        # Przerwanie pętli, jeśli pobrano wszystkie obiekty lub osiągnięto żądaną liczbę.
        if not resp.get("IsTruncated") or len(objs) >= wanted:
            break
        token = resp.get("NextContinuationToken")
    # Sortowanie obiektów po dacie modyfikacji (malejąco) i zwrócenie żądanej liczby najnowszych.
    return sorted(objs, key=lambda o: o["LastModified"], reverse=True)[:wanted]

# Funkcja wczytująca i parsująca plik JSON z S3 na podstawie jego klucza (nazwy).
def load_json(key: str):
    return json.loads(s3.get_object(Bucket=BUCKET_MAIN_DASHBOARD, Key=key)["Body"].read())

# Funkcja generująca wiersze HTML (<tr>) dla tabeli transakcji.
def rows_html(trades):
    # Użycie generatora do stworzenia stringa HTML dla każdej transakcji.
    return "\n".join(
        f"<tr><td>{t['open_time'][:16].replace('T',' ')}</td>"
        f"<td>{t['open_price']:.3f}</td><td>{t['direction']}</td>"
        f"<td>{t['sl_price']:.3f}</td><td>{t['tp_price']:.3f}</td>"
        # Warunkowe wyświetlanie ceny zamknięcia lub myślnika.
        f"<td>{'-' if t.get('close_price') is None else '{:.3f}'.format(t.get('close_price'))}</td>"
        # Wyświetlanie wyniku w pipsach, z atrybutem data-pips do stylizacji CSS.
        f"<td data-pips='{t.get('result_pips', 0):+.1f}'>{t.get('result_pips', 0):+.1f}</td></tr>"
        for t in trades
    )

# Funkcja renderująca główny, kompletny dashboard USD/JPY (plik index.html).
def render_main_usdjpy_dashboard(rate_labels, rate_values, strat_daily_data, tables_html_str, pnl_min_date_json_str) -> str:
    # Konwersja danych z Pythona do formatu JSON, który będzie wstrzyknięty do skryptu JavaScript w HTML.
    formatted_rate_labels_str = json.dumps(rate_labels)
    formatted_rate_values_str = json.dumps(rate_values)

    pnl_datasets_python_list = []
    x_labels_pnl_list = [] 
    
    # Przetwarzanie danych o wynikach strategii, jeśli są dostępne.
    if strat_daily_data:
        x_labels_pnl_list = strat_daily_data[0][2] 
        # Tworzenie listy datasetów dla wykresu PnL w formacie wymaganym przez Chart.js.
        for title, color, _, cum_values in strat_daily_data:
            pnl_datasets_python_list.append({
                "label": title,
                "data": cum_values,
                "borderColor": color,
                "tension": 0.1,
                "fill": False
            })

    # Konwersja list z danymi do PnL na stringi JSON.
    pnl_datasets_json_str = json.dumps(pnl_datasets_python_list)
    x_labels_pnl_json_str = json.dumps(x_labels_pnl_list)
    # Ustalenie maksymalnej daty na osi X wykresu PnL.
    max_date_pnl_json_str = json.dumps(x_labels_pnl_list[-1]) if x_labels_pnl_list else 'null'

    # Główny szablon HTML dla dashboardu. Zawiera style CSS i kod JavaScript dla wykresów.
    html_template = """<!doctype html><html lang="pl"><head><meta charset=utf-8>
    <title>Dashboard USD/JPY</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/luxon@3.x/build/global/luxon.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon"></script>
    <style>
        body { font-family: sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; display: flex; justify-content: center; align-items: flex-start; min-height: 100vh; } 
        .main-content-wrapper { width: 90%%; max-width: 1200px; background-color: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,.1); box-sizing: border-box; position: relative; } 
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
        .home-link { position: absolute; top: 15px; right: 15px; text-decoration: none; color: #555; z-index: 1000; transition: color 0.2s ease-in-out; display: inline-block; line-height: 0; } 
        .home-link svg { display: block; } 
        .home-link:hover { color: #007bff; } 
    </style>
    </head>
    <body>
        <div class="main-content-wrapper">
            <a href="https://3strategie.s3.eu-central-1.amazonaws.com/summary_dashboard.html" class="home-link" title="Strona główna podsumowania">%s</a>
            <h1>USD/JPY – Dashboard strategii</h1>
            <div class="chart-box"><canvas id="rateChart"></canvas></div>
            <div class="chart-box"><canvas id="pnlChart"></canvas></div>
            %s
        </div> 
        <script>
        let maxGlobalYAxisWidth = 0; 
        let chartInstances = [];
        const yAxisSyncPlugin = { id: 'yAxisSync', beforeLayout: (chart) => { if (chart.canvas.id === 'rateChart' && chartInstances.length === 0) { maxGlobalYAxisWidth = 0; } }, afterFit: (chart) => { if (chart.scales.y && chart.scales.y.id === 'y') { maxGlobalYAxisWidth = Math.max(maxGlobalYAxisWidth, chart.scales.y.width); } }, afterDraw: (chart) => { if (chart.scales.y && chart.scales.y.id === 'y') { if (chart.scales.y.width < maxGlobalYAxisWidth) { chart.scales.y.width = maxGlobalYAxisWidth; chart.update('none'); } } }, afterInit: (chart) => { chartInstances.push(chart); if (chartInstances.length === 2) { chartInstances.forEach(inst => { if (inst.scales.y && inst.scales.y.id === 'y') { inst.scales.y.width = maxGlobalYAxisWidth; } inst.update('none'); }); } } }; 
        Chart.register(yAxisSyncPlugin);
        new Chart(document.getElementById('rateChart'), { type: 'line', data: { labels: %s, datasets: [{ label: 'USD/JPY', data: %s, borderColor: '#2563eb', tension: 0.1, fill:false }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { tooltip: { callbacks: { label: ctx => 'Cena: ' + ctx.raw.toFixed(3) } } }, scales: { y: { id: 'y', ticks: { callback: function(value) { return value.toFixed(3); } } }, x: {} }, layout: { padding: { right: 20 } } } }); 
        new Chart(document.getElementById('pnlChart'), { type: 'line', data: { labels: %s, datasets: %s }, options: { responsive: true, maintainAspectRatio: false, plugins: { tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.raw + ' pips' } } }, scales: { y: { beginAtZero: true, id: 'y', ticks: { callback: function(value) { let formattedValue = value.toFixed(1); const desiredLength = 7; if (value > 0 && value <100) { formattedValue = '   +' + formattedValue; } if (value >=100) { formattedValue = ' +' + formattedValue; } formattedValue = ' ' + formattedValue; return formattedValue.padStart(desiredLength); } } }, x: { type: 'time', time: { unit: 'day', tooltipFormat: 'dd-MM-yyyy', displayFormats: { day: 'dd-MM-yyyy' } }, min: %s, max: %s } }, layout: { padding: { right: 20 } } } }); 
        </script></body></html>"""
    
    # Wstawienie danych do szablonu HTML przy użyciu operatora formatowania stringów (%).
    html_content = html_template % (
        HOME_ICON_SVG,
        tables_html_str,
        formatted_rate_labels_str,
        formatted_rate_values_str,
        x_labels_pnl_json_str,
        pnl_datasets_json_str,
        pnl_min_date_json_str,
        max_date_pnl_json_str
    )
    return html_content

# Funkcja renderująca uproszczony plik HTML, zawierający tylko wykres PnL dla USD/JPY.
def render_usdjpy_pnl_chart_only(strat_daily_data, pnl_min_date_json_str) -> str: 
    pnl_datasets_python_list = []
    x_labels_pnl_list = []

    # Przygotowanie danych do wykresu, analogicznie do funkcji render_main_usdjpy_dashboard.
    if strat_daily_data:
        x_labels_pnl_list = strat_daily_data[0][2] 
        for title, color, _, cum_values in strat_daily_data:
            pnl_datasets_python_list.append({
                "label": title,
                "data": cum_values,
                "borderColor": color, 
                "tension": 0.1,
                "fill": False
            })
            
    datasets_json_str = json.dumps(pnl_datasets_python_list)
    formatted_x_labels_pnl_str = json.dumps(x_labels_pnl_list)
    
    min_date_for_js = pnl_min_date_json_str 
    max_date_for_js = json.dumps(x_labels_pnl_list[-1]) if x_labels_pnl_list else 'null'

    # Szablon HTML dla samego wykresu PnL. Nie zawiera ikony "domku" ani tabel.
    html_template = """<!DOCTYPE html>
<html lang="pl" style="width: 100%%; height: 100%%; margin: 0; padding: 0;"> 
<head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Wykres PnL USD/JPY</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/luxon@3.x/build/global/luxon.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon"></script>
    <style>
        html, body { margin: 0; padding: 0; width: 100%%; height: 100%%; overflow: hidden; background-color: transparent; /* Usunięto position: relative, bo nie ma linku home */ }
        canvas#pnlChartUsdjpyOnly { display: block; width: 100%% !important; height: 100%% !important; }
    </style>
</head>
<body>
    <canvas id="pnlChartUsdjpyOnly"></canvas>
    <script> 
        document.addEventListener('DOMContentLoaded', function () {
            try {
                const ctx = document.getElementById('pnlChartUsdjpyOnly').getContext('2d');
                if (!ctx) { console.error('Nie udało się pobrać kontekstu 2D dla canvas USDJPY (PnL Only).'); return; } 
                new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: %s,    
                        datasets: %s  
                    },
                    options: { 
                        responsive: true, maintainAspectRatio: false,
                        plugins: { tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.raw + ' pips' } } }, 
                        scales: {
                            y: { beginAtZero: true, ticks: { callback: function(value) { let formattedValue = value.toFixed(1); const desiredLength = 7; if (value > 0) { formattedValue = '+' + formattedValue; } formattedValue = ' ' + formattedValue; return formattedValue.padStart(desiredLength); } } }, 
                            x: { type: 'time', time: { unit: 'day', tooltipFormat: 'dd-MM-yyyy', displayFormats: { day: 'dd-MM-yyyy' } }, min: %s, max: %s }
                        },
                        layout: { padding: 5 } 
                    }
                }); 
            } catch (e) { console.error('Błąd podczas inicjalizacji wykresu PnL USDJPY (PnL Only):', e); } 
        });
    </script>
</body>
</html>"""
    
    # Wstawienie danych do szablonu HTML.
    html_content = html_template % (
        formatted_x_labels_pnl_str,
        datasets_json_str,
        min_date_for_js, 
        max_date_for_js  
    )
    return html_content

# Definicja prostej, niestandardowej klasy strefy czasowej dla CEST (UTC+2).
class CEST(tzinfo):
    # Różnica w stosunku do UTC.
    def utcoffset(self, dt):
        return timedelta(hours=2) # UTC+2
    # Różnica czasu letniego (nieobsługiwana w tej prostej implementacji).
    def dst(self, dt):
        return timedelta(0)
    # Nazwa strefy czasowej.
    def tzname(self, dt):
        return "CEST"

# Utworzenie instancji niestandardowej strefy czasowej.
cest_timezone = CEST()    

# Główna funkcja obsługująca wywołanie Lambda.
def lambda_handler(event, context):
    global _last_ts

    # Pobranie listy 120 najnowszych plików z notowaniami.
    tick_objs = list_latest(PREFIX_TICKS, 120)
    if not tick_objs:
        return {"statusCode": 404, "body": "brak ticków"} 

    # Sprawdzenie, czy dane się zmieniły od ostatniego uruchomienia.
    latest_ts = tick_objs[0]["LastModified"].isoformat()
    if _last_ts == latest_ts:
        # Jeśli nie, funkcja kończy działanie, oszczędzając zasoby.
        return {"statusCode": 204, "body": "Not modified"}
    _last_ts = latest_ts

    # Wczytanie 15 najnowszych notowań do wygenerowania wykresu kursu.
    ticks = [load_json(o["Key"]) for o in reversed(tick_objs[:15])]
    if len(ticks) < 15:
        return {"statusCode": 500, "body": "Brak ≥15 ticków"}

    rate_labels = []
    
    # Pętla przetwarzająca notowania (ticks) w celu przygotowania danych do wykresu kursu.
    for t in ticks:
        # Parsowanie znacznika czasu z pliku JSON.
        utc_time = datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00"))
        
        # Konwersja czasu z UTC na lokalną strefę czasową (CEST).
        local_time = utc_time.astimezone(cest_timezone)
        
        # Formatowanie czasu do postaci HH:MM na potrzeby etykiet wykresu.
        rate_labels.append(local_time.strftime("%H:%M"))

    # Przygotowanie listy wartości kursu, zaokrąglonych do 3 miejsc po przecinku.
    rate_values = [round(t["rate"], 3) for t in ticks] 

    # Paleta kolorów dla poszczególnych strategii na wykresie PnL.
    palette = { 
        "classic": "rgba(255, 99, 132, 1)", 
        "anomaly": "rgba(54, 162, 235, 1)", 
        "fractal": "rgba(75, 192, 192, 1)"
    }
    # Mapowanie skróconych nazw strategii na ich pełne nazwy.
    mapping = [("classic", "Strategia 1 – Klasyczna"),
               ("anomaly", "Strategia 2 – Anomalie"),
               ("fractal", "Strategia 3 – Fraktal + SMA")]
               
    # Pobranie bieżącej daty w strefie UTC.
    today = datetime.now(timezone.utc).date()
    
    # Inicjalizacja pustej listy na etykiety osi X wykresu PnL.
    days_x_labels_weekdays = []
    # Ustawienie bieżącej daty jako punktu startowego do iteracji wstecz.
    current_date = today
    # Pętla zbierająca 14 ostatnich dni roboczych.
    while len(days_x_labels_weekdays) < 14:
        # Sprawdzenie, czy dzień jest dniem roboczym (poniedziałek=0, niedziela=6).
        if current_date.weekday() < 5:
            days_x_labels_weekdays.append(current_date.isoformat())
        # Przejście do poprzedniego dnia.
        current_date -= timedelta(days=1)
    
    # Odwrócenie listy, aby daty były w porządku chronologicznym (od najstarszej do najnowszej).
    days_x_labels_weekdays.reverse()
    days_x_labels = days_x_labels_weekdays


    # Ustawienie minimalnej (początkowej) daty dla osi X wykresu PnL.
    pnl_min_date_usdjpy_val = datetime(2025, 6, 2).isoformat() 
    # Konwersja daty na format JSON.
    pnl_min_date_usdjpy_json_str = json.dumps(pnl_min_date_usdjpy_val)

    # Inicjalizacja list i zmiennych do przechowywania danych strategii i alertów.
    strat_daily_data_list, tables_html_str, alerts_list = [], "", []
    now_utc = datetime.now(timezone.utc)

    # Pętla przetwarzająca każdą strategię zdefiniowaną w 'mapping'.
    for short, title in mapping:
        # Wczytanie 300 ostatnich transakcji dla danej strategii.
        trades = [load_json(o["Key"]) for o in list_latest(f"{PREFIX_TRD}{short}/", 300)]
        # Odfiltrowanie tylko zamkniętych transakcji.
        closed_trades = [t for t in trades if "close_time" in t]
        # Słownik do sumowania wyników (pips) dla każdego dnia.
        daily_sum = defaultdict(float)
        for tr in closed_trades: 
            daily_sum[tr["close_time"][:10]] += tr.get("result_pips", 0)
        # Obliczanie skumulowanego wyniku (PnL) dzień po dniu.
        running, cumulative = 0, []
        for d_label in days_x_labels: 
            running += daily_sum.get(d_label, 0)
            cumulative.append(round(running, 1))
        # Dodanie przetworzonych danych strategii do listy.
        strat_daily_data_list.append((title, palette[short], days_x_labels, cumulative))

        # Przygotowanie danych do tabeli HTML z ostatnimi transakcjami.
        last_trades = trades
        closed_last = [t for t in last_trades if "close_time" in t][:3] 
        # Obliczenie sumy pipsów ze wszystkich wczytanych transakcji.
        tot = sum(t.get("result_pips", 0) for t in last_trades)
        # Wygenerowanie fragmentu HTML z tabelą dla danej strategii.
        tables_html_str += f"""<div class="tbl">
<h2>{title} (Σ {tot:+.1f} pips)</h2>
<div class="tbl-inner">
<table><thead>
<tr><th>Open time</th><th>Open price</th><th>Dir</th><th>SL</th><th>TP</th><th>Close Price</th><th>Res Pips</th></tr>
</thead><tbody>{rows_html(last_trades)}</tbody></table></div></div>"""

        # Logika sprawdzająca, czy należy wygenerować alert email.
        if len(closed_last) == 3:
            results = [t.get("result_pips", 0) for t in closed_last]
            # Sprawdzenie, czy ostatnie 3 transakcje zostały zamknięte w ciągu ostatnich 10 minut.
            last_ct = max(datetime.fromisoformat(t["close_time"]) for t in closed_last)
            if abs((now_utc - last_ct).total_seconds()) < 600: 
                # Sprawdzenie warunku 3 wygranych lub 3 przegranych z rzędu.
                if all(r > 0 for r in results): alerts_list.append(f"{title}: 3 wygrane z rzędu")
                elif all(r < 0 for r in results): alerts_list.append(f"{title}: 3 przegrane z rzędu")

    # Wygenerowanie finalnego kodu HTML dla głównego dashboardu.
    html_main_dashboard_usdjpy = render_main_usdjpy_dashboard(
        rate_labels, rate_values, strat_daily_data_list, tables_html_str, pnl_min_date_usdjpy_json_str
    )
    # Zapisanie wygenerowanego pliku HTML do S3.
    s3.put_object(Bucket=BUCKET_MAIN_DASHBOARD, Key=KEY_HTML_MAIN_USDJPY,
                  Body=html_main_dashboard_usdjpy.encode("utf-8"), 
                  ContentType="text/html; charset=utf-8", CacheControl="no-cache") 

    # Wygenerowanie finalnego kodu HTML dla samego wykresu PnL.
    pnl_chart_only_html_usdjpy = render_usdjpy_pnl_chart_only(
        strat_daily_data_list, pnl_min_date_usdjpy_json_str
    )
    # Zapisanie wygenerowanego pliku HTML (tylko wykres) do S3.
    s3.put_object(Bucket=BUCKET_PNL_CHARTS, Key=KEY_PNL_CHART_ONLY_HTML_USDJPY, 
                  Body=pnl_chart_only_html_usdjpy.encode("utf-8"),
                  ContentType="text/html; charset=utf-8", CacheControl="no-cache")
    logger.info(f"📈 USD/JPY PnL chart only HTML updated → s3://{BUCKET_PNL_CHARTS}/{KEY_PNL_CHART_ONLY_HTML_USDJPY}")

    # Utworzenie URL do nowo wygenerowanego dashboardu.
    region = context.invoked_function_arn.split(":")[3]
    url_main_dashboard = f"http://{BUCKET_MAIN_DASHBOARD}.s3.{region}.amazonaws.com/{KEY_HTML_MAIN_USDJPY}" 
    logger.info("📈 USD/JPY Main dashboard updated → %s", url_main_dashboard) 

    # Sprawdzenie, czy są alerty do wysłania i czy skonfigurowano adresy e-mail.
    if alerts_list and EMAIL_FROM and EMAIL_TO:
        timestamp = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        try:
            # Wysłanie e-maila za pomocą usługi AWS SES.
            ses.send_email(
                Source=EMAIL_FROM, Destination={"ToAddresses": [EMAIL_TO]},
                Message={ "Subject": {"Data": f"USD/JPY – alert strategii"},
                          "Body": { "Html": { "Data": f"<p><strong>{timestamp}</strong></p>" + "<ul>" + "".join(f"<li>{a}</li>" for a in alerts_list) + "</ul>" + f'<p><a href="{url_main_dashboard}">Zobacz dashboard</a></p>' } } } 
            )
        except ClientError as e:
            logger.warning("SES error %s", e.response["Error"]["Message"])

    # Zwrócenie odpowiedzi o sukcesie, zawierającej URL do dashboardu.
    return {"statusCode": 200, "body": json.dumps({"dashboard": url_main_dashboard})}
