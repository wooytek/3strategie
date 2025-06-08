# lambda_function.py — dashboard-usdjpy-lambda
import os, json, boto3, logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone, tzinfo
from botocore.config import Config
from botocore.exceptions import ClientError

# Konfiguracja loggera do logowania informacji o przebiegu funkcji Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Ustawia poziom logowania na INFO, co oznacza, że będą wyświetlane tylko komunikaty INFO i wyższe (np. WARNING, ERROR)

# --- Zmienne globalne i konfiguracja S3/SES ---

BUCKET_MAIN_DASHBOARD = os.environ["S3BUCKET_DASH"] 
# BUCKET_PNL_CHARTS jest stałą nazwą bucketu S3 przeznaczonego do przechowywania wykresów "chart_only".
BUCKET_PNL_CHARTS = "3strategie" 

# Prefiksy dla obiektów w S3. Prefiksy te służą do logicznego grupowania i organizacji plików w buckecie S3,
# ułatwiając zarządzanie danymi (np. oddzielne foldery dla ticków i transakcji).
PREFIX_TICKS = "ticks/"    # Prefiks dla danych ticków (najnowszych kursów walutowych USD/JPY)
PREFIX_TRD = "trades/"    # Prefiks dla danych transakcji (szczegóły otwarcia/zamknięcia pozycji)

# Klucze (nazwy plików) dla generowanych dashboardów w S3.
# Te klucze określają nazwy plików, pod którymi pliki HTML zostaną zapisane w bucketach S3.
KEY_HTML_MAIN_USDJPY = "index.html" # Klucz dla głównego dashboardu USD/JPY
KEY_PNL_CHART_ONLY_HTML_USDJPY = "usdjpy_pnl_chart_only.html" # Klucz dla dashboardu zawierającego tylko wykres PnL

# Adresy e-mail do wysyłania alertów, pobierane ze zmiennych środowiskowych.
EMAIL_FROM = os.getenv("EMAIL_FROM") # Adres e-mail nadawcy alertów
EMAIL_TO = os.getenv("EMAIL_TO")     # Adres e-mail odbiorcy alertów

# Inicjalizacja klientów AWS Boto3 dla S3 (przechowywanie plików) i SES (wysyłka e-maili).
# Boto3 to SDK AWS dla Pythona, umożliwiający interakcję z usługami AWS.
s3 = boto3.client("s3") # Klient S3 do operacji na bucketach (put_object, list_objects_v2, get_object)
ses = boto3.client("ses", config=Config(connect_timeout=5, read_timeout=10)) # Klient SES do wysyłania e-maili, z dodatkowymi opcjami timeoutu

# Zmienna do przechowywania znacznika czasu ostatniej modyfikacji najnowszych ticków.
# Jest to mechanizm optymalizacyjny, który zapobiega zbędnym aktualizacjom dashboardu, jeśli dane wejściowe się nie zmieniły.
_last_ts: str | None = None

# Definicja SVG dla ikony domku. Ta ikona będzie używana w głównym dashboardzie
# jako link powrotny do strony podsumowania. Jest to kod SVG, który może być bezpośrednio wstawiony w HTML.
HOME_ICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" height="45px" viewBox="0 0 24 24" width="45px" fill="currentColor">
  <path d="M0 0h24v24H0V0z" fill="none"/>
  <path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8z"/>
</svg>"""

# --- Funkcje pomocnicze S3 ---

def list_latest(prefix: str, wanted: int = 300):
    """
    Lista najnowszych obiektów w buckecie S3 z danym prefiksem.
    Pobiera obiekty posortowane malejąco wg daty modyfikacji (najnowsze najpierw).
    
    `prefix`: Prefiks do filtrowania obiektów (np. "ticks/" lub "trades/").
    `wanted`: Maksymalna liczba obiektów do zwrócenia. Domyślnie 300.
    """
    objs, token = [], None # Inicjalizacja listy obiektów i tokena do paginacji
    while True:
        kw = dict(Bucket=BUCKET_MAIN_DASHBOARD, Prefix=prefix, MaxKeys=1000) # Parametry zapytania do S3
        if token:
            kw["ContinuationToken"] = token # Jeśli jest token, użyj go do pobrania kolejnej strony wyników
        resp = s3.list_objects_v2(**kw) # Wykonaj zapytanie do S3
        objs.extend(resp.get("Contents", [])) # Dodaj pobrane obiekty do listy
        if not resp.get("IsTruncated") or len(objs) >= wanted:
            break # Zakończ pętlę, jeśli wszystkie obiekty zostały pobrane (IsTruncated=False) lub osiągnięto limit `wanted`
        token = resp.get("NextContinuationToken") # Pobierz token dla kolejnej strony wyników
    return sorted(objs, key=lambda o: o["LastModified"], reverse=True)[:wanted] # Posortuj obiekty wg daty modyfikacji (malejąco) i zwróć `wanted` najnowszych

def load_json(key: str):
    """
    Pobiera i parsuje plik JSON z bucketu S3.
    
    `key`: Klucz (nazwa pliku) obiektu JSON w S3.
    """
    # Pobiera obiekt z S3, odczytuje jego zawartość (Body) i dekoduje z JSON na obiekt Pythona (słownik/lista).
    return json.loads(s3.get_object(Bucket=BUCKET_MAIN_DASHBOARD, Key=key)["Body"].read())

# --- Funkcje generujące HTML ---

def rows_html(trades):
    """
    Generuje wiersze HTML (<tr>) dla tabeli transakcji.
    Formatuje pola daty/czasu oraz wartości pipsów dla czytelnego wyświetlania.
    
    `trades`: Lista obiektów transakcji (słowników), gdzie każdy obiekt reprezentuje jedną transakcję.
    """
    return "\n".join(
        f"<tr><td>{t['open_time'][:16].replace('T',' ')}</td>" # Czas otwarcia transakcji, formatowany na HH:MM (np. '2025-06-05 10:30')
        f"<td>{t['open_price']:.3f}</td><td>{t['direction']}</td>" # Cena otwarcia i kierunek transakcji ('buy' lub 'sell')
        f"<td>{t['sl_price']:.3f}</td><td>{t['tp_price']:.3f}</td>" # Poziomy Stop Loss i Take Profit
        # Cena zamknięcia: wyświetla '-' jeśli brak wartości (transakcja otwarta), w przeciwnym razie formatuje do 3 miejsc po przecinku
        f"<td>{'-' if t.get('close_price') is None else '{:.3f}'.format(t.get('close_price'))}</td>"
        # Wynik w pipsach: formatuje ze znakiem (+/-) i jednym miejscem po przecinku. Atrybut 'data-pips' jest używany do stylizacji CSS.
        f"<td data-pips='{t.get('result_pips', 0):+.1f}'>{t.get('result_pips', 0):+.1f}</td></tr>"
        for t in trades # Iteracja po każdej transakcji w liście 'trades'
    )

def render_main_usdjpy_dashboard(rate_labels, rate_values, strat_daily_data, tables_html_str, pnl_min_date_json_str) -> str:
    """
    Funkcja renderująca główny dashboard USD/JPY (generuje plik index.html).
    Zawiera wykresy kursu USD/JPY, wykresy skumulowanego PnL dla strategii oraz tabele ostatnich transakcji.
    
    `rate_labels`: Etykiety czasowe dla wykresu kursu.
    `rate_values`: Wartości kursów dla wykresu.
    `strat_daily_data`: Dane dzienne dla każdej strategii (tytuł, kolor, etykiety X, skumulowane wartości PnL).
    `tables_html_str`: Ciąg HTML zawierający wszystkie tabele transakcji.
    `pnl_min_date_json_str`: Minimalna data dla wykresu PnL w formacie JSON.
    """
    formatted_rate_labels_str = json.dumps(rate_labels) # Konwertuje listę etykiet na format JSON string
    formatted_rate_values_str = json.dumps(rate_values) # Konwertuje listę wartości na format JSON string

    pnl_datasets_python_list = [] # Lista do przechowywania obiektów dataset dla wykresu PnL Chart.js
    x_labels_pnl_list = [] # Lista etykiet X (dat) dla wykresu PnL
    
    if strat_daily_data:
        x_labels_pnl_list = strat_daily_data[0][2] # Pobiera etykiety X (daty) z pierwszej strategii (zakładamy, że są takie same dla wszystkich)
        for title, color, _, cum_values in strat_daily_data: # Iteracja po danych każdej strategii
            pnl_datasets_python_list.append({
                "label": title,       # Nazwa strategii jako etykieta na wykresie
                "data": cum_values,   # Skumulowane wartości PnL (profit and loss) dla tej strategii
                "borderColor": color, # Kolor linii wykresu dla tej strategii
                "tension": 0.1,       # Wygładzenie linii wykresu (mniejsza wartość = ostrzejsza linia)
                "fill": False         # Nie wypełniaj obszaru pod linią wykresu
            })

    pnl_datasets_json_str = json.dumps(pnl_datasets_python_list) # Konwertuje listę datasetów PnL na JSON string
    x_labels_pnl_json_str = json.dumps(x_labels_pnl_list)     # Konwertuje listę etykiet X PnL na JSON string
    # Maksymalna data dla wykresu PnL. Jeśli brak danych, ustawia na 'null'.
    max_date_pnl_json_str = json.dumps(x_labels_pnl_list[-1]) if x_labels_pnl_list else 'null'

    # Szablon HTML dla głównego dashboardu. Zawiera strukturę strony, style CSS oraz kod JavaScript dla wykresów Chart.js.
    html_template = """<!doctype html><html lang="pl"><head><meta charset=utf-8>
    <title>Dashboard USD/JPY</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/luxon@3.x/build/global/luxon.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon"></script>
    <style>
        /* Globalne style dla elementu body */
        body { font-family: sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; display: flex; justify-content: center; align-items: flex-start; min-height: 100vh; }
        /* Główny kontener treści, centrujący zawartość i nadający jej styl */
        .main-content-wrapper { width: 90%%; max-width: 1200px; background-color: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,.1); box-sizing: border-box; position: relative; }
        /* Nagłówki h1 i h2 */
        h1 { text-align: center; color: #333; margin-bottom: 30px; }
        h2 { color: #333; text-align: left; margin-top: 30px; margin-bottom: 15px; }
        /* Kontener dla wykresu, zapewniający odpowiednie wymiary */
        .chart-box { width: 100%%; height: 350px; margin: 20px auto; display: flex; justify-content: center; align-items: center; }
        canvas { max-width: 100%%; height: 100%%; }
        /* Style dla tabeli transakcji */
        .tbl { width: 100%%; margin: 20px auto; text-align: left; }
        .tbl-inner { max-height: 300px; overflow-y: auto; border: 1px solid #e0e0e0; border-radius: 5px; box-shadow: inset 0 0 5px rgba(0,0,0,.05); }
        table { width: 100%%; border-collapse: collapse; margin: 0; font-size: 0.9em; min-width: 600px; }
        th, td { padding: 12px 15px; border-bottom: 1px solid #f0f0f0; text-align: left; }
        th { background-color: #e9ecef; color: #495057; font-weight: 600; position: sticky; top: 0; z-index: 2; }
        tbody tr:nth-child(even) { background-color: #f8f9fa; }
        tbody tr:hover { background-color: #e2e6ea; }
        td:last-child { text-align: left; font-weight: bold; }
        /* Kolorowanie pipsów w tabeli na podstawie wartości */
        td[data-pips^="+"] { color: #28a745; } /* Zielony dla zysku */
        td[data-pips^="-"] { color: #dc3545; } /* Czerwony dla straty */
        /* Styl dla linku do strony głównej (ikona domku) */
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
            %s </div>
        <script>
        let maxGlobalYAxisWidth = 0; // Zmienna globalna do synchronizacji szerokości osi Y
        let chartInstances = [];     // Lista instancji wykresów do synchronizacji
        // Plugin Chart.js do synchronizacji szerokości osi Y między wykresami.
        // Zapewnia, że oba wykresy (kursu i PnL) mają taką samą szerokość osi Y, co poprawia ich wyrównanie wizualne.
        const yAxisSyncPlugin = { 
            id: 'yAxisSync', 
            beforeLayout: (chart) => { 
                if (chart.canvas.id === 'rateChart' && chartInstances.length === 0) { 
                    maxGlobalYAxisWidth = 0; // Resetuj szerokość przed rysowaniem pierwszego wykresu
                } 
            }, 
            afterFit: (chart) => { 
                if (chart.scales.y && chart.scales.y.id === 'y') { 
                    maxGlobalYAxisWidth = Math.max(maxGlobalYAxisWidth, chart.scales.y.width); // Aktualizuj maksymalną szerokość osi Y
                } 
            }, 
            afterDraw: (chart) => { 
                if (chart.scales.y && chart.scales.y.id === 'y') { 
                    if (chart.scales.y.width < maxGlobalYAxisWidth) { 
                        chart.scales.y.width = maxGlobalYAxisWidth; // Ustaw szerokość osi Y na maksymalną
                        chart.update('none'); // Zaktualizuj wykres, aby zastosować nową szerokość
                    } 
                } 
            }, 
            afterInit: (chart) => { 
                chartInstances.push(chart); // Dodaj instancję wykresu do listy
                if (chartInstances.length === 2) { // Kiedy oba wykresy są zainicjalizowane
                    chartInstances.forEach(inst => { 
                        if (inst.scales.y && inst.scales.y.id === 'y') { 
                            inst.scales.y.width = maxGlobalYAxisWidth; // Ustaw wszystkim wykresom maksymalną szerokość
                        } 
                        inst.update('none'); // Zaktualizuj wykresy
                    }); 
                } 
            } 
        };
        Chart.register(yAxisSyncPlugin); // Rejestracja customowego pluginu

        // Inicjalizacja wykresu kursu USD/JPY (rateChart)
        new Chart(document.getElementById('rateChart'), { 
            type: 'line', // Typ wykresu: liniowy
            data: { 
                labels: %s, // Etykiety na osi X (czas)
                datasets: [{ label: 'USD/JPY', data: %s, borderColor: '#2563eb', tension: 0.1, fill:false }] // Dane dla wykresu kursu
            }, 
            options: { 
                responsive: true, maintainAspectRatio: false, // Responsywność i utrzymanie proporcji
                plugins: { 
                    tooltip: { callbacks: { label: ctx => 'Cena: ' + ctx.raw.toFixed(3) } } // Formatowanie etykiety tooltipa
                }, 
                scales: { 
                    y: { 
                        id: 'y', // ID osi Y, używane przez plugin do synchronizacji
                        ticks: { 
                            callback: function(value) { return value.toFixed(3); } // Formatowanie wartości na osi Y
                        } 
                    }, 
                    x: {} // Oś X (domyślne ustawienia, Chart.js sam dopasuje się do etykiet)
                }, 
                layout: { padding: { right: 20 } } // Dodatkowy padding po prawej stronie wykresu
            } 
        });

        // Inicjalizacja wykresu skumulowanego PnL dla strategii (pnlChart)
        new Chart(document.getElementById('pnlChart'), { 
            type: 'line', // Typ wykresu: liniowy
            data: { 
                labels: %s,    // Etykiety na osi X (daty)
                datasets: %s   // Dane dla każdej strategii (wiele linii)
            }, 
            options: { 
                responsive: true, maintainAspectRatio: false, 
                plugins: { 
                    tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.raw + ' pips' } } // Formatowanie etykiety tooltipa
                }, 
                scales: { 
                    y: { 
                        beginAtZero: true, // Oś Y zaczyna się od zera
                        id: 'y',          // ID osi Y, używane przez plugin do synchronizacji
                        ticks: { 
                            callback: function(value) { 
                                let formattedValue = value.toFixed(1); 
                                const desiredLength = 7; 
                                // Dodatkowe formatowanie dla czytelności (np. dodawanie znaku + i wyrównywanie)
                                if (value > 0 && value <100) { 
                                    formattedValue = '   +' + formattedValue; 
                                } 
                                if (value >=100) { 
                                    formattedValue = ' +' + formattedValue; 
                                } 
                                formattedValue = ' ' + formattedValue; 
                                return formattedValue.padStart(desiredLength); 
                            } 
                        } 
                    }, 
                    x: { 
                        type: 'time', // Typ osi X: czasowy
                        time: { 
                            unit: 'day', // Jednostka czasu: dzień
                            tooltipFormat: 'dd-MM-yyyy', // Format tooltipa
                            displayFormats: { day: 'dd-MM-yyyy' } // Format wyświetlania dat
                        }, 
                        min: %s, // Minimalna data na osi X
                        max: %s  // Maksymalna data na osi X
                    } 
                }, 
                layout: { padding: { right: 20 } } // Dodatkowy padding po prawej stronie wykresu
            } 
        });
        </script></body></html>"""
    
    # Formatowanie szablonu HTML danymi. Wartości %s zostaną zastąpione przez odpowiednie zmienne Pythonowe.
    html_content = html_template % (
        HOME_ICON_SVG, # Ikona domku w nagłówku
        tables_html_str, # Ciąg HTML z tabelami transakcji
        formatted_rate_labels_str, # Etykiety dla wykresu kursu
        formatted_rate_values_str, # Wartości dla wykresu kursu
        x_labels_pnl_json_str, # Etykiety X dla wykresu PnL
        pnl_datasets_json_str, # Dane dla wykresu PnL
        pnl_min_date_json_str, # Minimalna data dla wykresu PnL
        max_date_pnl_json_str # Maksymalna data dla wykresu PnL
    )
    return html_content

def render_usdjpy_pnl_chart_only(strat_daily_data, pnl_min_date_json_str) -> str:
    """
    Funkcja renderująca tylko wykres PnL dla USD/JPY (generuje plik usdjpy_pnl_chart_only.html).
    Ten plik jest uproszczoną wersją dashboardu, przeznaczoną do osadzania wykresu w innych miejscach,
    nie zawiera ikony "home" ani tabel transakcji.
    
    `strat_daily_data`: Dane dzienne dla każdej strategii.
    `pnl_min_date_json_str`: Minimalna data dla wykresu PnL w formacie JSON.
    """
    pnl_datasets_python_list = []
    x_labels_pnl_list = []

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

    # Uproszczony szablon HTML dla samego wykresu PnL - bez linku home i dodatkowych elementów.
    html_template = """<!DOCTYPE html>
<html lang="pl" style="width: 100%%; height: 100%%; margin: 0; padding: 0;"> 
<head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Wykres PnL USD/JPY</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/luxon@3.x/build/global/luxon.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon"></script>
    <style>
        /* Minimalne style dla osadzonego wykresu, aby zajmował całą dostępną przestrzeń */
        html, body { margin: 0; padding: 0; width: 100%%; height: 100%%; overflow: hidden; background-color: transparent; }
        canvas#pnlChartUsdjpyOnly { display: block; width: 100%% !important; height: 100%% !important; }
    </style>
</head>
<body>
    <canvas id="pnlChartUsdjpyOnly"></canvas>
    <script> 
        document.addEventListener('DOMContentLoaded', function () {
            try {
                const ctx = document.getElementById('pnlChartUsdjpyOnly').getContext('2d');
                if (!ctx) { 
                    console.error('Nie udało się pobrać kontekstu 2D dla canvas USDJPY (PnL Only).');
                    return; 
                } 
                new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: %s,    # Etykiety X dla wykresu PnL (daty)
                        datasets: %s  # Dane dla wykresu PnL
                    },
                    options: { 
                        responsive: true, maintainAspectRatio: false,
                        plugins: { tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.raw + ' pips' } } }, 
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
                                min: %s, 
                                max: %s 
                            }
                        },
                        layout: { padding: 5 } 
                    }
                });
            } catch (e) { 
                console.error('Błąd podczas inicjalizacji wykresu PnL USDJPY (PnL Only):', e);
            } 
        });
    </script>
</body>
</html>"""
    
    html_content = html_template % (
        formatted_x_labels_pnl_str,
        datasets_json_str,
        min_date_for_js, 
        max_date_for_js  
    )
    return html_content

# Klasa do obsługi strefy czasowej CEST (Central European Summer Time)

class CEST(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=2) # Offset UTC+2
    def dst(self, dt):
        return timedelta(0) # Brak obsługi DST w tej uproszczonej klasie
    def tzname(self, dt):
        return "CEST" # Nazwa strefy czasowej

cest_timezone = CEST()    

# --- Główna funkcja Lambda handler ---
def lambda_handler(event, context):
    global _last_ts # Użycie zmiennej globalnej do śledzenia znacznika czasu ostatniej modyfikacji danych.

    # Pobierz najnowsze ticki (dane kursów walut) z S3. Ograniczenie do 120 najnowszych.
    tick_objs = list_latest(PREFIX_TICKS, 120)
    if not tick_objs:
        logger.warning("Brak obiektów ticków w S3.")
        return {"statusCode": 404, "body": "brak ticków"} 

    # Sprawdź, czy dane zostały zmodyfikowane od ostatniego uruchomienia funkcji Lambda.
    # Jest to optymalizacja, która pozwala uniknąć generowania dashboardu, jeśli dane wejściowe są takie same.
    latest_ts = tick_objs[0]["LastModified"].isoformat() # Pobiera znacznik czasu ostatniej modyfikacji najnowszego ticka
    if _last_ts == latest_ts:
        logger.info("Dane niezmienione od ostatniego uruchomienia. Zwracam 204 Not Modified.")
        return {"statusCode": 204, "body": "Not modified"} # Zwróć 204 Not Modified, jeśli dane się nie zmieniły
    _last_ts = latest_ts # Zaktualizuj znacznik czasu ostatniej modyfikacji

    # Wczytaj dane z ostatnich 15 ticków (odwrócona kolejność, aby najnowsze były na końcu listy).
    # Wykres kursu potrzebuje danych chronologicznie, stąd odwrócenie kolejności.
    ticks = [load_json(o["Key"]) for o in reversed(tick_objs[:15])]
    if len(ticks) < 15:
        logger.warning("Brak wystarczającej liczby ticków (wymagane >=15, znaleziono %d).", len(ticks))
        return {"statusCode": 500, "body": "Brak ≥15 ticków"}

    rate_labels = [] # Lista etykiet czasowych dla wykresu kursu
    for t in ticks:
        # Parsowanie czasu UTC z ticka. Zakładamy format ISO 8601, gdzie 'Z' oznacza UTC.
        utc_time = datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00"))
        
        # Konwersja czasu na lokalną strefę czasową (UTC+2 / CEST).
        local_time = utc_time.astimezone(cest_timezone)
        
        # Formatowanie na HH:MM (godzina:minuta) dla etykiet wykresu.
        rate_labels.append(local_time.strftime("%H:%M"))

    rate_values = [round(t["rate"], 3) for t in ticks] # Wartości kursów (ceny) dla wykresu, zaokrąglone do 3 miejsc po przecinku.

    # Definicja palety kolorów dla różnych strategii. Kolory są używane na wykresach PnL.
    palette = { 
        "classic": "rgba(255, 99, 132, 1)",   # Czerwony
        "anomaly": "rgba(54, 162, 235, 1)",   # Niebieski
        "fractal": "rgba(75, 192, 192, 1)"    # Zielony
    }
    # Mapowanie krótkich identyfikatorów strategii na pełne, czytelne nazwy.
    mapping = [("classic", "Strategia 1 – Klasyczna"),
               ("anomaly", "Strategia 2 – Anomalie"),
               ("fractal", "Strategia 3 – Fraktal + SMA")]

    # Generowanie etykiet X (dat) dla wykresu PnL. Obejmuje ostatnie 14 dni, od najstarszego do najnowszego.
    today = datetime.now(timezone.utc).date() # Pobiera dzisiejszą datę w UTC
    days_x_labels = [(today - timedelta(days=i)).isoformat() for i in range(13, -1, -1)] # Generuje daty dla ostatnich 14 dni

    # Minimalna data dla wykresu PnL, zakodowana na sztywno.
    # Ustawienie stałej daty początkowej może być przydatne do porównywania wyników w dłuższym okresie.
    pnl_min_date_usdjpy_val = datetime(2025, 6, 2).isoformat() 
    pnl_min_date_usdjpy_json_str = json.dumps(pnl_min_date_usdjpy_val)

    # Inicjalizacja list i stringów do przechowywania danych dla dashboardu
    strat_daily_data_list = [] # Lista do przechowywania danych dziennych PnL dla każdej strategii
    tables_html_str = ""       # Ciąg HTML, do którego będą dodawane tabele transakcji
    alerts_list = []           # Lista komunikatów alertów (np. 3 wygrane/przegrane z rzędu)
    now_utc = datetime.now(timezone.utc) # Aktualny czas UTC, używany do sprawdzania świeżości transakcji

    # Przetwarzanie danych dla każdej zdefiniowanej strategii
    for short, title in mapping:
        # Pobierz do 300 najnowszych transakcji dla danej strategii z S3.
        trades = [load_json(o["Key"]) for o in list_latest(f"{PREFIX_TRD}{short}/", 300)]
        closed_trades = [t for t in trades if "close_time" in t] # Filtruj tylko transakcje, które zostały zamknięte

        # Oblicz sumę pipsów dla każdego dnia (dla zamkniętych transakcji).
        daily_sum = defaultdict(float) # Słownik do przechowywania sumy pipsów dla każdej daty
        for tr in closed_trades: 
            daily_sum[tr["close_time"][:10]] += tr.get("result_pips", 0) # Sumuje pipsy dla danego dnia zamknięcia

        # Oblicz skumulowane PnL dla ostatnich dni (na podstawie days_x_labels).
        running, cumulative = 0, [] # `running` to bieżąca suma, `cumulative` to lista skumulowanych wartości
        for d_label in days_x_labels: 
            running += daily_sum.get(d_label, 0) # Dodaj dzienną sumę do bieżącej sumy
            cumulative.append(round(running, 1)) # Dodaj zaokrągloną skumulowaną sumę do listy
        
        # Dodaj dane strategii (tytuł, kolor, etykiety X, skumulowane wartości PnL) do listy,
        # która zostanie użyta do generowania wykresu PnL.
        strat_daily_data_list.append((title, palette[short], days_x_labels, cumulative))

        last_trades = trades # `last_trades` tutaj to wszystkie wczytane transakcje (do 300)

        # Oblicz sumę pipsów dla WSZYSTKICH wczytanych transakcji danej strategii.
        # Ta suma (`tot`) będzie wyświetlona w nagłówku tabeli.
        tot = sum(t.get("result_pips", 0) for t in last_trades)
        
        # Dodaj sekcję tabeli HTML do głównego HTML dashboardu.
        # `rows_html(last_trades)` generuje wiersze dla WSZYSTKICH pobranych transakcji,
        # co oznacza, że tabela pokaże wszystkie dostępne transakcje dla tej strategii.
        tables_html_str += f"""<div class="tbl">
<h2>{title} (Σ {tot:+.1f} pips)</h2>
<div class="tbl-inner">
<table><thead>
<tr><th>Open time</th><th>Open price</th><th>Dir</th><th>SL</th><th>TP</th><th>Close Price</th><th>Res Pips</th></tr>
</thead><tbody>{rows_html(last_trades)}</tbody></table></div></div>"""

        # Sprawdź warunki dla alertów (np. 3 wygrane/przegrane z rzędu).
        # Bierzemy pod uwagę tylko ostatnie 3 zamknięte transakcje.
        closed_last = [t for t in last_trades if "close_time" in t][:3] 
        if len(closed_last) == 3: # Jeśli są dokładnie 3 ostatnie zamknięte transakcje
            results = [t.get("result_pips", 0) for t in closed_last] # Pobierz wyniki pipsów dla tych transakcji
            last_ct = max(datetime.fromisoformat(t["close_time"]) for t in closed_last) # Znajdź czas zamknięcia najnowszej z tych 3 transakcji
            # Sprawdź, czy ostatnia z tych 3 transakcji była bardzo niedawno (w ciągu ostatnich 10 minut).
            if abs((now_utc - last_ct).total_seconds()) < 600: 
                if all(r > 0 for r in results): alerts_list.append(f"{title}: 3 wygrane z rzędu") # Jeśli wszystkie 3 były wygrane
                elif all(r < 0 for r in results): alerts_list.append(f"{title}: 3 przegrane z rzędu") # Jeśli wszystkie 3 były przegrane

    # --- Generowanie i zapisywanie plików HTML do S3 ---

    # Generowanie głównego dashboardu USD/JPY (`index.html`).
    html_main_dashboard_usdjpy = render_main_usdjpy_dashboard(
        rate_labels, rate_values, strat_daily_data_list, tables_html_str, pnl_min_date_usdjpy_json_str
    )
    # Zapis głównego dashboardu do S3. Ustawia odpowiedni Content-Type i Cache-Control na "no-cache",
    # aby przeglądarki zawsze pobierały najnowszą wersję.
    s3.put_object(Bucket=BUCKET_MAIN_DASHBOARD, Key=KEY_HTML_MAIN_USDJPY,
                  Body=html_main_dashboard_usdjpy.encode("utf-8"), 
                  ContentType="text/html; charset=utf-8", CacheControl="no-cache")
    
    # Generowanie dashboardu tylko z wykresem PnL dla USD/JPY (`usdjpy_pnl_chart_only.html`).
    pnl_chart_only_html_usdjpy = render_usdjpy_pnl_chart_only(
        strat_daily_data_list, pnl_min_date_usdjpy_json_str
    )
    # Zapis dashboardu tylko z wykresem PnL do S3.
    s3.put_object(Bucket=BUCKET_PNL_CHARTS, Key=KEY_PNL_CHART_ONLY_HTML_USDJPY, 
                  Body=pnl_chart_only_html_usdjpy.encode("utf-8"),
                  ContentType="text/html; charset=utf-8", CacheControl="no-cache")
    logger.info(f"📈 USD/JPY PnL chart only HTML updated → s3://{BUCKET_PNL_CHARTS}/{KEY_PNL_CHART_ONLY_HTML_USDJPY}")

    # Logowanie URL głównego dashboardu dla łatwego dostępu.
    # Region jest pobierany z ARN funkcji Lambda.
    region = context.invoked_function_arn.split(":")[3]
    url_main_dashboard = f"http://{BUCKET_MAIN_DASHBOARD}.s3.{region}.amazonaws.com/{KEY_HTML_MAIN_USDJPY}" 
    logger.info("📈 USD/JPY Main dashboard updated → %s", url_main_dashboard) 

    # --- Wysyłka alertów e-mailowych (jeśli są jakieś alerty i skonfigurowano adresy e-mail) ---
    if alerts_list and EMAIL_FROM and EMAIL_TO:
        timestamp = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC") # Formatowanie bieżącego czasu dla e-maila
        try:
            ses.send_email(
                Source=EMAIL_FROM, Destination={"ToAddresses": [EMAIL_TO]},
                Message={ "Subject": {"Data": f"USD/JPY – alert strategii"}, # Temat e-maila
                          # Treść e-maila w formacie HTML, zawierająca listę alertów i link do dashboardu.
                          "Body": { "Html": { "Data": f"<p><strong>{timestamp}</strong></p>" + "<ul>" + "".join(f"<li>{a}</li>" for a in alerts_list) + "</ul>" + f'<p><a href="{url_main_dashboard}">Zobacz dashboard</a></p>' } } } 
            )
            logger.info("Alert e-mail wysłany.")
        except ClientError as e:
            logger.warning("Błąd podczas wysyłania e-maila przez SES: %s", e.response["Error"]["Message"])

    # Zwróć status HTTP 200 i URL dashboardu jako odpowiedź funkcji Lambda.
    return {"statusCode": 200, "body": json.dumps({"dashboard": url_main_dashboard})}
