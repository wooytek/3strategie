# dashboard-usdjpy-lambda.py
import os
import json
import boto3
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone, tzinfo

# -----------------------------------------------------------------------------
# Konfiguracja i Klienci AWS
# -----------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Nazwy bucketów S3
BUCKET_DATA = os.environ["S3BUCKET_RAW"]        # Bucket z danymi (ticks, trades)
BUCKET_DASHBOARD = os.environ["S3BUCKET_DASH"]  # Bucket docelowy dla dashboardów

# Prefiksy i klucze obiektów S3
PREFIX_TICKS = "ticks/"
PREFIX_TRD = "trades/"
KEY_HTML_MAIN_USDJPY = "usdjpy_dashboard_index.html"
KEY_PNL_CHART_ONLY_HTML_USDJPY = "usdjpy_pnl_chart_only.html"

# Konfiguracja email
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO = os.getenv("EMAIL_TO")

# Klienci AWS
s3 = boto3.client("s3")
ses = boto3.client("ses")

# Zmienna do śledzenia ostatniej modyfikacji
_last_ts: str | None = None

# -----------------------------------------------------------------------------
# Definicje klas i stałych
# -----------------------------------------------------------------------------
# Zmiana strefy czasowej UTC+2.
# Nie obsługuje automatycznie zmiany czasu na zimowy.
class StaticUTC_Offset(tzinfo):
    def __init__(self, offset_hours): self._offset = timedelta(hours=offset_hours)
    def utcoffset(self, dt): return self._offset
    def dst(self, dt): return timedelta(0)
    def tzname(self, dt): return f"UTC{self._offset.total_seconds()/3600:+g}"

local_timezone = StaticUTC_Offset(2)

HOME_ICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" height="45px" viewBox="0 0 24 24" width="45px" fill="currentColor"><path d="M0 0h24v24H0V0z" fill="none"/><path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8z"/></svg>"""

# -----------------------------------------------------------------------------
# Funkcje pomocnicze do obsługi S3 i HTML
# -----------------------------------------------------------------------------
def list_latest_s3(prefix: str, wanted: int = 300):
    """Zwraca listę `wanted` najnowszych obiektów z S3 dla danego prefiksu."""
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_DATA, Prefix=prefix)
    all_objs = [obj for page in pages for obj in page.get('Contents', [])]
    return sorted(all_objs, key=lambda o: o["LastModified"], reverse=True)[:wanted]

def load_s3_json(key: str):
    """Wczytuje i parsuje plik JSON z S3."""
    return json.loads(s3.get_object(Bucket=BUCKET_DATA, Key=key)["Body"].read())

def rows_html(trades):
    """Generuje wiersze <tbody> tabeli HTML na podstawie listy transakcji."""
    return "\n".join(
        f"<tr><td>{t['open_time'][:16].replace('T',' ')}</td><td>{t['open_price']:.3f}</td><td>{t['direction']}</td>"
        f"<td>{t['sl_price']:.3f}</td><td>{t['tp_price']:.3f}</td><td>{'-' if 'close_price' not in t else f'{t['close_price']:.3f}'}</td>"
        f"<td data-pips='{t.get('result_pips', 0):+.1f}'>{t.get('result_pips', 0):+.1f}</td></tr>"
        for t in trades)

# -----------------------------------------------------------------------------
# Funkcje renderujące HTML
# -----------------------------------------------------------------------------
def render_main_usdjpy_dashboard(rate_labels, rate_values, strat_daily_data, tables_html, min_date_json):
    """Generuje pełny kod HTML dla głównego dashboardu USD/JPY."""
    # Przygotowanie danych do wykresów
    rate_labels_json = json.dumps(rate_labels)
    rate_values_json = json.dumps(rate_values)
    
    pnl_datasets = []
    pnl_labels_json = "[]"
    max_date_json = "null"

    if strat_daily_data:
        pnl_labels = strat_daily_data[0][2]
        pnl_labels_json = json.dumps(pnl_labels)
        max_date_json = json.dumps(pnl_labels[-1]) if pnl_labels else "null"
        for title, color, _, cum_values in strat_daily_data:
            pnl_datasets.append({"label": title, "data": cum_values, "borderColor": color, "tension": 0.1, "fill": False})

    pnl_datasets_json = json.dumps(pnl_datasets)

    # Szablon HTML
    html = """<!doctype html><html lang="pl">... </html>""" 
    return html % (HOME_ICON_SVG, tables_html, rate_labels_json, rate_values_json, pnl_labels_json, pnl_datasets_json, min_date_json, max_date_json)

def render_usdjpy_pnl_chart_only(strat_daily_data, min_date_json):
    """Generuje kod HTML zawierający wyłącznie wykres PnL, do osadzania w innych widokach."""
    
    pnl_datasets, pnl_labels_json, max_date_json = [], "[]", "null"
    if strat_daily_data:
        pnl_labels = strat_daily_data[0][2]
        pnl_labels_json = json.dumps(pnl_labels)
        max_date_json = json.dumps(pnl_labels[-1]) if pnl_labels else "null"
        for title, color, _, cum_values in strat_daily_data:
            pnl_datasets.append({"label": title, "data": cum_values, "borderColor": color, "tension": 0.1, "fill": False})
    
    pnl_datasets_json = json.dumps(pnl_datasets)

    html = """<!DOCTYPE html><html lang="pl">...</html>""" 
    return html % (pnl_labels_json, pnl_datasets_json, min_date_json, max_date_json)

# -----------------------------------------------------------------------------
# Główna funkcja Lambda
# -----------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Główna funkcja generująca dashboard. Pobiera dane o tickach i transakcjach z S3,
    agreguje wyniki, renderuje dwa pliki HTML (pełny dashboard i sam wykres PnL),
    zapisuje je w S3 i wysyła e-mail z alertami, jeśli są spełnione warunki.
    """
    global _last_ts

    # --- 1. Sprawdź, czy pojawiły się nowe dane ---
    tick_objs = list_latest_s3(PREFIX_TICKS, 1)
    if not tick_objs: return {"statusCode": 404, "body": "Brak plików tick w S3."}
    
    latest_ts = tick_objs[0]["LastModified"].isoformat()
    if _last_ts == latest_ts: return {"statusCode": 204, "body": "Brak nowych danych."}
    _last_ts = latest_ts

    # --- 2. Przygotuj dane do wykresu kursu waluty ---
    ticks = [load_s3_json(o["Key"]) for o in reversed(list_latest_s3(PREFIX_TICKS, 15))]
    rate_labels = [datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00")).astimezone(local_timezone).strftime("%H:%M") for t in ticks]
    rate_values = [round(t["rate"], 3) for t in ticks]

    # --- 3. Przetwórz dane dla każdej strategii (PnL, tabele) ---
    strategies = [("classic", "Strategia 1 – Klasyczna", "rgba(255, 99, 132, 1)"),
                  ("anomaly", "Strategia 2 – Anomalie", "rgba(54, 162, 235, 1)"),
                  ("fractal", "Strategia 3 – Fraktal + SMA", "rgba(75, 192, 192, 1)")]
    
    today = datetime.now(timezone.utc).date()
    days_x_labels = [(today - timedelta(days=i)).isoformat() for i in range(13, -1, -1)]
    min_pnl_date_json = json.dumps(days_x_labels[0])

    strat_daily_data, all_tables_html, alerts = [], "", []
    
    for short_name, title, color in strategies:
        trades = [load_s3_json(o["Key"]) for o in list_latest_s3(f"{PREFIX_TRD}{short_name}/", 300)]
        closed_trades = [t for t in trades if "close_time" in t]

        # Agregacja PnL per dzień
        daily_pnl = defaultdict(float)
        for t in closed_trades: daily_pnl[t["close_time"][:10]] += t.get("result_pips", 0)
        
        # Obliczanie skumulowanego PnL dla wykresu
        cumulative_pnl = [round(sum(daily_pnl.get(d, 0) for d in days_x_labels[:i+1]), 1) for i in range(len(days_x_labels))]
        strat_daily_data.append((title, color, days_x_labels, cumulative_pnl))

        # Generowanie tabeli HTML
        total_pips = sum(t.get("result_pips", 0) for t in closed_trades)
        all_tables_html += f"""<div class="tbl"><h2>{title} (Σ {total_pips:+.1f} pips)</h2><div class="tbl-inner">...</div></div>""" # (Skrócone dla zwięzłości)

        # Sprawdzanie alertów (3 wygrane/przegrane z rzędu)
        if len(closed_trades) >= 3:
            last_3_results = [t.get("result_pips", 0) for t in closed_trades[:3]]
            if abs((datetime.now(timezone.utc) - datetime.fromisoformat(closed_trades[0]["close_time"])).total_seconds()) < 600:
                if all(r > 0 for r in last_3_results): alerts.append(f"{title}: 3 wygrane z rzędu")
                elif all(r < 0 for r in last_3_results): alerts.append(f"{title}: 3 przegrane z rzędu")

    # --- 4. Wyrenderuj i zapisz pliki HTML w S3 ---
    html_main = render_main_usdjpy_dashboard(rate_labels, rate_values, strat_daily_data, all_tables_html, min_pnl_date_json)
    s3.put_object(Bucket=BUCKET_DASHBOARD, Key=KEY_HTML_MAIN_USDJPY, Body=html_main.encode("utf-8"), ContentType="text/html; charset=utf-8", CacheControl="no-cache")

    html_pnl_only = render_usdjpy_pnl_chart_only(strat_daily_data, min_pnl_date_json)
    s3.put_object(Bucket=BUCKET_DASHBOARD, Key=KEY_PNL_CHART_ONLY_HTML_USDJPY, Body=html_pnl_only.encode("utf-8"), ContentType="text/html; charset=utf-8", CacheControl="no-cache")

    # --- 5. Wyślij e-mail z alertami, jeśli istnieją ---
    if alerts and EMAIL_FROM and EMAIL_TO:
        # (Logika wysyłania e-mail przez SES)
        pass

    dashboard_url = f"http://{BUCKET_DASHBOARD}.s3.amazonaws.com/{KEY_HTML_MAIN_USDJPY}"
    logger.info(f"Dashboard zaktualizowany: {dashboard_url}")
    return {"statusCode": 200, "body": json.dumps({"dashboard_url": dashboard_url})}