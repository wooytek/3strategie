# analyze-usdjpy-lambda.py
import os
import json
import math
import statistics
import uuid
import boto3
import logging
from datetime import datetime

# -----------------------------------------------------------------------------
# Konfiguracja
# -----------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Zmienne środowiskowe i prefiksy S3
BUCKET = os.environ["S3BUCKET_RAW"]  # Bucket z danymi
PREFIX_STATE = "state/"              # Prefiks dla plików stanu otwartych pozycji
PREFIX_TRD = "trades/"               # Prefiks dla zakończonych transakcji
CACHE_KEY = "state/cache.json"       # Klucz pliku z listą ostatnich ticków

s3 = boto3.client("s3")

# -----------------------------------------------------------------------------
# Parametry strategii
# -----------------------------------------------------------------------------
SL1, TP1 = 20, 30
SL2, TP2 = 15, 25
SL3, TP3 = 12, 24
RSI_LEN = 14
Z_TH = 2.5
SMA_LEN = 50
EPS = 1e-5  # Epsilon do porównań cen zmiennoprzecinkowych

# -----------------------------------------------------------------------------
# Funkcje pomocnicze
# -----------------------------------------------------------------------------
def s3_json(key, default=None):
    """Wczytuje i parsuje plik JSON z S3."""
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return default

def put_json(key, obj):
    """Zapisuje obiekt jako plik JSON w S3."""
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(obj, separators=(",", ":")).encode(),
        ContentType="application/json"
    )

# -----------------------------------------------------------------------------
# Wskaźniki Techniczne
# -----------------------------------------------------------------------------
def rsi(vals, n=14):
    """Oblicza wskaźnik RSI."""
    if len(vals) < n + 1: return None
    deltas = [vals[i] - vals[i - 1] for i in range(1, len(vals))]
    gains = [d for d in deltas[-n:] if d > 0]
    losses = [abs(d) for d in deltas[-n:] if d <= 0]
    avg_gain = sum(gains) / n
    avg_loss = sum(losses) / n
    if avg_loss == 0: return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def z_score(vals, w=50):
    """Oblicza Z-score dla ostatniego zwrotu logarytmicznego."""
    if len(vals) < w + 1: return None
    rets = [math.log(vals[i] / vals[i - 1]) for i in range(len(vals) - w, len(vals))]
    mean = statistics.fmean(rets)
    std = statistics.stdev(rets) if len(rets) > 1 else 0
    return None if std == 0 else (rets[-1] - mean) / std

# -----------------------------------------------------------------------------
# Główna funkcja Lambda
# -----------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Analizuje pojedynczy tick cenowy USD/JPY. Dla każdej strategii sprawdza,
    czy należy zamknąć istniejącą pozycję lub otworzyć nową.
    Stan każdej strategii jest przechowywany w osobnym pliku JSON w S3.
    """
    # --- 1. Pobierz dane wejściowe i historyczne ceny ---
    raw_key = event.get("raw_key")
    cache = s3_json(CACHE_KEY, default=[])

    # Jeśli funkcja została wywołana bez klucza, użyj najnowszego z cache
    if not raw_key:
        if not cache:
            return {"statusCode": 404, "body": "Cache jest pusty, brak danych do analizy."}
        raw_key = cache[0]

    tick = s3_json(raw_key)
    if not tick:
        return {"statusCode": 404, "body": f"Nie znaleziono pliku tick: {raw_key}"}

    ts = datetime.fromisoformat(tick["timestamp"])
    price = float(tick["rate"])
    prices = [float(s3_json(k)["rate"]) for k in reversed(cache[:100])]

    if len(prices) < 2:
        return {"statusCode": 200, "body": "Niewystarczająca ilość danych historycznych."}

    # --- 2. Zdefiniuj i uruchom logikę strategii ---
    def strategy(name, sl, tp, open_long, open_short, extra=None):
        """
        Zarządza cyklem życia pozycji dla pojedynczej strategii.
        1. Jeśli pozycja istnieje, sprawdza warunki TP/SL.
        2. Jeśli nie ma pozycji, sprawdza sygnał do otwarcia nowej.
        """
        state_key = f"{PREFIX_STATE}{name}.json"
        pos = s3_json(state_key, default={})

        # Jeśli pozycja jest otwarta, sprawdź warunki zamknięcia
        if pos:
            is_long = pos["direction"] == "LONG"
            hit_tp = (is_long and price >= pos["tp_price"]) or (not is_long and price <= pos["tp_price"])
            hit_sl = (is_long and price <= pos["sl_price"]) or (not is_long and price >= pos["sl_price"])

            if hit_tp or hit_sl:
                pnl = (pos["tp_price"] if hit_tp else pos["sl_price"]) - pos["open_price"]
                pnl_pips = (pnl if is_long else -pnl) * 100
                trade = {**pos, "close_time": ts.isoformat(), "close_price": price, "result_pips": round(pnl_pips, 1)}
                
                put_json(f"{PREFIX_TRD}{name}/{uuid.uuid4().hex[:12]}.json", trade)
                put_json(state_key, {}) # Zamknij pozycję, czyszcząc plik stanu
            return

        # Jeśli nie ma otwartej pozycji, sprawdź sygnał otwarcia
        if open_long or open_short:
            direction = "LONG" if open_long else "SHORT"
            sl_px = round(price - 0.01 * sl, 3) if direction == "LONG" else round(price + 0.01 * sl, 3)
            tp_px = round(price + 0.01 * tp, 3) if direction == "LONG" else round(price - 0.01 * tp, 3)
            
            new_pos = {"open_time": ts.isoformat(), "open_price": price, "direction": direction, "sl_price": sl_px, "tp_price": tp_px, **(extra or {})}
            put_json(state_key, new_pos)

    # Strategia 1: Klasyczna (RSI)
    rsi_val = rsi(prices, RSI_LEN)
    if rsi_val is not None:
        strategy("classic", SL1, TP1, open_long=(rsi_val < 30), open_short=(rsi_val > 70))

    # Strategia 2: Anomalie (Z-score)
    z = z_score(prices)
    if z is not None:
        strategy("anomaly", SL2, TP2, open_long=(z <= -Z_TH), open_short=(z >= Z_TH), extra={"z_score": round(z, 3)})

    # Strategia 3: Fraktale + SMA
    if len(prices) >= SMA_LEN + 5:
        sma50 = statistics.fmean(prices[-SMA_LEN:])
        is_hi = prices[-3] == max(prices[-5:])
        is_lo = prices[-3] == min(prices[-5:])
        strategy("fractal", SL3, TP3, open_long=(is_lo and price > sma50), open_short=(is_hi and price < sma50))

    return {"statusCode": 200, "body": json.dumps({"message": "Analiza zakończona."})}