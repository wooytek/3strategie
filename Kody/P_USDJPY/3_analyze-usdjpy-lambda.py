# lambda_function.py — analyze-usdjpy-lambda  (S3-only, 3 strategie)
import os, json, math, statistics, uuid, boto3, logging
from datetime import datetime, timezone

# Konfiguracja loggera do logowania informacji o przebiegu funkcji Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Ustawia poziom logowania na INFO

# --- Zmienne globalne i konfiguracja S3 ---
# BUCKET odczytuje nazwę bucketu S3 dla surowych danych ticków z zmiennej środowiskowej.
# Domyślnie jest to "usdjpy-bucket".
BUCKET = os.environ["S3BUCKET_RAW"] 

# Prefiksy do organizacji obiektów w buckecie S3
PREFIX_STATE = "state/" # Prefiks dla plików stanu strategii (czy pozycja jest otwarta, czy zamknięta)
PREFIX_TRD = "trades/" # Prefiks dla plików zapisujących szczegóły zamkniętych transakcji

# Klucz dla pliku cache, który przechowuje listę kluczy (nazw plików) ostatnich ticków.
CACHE_KEY = "state/cache.json" 

# Inicjalizacja klienta AWS Boto3 dla S3 (do interakcji z bucketami S3).
s3 = boto3.client("s3")

# --- Parametry strategii handlowych ---
# Parametry Stop Loss (SL) i Take Profit (TP) dla każdej z trzech strategii.
# Wartości te są wyrażone w pipsach (jednostkach zmiany kursu walutowego).
SL1, TP1 = 20, 30 # Stop Loss i Take Profit dla strategii 1 (classic)
SL2, TP2 = 15, 25 # Stop Loss i Take Profit dla strategii 2 (anomaly)
SL3, TP3 = 12, 24 # Stop Loss i Take Profit dla strategii 3 (fractal + SMA)

RSI_LEN = 14 # Długość okresu dla wskaźnika Relative Strength Index (RSI).
Z_TH = 2.5 # Wartość progowa Z-score dla strategii anomalii.
SMA_LEN = 50 # Długość okresu dla wskaźnika Simple Moving Average (SMA).
EPS = 1e-5 # Mała wartość epsilon, używana do porównywania cen (np. w celu uniknięcia problemów z dokładnością float).

# --- Funkcje pomocnicze S3 ---
def s3_json(key, default=None):
    """
    Pobiera i parsuje plik JSON z bucketu S3.
    Jeśli plik nie istnieje (NoSuchKey), zwraca wartość domyślną.
    
    `key`: Klucz (nazwa pliku) obiektu JSON w S3.
    `default`: Wartość do zwrócenia, jeśli plik nie zostanie znaleziony. Domyślnie None.
    """
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return default

def put_json(key, obj):
    """
    Zapisuje obiekt Pythona jako plik JSON do bucketu S3.
    
    `key`: Klucz (nazwa pliku), pod którym obiekt zostanie zapisany w S3.
    `obj`: Obiekt Pythona (np. słownik, lista) do zapisania jako JSON.
    """
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(obj, separators=(",", ":")).encode(), # Konwersja obiektu na JSON string i kodowanie do bajtów. separators=(",", ":") optymalizuje rozmiar JSON.
        ContentType="application/json" # Ustawia typ zawartości pliku na JSON.
    )

# --- Funkcje wskaźników technicznych ---
def rsi(vals, n=14):
    """
    Oblicza wartość wskaźnika Relative Strength Index (RSI).
    RSI jest wskaźnikiem momentum, który mierzy szybkość i zmianę ruchów cen.
    
    `vals`: Lista ostatnich wartości cenowych.
    `n`: Okres obliczeniowy RSI (domyślnie 14).
    """
    if len(vals) < n + 1:
        return None # Potrzeba co najmniej n+1 wartości do obliczenia RSI
    
    # Oblicz zyski (gains) i straty (losses) dla każdego okresu.
    # gain = max(cena_teraz - cena_wczesniej, 0)
    # loss = max(cena_wczesniej - cena_teraz, 0)
    gains = [max(vals[i] - vals[i-1], 0) for i in range(-n, 0)]
    losses = [max(vals[i-1] - vals[i], 0) for i in range(-n, 0)]
    
    avg_loss = sum(losses) / n # Średnia strata
    
    # Oblicz RSI. Jeśli średnia strata wynosi 0, RSI wynosi 100 (brak strat).
    return 100 if avg_loss == 0 else 100 - 100 / (1 + sum(gains)/n/avg_loss)

def z_score(vals, w=50):
    """
    Oblicza Z-score dla ostatniej zmiany ceny w oparciu o logarytmiczne zwroty z danego okna.
    Z-score mierzy, ile odchyleń standardowych dana obserwacja jest od średniej.
    
    `vals`: Lista ostatnich wartości cenowych.
    `w`: Długość okna (domyślnie 50) do obliczania średniej i odchylenia standardowego.
    """
    if len(vals) < w + 1:
        return None # Potrzeba co najmniej w+1 wartości
    
    # Oblicz logarytmiczne zwroty (returns) dla danego okna.
    rets = [math.log(vals[i]/vals[i-1]) for i in range(-w, 0)]
    
    # Oblicz średnią i odchylenie standardowe zwrotów (z wyłączeniem najnowszego zwrotu do obliczenia statystyk).
    mean, std = statistics.fmean(rets[:-1]), statistics.stdev(rets[:-1])
    
    # Oblicz Z-score. Jeśli odchylenie standardowe wynosi 0, Z-score jest nieokreślony (brak zmienności).
    return None if std == 0 else (rets[-1] - mean) / std

# --- Główna funkcja Lambda handler ---
def lambda_handler(event, context):
    """
    Główna funkcja, która jest wywoływana przez AWS Lambda.
    Analizuje najnowsze dane ticków i na ich podstawie zarządza pozycjami handlowymi dla trzech strategii.
    
    `event`: Słownik zawierający dane wejściowe dla funkcji Lambda (np. klucz nowo dodanego ticka).
    `context`: Obiekt kontekstu funkcji Lambda.
    """
    rid = context.aws_request_id # Unikalny ID żądania Lambda, przydatny do logowania

    # 1️⃣ Pobranie klucza najnowszego pliku ticków i stanu cache
    raw_key = event.get("raw_key") # Klucz (nazwa pliku) nowo dodanego ticka, przekazany w evencie
    cache = s3_json(CACHE_KEY, default=[]) # Wczytuje listę kluczy ostatnich ticków z cache S3

    # Jeśli nie podano `raw_key` w evencie, użyj najnowszego ticka z cache.
    if not raw_key:
        if not cache:
            logger.error("%s cache empty – nothing to analyze", rid)
            return {"statusCode": 404, "body": "cache empty"}
        raw_key = cache[0] # Użyj pierwszego (najnowszego) klucza z cache

    # Wczytaj dane ticka z S3 na podstawie `raw_key`.
    tick = s3_json(raw_key)
    if not tick:
        logger.error("%s tick %s not found", rid, raw_key)
        return {"statusCode": 404, "body": "tick missing"}

    ts = datetime.fromisoformat(tick["timestamp"]) # Czas ticka (np. '2025-06-05T10:30:00Z')
    price = float(tick["rate"]) # Kurs walutowy z ticka

    # 2️⃣ Pobranie listy ostatnich 100 cen
    # Wczytuje dane dla ostatnich 100 ticków z cache (lub mniej, jeśli nie ma tylu w cache).
    # `reversed(cache[:100])` zapewnia, że ceny są w kolejności chronologicznej (od najstarszej do najnowszej).
    prices = [s3_json(k)["rate"] for k in reversed(cache[:100])]
    if len(prices) < 2:
        # Potrzeba co najmniej 2 ceny do obliczenia zwrotów i wskaźników.
        logger.info("%s Not enough data for analysis (need at least 2 prices). Found: %d", rid, len(prices))
        return {"statusCode": 200, "body": "not enough data"}

    # 3️⃣ Implementacja strategii handlowych
    def strategy(name, sl, tp, open_long, open_short, extra=None):
        """
        Zarządza logiką otwierania i zamykania pozycji dla pojedynczej strategii.
        
        `name`: Nazwa strategii (np. "classic", "anomaly", "fractal").
        `sl`: Wartość Stop Loss w pipsach.
        `tp`: Wartość Take Profit w pipsach.
        `open_long`: Wartość boolowska, czy sygnał do otwarcia pozycji LONG jest aktywny.
        `open_short`: Wartość boolowska, czy sygnał do otwarcia pozycji SHORT jest aktywny.
        `extra`: Dodatkowe dane do zapisania w pozycji (np. wartość Z-score).
        """
        state_key = f"{PREFIX_STATE}{name}.json" # Klucz pliku stanu dla tej strategii
        pos = s3_json(state_key, default={}) # Wczytuje aktualny stan pozycji (otwarta/zamknięta)

        # --- Istnieje otwarta pozycja ---
        if pos:
            dir_long = pos["direction"] == "LONG" # Sprawdź, czy pozycja jest LONG
            
            # Sprawdź, czy cena osiągnęła Take Profit (TP) lub Stop Loss (SL).
            # Porównania używają EPS dla dokładności zmiennoprzecinkowej.
            hit_tp = price >= pos["tp_price"] - EPS if dir_long else price <= pos["tp_price"] + EPS
            hit_sl = price <= pos["sl_price"] + EPS if dir_long else price >= pos["sl_price"] - EPS
            
            if hit_tp or hit_sl:
                # Oblicz PnL (Profit and Loss) w oparciu o to, czy pozycja osiągnęła TP czy SL.
                pnl = (pos["tp_price"] if hit_tp else pos["sl_price"]) - pos["open_price"]
                pnl = pnl if dir_long else -pnl # Odwróć PnL dla pozycji SHORT
                
                # Przygotuj dane zamkniętej transakcji.
                trade = {
                    **pos, # Kopiuje wszystkie dane z otwartej pozycji
                    "close_time": ts.isoformat(), # Czas zamknięcia
                    "close_price": price, # Cena zamknięcia
                    "result_pips": round(pnl * 100, 1) # Wynik w pipsach, zaokrąglony do 1 miejsca po przecinku
                }
                # Zapisz zamkniętą transakcję do S3 z unikalnym ID.
                put_json(f"{PREFIX_TRD}{name}/{uuid.uuid4().hex[:12]}.json", trade)
                
                put_json(state_key, {}) # Zaktualizuj stan na "zamkniętą pozycję" (pusty słownik)
                logger.info("%s Strategy %s: Position closed (hit %s). Pips: %.1f", rid, name, "TP" if hit_tp else "SL", trade["result_pips"])
            return # Zakończ, jeśli pozycja jest otwarta lub została właśnie zamknięta

        # --- Brak otwartej pozycji → Sprawdź sygnały otwarcia ---
        if not (open_long or open_short):
            return # Nie ma sygnału do otwarcia pozycji
        
        # Określ kierunek pozycji i oblicz ceny SL/TP.
        direction = "LONG" if open_long else "SHORT"
        
        # Obliczenia SL/TP w oparciu o pipsy (0.01 USD dla 1 pipsa w USD/JPY).
        sl_px = round(price - 0.01*sl, 3) if direction == "LONG" else round(price + 0.01*sl, 3)
        tp_px = round(price + 0.01*tp, 3) if direction == "LONG" else round(price - 0.01*tp, 3)
        
        # Zapisz dane nowo otwartej pozycji do S3.
        put_json(state_key, {
            "open_time": ts.isoformat(), # Czas otwarcia
            "open_price": price, # Cena otwarcia
            "direction": direction, # Kierunek (LONG/SHORT)
            "sl_price": sl_px, # Cena Stop Loss
            "tp_price": tp_px, # Cena Take Profit
            **(extra or {}) # Dodatkowe dane, jeśli istnieją
        })
        logger.info("%s Strategy %s: Position opened %s at %.3f. SL: %.3f, TP: %.3f", rid, name, direction, price, sl_px, tp_px)


    # --- Wykonanie strategii ---

    # Strategia 1: Klasyczna RSI
    # Otwórz LONG, jeśli RSI spadnie poniżej 30 (przesprzedanie).
    # Otwórz SHORT, jeśli RSI wzrośnie powyżej 70 (przekupienie).
    rsi_val = rsi(prices, RSI_LEN)
    strategy("classic", SL1, TP1,
             open_long=rsi_val is not None and rsi_val < 30,
             open_short=rsi_val is not None and rsi_val > 70)

    # Strategia 2: Anomalia Z-score
    # Otwórz LONG, jeśli Z-score jest bardzo niski (cena znacząco spadła).
    # Otwórz SHORT, jeśli Z-score jest bardzo wysoki (cena znacząco wzrosła).
    z = z_score(prices)
    strategy("anomaly", SL2, TP2,
             open_long=z is not None and z <= -Z_TH,
             open_short=z is not None and z >= Z_TH,
             extra={"z_score": round(z or 0, 3)}) # Dodatkowo zapisz wartość Z-score

    # Strategia 3: Fraktal + SMA
    # Otwórz LONG, jeśli pojawi się fraktal "low" (dołek) i cena jest powyżej SMA.
    # Otwórz SHORT, jeśli pojawi się fraktal "high" (szczyt) i cena jest poniżej SMA.
    if len(prices) >= SMA_LEN + 5: # Upewnij się, że jest wystarczająco danych do obliczenia SMA i fraktali
        sma50 = statistics.fmean(prices[-SMA_LEN:]) # Oblicz Simple Moving Average dla ostatnich SMA_LEN cen
        
        # Sprawdź warunki fraktali (cena środkowa jest najniższa/najwyższa w 5-okresowym oknie).
        is_hi = prices[-3] == max(prices[-5:]) # Fraktal "high"
        is_lo = prices[-3] == min(prices[-5:]) # Fraktal "low"
        
        strategy("fractal", SL3, TP3,
                 open_long=is_lo and price > sma50, # Otwórz LONG: fraktal "low" i cena powyżej SMA
                 open_short=is_hi and price < sma50) # Otwórz SHORT: fraktal "high" i cena poniżej SMA

    # Zwróć informację o zakończeniu analizy.
    return {"statusCode": 200, "body": json.dumps({"msg": "analysis done"})}
