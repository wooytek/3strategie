# lambda_function.py — fetch-usdjpy-lambda (PROD)
import os, json, urllib.parse, urllib.request
from datetime import datetime, timezone
from decimal import Decimal # Użycie Decimal do dokładnych obliczeń walutowych, aby uniknąć problemów z zmiennoprzecinkową arytmetyką (float)
import boto3

ssm = boto3.client("ssm")

# Pobranie klucza API z Parameter Store w SSM.
API_KEY = ssm.get_parameter(
    Name="/currency-db/apikey", WithDecryption=True
)["Parameter"]["Value"]


BUCKET = os.environ["S3BUCKET_RAW"] 

# Klucz obiektu w S3, który pełni rolę cache'a.
# Ten plik JSON będzie przechowywał listę kluczy (ścieżek do plików) ostatnich ticków.
CACHE_KEY = "state/cache.json"

# Inicjalizacja klienta AWS S3 do interakcji z bucketami S3.
s3 = boto3.client("s3")

def lambda_handler(event, _):
    """
    Główna funkcja Lambda, która jest wywoływana w celu pobrania najnowszego kursu USD/JPY,
    zapisania go do S3 i zaktualizowania cache'a ticków.
    """
    # 1️⃣ Pobranie kursu USD/JPY z zewnętrznego API
    # Konstrukcja URL do API konwersji walut (exconvert.com).
    # Parametry takie jak 'access_key', 'from', 'to', 'amount' są kodowane do URL.
    url = "https://api.exconvert.com/convert?" + urllib.parse.urlencode({
        "access_key": API_KEY,
        "from": "USD", "to": "JPY", "amount": "1" # Konwersja 1 USD na JPY
    })
    
    # Wykonanie zapytania HTTP do API i parsowanie odpowiedzi JSON.
    data = json.loads(urllib.request.urlopen(url, timeout=10).read().decode())
    
    # Wyodrębnienie kursu walutowego z odpowiedzi API.
    # Sprawdza zarówno 'rate' jak i 'JPY' w 'result' i konwertuje na typ Decimal dla precyzji.
    rate = Decimal(str(data["result"].get("rate") or data["result"]["JPY"]))
    
    # Pobranie aktualnego czasu UTC.
    ts = datetime.now(tz=timezone.utc)

    # 2️⃣ Klucz i zawartość pliku ticka
    # Generowanie unikalnego klucza (ścieżki do pliku) dla nowo pobranego ticka.
    # Format klucza to "ticks/RRRRMMDDTHHMMSSZ.json", gdzie Z oznacza UTC.
    key = f"ticks/{ts:%Y%m%dT%H%M%SZ}.json"
    
    # Tworzenie zawartości pliku ticka w formacie JSON (timestamp i kurs).
    body = {"timestamp": ts.isoformat(), "rate": float(rate)} # Konwersja Decimal na float do zapisu w JSON

    # Zapisanie pliku ticka do bucketu S3.
    # Body jest kodowane do UTF-8, a ContentType ustawiony na application/json.
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body).encode("utf-8"),
        ContentType="application/json"
    )

    # 3️⃣ Aktualizacja cache
    # Próba wczytania istniejącego cache'a z S3.
    # Jeśli plik cache'a nie istnieje (NoSuchKey), inicjalizuje pustą listę.
    try:
        cache = json.loads(s3.get_object(Bucket=BUCKET, Key=CACHE_KEY)["Body"].read())
    except s3.exceptions.NoSuchKey:
        cache = []

    # Dodanie klucza nowo utworzonego pliku ticka na początek listy cache'a (najnowsze ticki na początku).
    if key not in cache: # Zapobiega duplikatom
        cache.insert(0, key)
        # Ograniczenie rozmiaru cache'a do 500 najnowszych ticków.
        cache = cache[:500]
        
        # Zapisanie zaktualizowanego cache'a z powrotem do S3.
        s3.put_object(
            Bucket=BUCKET, Key=CACHE_KEY,
            Body=json.dumps(cache).encode("utf-8"),
            ContentType="application/json"
        )

    # Zwrócenie odpowiedzi HTTP 200 z pobranym kursem i kluczem S3.
    return {
        "statusCode": 200,
        "body": json.dumps({"rate": str(rate), "s3_key": key}) # Konwersja Decimal na string do odpowiedzi JSON
    }
