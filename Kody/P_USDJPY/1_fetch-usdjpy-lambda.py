# fetch-usdjpy-lambda.py
import os
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
import boto3

# -----------------------------------------------------------------------------
# Konfiguracja i Klienci AWS
# -----------------------------------------------------------------------------
# Inicjalizacja klienta SSM do pobrania klucza API
ssm = boto3.client("ssm")
API_KEY = ssm.get_parameter(
    Name="/currency-db/apikey", WithDecryption=True
)["Parameter"]["Value"]

# Konfiguracja S3
s3 = boto3.client("s3")
BUCKET = os.environ["S3BUCKET_RAW"]  # Bucket docelowy
CACHE_KEY = "state/cache.json"       # Klucz pliku cache z listą najnowszych ticków

# -----------------------------------------------------------------------------
# Główna funkcja Lambda
# -----------------------------------------------------------------------------
def lambda_handler(event, _):
    """
    Pobiera aktualny kurs USD/JPY z zewnętrznego API, zapisuje go jako nowy
    plik "tick" w S3, a następnie aktualizuje plik cache z listą ostatnich ticków.
    """
    # Aktualny kurs USD/JPY z API 
    url = "https://api.exconvert.com/convert?" + urllib.parse.urlencode({
        "access_key": API_KEY,
        "from": "USD", "to": "JPY", "amount": "1"
    })
    data = json.loads(urllib.request.urlopen(url, timeout=10).read().decode())
    rate = Decimal(str(data["result"].get("rate") or data["result"]["JPY"]))
    ts = datetime.now(tz=timezone.utc)

    # Zapis nowego pliku ticka w S3 
    key = f"ticks/{ts:%Y%m%dT%H%M%SZ}.json"
    body = {"timestamp": ts.isoformat(), "rate": float(rate)}

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(body).encode("utf-8"),
        ContentType="application/json"
    )

    # Aktualizacja pliku cache z listą kluczy ticków 
    try:
        cache = json.loads(s3.get_object(Bucket=BUCKET, Key=CACHE_KEY)["Body"].read())
    except s3.exceptions.NoSuchKey:
        cache = []

    # Nowy klucz na początek listy i ograniczenie do 500
    if key not in cache:
        cache.insert(0, key)
        s3.put_object(
            Bucket=BUCKET,
            Key=CACHE_KEY,
            Body=json.dumps(cache[:500]).encode("utf-8"),
            ContentType="application/json"
        )

    return {
        "statusCode": 200,
        "body": json.dumps({"rate": str(rate), "s3_key": key})
    }