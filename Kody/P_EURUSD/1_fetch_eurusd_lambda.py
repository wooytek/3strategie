import os
import json
import logging
import urllib.parse
import urllib.request
import psycopg2
import psycopg2.extensions
import decimal
from datetime import datetime, timezone

# Importujemy boto3 do interakcji z usługami AWS
import boto3

# ───────────────────────────
# Konfiguracja logów
# ────────────────────────S───
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ───────────────────────────
# Psycopg2 - automatyczna konwersja Decimal na float
# ───────────────────────────
def cast_decimal_to_float(value, cursor):
    if value is None:
        return None
    return float(value)

psycopg2.extensions.register_type(
    psycopg2.extensions.new_type(
        psycopg2.extensions.DECIMAL.values,
        'DEC2FLOAT',
        cast_decimal_to_float
    )
)

# Inicjalizacja klienta SSM poza funkcją handler, dla optymalizacji (cold starts)
ssm_client = boto3.client('ssm')


# Funkcja główna Lambda

def lambda_handler(event, context):
    try:
        # 1️⃣ Pobieranie secretu z Parameter Store
        try:
            response = ssm_client.get_parameter(
                Name="/currency-db/apikey",
                WithDecryption=True
            )
            api_key = response["Parameter"]["Value"]
            logger.info("API_KEY pobrany z AWS Parameter Store.")
        except ssm_client.exceptions.ParameterNotFound:
            error_msg = "Parameter '/currency-db/apikey' not found in Parameter Store."
            logger.error(error_msg)
            raise EnvironmentError(error_msg)
        except Exception as e:
            error_msg = f"Błąd podczas pobierania API_KEY z Parameter Store: {e}"
            logger.error(error_msg)
            raise EnvironmentError(error_msg)

        # 2️⃣ Zmienne środowiskowe (pozostałe dane do bazy)
        env_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
        missing = [v for v in env_vars if not os.getenv(v)]
        if missing:
            error_msg = f"Brakuje zmiennych środowiskowych bazy danych: {', '.join(missing)}"
            logger.error(error_msg)
            raise EnvironmentError(error_msg)

        db_host     = os.environ["DB_HOST"]
        db_name     = os.environ["DB_NAME"]
        db_user     = os.environ["DB_USER"]
        db_password = os.environ["DB_PASSWORD"]
        db_port     = os.getenv("DB_PORT", "5432")

        # 3️⃣ Zapytanie do API (bez biblioteki requests)
        params = {
            "access_key": api_key,
            "from": "EUR",
            "to": "USD",
            "amount": "1"
        }
        url = "https://api.exconvert.com/convert?" + urllib.parse.urlencode(params)
        headers = {"User-Agent": "fetch-eur-usd-lambda/1.0"}

        logger.info("Wysyłanie zapytania do API: %s", url)
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} z API: {resp.read().decode()}")
            data = json.loads(resp.read().decode())

        # 4️⃣ Parsowanie odpowiedzi
        result = data.get("result", {})
        rate_value = result.get("rate") or result.get("USD")
        
        # Dodatkowa walidacja, czy kurs został faktycznie pobrany
        if rate_value is None:
            raise ValueError(f"Brak klucza 'rate' lub 'USD' w odpowiedzi API: {data}")
            
        rate = float(rate_value)
        logger.info("Pobrany kurs EUR/USD: %.6f", rate)

        # 5️⃣ Połączenie z bazą PostgreSQL i zapis
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        with conn, conn.cursor() as cur:
            # Zapisz timestamp jako UTC, aby być konsekwentnym z odczytem w eurusd-analyzer
            cur.execute(
                "INSERT INTO eurusd_rates (timestamp, rate) VALUES (%s, %s)",
                (datetime.now(timezone.utc), rate) # Używamy datetime.now(timezone.utc) dla jawności
            )

        logger.info("Zapisano kurs do bazy.")
        return {
            "statusCode": 200,
            "body": json.dumps({"rate": rate})
        }

    except Exception as exc:
        logger.exception("Błąd w funkcji Lambda") # Użycie exception loguje pełny traceback
        return {
            "statusCode": 500,
            "body": f"Błąd: {str(exc)}"
        }
