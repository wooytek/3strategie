import os
import json
import logging
import urllib.parse
import urllib.request
import psycopg2
import psycopg2.extensions
from datetime import datetime, timezone
import boto3



# Konfiguracja loggera
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Konwersja typów numerycznych z bazy danych
def cast_decimal_to_float(value, cursor):
    """Konwertuje typ Decimal z bazy danych na float w Pythonie."""
    return float(value) if value is not None else None

psycopg2.extensions.register_type(
    psycopg2.extensions.new_type(
        psycopg2.extensions.DECIMAL.values, 'DEC2FLOAT', cast_decimal_to_float
    )
)

# Inicjalizacja klienta AWS Systems Manager 
ssm_client = boto3.client('ssm') [cite: 111]

# -----------------------------------------------------------------------------
# Główna funkcja Lambda
# -----------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Pobiera aktualny kurs EUR/USD z zewnętrznego API, a następnie zapisuje go
    w bazie danych PostgreSQL wraz ze znacznikiem czasu UTC.
    """
    try:
        # Pobranie klucza API z AWS Parameter Store ---
        try:
            response = ssm_client.get_parameter(Name="/currency-db/apikey", WithDecryption=True) [cite: 112]
            api_key = response["Parameter"]["Value"]
            logger.info("Klucz API został pomyślnie pobrany z AWS Parameter Store.")
        except ssm_client.exceptions.ParameterNotFound:
            error_msg = "Parametr '/currency-db/apikey' nie został znaleziony w Parameter Store." [cite: 113]
            logger.error(error_msg) [cite: 114]
            raise EnvironmentError(error_msg)
        except Exception as e:
            error_msg = f"Błąd podczas pobierania klucza API z Parameter Store: {e}"
            logger.error(error_msg)
            raise EnvironmentError(error_msg)

        # Wczytanie danych dostępowych do bazy danych ze zmiennych środowiskowych ---
        required_env_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)] [cite: 115]
        if missing_vars:
            error_msg = f"Brakujące zmienne środowiskowe: {', '.join(missing_vars)}"
            logger.error(error_msg)
            raise EnvironmentError(error_msg)

        db_host = os.environ["DB_HOST"]
        db_name = os.environ["DB_NAME"]
        db_user = os.environ["DB_USER"] [cite: 116]
        db_password = os.environ["DB_PASSWORD"] [cite: 116]
        db_port = os.getenv("DB_PORT", "5432")

        # zapytanie do API o aktualny kurs waluty
        params = {"access_key": api_key, "from": "EUR", "to": "USD", "amount": "1"} [cite: 117]
        url = "https://api.exconvert.com/convert?" + urllib.parse.urlencode(params) [cite: 118]
        headers = {"User-Agent": "fetch-eur-usd-lambda/1.0"}

        logger.info(f"Wysyłanie zapytania do API: {url}")
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Błąd API, status HTTP: {resp.status}, odpowiedź: {resp.read().decode()}")
            api_data = json.loads(resp.read().decode())

        # odpowiedź API i zapis kursu w bazie danych
        result_data = api_data.get("result", {})
        rate_value = result_data.get("rate") or result_data.get("USD")
        
        if rate_value is None:
            raise ValueError(f"Nie znaleziono kursu ('rate' lub 'USD') w odpowiedzi API: {api_data}")
            
        rate = float(rate_value) [cite: 120]
        logger.info(f"Pobrany kurs EUR/USD: {rate:.6f}")

        conn_params = {"host": db_host, "dbname": db_name, "user": db_user, "password": db_password, "port": db_port} [cite: 121]
        with psycopg2.connect(**conn_params) as conn, conn.cursor() as cur:
            
            cur.execute(
                "INSERT INTO eurusd_rates (timestamp, rate) VALUES (%s, %s)",
                (datetime.now(timezone.utc), rate) [cite: 122]
            )

        logger.info("Kurs został pomyślnie zapisany w bazie danych.")
        return {
            "statusCode": 200,
            "body": json.dumps({"rate": rate})
        }

    except Exception as exc:
        logger.exception("Wystąpił krytyczny błąd w funkcji Lambda.")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(exc)})
        }