import os
import json
import logging
import urllib.parse
import urllib.request
import psycopg2
import psycopg2.extensions
import decimal
from datetime import datetime, timezone


import boto3


# Konfiguracja logów
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Ustawia poziom logowania na INFO


# Psycopg2 - automatyczna konwersja Decimal na float
# Rejestracja niestandardowej konwersji typu dla Psycopg2.

def cast_decimal_to_float(value, cursor):
    """
    Funkcja pomocnicza do konwersji wartości Decimal na float.
    Używana przez Psycopg2 do automatycznego mapowania typów.
    """
    if value is None:
        return None
    return float(value)

psycopg2.extensions.register_type(
    psycopg2.extensions.new_type(
        psycopg2.extensions.DECIMAL.values, # Określa typy PostgreSQL do zmapowania (wszystkie typy DECIMAL)
        'DEC2FLOAT',                       # Nazwa nowej konwersji (dowolna nazwa)
        cast_decimal_to_float              # Funkcja konwertująca
    )
)

# Inicjalizacja klienta SSM (Systems Manager Parameter Store) poza funkcją handler.
# Pozwala to na ponowne wykorzystanie klienta w kolejnych "gorących" uruchomieniach funkcji Lambda (optymalizacja "cold starts").
ssm_client = boto3.client('ssm')


# Funkcja główna Lambda

def lambda_handler(event, context):
    """
    Główna funkcja Lambda, która jest wywoływana w celu:
    1. Pobrania klucza API z AWS Parameter Store.
    2. Pobrania danych połączenia z bazą danych PostgreSQL ze zmiennych środowiskowych.
    3. Wykonania zapytania do zewnętrznego API w celu pobrania kursu EUR/USD.
    4. Zapisania pobranego kursu do bazy danych PostgreSQL.
    
    `event`: Słownik zawierający dane wejściowe dla funkcji Lambda (nieużywane bezpośrednio w tej funkcji,
             gdyż jest ona prawdopodobnie wyzwalana harmonogramem - EventBridge/CloudWatch Events).
    `context`: Obiekt kontekstu funkcji Lambda (nieużywane bezpośrednio).
    """
    try:
        # 1️⃣ Pobieranie secretu (klucza API) z Parameter Store
        # Funkcja get_parameter służy do pobierania wartości parametrów.
        # `Name`: Nazwa parametru w Parameter Store.
        # `WithDecryption=True`: Konieczne, jeśli wartość parametru jest zaszyfrowana (np. przy użyciu AWS KMS).
        try:
            response = ssm_client.get_parameter(
                Name="/currency-db/apikey",
                WithDecryption=True
            )
            api_key = response["Parameter"]["Value"]
            logger.info("API_KEY pobrany z AWS Parameter Store.")
        except ssm_client.exceptions.ParameterNotFound:
            # Obsługa błędu, jeśli parametr nie zostanie znaleziony.
            error_msg = "Parameter '/currency-db/apikey' not found in Parameter Store."
            logger.error(error_msg)
            raise EnvironmentError(error_msg) # Podnoszenie błędu środowiskowego
        except Exception as e:
            # Ogólna obsługa innych błędów podczas pobierania API_KEY.
            error_msg = f"Błąd podczas pobierania API_KEY z Parameter Store: {e}"
            logger.error(error_msg)
            raise EnvironmentError(error_msg)

        # 2️⃣ Pobieranie zmiennych środowiskowych dla połączenia z bazą danych
        # Lista wymaganych zmiennych środowiskowych dla połączenia z bazą danych.
        env_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
        # Sprawdzenie, czy wszystkie wymagane zmienne środowiskowe są ustawione.
        missing = [v for v in env_vars if not os.getenv(v)]
        if missing:
            error_msg = f"Brakuje zmiennych środowiskowych bazy danych: {', '.join(missing)}"
            logger.error(error_msg)
            raise EnvironmentError(error_msg)

        # Przypisanie wartości zmiennych środowiskowych do lokalnych zmiennych.
        db_host = os.environ["DB_HOST"]
        db_name = os.environ["DB_NAME"]
        db_user = os.environ["DB_USER"]
        db_password = os.environ["DB_PASSWORD"]
        # Port bazy danych, domyślnie 5432 dla PostgreSQL, jeśli nie jest ustawiony.
        db_port = os.getenv("DB_PORT", "5432")

        # 3️⃣ Zapytanie do zewnętrznego API (bez użycia biblioteki requests)
        # Parametry zapytania do API do pobrania kursu EUR/USD.
        params = {
            "access_key": api_key,
            "from": "EUR",
            "to": "USD",
            "amount": "1"
        }
        # Kodowanie parametrów i budowanie pełnego URL.
        url = "https://api.exconvert.com/convert?" + urllib.parse.urlencode(params)
        
        # Ustawienie nagłówka User-Agent dla zapytania. Jest to dobra praktyka.
        headers = {"User-Agent": "fetch-eur-usd-lambda/1.0"}

        logger.info("Wysyłanie zapytania do API: %s", url)
        
        # Utworzenie obiektu Request i wykonanie zapytania.
        req = urllib.request.Request(url, headers=headers)
        # Otworzenie połączenia URL, z timeoutem 10 sekund. Użycie `with` zapewnia zamknięcie połączenia.
        with urllib.request.urlopen(req, timeout=10) as resp:
            # Sprawdzenie kodu statusu odpowiedzi HTTP.
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} z API: {resp.read().decode()}")
            # Odczytanie i dekodowanie odpowiedzi, a następnie parsowanie JSON.
            data = json.loads(resp.read().decode())

        # 4️⃣ Parsowanie odpowiedzi API
        result = data.get("result", {}) # Bezpieczne pobranie klucza 'result', domyślnie pusty słownik
        # Pobranie kursu walutowego, może być pod kluczem 'rate' lub 'USD'.
        rate_value = result.get("rate") or result.get("USD")
        
        # Dodatkowa walidacja, czy kurs został faktycznie pobrany.
        if rate_value is None:
            raise ValueError(f"Brak klucza 'rate' lub 'USD' w odpowiedzi API: {data}")
            
        rate = float(rate_value) # Konwersja kursu na typ float
        logger.info("Pobrany kurs EUR/USD: %.6f", rate)

        # 5️⃣ Połączenie z bazą PostgreSQL i zapis danych
        # Nawiązanie połączenia z bazą danych PostgreSQL przy użyciu Psycopg2.
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        # Użycie `with conn` gwarantuje, że połączenie zostanie prawidłowo zamknięte i transakcje zatwierdzone.
        with conn, conn.cursor() as cur:
            # Wykonanie zapytania SQL INSERT do tabeli 'eurusd_rates'.
            # Timestamp jest jawnie ustawiany na UTC
            cur.execute(
                "INSERT INTO eurusd_rates (timestamp, rate) VALUES (%s, %s)",
                (datetime.now(timezone.utc), rate) 
            )

        logger.info("Zapisano kurs do bazy.")
        
        # Zwrócenie odpowiedzi HTTP 200 z pobranym kursem.
        return {
            "statusCode": 200,
            "body": json.dumps({"rate": rate})
        }

    except Exception as exc:
        # Ogólna obsługa błędów. `logger.exception` loguje pełny traceback.
        logger.exception("Błąd w funkcji Lambda") 
        # Zwrócenie statusu 500 (Internal Server Error) w przypadku błędu.
        return {
            "statusCode": 500,
            "body": f"Błąd: {str(exc)}"
        }
