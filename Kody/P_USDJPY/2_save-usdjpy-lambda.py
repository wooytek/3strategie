import json
import os
import logging
import boto3
import threading

# Konfiguracja loggera do logowania informacji o przebiegu funkcji Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Ustawia poziom logowania na INFO

# Inicjalizacja klienta AWS Lambda.
lambda_cli = boto3.client("lambda")

# Nazwa funkcji Lambda, która ma być wywoływana asynchronicznie do analizy danych.
ANALYZE_FN = os.environ.get('ANALYZE_LAMBDA') 

# Logowanie informacji o środowisku i nazwie funkcji do analizy.
# Pomaga to w weryfikacji, czy konfiguracja została poprawnie załadowana.
logger.info("Środowisko OK – ANALYZE_LAMBDA = %s", ANALYZE_FN)

def async_invoke(payload):
    """
    Wywołuje funkcję Lambda określoną przez `ANALYZE_FN` asynchronicznie.
    Wywołanie asynchroniczne ('Event') oznacza, że funkcja wywołująca (save-lambda)
    nie czeka na zakończenie wywołanej funkcji (analyze-lambda).
    
    `payload`: Dane (zwykle słownik JSON) do przekazania wywoływanej funkcji.
    """
    try:
        response = lambda_cli.invoke(
            FunctionName=ANALYZE_FN, # Nazwa funkcji Lambda do wywołania
            InvocationType='Event',  # Typ wywołania: asynchroniczne (nie czeka na odpowiedź)
            Payload=json.dumps(payload) # Dane przekazywane w formacie JSON
        )
        # Logowanie statusu wywołania. 'StatusCode' 202 oznacza, że wywołanie zostało przyjęte.
        logger.info("Invoke wysłany – StatusCode: %s", response['StatusCode'])
    except Exception as e:
        # Obsługa błędów, jeśli wywołanie funkcji Lambda nie powiedzie się.
        logger.error("Błąd invoke: %s", str(e))

def lambda_handler(event, context):
    """
    Główna funkcja Lambda, która jest wywoływana w odpowiedzi na zdarzenia (np. nowe pliki w S3).
    Jej zadaniem jest pobranie klucza nowo dodanego obiektu z S3 i asynchroniczne wywołanie
    funkcji analitycznej.
    
    `event`: Słownik zawierający dane zdarzenia. W tym przypadku oczekuje się zdarzenia z S3.
    `context`: Obiekt kontekstu funkcji Lambda.
    """
    logger.info("=== save-usdjpy-lambda start ===")
    try:
        # Pobranie informacji o nowym obiekcie S3 z rekordu zdarzenia.
        # Zakłada, że zdarzenie S3 ma strukturę z 'Records'[0]['s3']['object']['key'].
        rec = event['Records'][0]
        key = rec['s3']['object']['key'] # Klucz (nazwa pliku) nowo utworzonego obiektu w S3
        logger.info("Nowy tick w S3: %s", key)

        # Przygotowanie ładunku (payload) dla funkcji analitycznej.
        # Przekazujemy klucz nowo dodanego pliku raw_key, aby funkcja analityczna mogła go przetworzyć.
        payload = {'raw_key': key}
        logger.info("Przygotowany payload do invoke: %s", payload)

        # Uruchomienie wywołania funkcji `async_invoke` w osobnym wątku.
        # `save-lambda` może szybko zwrócić odpowiedź, podczas gdy `analyze-lambda` pracuje w tle.
        threading.Thread(target=async_invoke, args=(payload,)).start()

        logger.info("Zakończono save-lambda (analyze uruchomiona w tle)")

        # Zwrócenie odpowiedzi HTTP 202 (Accepted), wskazującej, że żądanie zostało przyjęte do przetworzenia.
        return {
            'statusCode': 202,
            'body': f'Analyze triggered for {key}'
        }

    except Exception as e:
        # Ogólna obsługa błędów, jeśli coś pójdzie nie tak w funkcji `save-lambda`.
        logger.error("Błąd w save-lambda: %s", str(e))
        # Zwrócenie statusu 500 (Internal Server Error) w przypadku błędu.
        return {
            'statusCode': 500,
            'body': str(e)
        }
