# save-usdjpy-lambda.py
import json
import os
import logging
import boto3
import threading

# -----------------------------------------------------------------------------
# Konfiguracja
# -----------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicjalizacja klienta Lambda
lambda_cli = boto3.client("lambda")

ANALYZE_FN = os.environ.get('ANALYZE_LAMBDA')

# -----------------------------------------------------------------------------
# Funkcje pomocnicze
# -----------------------------------------------------------------------------
def async_invoke(payload):
    """
    Asynchronicznie wywołuje funkcję Lambda do analizy strategii,
    przekazując jej klucz nowego obiektu S3.
    """
    if not ANALYZE_FN:
        logger.error("Brak zdefiniowanej nazwy funkcji ANALYZE_LAMBDA.")
        return
    try:
        response = lambda_cli.invoke(
            FunctionName=ANALYZE_FN,
            InvocationType='Event',  # Wywołanie asynchroniczne
            Payload=json.dumps(payload)
        )
        logger.info(f"Wywołanie analizy dla {payload.get('raw_key')} zlecone. StatusCode: {response['StatusCode']}")
    except Exception as e:
        logger.error(f"Błąd podczas asynchronicznego wywołania funkcji {ANALYZE_FN}: {str(e)}")

# -----------------------------------------------------------------------------
# Główna funkcja Lambda
# -----------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Funkcja uruchamiana przez zdarzenie S3 (stworzenie nowego obiektu).
    Jej zadaniem jest natychmiastowe uruchomienie w tle funkcji
    analizującej (`analyze-usdjpy-lambda`).
    """
    try:
        # Pobranie nowego klucza obiektu z danych zdarzenia
        key = event['Records'][0]['s3']['object']['key']
        logger.info(f"Wykryto nowy plik tick w S3: {key}")

        payload = {'raw_key': key}

        # Uruchomienie wywołania w osobnym wątku
        threading.Thread(target=async_invoke, args=(payload,)).start()

        return {
            'statusCode': 202, # Accepted
            'body': f'Analiza dla klucza {key} została zlecona.'
        }

    except (KeyError, IndexError) as e:
        logger.error(f"Błąd parsowania zdarzenia S3: {str(e)}")
        return {'statusCode': 400, 'body': 'Niepoprawny format zdarzenia S3.'}
    except Exception as e:
        logger.error(f"Nieoczekiwany błąd w save-usdjpy-lambda: {str(e)}")
        return {'statusCode': 500, 'body': str(e)}