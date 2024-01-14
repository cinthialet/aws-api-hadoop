#########
# Script da Lambda
#########

import json
import boto3
import requests
import time
from datetime import datetime, timedelta
import os

def lambda_handler(_event, _context):
    """
    Realiza a coleta de dados de uma API financeira (Alpha Vantage) a cada 2 minutos, por um total de 10 minutos,
    e salva os resultados em arquivos JSON no Amazon S3.

    As variáveis de ambiente são utilizadas para definir os parâmetros da API e o nome do bucket do S3.
    A função registra cada ação no CloudWatch para monitoramento.

    Args:
        event: Objeto de evento que disparou a função Lambda (não utilizado neste script).
        context: Objeto de contexto fornecido pela AWS Lambda (não utilizado neste script).

    Raises:
        HTTPError: Erro se a API retornar um código de status HTTP não bem-sucedido.
        RequestException: Erro para problemas gerais de requisição (rede, etc.).
    """
    
    # Carrega as variáveis de ambiente da Lambda
    API_KEY = os.getenv('API_KEY')
    FUNCTION = os.getenv('FUNCTION')
    SYMBOL = os.getenv('SYMBOL')
    INTERVAL = os.getenv('INTERVAL')
    BUCKET_NAME = os.getenv('BUCKET_NAME')

    # URL da API Alpha Vantage com os parâmetros definidos
    url = f'https://www.alphavantage.co/query?function={FUNCTION}&symbol={SYMBOL}&interval={INTERVAL}&apikey={API_KEY}'

    # Cliente S3
    s3_client = boto3.client('s3')

    # Nome da pasta baseado na data de execução do script
    folder_name = f"api-extract-{datetime.now().strftime('%Y%m%d')}"

    # Calculando o tempo de término para daqui a 10 minutos
    end_time = datetime.now() + timedelta(minutes=10)

    while datetime.now() < end_time:
        try:
            # Faz a chamada para a API para pegar os dados
            response = requests.get(url)
            response.raise_for_status()  # Levanta um erro se o status não for 200

            # Gera um nome de arquivo com o timestamp atual até registro de minuto
            file_name = f"{folder_name}/{datetime.now().strftime('%Y%m%d-%H%M')}-api-response.json"

            # Salva a resposta em um arquivo JSON no S3 - put_object
            s3_client.put_object(Body=json.dumps(response.json()), Bucket=BUCKET_NAME, Key=file_name)

            print(f"Resposta salva no S3: {file_name}")

        except requests.HTTPError as e:
            # Erro específico para códigos de status HTTP ruins
            print(f"Erro ao acessar a API: Status Code {response.status_code}")

        except requests.RequestException as e:
            # Outros erros de requisição
            print(f"Ocorreu um erro ao fazer a chamada à API: {e}")

        # Espera por 2 minutos antes da próxima iteração
        time.sleep(120)

    # Print para monitoramento no cloudwatch
    print(f"Coleta e armazenamento de dados finalizados com sucesso. Os arquivos foram salvos em {BUCKET_NAME}/{folder_name}")
