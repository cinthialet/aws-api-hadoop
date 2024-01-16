import json
import boto3
import requests
from datetime import datetime, timedelta
import os

def lambda_handler(_event, _context):
    """
    Coleta dados da ação especificada da API Alpha Vantage para cada dia útil da semana anterior. 
    A função é acionada segunda-feira às 9 AM UTC e realiza uma única requisição, compilando dados para 
    cada dia útil de semanas anteriores em um arquivo JSON.

    O script coleta dados dos últimos 5 dias úteis da semana anterior (de segunda a sexta-feira das semanas anteriores).
    Os dados são salvos em um folder 'raw' no S3, especificado pela variável de ambiente BUCKET_LAYER.
    """

    API_KEY = os.getenv('API_KEY')
    SYMBOL = os.getenv('SYMBOL') 
    FUNCTION = os.getenv('FUNCTION') 
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    BUCKET_LAYER = os.getenv('BUCKET_LAYER')  # Nome do folder para salvar os dados, raw

    s3_client = boto3.client('s3')

    # Dicionário para armazenar os dados dos dias úteis
    data_api = {}

    # Itera sobre os últimos 5 dias úteis (evitando fim de semana)
    for i in range(3, 8):  # 3 a 7 incluído, 8 não entra.
        
        # Calcula a data para a requisição (dia útil da semana anterior)
        date_for_request = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')

        # Monta a URL da requisição para a API Alpha Vantage
        url = f'https://www.alphavantage.co/query?function={FUNCTION}&symbol={SYMBOL}&apikey={API_KEY}&datatype=json'

        # Faz a requisição para a API Alpha Vantage
        response = requests.get(url)
        data = response.json()

        # Armazena os dados coletados no dicionário, com a chave sendo a data de requisição
        data_api[date_for_request] = data

    # Define a data atual no formato ano-mes-dia
    current_date = datetime.now().strftime("%Y%m%d")

    # Define o nome do arquivo que será salvo no S3, incluindo a timestamp
    file_name = f"{BUCKET_LAYER}/{current_date}_raw-data-api-response.json"


    # Salva todos os dados coletados no arquivo JSON no S3
    s3_client.put_object(Body=json.dumps(data_api), Bucket=BUCKET_NAME, Key=file_name)

    # Registra no log o arquivo salvo
    print(f"Resposta salva no S3: {file_name}")

    print(f"Coleta e armazenamento de dados finalizados com sucesso. Os dados da semana foram salvos em {BUCKET_NAME}/{BUCKET_LAYER}")