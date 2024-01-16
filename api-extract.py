import json
import boto3
import requests
from datetime import datetime
import os

def lambda_handler(_event, _context):
    """
    Executa uma função AWS Lambda para coletar dados financeiros usando a API Alpha Vantage e armazenar o resultado no Amazon S3.

    Esta função é projetada para ser acionada manualmente ou por um evento programado. Ela consulta a API Alpha Vantage para 
    obter dados financeiros do ativo especificado e armazena o resultado em um arquivo JSON no S3.

    Variáveis de ambiente:
    - API_KEY: Chave da API para acessar a API Alpha Vantage.
    - SYMBOL: Símbolo do ativo financeiro.
    - FUNCTION: Tipo de função da API Alpha Vantage a ser utilizada (por exemplo, 'TIME_SERIES_DAILY').
    - BUCKET_NAME: Nome do bucket do S3 onde os dados serão armazenados.
    - BUCKET_LAYER: Camada ou diretório dentro do bucket do S3 para armazenamento dos dados.

    A função cria um cliente S3, constrói a URL de requisição para a API Alpha Vantage com os parâmetros especificados, 
    realiza a requisição e armazena a resposta em um arquivo JSON no S3. O nome do arquivo inclui a data atual para 
    facilitar o rastreamento e a organização.
    """
    
    # carregar variáveis de ambiente
    API_KEY = os.getenv('API_KEY')
    SYMBOL = os.getenv('SYMBOL') 
    FUNCTION = os.getenv('FUNCTION') 
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    BUCKET_LAYER = os.getenv('BUCKET_LAYER')  # Nome do folder para salvar os dados, raw

    # cliente para interagir com o s3
    s3_client = boto3.client('s3')

    # Monta a URL da requisição para a API Alpha Vantage
    url = f'https://www.alphavantage.co/query?function={FUNCTION}&symbol={SYMBOL}&apikey={API_KEY}&datatype=json'

    # Faz a requisição para a API Alpha Vantage e armazena a resposta
    response = requests.get(url)
    data_api = response.json()

    # Define a data atual no formato ano-mes-dia
    current_date = datetime.now().strftime("%Y%m%d")

    # Define o nome do arquivo que será salvo no S3, incluindo a timestamp
    file_name = f"{BUCKET_LAYER}/{current_date}_raw-data-api-response.json"

    # Salva os dados coletados no arquivo JSON no S3
    s3_client.put_object(Body=json.dumps(data_api), Bucket=BUCKET_NAME, Key=file_name)

    # Registra no log o arquivo salvo
    print(f"Resposta salva no S3: {file_name}")

    print(f"Coleta e armazenamento de dados finalizados com sucesso. Os dados foram salvos em {BUCKET_NAME}/{BUCKET_LAYER}")

