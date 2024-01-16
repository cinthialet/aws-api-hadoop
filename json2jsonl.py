import json
import boto3
import os

def lambda_handler(event, _context):
    """
    Função AWS Lambda para converter arquivos JSON em formato JSONL, focando especificamente nos dados da "Time Series (Daily)".

    Esta função é acionada por eventos do Amazon S3, especificamente quando arquivos JSON são carregados em um bucket específico. 
    Ela processa esses arquivos, convertendo-os para o formato JSONL (JSON Line) para facilitar o processamento e análise posterior.

    Variáveis de ambiente:
    - BUCKET_NAME: Nome do bucket do S3 onde os dados JSONL serão armazenados.
    - DESTINY_LAYER: Camada do bucket do S3 para armazenar os dados convertidos.
    
    A função percorre cada registro no evento acionador, baixa o arquivo JSON correspondente do S3, extrai os 
    dados da "Time Series (Daily)" e converte cada entrada dessa série em uma linha separada no formato JSONL. 
    O arquivo JSONL resultante é então salvo de volta no S3, na camada especificada pelas variáveis de ambiente.

    Nota: Esta função é projetada para ser acionada por eventos do S3 e requer que as variáveis de ambiente 
    sejam configuradas corretamente no AWS Lambda para seu funcionamento adequado.
    """
    
    # cliente para interagir com o s3
    s3_client = boto3.client('s3')

    # Carrega variáveis de ambiente
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    DESTINY_LAYER = os.getenv('DESTINY_LAYER')

    print(f"Variáveis de ambiente carregadas: BUCKET_NAME={BUCKET_NAME}, DESTINY_LAYER={DESTINY_LAYER}")

    # Processa cada arquivo que acionou o evento
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        source_key = record['s3']['object']['key']

        print(f"Processando arquivo: {source_key} do bucket {source_bucket}")

        # Baixa o arquivo do S3
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        file_content = response['Body'].read().decode('utf-8')
        json_data = json.loads(file_content)

        print(f"Arquivo {source_key} baixado e lido com sucesso")

        # Extrai somente os dados da "Time Series (Daily)"
        time_series_data = json_data.get("Time Series (Daily)", {})

        jsonl_content = '' #inicia como string vazia
        # Gera json-line para cada dia extraído dos dados

        for day in time_series_data.values():
            jsonl_content += json.dumps(day) + "\n"

        # Define o nome do arquivo de saída, mantendo carimbo de tempo e alterando a camada e extensão do arquivo
        output_key = f"{DESTINY_LAYER}/{source_key.split('/')[-1].replace('raw-data-api-response.json', 'converted-data-api-response.jsonl')}"

        # Salva o arquivo JSONL no S3
        s3_client.put_object(Body=jsonl_content, Bucket=BUCKET_NAME, Key=output_key)

        print(f"Arquivo convertido salvo: {output_key}")

    print("Processamento de todos os arquivos concluído")

"""
O formato JSONL difere do JSON tradicional por armazenar cada registro de dados em uma única linha. 
Este formato é particularmente útil em ambientes de processamento distribuído, como o Hadoop, pois permite 
a leitura e escrita de dados de forma mais eficiente. 

A conversão de arquivos JSON para JSONL (JSON Line) é essencial para o processamento eficiente em clusters 
Hadoop gerenciados pelo Amazon EMR. O formato JSONL, que armazena cada registro em uma linha separada, permite 
o processamento paralelo e distribuído de dados, fundamental em ambientes Hadoop. Esta abordagem facilita operações 
de streaming e map-reduce, melhorando a eficiência e a escalabilidade do processamento de grandes conjuntos de dados 
em ambientes EMR.
"""