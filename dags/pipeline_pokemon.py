from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import requests
import concurrent.futures
import logging
import sys
sys.path.insert(0, "/opt/airflow/scripts")
from minio_connection import upload_json_bucket, read_jsons_folder, upload_parquet_bucket
from processing_bronze_to_silver import process_category
from processing_silver_to_gold import process_analysis

# Configuração de logs
logging.basicConfig(level=logging.INFO)

BASE_URL = "https://api.tcgdex.net/v2/pt/cards"

@dag(
    dag_id='pipeline_pokemon',
    description='DAG que executa a pipeline dos dados de cartas de pokemon, fazendo a ingestão tranformação silver e analises na gold.',
    schedule_interval=None, # A DAG não terá agendamento recorrente
    start_date=datetime(2024, 5, 9),
    catchup=False # A DAG não deve fazer execuções passadas
)
def pipeline():
    
    # Task para buscar os dados das cartas da API
    @task
    def fetch_cards_data():
        try:
            response = requests.get(BASE_URL)
            # Lança uma exceção para erros HTTP
            response.raise_for_status()  
            cards_data = response.json()
            return cards_data
        except requests.RequestException as e:
            logging.error(f"Erro ao buscar os dados da URL: {str(e)}")
            return None

    # Task para salvar os dados da base de cartas no MinIO
    @task
    def save_cards_data_to_minio(cards_data):
        if cards_data:
            # Nome do bucket que vai salvar os dados
            bucket_name = 'bronze'
            # Nome da pasta que vai salvar 
            folder_name = 'cards'
            # Nome do arquivo
            file_name = 'cards_data.json'  
            upload_json_bucket(bucket_name, folder_name, file_name, cards_data)
        else:
            logging.warning("Nenhum dado de cartas foi obtido para salvar no MinIO.")

    # Task para ler os dados das cartas do MinIO
    @task
    def read_cards_data_from_minio():
        # Nome do bucket onde vai ler
        bucket_name = 'bronze'
        # Nome da pasta que vai ler
        folder_name = 'cards' 
        df_base_cards = read_jsons_folder(bucket_name, folder_name)
        
        # Extrai os IDs das cartas lidas do MinIO
        ids = [data.get("id") for inner_list in df_base_cards for data in inner_list]

        # Utiliza ThreadPoolExecutor para paralelizar as requisições HTTP
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(save_pokemon_card, card_id, bucket_name) for card_id in ids]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    # Função para salvar as cartas no MinIO
    def save_pokemon_card(card_id, bucket_name):
        url = f'{BASE_URL}/{card_id}'
        try:
            response = requests.get(url)
            # Lança uma exceção para erros HTTP
            response.raise_for_status()
            card_data = response.json()
            # Cria uma pasta para cada categoria de carta
            folder_name = card_data.get("category", "")
            # Faz o nome do arquivo com o id da carta
            file_name = f"card_{card_id}.json"
            upload_json_bucket(bucket_name, folder_name.lower(), file_name, card_data)
            logging.info(f"Dados do card {card_id} obtidos com sucesso.")
        except requests.RequestException as e:
            logging.error(f"Erro ao buscar os dados da URL {url}: {str(e)}")

    # Definindo TaskGroup para a ingestão de dados
    with TaskGroup(group_id='ingestion_to_bronze') as tg_ingestion:
        # Define a sequência de execução das tasks e a dependência entre as tasks
        save_cards_data_to_minio(fetch_cards_data()) >> read_cards_data_from_minio()


    # Define as categorias de cartas que serão processadas
    # Também é o nome das caterios que são as pastas salvas no bucket MinIO
    categories = ['Pokemon', 'Energia', 'Treinador']

    # Task para processar as cartas de uma categoria
    @task
    def process_card(category):
        # Nome do bucket onde estão os dados
        bucket_name = 'bronze' 
        # Processa os dados e retorna um DataFrame
        data_frame = process_category(bucket_name=bucket_name, folder_name=category.lower())  
        return data_frame

    # Task para salvar os dados processados em formato Parquet no MinIO
    @task
    def save_parquet(category, data_frame):
        # Nome do bucket onde os dados serão salvos
        bucket_name = 'silver'
        upload_parquet_bucket(bucket_name=bucket_name, folder_name=None, file_name=category.lower(), data_frame=data_frame)           

    # Definindo TaskGroup para transformação de dados
    with TaskGroup(group_id='transformation_bronze_to_silver') as tg_bronze_to_silver:
        # Criado grupos de tarefas para cada categoria
        for category in categories:
            with TaskGroup(group_id=f'group_{category.lower()}') as tg:
                # Processa os dados da categoria
                data_frame = process_card.override(task_id=f'process_card_{category.lower()}')(category)
                # Salva os dados processados no bucket 'silver'
                save_parquet.override(task_id=f'save_parquet_{category.lower()}')(category, data_frame)

    # Task para processar a análise dos dados das cartas
    @task
    def process_card_analysis():
        # Nome do bucket onde estão os dados
        bucket_name = 'silver'
        # Processa os dados
        process_analysis(bucket_name=bucket_name, folder_name=None)
    
    # Definindo TaskGroup para a transformação de dados finais
    with TaskGroup(group_id='transformation_silver_to_gold') as tg_silver_to_gold:
        process_card_analysis()

    # # Define a sequência de execução das tasks e a dependência entre os grupos de tasks
    tg_ingestion >> tg_bronze_to_silver >> tg_silver_to_gold

pipeline()
