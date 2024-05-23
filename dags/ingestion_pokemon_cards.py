from airflow.decorators import dag, task
from datetime import datetime
import requests
import concurrent.futures
import logging
import sys

# Adiciona o caminho do diretório dos scripts ao sys.path
sys.path.insert(0, "/opt/airflow/scripts")
from minio_connection import upload_json_bucket, read_jsons_folder

# URL base para a API de cartas de Pokémon
BASE_URL = "https://api.tcgdex.net/v2/pt/cards"

@dag(
    dag_id='ingestion_pokemon_cards',
    description='DAG para buscar dados de Pokemon da API e salvar no MinIO.',
    schedule_interval=None,  # A DAG não terá agendamento recorrente
    start_date=datetime(2024, 5, 9),
    catchup=False  # A DAG não deve fazer execuções passadas
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

    # Define a sequência de execução das tasks e a dependência entre as tasks
    save_cards_data_to_minio(fetch_cards_data()) >> read_cards_data_from_minio()

# Executa a pipeline
pipeline()
