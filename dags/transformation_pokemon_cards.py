from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import sys

# Adiciona o caminho do diretório dos scripts ao sys.path
sys.path.insert(0, "/opt/airflow/scripts")
from minio_connection import upload_parquet_bucket
from processing_bronze_to_silver import process_category

@dag(
    dag_id='transformation_pokemon_cards',
    description='DAG para transformar os dados de Pokemon de formato JSON para formato Parquet.',
    schedule_interval=None, # A DAG não terá agendamento recorrente
    start_date=datetime(2024, 5, 9),
    catchup=False  # A DAG não deve fazer execuções passadas
)
def pipeline():

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

    # Criado grupos de tarefas para cada categoria
    for category in categories:
        with TaskGroup(group_id=f'group_{category.lower()}') as tg:
            # Processa os dados da categoria
            data_frame = process_card.override(task_id=f'process_card_{category.lower()}')(category)
            # Salva os dados processados no bucket 'silver'
            save_parquet.override(task_id=f'save_parquet_{category.lower()}')(category, data_frame)
            # Finaliza o grupo de tarefas
            tg

# Executa a pipeline
pipeline()
