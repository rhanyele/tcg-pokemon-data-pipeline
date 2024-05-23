from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import sys

# Adiciona o caminho do diretório dos scripts ao sys.path
sys.path.insert(0, "/opt/airflow/scripts")
from processing_silver_to_gold import process_analysis

@dag(
    dag_id='transformation_analysis_cards',
    description='DAG para transformar os dados que estão na silver em arquivos de análises.',
    schedule_interval=None,  # A DAG não terá agendamento recorrente
    start_date=datetime(2024, 5, 9),
    catchup=False  # A DAG não deve fazer execuções passadas
)
def pipeline():

    # Task para processar a análise dos dados das cartas
    @task
    def process_card_analysis():
        # Nome do bucket onde estão os dados
        bucket_name = 'silver'
        # Processa os dados
        process_analysis(bucket_name=bucket_name, folder_name=None)

    # Define a task e a executa
    process_card_analysis()

# Executa a pipeline
pipeline()
