import json
import io
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from urllib3.exceptions import MaxRetryError
from airflow.hooks.base_hook import BaseHook

# Função para conectar ao MinIO
def connect_minio():
    # Nome da sua conexão no Airflow
    conn_id = 'minio_generic'
    conn_details = BaseHook.get_connection(conn_id)

    # Configurações da conexão
    minio_endpoint = conn_details.host
    access_key = conn_details.login
    secret_key = conn_details.password
    try:
        # Criando cliente do MinIO
        minio_client = Minio(
            endpoint=minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        logging.info("Conexão realizada com sucesso.")
        return minio_client
    except Exception as e:
        logging.error(f"Erro ao se conectar ao MinIO: {str(e)}.")
        return None

# Função para definir o nome completo do arquivo incluindo a pasta
def full_file_name_define(folder_name, file_name):
    if folder_name:
        # Caminho completo com pasta dentro do bucket
        full_file_name = f'{folder_name}/{file_name}'  
    else:
        # Salva na raiz do bucket
        full_file_name = file_name 
    logging.info(f"Nome completo: {full_file_name}")
    return full_file_name  

# Função para verificar se o bucket existe, se não existir cria o bucket
def verify_bucket(bucket_name):
    minio_client = connect_minio()
    logging.info(f"Verificando o bucket: {bucket_name}.") 
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        logging.info(f"Criado o bucket {bucket_name}.")
    else:
        logging.info(f"O bucket {bucket_name} já existe.")

# Função para fazer o upload de um arquivo JSON no bucket
def upload_json_bucket(bucket_name, folder_name, file_name, json_data):
    minio_client = connect_minio()
    full_file_name = full_file_name_define(folder_name, file_name)
    verify_bucket(bucket_name)
    logging.info(f"Inserindo no bucket: {bucket_name}, pasta: {folder_name}, arquivo: {file_name}.")
    try:    
        # Criar um buffer de bytes a partir dos dados JSON
        data = json.dumps(json_data).encode('utf-8')
        data_stream = io.BytesIO(data)

        # Salvar o objeto JSON no MinIO
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=full_file_name,
            data=data_stream,
            length=len(data),
            content_type='application/octet-stream'
        )
        logging.info(f"Arquivo JSON salvo com sucesso em {bucket_name}/{full_file_name}.")
    except MaxRetryError:
        logging.error(f"Erro de conexão ao tentar acessar o bucket {bucket_name}.")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo JSON: {str(e)}.")

# Função para fazer o upload de um arquivo Parquet no bucket
def upload_parquet_bucket(bucket_name, folder_name, file_name, data_frame):
    minio_client = connect_minio()
    full_file_name = full_file_name_define(folder_name, file_name)
    verify_bucket(bucket_name)
    logging.info(f"Inserindo no bucket: {bucket_name}, pasta: {folder_name}, arquivo: {file_name}.")
    try:
        # Convertendo o DataFrame para uma tabela PyArrow
        table = pa.Table.from_pandas(data_frame)
        
        # Escrever os dados em um buffer da memória
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)

        # Fazer upload do arquivo Parquet para o MinIO
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=f'{full_file_name}.parquet',
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )  
        logging.info(f"Arquivo Parquet '{full_file_name}' salvo no MinIO com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo Parquet: {str(e)}.")

# Função para ler os arquivos JSON de uma pasta do bucket e retornar uma lista
def read_jsons_folder(bucket_name, folder_name):
    minio_client = connect_minio()
    logging.info(f"Lendo o bucket: {bucket_name}, pasta: {folder_name}.")
    try:
        objects = minio_client.list_objects(bucket_name, prefix=folder_name, recursive=True)
        # Lista para armazenar os dados de todos os arquivos JSON
        all_json_data = []      
        for obj in objects:
            logging.info(f"Lendo objeto: {obj.object_name}.")
            # Valida a extensão do arquivo
            if obj.object_name.endswith('.json'):
                try:
                    response = minio_client.get_object(bucket_name, obj.object_name)
                    json_data = json.load(io.StringIO(response.read().decode('utf-8')))
                    all_json_data.append(json_data)  # Adiciona os dados do arquivo JSON à lista
                except Exception as e:
                    logging.error(f"Erro ao processar o arquivo {obj.object_name}: {str(e)}")          
        # Retorna a lista de dados JSON lidos
        return all_json_data  
    except MaxRetryError:
        logging.error(f"Erro de conexão ao tentar acessar o bucket {bucket_name}")
    except Exception as e:
        logging.error(f"Erro ao listar ou ler arquivos JSON: {str(e)}")

# Função para ler os arquivos Parquet de uma pasta do bucket e retornar um DataFrame 
def read_parquet_folder(bucket_name, folder_name):
    minio_client = connect_minio()
    logging.info(f"Lendo o bucket: {bucket_name}, pasta: {folder_name}")
    try:
        objects = minio_client.list_objects(bucket_name, prefix=folder_name, recursive=True)
        # Lista para armazenar os dados de todos os arquivos Parquet
        all_data_frames = []
        for obj in objects:
            # Valida a extensão do arquivo
            if obj.object_name.endswith('.parquet'):
                try:
                    response = minio_client.get_object(bucket_name, obj.object_name)
                    parquet_data = io.BytesIO(response.read())
                    table = pq.read_table(parquet_data)
                    data_frame = table.to_pandas()
                    all_data_frames.append(data_frame)
                except Exception as e:
                    logging.error(f"Erro ao processar o arquivo {obj.object_name}: {str(e)}")
        # Concatena os arquivos Parquet em um único DataFrame
        if all_data_frames:
            concat_data_frame = pd.concat(all_data_frames, ignore_index=True)
            return concat_data_frame
        else:
            logging.warning(f"Nenhum arquivo Parquet encontrado na pasta '{folder_name}'.")
            return pd.DataFrame()  # Retorna um DataFrame vazio se nenhum arquivo Parquet for encontrado
    except Exception as e:
        logging.error(f"Erro ao listar ou ler arquivos Parquet: {str(e)}")
