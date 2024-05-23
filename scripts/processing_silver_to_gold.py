import logging
from minio_connection import read_parquet_folder, upload_parquet_bucket
from Model.analysisModel import *

# Função para analisar a distribuição de cartas por categoria
def analyze_distribution(df):
    # Conta a quantidade de cada categoria
    data = df['categoria'].value_counts().reset_index()
    data.columns = ['categoria', 'quantidade']
    
    # Valida a análise de distribuição
    if validate_analyze_distribution(data):
        logging.info(f"Análise de distribuição de cartas por categoria válida: {data}")
        return data
    else:
        logging.error(f"Análise de distribuição de cartas por categoria inválida: {data}")
        return None

# Função para analisar a raridade das cartas por categoria
def analyze_rarity_by_category(df):
    # Agrupa por categoria e conta a quantidade de cada raridade
    data = df.groupby('categoria')['raridade'].value_counts().reset_index(name='quantidade')
    
    # Valida a análise de raridade por categoria
    if validate_analyze_rarity_by_category(data):
        logging.info(f"Análise de raridade por categoria válida: {data}")
        return data
    else:
        logging.error(f"Análise de raridade por categoria inválida: {data}")
        return None

# Função para analisar as expansões mais populares
def analyze_popular_expansions(df):
    # Conta a quantidade de cada expansão
    data = df['expansaoNome'].value_counts().reset_index()
    data.columns = ['expansao_nome', 'quantidade']
    
    # Valida a análise de popularidade das expansões
    if validate_analyze_popular_expansions(data):
        logging.info(f"Análise de cartas por expansão válida: {data}")
        return data
    else:
        logging.error(f"Análise de cartas por expansão inválida: {data}")
        return None

# Função para analisar os tipos de Pokémon
def analyze_pokemon_types(df):
    # Filtra cartas de Pokémon e conta a quantidade de cada tipo
    data = df[df['categoria'] == 'Pokemon']['tipo'].value_counts().reset_index()
    data.columns = ['tipo', 'quantidade']
    
    # Valida a análise de tipos de Pokémon
    if validate_analyze_pokemon_types(data):
        logging.info(f"Análise de Pokémons por tipo válida: {data}")
        return data
    else:
        logging.error(f"Análise de Pokémons por tipo inválida: {data}")
        return None

# Função para analisar os estágios de evolução dos Pokémon
def analyze_evolution_stages(df):
    # Filtra cartas de Pokémon e conta a quantidade de cada estágio de evolução
    data = df[df['categoria'] == 'Pokemon']['estagio'].value_counts().reset_index()
    data.columns = ['estagio', 'quantidade']
    
    # Valida a análise de estágios de evolução
    if validate_analyze_evolution_stages(data):
        logging.info(f"Análise de Pokémons por estágio válida: {data}")
        return data
    else:
        logging.error(f"Análise de Pokémons por estágio inválida: {data}")
        return None

# Função para analisar a quantidade de Pokémon por expansão
def analyze_pokemon_by_expansion(df):
    # Filtra cartas de Pokémon e conta a quantidade de cada expansão
    data = df[df['categoria'] == 'Pokemon']['expansaoNome'].value_counts().reset_index()
    data.columns = ['expansao_nome', 'quantidade']
    
    # Valida a análise de Pokémon por expansão
    if validate_analyze_pokemon_by_expansion(data):
        logging.info(f"Análise de Pokémons por expansão válida: {data}")
        return data
    else:
        logging.error(f"Análise de Pokémons por expansão inválida: {data}")
        return None

# Função para fazer upload do resultado da análise para o MinIO
def upload_analysis_result(file_name, data_frame, analyze_type):
    try:
        bucket_name = "gold"
        folder_name = "analysis"
        
        # Executa a função de análise
        result_df = analyze_type(data_frame)
        
        # Faz o upload do resultado da análise
        upload_parquet_bucket(bucket_name=bucket_name, folder_name=folder_name, file_name=file_name, data_frame=result_df)
    except Exception as e:
        logging.error(f"Falha ao fazer upload de {file_name}: {str(e)}")

# Função principal para processar as análises e fazer upload para o MinIO
def process_analysis(bucket_name, folder_name):
    try:
        logging.info("Lendo dados do bucket MinIO")
        
        # Lê os dados do bucket e da pasta especificada
        card_data = read_parquet_folder(bucket_name, folder_name)
        
        logging.info("Iniciando o processo de análise e carga para o MinIO")
        
        # Lista de funções de análise e seus nomes de arquivo correspondentes
        analyze_functions = [
            ("distribuicao_por_categoria", analyze_distribution),
            ("raridade_por_categoria", analyze_rarity_by_category),
            ("cartas_por_expansao", analyze_popular_expansions),
            ("pokemon_por_tipo", analyze_pokemon_types),
            ("pokemon_por_estagios_evolucao", analyze_evolution_stages),
            ("pokemon_por_expansao", analyze_pokemon_by_expansion)
        ]
        
        # Executa cada função de análise e faz upload do resultado
        for file_name, analyze_function in analyze_functions:
            upload_analysis_result(file_name=file_name, data_frame=card_data, analyze_type=analyze_function)
            
    except Exception as e:
        logging.error(f"Erro ao processar análises: {str(e)}")
        return None
