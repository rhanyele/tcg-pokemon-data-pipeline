# Colocar o nome do container no docker compose
# Executar o comando par criar o docker e instalar as dependencias do poetry
# docker build . -t poetry-apache/airflow:2.9.1
FROM apache/airflow:2.9.1

# Copiar os arquivos de configuração do poetry
COPY poetry.lock pyproject.toml /app/

# Atualizar o pip e instalar o poetry
RUN pip install --upgrade pip && \
    pip install poetry

# Definir o diretório de trabalho
WORKDIR /app

# Instalar as dependências do projeto com poetry
RUN poetry install