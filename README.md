# Projeto Pokémon TCG Data Pipeline

O projeto cria uma solução de pipeline de dados para coletar, transformar e analisar informações sobre as cartas de Pokémon TCG (Trading Card Game), utilizando a arquitetura medalhão. O projeto é uma aplicação Python desenvolvida com o Airflow e MinIO data storage. Os dados são processados utilizando o pandas e o pyarrow e armazenados em formato Parquet, que é um formato otimizado para a consulta.

Pokémon TCG é um jogo em que cada um dos jogadores usa um baralho (deck) com 60 cartas para batalhar e ver quem é o vencedor, seguindo determinadas regras. O jogo é composto basicamente por cartas do tipo: Pokémon, treinador e energias.

![tcg-pokemon](https://github.com/rhanyele/tcg-pokemon-data-pipeline/assets/10997593/9ddef34f-44bb-4dbf-8a85-82e6c88ce95b)

### Documentação da API
- [Pokémon TCG API](https://www.tcgdex.dev/)

## Estrutura do projeto
```bash
- dags
  - ingestion_pokemon_cards.py
  - pipeline_pokemon.py
  - transformation_analysis_cards.py
  - transformation_pokemon_cards.py
- scripts
  - Model
    - analysisModel.py
    - cardModel.py
  - minio_connection.py
  - processing_bronze_to_silver.py
  - processing_silver_to_gold.py
- docker-compose-minio.yml
- docker-compose.yml
- Dockerfile
```

## Funcionalidades
- **Ingestion Pokémon Cards:** Busca os dados na API, faz a ingestão dos dados na camada bronze em formato JSON.
- **Transformation Pokémon Cards:** Faz a escolha dos campos, valida com o modelo e salva em formato Parquet na camada silver.
- **Transformation Analysis Cards:** Cria as análises, valida no modelo de análise e salva em formato Parquet na camada gold.
- **Pipeline Pokémon:** Executa todo o processo descrito.


## Requisitos
- Python
- Poetry
- Docker
- Docker Compose

## Instalação
1. Clone este repositório:

   ```bash
   git clone https://github.com/rhanyele/tcg-pokemon-data-pipeline.git
   ```

2. Acesse o diretório do projeto:

   ```bash
   cd tcg-pokemon-data-pipeline
   ```

3. Instale as dependências usando Poetry:

   ```bash
   poetry install
   ```
   
4. Crie a imagem do docker com o Airflow e as dependências do Poetry instaladas:

   ```bash
   docker build . -t poetry-apache/airflow:2.9.1
   ```

5. Execute o docker compose para criar os containers do Airflow:

   ```bash
   docker compose up
   ```

6. Execute o docker compose para criar o container do MinIO:

   ```bash
   docker-compose -f docker-compose-minio.yml up
   ```

## Uso
Crie a conexão do Airflow com o MinIO:

![Conexão Airflow MinIO](https://github.com/rhanyele/tcg-pokemon-data-pipeline/assets/10997593/27e3d85f-7c5b-403d-a2cf-d3a9a6f2290d)

Execute a Dag para fazer todo o processo de carga e processamento.

![Airflow](https://github.com/rhanyele/tcg-pokemon-data-pipeline/assets/10997593/9c4f8101-ddb6-449b-83b1-835a0a462bff)

Os dados serão armazenados no MinIO:

![MinIO](https://github.com/rhanyele/tcg-pokemon-data-pipeline/assets/10997593/9ba2d850-519e-43a2-b766-6415638fea1a)

## Contribuição
Sinta-se à vontade para contribuir com novos recursos, correções de bugs ou melhorias de desempenho. Basta abrir uma issue ou enviar um pull request!

## Autor
[Rhanyele Teixeira Nunes Marinho](https://github.com/rhanyele)

## Licença
Este projeto está licenciado sob a [MIT License](LICENSE).
