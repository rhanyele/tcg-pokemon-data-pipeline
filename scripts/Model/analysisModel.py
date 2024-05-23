import logging
from typing import List, Union
from pydantic import BaseModel, ValidationError

# Definição do modelo
class CategoriaDistModel(BaseModel):
    categoria: str
    quantidade: int

class RaridadePorCategoriaModel(BaseModel):
    categoria: str
    raridade: str
    quantidade: int

class ExpansoesPopularesModel(BaseModel):
    expansao_nome: str
    quantidade: int

class TiposPokemonModel(BaseModel):
    tipo: str
    quantidade: int

class EstagiosEvolucaoModel(BaseModel):
    estagio: str
    quantidade: int

class PokemonPorExpansaoModel(BaseModel):
    expansao_nome: str
    quantidade: int

# Função para validar os dados
def validate_analyze(card_data, model):
    try:
         # Cria uma instância do modelo
        model(**card_data)
        return True
    except ValidationError as e:
        logging.error("Erro na criação de carta:", exc_info=True)
        return False

# Função para validar a análise de distribuição de categorias
def validate_analyze_distribution(card_data):
    # Converte os dados para uma lista de dicionários e valida cada linha
    for row in card_data.to_dict(orient='records'):
        if not validate_analyze(row, CategoriaDistModel):
            return False
    return True

# Função para validar a análise de raridade por categoria
def validate_analyze_rarity_by_category(card_data):
    # Converte os dados para uma lista de dicionários e valida cada linha
    for row in card_data.to_dict(orient='records'):
        if not validate_analyze(row, RaridadePorCategoriaModel):
            return False
    return True

# Função para validar a análise de expansões populares
def validate_analyze_popular_expansions(card_data):
    # Converte os dados para uma lista de dicionários e valida cada linha
    for row in card_data.to_dict(orient='records'):
        if not validate_analyze(row, ExpansoesPopularesModel):
            return False
    return True

# Função para validar a análise de tipos de Pokémon
def validate_analyze_pokemon_types(card_data):
    # Converte os dados para uma lista de dicionários e valida cada linha
    for row in card_data.to_dict(orient='records'):
        if not validate_analyze(row, TiposPokemonModel):
            return False
    return True

# Função para validar a análise de estágios de evolução dos Pokémon
def validate_analyze_evolution_stages(card_data):
    # Converte os dados para uma lista de dicionários e valida cada linha
    for row in card_data.to_dict(orient='records'):
        if not validate_analyze(row, EstagiosEvolucaoModel):
            return False
    return True

# Função para validar a análise de Pokémon por expansão
def validate_analyze_pokemon_by_expansion(card_data):
    # Converte os dados para uma lista de dicionários e valida cada linha
    for row in card_data.to_dict(orient='records'):
        if not validate_analyze(row, PokemonPorExpansaoModel):
            return False
    return True