import logging
from typing import Optional
from pydantic import BaseModel, ValidationError

# Definição do modelo
class CardModel(BaseModel):
    categoria: str
    id: str
    nome: str
    raridade: str
    expansaoId: str
    expansaoNome: Optional[str]
    tipo: Optional[str]
    permitidoStandard: Optional[bool]
    permitidoExpanded: Optional[bool]

# Definição do modelo Pokémon, que herda do modelo CardModel
class PokemonModel(CardModel):
    pokedexId: Optional[str]
    estagio: Optional[str]

# Função para validar os dados
def validate_card(card_data, model):
    try:
        # Cria uma instância do modelo
        model(**card_data)
        return True
    except ValidationError as e:
        logging.error("Erro na criação de carta:", exc_info=True)
        return False

# Função para validar os dados de uma carta de pokémon
def validate_pokemon_card(card_data):
    return validate_card(card_data, PokemonModel)

# Função para validar os dados de uma carta de energia
def validate_energy_card(card_data):
    return validate_card(card_data, CardModel)

# Função para validar os dados de uma carta de treinador
def validate_trainer_card(card_data):
    return validate_card(card_data, CardModel)
