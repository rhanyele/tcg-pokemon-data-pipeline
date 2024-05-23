import logging
import pandas as pd
from minio_connection import read_jsons_folder
from Model.cardModel import validate_energy_card, validate_pokemon_card, validate_trainer_card

# Função para processar cartas de Pokémon
def process_pokemon(data):
    pokemon = {
        "categoria": data.get("category"),
        "id": data.get("id"),
        "nome": data.get("name"),
        "raridade": data.get("rarity"),
        "expansaoId": data.get("set", {}).get("id", None),
        "expansaoNome": data.get("set", {}).get("name", None),
        "pokedexId": data.get("dexId", [None])[0],
        "tipo": data.get("types", [None])[0],
        "estagio": data.get("stage", None),
        "permitidoStandard": data.get("legal", {}).get("standard", None),
        "permitidoExpanded": data.get("legal", {}).get("expanded", None)
    }
    # Valida a carta de Pokémon
    if validate_pokemon_card(pokemon):
        logging.info(f"Carta de Pokémon válida: {pokemon}")
        return pokemon
    else:
        logging.error(f"Carta de Pokémon inválida: {pokemon}")
        return None

# Função para processar cartas de energia
def process_energy(data):
    energy = {
        "categoria": data.get("category"),
        "id": data.get("id"),
        "nome": data.get("name"),
        "raridade": data.get("rarity"),
        "expansaoId": data.get("set", {}).get("id", None),
        "expansaoNome": data.get("set", {}).get("name", None),
        "tipo": data.get("energyType", None),
        "permitidoStandard": data.get("legal", {}).get("standard", None),
        "permitidoExpanded": data.get("legal", {}).get("expanded", None)
    }
    # Valida a carta de energia
    if validate_energy_card(energy):
        logging.info(f"Carta de energia válida: {energy}")
        return energy
    else:
        logging.error(f"Carta de energia inválida: {energy}")
        return None

# Função para processar cartas de treinador
def process_trainer(data):
    trainer = {
        "categoria": data.get("category"),
        "id": data.get("id"),
        "nome": data.get("name"),
        "raridade": data.get("rarity"),
        "expansaoId": data.get("set", {}).get("id", None),
        "expansaoNome": data.get("set", {}).get("name", None),
        "tipo": data.get("trainerType"),
        "permitidoStandard": data.get("legal", {}).get("standard", None),
        "permitidoExpanded": data.get("legal", {}).get("expanded", None)
    }
    # Valida a carta de treinador
    if validate_trainer_card(trainer):
        logging.info(f"Carta de treinador válida: {trainer}")
        return trainer
    else:
        logging.error(f"Carta de treinador inválida: {trainer}")
        return None

# Função para processar as cartas por categoria
def process_category(bucket_name, folder_name):
    try:
        category = folder_name
        cards = read_jsons_folder(bucket_name, category)
        # Lista para armazenar os dados extraídos
        data_frame_list = []
        for card in cards:
            try:
                if category == 'pokemon':
                    logging.info(f'Processando carta de Pokémon: {card}')
                    extracted_data = process_pokemon(card)
                    logging.info(f'Carta de Pokémon processada: {extracted_data}')
                elif category == 'energia':
                    logging.info(f'Processando carta de energia: {card}')
                    extracted_data = process_energy(card)
                    logging.info(f'Carta de energia processada: {extracted_data}')
                elif category == 'treinador':
                    logging.info(f'Processando carta de treinador: {card}')
                    extracted_data = process_trainer(card)
                    logging.info(f'Carta de treinador processada: {extracted_data}')
                else:
                    # Ignorar categorias desconhecidas
                    logging.warning(f'Categoria desconhecida: {category}')
                    continue
                # Verifica se a extração não está vazia e acrescenta no DataFrame
                if extracted_data is not None:
                    data_frame = pd.DataFrame([extracted_data])
                    data_frame_list.append(data_frame)
            except Exception as e:
                logging.error(f"Erro ao processar a carta: {card}. Erro: {str(e)}")
        if data_frame_list:
            # Se processar mais de uma carta, concatena todas em um DataFrame
            final_data_frame = pd.concat(data_frame_list, ignore_index=True)
            return final_data_frame
        else:
            logging.warning(f"Nenhum dado foi lido para a categoria '{category}'.")
            return None
    except Exception as e:
        logging.error(f"Erro ao listar ou ler arquivos JSON: {str(e)}")
        return None
