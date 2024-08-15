import os
import json
from flatten_json import flatten
import pandas as pd
import requests
import dotenv
import json
import logging.config
import logging.handlers
import os
from .logging.logging_setup import setup_logging

logger = logging.getLogger(__name__)

dotenv.load_dotenv()


def test(string = "un deux un deux test") -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"{__file__ = }")
    print(f"{script_dir = }")
    print(f'{string = }')


def test_logging():
    setup_logging()

    logger.error("éàù", extra={"x": "hello"})
    logger.debug("debug message", extra={"x": "hello"})
    logger.info("info message")
    logger.warning("warning message")
    logger.error("error message")
    logger.critical("critical message")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("exception message")


def store_json_file(file_path, data):
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)
        logger.info(f"Données enregistrées dans '{file_path}'.")


def retrieve_json(file_path):
    try:
        with open(file_path, 'r') as f:
            flight_data = json.load(f)
    except FileNotFoundError:
        logger.exception(f"Le fichier {f} n'a pas été trouvé.")
    except json.JSONDecodeError:
        logger.exception(f"Erreur de syntaxe dans le fichier JSON.")
    return flight_data


def build_data_storage_path(file_name, data_stage):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = script_dir.split("dst_airlines")[-1]
    path = os.path.join(project_root, 'data', data_stage)

    if not os.path.exists(path):
        logger.error(f"Le stage {data_stage} n'a pas de dossier correspondant dans {project_root}.")
        return None
    else:
        return os.path.join(path, file_name)


def flatten_list_of_dict(dicts):
    return pd.DataFrame([flatten(d) for d in dicts])


def get_public_ip_address():
    ipfy_url = os.getenv("IPFY_URL")
    try:
        response = requests.get(ipfy_url)
        ip_info = response.json()
        public_ip = ip_info['ip']
        return public_ip
    except requests.RequestException:
        logger.exception("Erreur de récupération de l'adresse IP")
        return None
    

def build_lh_api_headers(api_token, public_ip):
    headers = {
        'Authorization': f'Bearer {api_token}',
        'X-originating-IP': public_ip
    }
    return headers