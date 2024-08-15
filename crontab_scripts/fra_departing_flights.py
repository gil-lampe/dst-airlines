from dst_airlines.data.lufthansa_api_flights import collect_fullday_departing_flights
from dst_airlines.logging import logging_setup
from dst_airlines import utils
import dotenv
import os

def main():
    # Configuration de la collecte des logs
    logging_setup.setup_logging()

    fra_iata = "FRA"

    # Récupération des variables d'envrionnement
    dotenv.load_dotenv()

    # Récupération de l'adresse IP
    public_ip = utils.get_public_ip_address()

    # Construction du header de la requête
    api_token = os.getenv('API_TOKEN')

    # Collecte des données d'hier (comportement par défaut quand la date et l'heure ne sont pas spécifiées) pour FRA
    collect_fullday_departing_flights(api_token=api_token, public_ip=public_ip, airport_iata=fra_iata)

if __name__ == "__main__":
    main()