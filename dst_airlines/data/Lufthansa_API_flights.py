import requests 
import json
import os
from datetime import datetime
import time

# LUFTHANSA
# get token
# curl "https://api.lufthansa.com/v1/oauth/token" -X POST -d "client_id=wd4b8gk6uu2psa6ywp65s8m7b" -d "client_secret=PjFqxXDe9R" -d "grant_type=client_credentials"

API_TOKEN = 's9qtthxz3p2ku9p3pywtc5uj' # Token Gil

URL = "https://api.lufthansa.com/v1"


def fetch_departing_flights(airport_iata, headers, date='', start_time="00:00", debug=False):
    """Fetch departing flights from the given airport_iata starting from start_time till midnight

    Args:
        airport_iata (str): IATA of the airport
        headers (dict): dictionnary containing "Authorization" and "X-originating-IP" keys to authenticate the request
        date (str, optional): target date, format should be YYYY-MM-DD, if empty string is given, date will be set up as today. Defaults to ''.
        start_time (str, optional): Start time from which fetching will start. Defaults to "00:00".
        debug (bool, optional): Indication to print debug messages. Defaults to False.

    Returns:
        list: list containing each chunk of data, each element is an API call
    """
    ###
    # Création d'une fonction interne pour incrémenter l'endpoint de 4h (par défaut)
    # l'API ne renvoie que des données sur une tranche ("range") de 4h
    ###
    def increment_range(flight_endpoint):
        """Increment the starting time of the flight_endpoint of 4h (API range)

        Args:
            flight_endpoint (str): complete flight endpoint (exept the limit & offset)

        Returns:
            tuple: Tuple composed of a boolean indicating if the incremented time is outranged (> 24h) and the new endpoint
        """
        range = 4
        start_range = flight_endpoint[-5:]
        hour_int = int(start_range.split(":")[0])
        minutes_str = start_range.split(":")[1]
        new_flight_endpoint = ""

        hour_int += range
        if hour_int >= 24:
            print(f"Nouveau range dépasse 24h (heure après incrémentation : {hour_int}h)") if debug else None
            outranged = True
            return (outranged, new_flight_endpoint)
        else :
            hour_str = "0" + str(hour_int) if hour_int < 10 else str(hour_int)
            start_time = hour_str + ":" + minutes_str           
            new_flight_endpoint = flight_endpoint[:-5] + start_time

            print("Nouvelle heure de début : ", start_time) if debug else None
            print("Nouveau endpoint : ", new_flight_endpoint) if debug else None
            outranged = False
            return (outranged, new_flight_endpoint)

    ###
    # Définition des variables
    ###
    airport_endpoint = f"/operations/flightstatus/departures/{airport_iata}/"

    if date == '':
        date = datetime.today().strftime('%Y-%m-%d')

    flight_endpoint =  airport_endpoint + date + "T" + start_time

    flights_data_list = []
    LIMIT = 50 # nombre max par page - l'API ne renvoie pas plus de 50 vols
    offset = 0 # démarre à la 1e page
    total_range_count = 0 # nombre d'éléments dans le range

    count = 0

    ###
    # Boucle pour parcourir les ranges de 4h puis la pagination des données sur ces ranges
    # En effet, si on requête les données à l'heure "08:00", l'API va être fournir les résultats
    # par page de 50 éléments max sur les 4h suivantes (donc de 08:00 à 12:00) 
    ###
    while True:
        count += 1
        ###
        # Récupération de la requête
        ###
        request = f"{URL}{flight_endpoint}?limit={LIMIT}&offset={offset}"
        response = requests.get(request, headers = headers)
        print(f"Requête envoyée à : {request}") if debug else None
        print(f"Code réponse : {response.status_code}") if debug else None

        ###
        # Si la requête renvoie des données sur ce range & offset (code 200 = OK)
        ###
        if response.status_code == 200:
            # Récupération des données
            flights_data = response.json()
            flights_data_list.append(flights_data)

            print("Length of the result: ", len(flights_data["FlightStatusResource"]["Flights"]["Flight"])) if debug else None
            
            # Nombre total d'éléments dans ce range de 4h
            total_range_count = flights_data["FlightStatusResource"]["Meta"]["TotalCount"]
        
            print("Offset: ", offset, " / ", total_range_count) if debug else None
            offset += LIMIT

            # Si l'offset est supérieur au total d'éléments du range, incrémentation du range
            if offset > total_range_count:
                offset = 0
                outranged, flight_endpoint = increment_range(flight_endpoint)
                # Si le range incrémenté commence le lendemain (> 24h) on arrête
                if outranged:
                    break

        ###
        # Si la requête ne renvoie pas de données (code 404 = Not Found)
        ###
        elif response.status_code == 404:
            print(f"Erreur 404, pas de données sur le range de 4h commençant à : {start_time} avec pour offset : {offset}") if debug else None

            # Incrémentation du range car il n'y a pas de données de celui-ci
            outranged, flight_endpoint = increment_range(flight_endpoint)
            offset = 0

            # Si le range incrémenté commence le lendemain (> 24h) on arrête
            if outranged:
                break

        ###
        # Si on a réalisé trop de requêtes en 1s
        ###
        elif response.status_code == 403 and response.json()["Error"] == "Account Over Queries Per Second Limit":
            time.sleep(1)
            print("Trop de requêtes par seconde, attente d'une 1s") if debug else None
        
        ###
        # Si une autre erreur survient, on arrête
        ###
        else:
            print(f"Erreur sur la requête de {flight_endpoint}: {response.status_code}") if debug else None
            break


    print("Nombre de requêtes :", count) if debug else None
    return flights_data_list


def store_json(file_path, data):
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    print(f"Données enregistrées dans '{file_path}'.")


def main():
    print("Le script est exécuté directement.")

    airport_iata = "FRA"
    today_formated = datetime.today().strftime('%Y-%m-%d')

    # Construction du chemin pour stocker les données
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, '..', '..', 'data', 'raw')
    file_name = f"{airport_iata}_dep_flights_{today_formated}.json"
    file_path = os.path.join(path, file_name)

    # Récupération de l'adresse IP
    try:
        response = requests.get('https://api.ipify.org?format=json')
        ip_info = response.json()
        public_ip = ip_info['ip']
    except requests.RequestException:
        print("Erreur de récupération de l'adresse IP")
        return None

    headers = {
        'Authorization': f'Bearer {API_TOKEN}',
        'X-originating-IP': public_ip
    }

    flights_data = fetch_departing_flights(airport_iata, headers, start_time="00:00", debug=True)
    store_json(file_path, flights_data)


if __name__ == "__main__":
    main()