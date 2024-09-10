import requests 
import json
import csv
import os
from datetime import datetime
import time

# LUFTHANSA
# get token
# curl "https://api.lufthansa.com/v1/oauth/token" -X POST -d "client_id=wd4b8gk6uu2psa6ywp65s8m7b" -d "client_secret=PjFqxXDe9R" -d "grant_type=client_credentials"

# get IP addr :
# ip addr | grep eth0
API_KEY = 'w73hpwsd499j4f2u2x9grg3g'
url = "https://api.lufthansa.com/v1"
headers = {
    'Authorization': f'Bearer {API_KEY}',
    'X-originating-IP': '172.29.180.161'
}

# Dossier où enregistrer les fichiers CSV  // le crée si nécessaire :
output_dir = '/home/sanou/DST-Airlines/data/1_raw'
os.makedirs(output_dir, exist_ok=True)

# Liste des endpoints et des noms de fichiers correspondants
endpoints = {
    "airports": "/mds-references/airports",
    "cities": "/mds-references/cities",
    "airlines": "/mds-references/airlines",
    "aircraft": "/mds-references/aircraft",
    # "landing_rep": "/flight_operations/crew_services/COMMON_LANDING_REPORT"
}

# Requête nos endpoints et obtenir les données JSON
# def fetch_data(endpoint, headers):
#     response = requests.get(f"{url}{endpoint}", headers=headers)
#     if response.status_code == 200:
#         return response.json()
#     else:
#         print(f"Erreur sur la requête de {endpoint}: {response.status_code}")
#         return None

def fetch_all_data(endpoint, headers):
    all_data = []
    limit = 100 # nombre max par page
    offset = 0 # démarre à la 1e page
    call_count = 0
    
    while True:
        response = requests.get(f"{url}{endpoint}?limit={limit}&offset={offset}", headers = headers)
        call_count += 1
        print(f"Requête envoyée à : {url}{endpoint}?limit={limit}&offset={offset}")
        print(f"Code réponse : {response.status_code}")
        
        if response.status_code == 200:
            json_data = response.json()
            
            # data = json_data
            data = transform_data(json_data, endpoint.split("/")[-1])
            
            if not data:
                break
            
            all_data.extend(data)
            offset += limit
            
        else:
            print(f"Erreur sur la requête de {endpoint}: {response.status_code}")
            break
        
                    
        if call_count %5 == 0:
            time.sleep(1)
            
    return all_data

# Transforme les données JSON en liste de dictionnaires (rows pour CSV)
def transform_data(json_data, key):
    if not json_data:
        return []

    # Ajuster les transformations pour chaque type de données
    if key == 'airports':
        return json_data.get('AirportResource', {}).get('Airports', {}).get('Airport', [])
    if key == 'cities':
        return json_data.get('CityResource', {}).get('Cities', {}).get('City', [])
    if key == 'airlines':
        return json_data.get('AirlineResource', {}).get('Airlines', {}).get('Airline', [])
    if key == 'aircraft':
        return json_data.get('AircraftResource', {}).get('AircraftSummaries', {}).get('AircraftSummary', [])

#     return []
    # if 'Airports' in json_data:
    #     return json_data['Airports']
    # if 'cities' in json_data:
    #     return json_data['cities']
    # if 'airlines' in json_data:
    #     return json_data['airlines']
    # if 'aircraft' in json_data:
    #     return json_data['aircraft']

# Enregistrer les données dans un fichier CSV
def save_to_csv(data, filename):
    if not data:
        print(f"Aucune donnée à enregistrer pour {filename}")
        return

    # Récupérer les clés (headers) du premier dictionnaire
    timestamp = datetime.now().strftime("%Y-%m-%d")
    headers = data[0].keys()
    filename_timestamp = f"{filename}_{timestamp}.csv"
    file_path = os.path.join(output_dir, filename_timestamp)

    with open(file_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    print(f"Données CSV enregistrées dans '{filename}'.")

def save_to_json(data, filename):
    if not data:
        print(f"Aucune donnée à enregistrer pour {file_path}")
        return
    
    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename_timestamp = f"{filename}_{timestamp}.json"
    file_path = os.path.join(output_dir, filename_timestamp)
    
    with open(file_path, "w", encoding = "utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)
    
    print(f"Données JSON enregistrées dans '{file_path}'.")

# Boucle pour parcourir les endpoints :
for key, endpoint in endpoints.items():
    all_data = fetch_all_data(endpoint, headers)
    csv_filename = f'{key}_data'
    json_filename = f'{key}_data'
    save_to_csv(all_data, csv_filename)
    save_to_json(all_data, json_filename)


# FONCTIONNE JSON
# # Boucle pour parcourir les endpoints et sauvegarder en .json
# for key, endpoint in endpoints.items():
#     response = requests.get(f"{url}{endpoint}", headers=headers)
#     if response.status_code == 200:
#         data = response.json()
#         with open(f'{key}_data.json', 'w') as json_file:
#             json.dump(data, json_file, indent=4)
#         print(f"Données pour {key} enregistrées dans '{key}_data.json'.")
#     else:
#         print(f'Erreur lors de la requête pour {key}: {response.status_code}')