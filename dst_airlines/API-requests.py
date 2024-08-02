import requests
from pymongo import MongoClient

################
#  LUFTHANSA   #
################

# Remplace par ta clé API Lufthansa
API_KEY = 'zajv223nvjf6e8773wzmb474'
url = "https://api.lufthansa.com/v1"
endpoint = "/mds-references/airports"
headers = {
    'Authorization': f'Bearer {API_KEY}',
    'X-originating-IP': '172.29.180.161'
}

# Exemple de requête pour obtenir le statut d'un vol
response = requests.get(f"{url}{endpoint}", headers=headers)

if response.status_code == 200:
    flight_data = response.json()
    print(flight_data)
else:
    print(f'Error: {response.status_code}')


##################
# AVIATION STACK # 3 requêtes par jour
##################

# Config MongoDB
mongo_host = 'localhost'
mongo_port = 27018
mongo_db = 'flight_data'
mongo_collection = 'flights'

# Connexion MongoDB
client = MongoClient(mongo_host, mongo_port)
db = client[mongo_db]
collection = db[mongo_collection]

# Clé API AviationStack
API_KEY = '7ff112e3d87a82c73966b6f36a5ac53c'
base_url = 'http://api.aviationstack.com/v1/'
endpoint = 'flights'

# Paramètres de la requête
params = {
    'access_key': API_KEY
}

# Faire la requête à l'API
response = requests.get(f"{base_url}{endpoint}", params=params)

if response.status_code == 200:
    flights_data = response.json()['data']
    result = collection.insert_many(flights_data)
    print(f"{len(result.inserted_ids)} documents insérés avec succès.")
else:
    print(f"Problème de requête. {response.status_code}")







# # Vérifier le statut de la réponse
# if response.status_code == 200:
#     # Si la requête est réussie, récupérer les données JSON
#     airports_data = response.json()['data']

#     # Se connecter à la base de données MySQL
#     try:
#         conn = mysql.connector.connect(
#             host='localhost',
#             port=3307,
#             user='sanou',
#             password='password',
#             database='flight'
#         )
#         cursor = conn.cursor()
        
#         # Création de la base de donnée si elle n'existe pas déjà
#         create_table_query = """
#         CREATE TABLE IF NOT EXISTS air
#         """
# ##
# ##
# ##
#         # Insérer les données dans la table airports
#         for airport in airports_data:
#             insert_query = """
#                 INSERT INTO airports (airport_id, name, city, country, iata, icao, latitude, longitude, altitude, timezone, dst)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             """
#             cursor.execute(insert_query, (
#                 airport['id'],
#                 airport['airport_name'],
#                 airport['city'],
#                 airport['country'],
#                 airport['iata_code'],
#                 airport['icao_code'],
#                 airport['latitude'],
#                 airport['longitude'],
#                 airport['altitude'],
#                 airport['timezone'],
#                 airport['dst']
#             ))

#         # Commit the transaction
#         conn.commit()
#         print("Data inserted successfully")

#     except mysql.connector.Error as err:
#         if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#             print("Something is wrong with your user name or password")
#         elif err.errno == errorcode.ER_BAD_DB_ERROR:
#             print("Database does not exist")
#         else:
#             print(err)
#     else:
#         cursor.close()
#         conn.close()
# else:
#     # En cas d'erreur, afficher le statut
#     print(f'Error: {response.status_code}')

