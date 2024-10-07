import openmeteo_requests
from .. import utils
import requests_cache
import pandas as pd
from retry_requests import retry
import time

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

mois_jour = "09-04" # format MM-DD
date_traitement = f"2024-{mois_jour}"

flights_df = f"   {date_traitement}  "
airports_df = "     "

# flights_df = pd.read_csv(f"/home/sanou/DST-Airlines/data/2_interim/flights/FRA_dep_flights_{date_traitement}_conso_flatten.csv")
# airports_df = pd.read_csv("/home/sanou/DST-Airlines/data/4_external/airport_names.csv")

# ETL :
flights_df = flights_df.dropna(subset=['Arrival_ScheduledTimeUTC_DateTime'])
flights_df['Arrival_ScheduledTimeUTC_DateTime'] = flights_df['Arrival_ScheduledTimeUTC_DateTime'].str.replace('Z', '', regex=False)
flights_df['Arrival_ScheduledTimeUTC_DateTime'] = pd.to_datetime(flights_df['Arrival_ScheduledTimeUTC_DateTime'])

# Récupérer les coordonnées longitude/latitude des aéroports depuis airports_df
def get_coordinates(airport_code, airports_df):
    ''' Get latitude, longitude from airports_df with flights_df AirportCode
    
    Args :
    airport_code (str) : 3 letters to indicate which Airport is chosen
    airports_df (df) : /home/sanou/DST-Airlines/data/4_external/airport_names.csv
    
    Returns :
    latitude (float) : latitude of the Airport
    longitude (float) : longitude of the Airport
    '''
    airport = airports_df[airports_df['iata_code'] == airport_code]
    if not airport.empty:
        latitude = airport.iloc[0]['latitude_deg']
        longitude = airport.iloc[0]['longitude_deg']
        return latitude, longitude
    else:
        return None, None

# Variables = éléments de l'api Meteo que l'on veut mettre en avant dans notre database
variables = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability",
             "precipitation", "rain", "showers", "snowfall", "snow_depth", "weather_code", "pressure_msl", "surface_pressure", 
             "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", 
             "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", 
             "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", 
             "wind_gusts_10m", "temperature_80m", "temperature_120m", "temperature_180m", "soil_temperature_0cm", 
             "soil_temperature_6cm", "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", 
             "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm"]

weather_data = []
url = "https://api.open-meteo.com/v1/forecast"

request_count = 0

# Boucle FOR pour traiter chaques vols présents dans le fichier flights_df
for index, flight in flights_df.iterrows():
    # Les arguments pour l'API
    arrival_airport_code = flight['Arrival_AirportCode']
    date_time = flight["Arrival_ScheduledTimeUTC_DateTime"]
    latitude, longitude = get_coordinates(arrival_airport_code, airports_df)
    
    start_time = date_time.strftime('%Y-%m-%dT%H:%M')
    # end_time = (date_time + pd.Timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M')
    
    if latitude is None or longitude is None:
        print(f"Coordonnées non trouvées pour l'aéroport {arrival_airport_code}")
        continue
    
    # Les paramètres de la requête API
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(variables),
        "timezone": "Europe/London",
        # "forecast_days": 1,
        # "past_days":1,
        "start_date": date_time.date().strftime('%Y-%m-%d'),
        "end_date": date_time.date().strftime('%Y-%m-%d'),
        "start_hour": start_time,
        "end_hour": start_time
    }
    
    responses = openmeteo.weather_api(url, params=params)
    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {
        "Flight_DateTime": flight['Arrival_ScheduledTimeUTC_DateTime'],
        "Airport_Code": arrival_airport_code,
        "Latitude": latitude,
        "Longitude": longitude,
    }
    # Ajout de chaque variables dans le weather_df
    for i, var in enumerate(variables):
        hourly_data[var] = hourly.Variables(i).ValuesAsNumpy()
    
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    weather_data.append(hourly_dataframe)
    
    request_count += 1
    
    # Compteur de requêtes bloquant pour éviter les erreurs de scripts
    if request_count %150 == 0:
        print(f"Processed {request_count} requests. Pausing for 60 seconds.")
        for remaining in range(60, 0, -10):
            print(f"{remaining} secondes avant reprise du script.")
            time.sleep(10)
        time.sleep(remaining)

# Récupération du nouveau fichier .csv   
if weather_data:
    final_df = pd.concat(weather_data, ignore_index=True)
    final_df.to_csv(f'/home/sanou/DST-Airlines/data/2_interim/weather_hourly/AA_hourly_weather_{date_traitement}.csv', index=False)
    print(f'CSV enregistré dans /home/sanou/DST-Airlines/data/2_interim/weather_hourly/AA_hourly_weather_{date_traitement}.csv')
else:
    print("Aucune donnée n'a été récupérée.")