import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# list_jour=[25,26,27,28]
mois_jour = "08-29" # format MM-DD
date_traitement = f"2024-{mois_jour}"

flights_df = pd.read_csv(f"/home/sanou/DST-Airlines/data/2_interim/FRA_dep_flights_{date_traitement}_conso_flatten.csv")
airports_df = pd.read_csv("/home/sanou/DST-Airlines/data/4_external/airport_names.csv")

# ETL :
# J'arrondis les valeurs latitude & longitude du DF airports à 2 chiffres pour rentrer dans l'API
# airports_df['latitude_deg'] = airports_df['latitude_deg'].round(2)
# airports_df['longitude_deg'] = airports_df['longitude_deg'].round(2)

# Suppression des NaN sur Arrival_ActualTimeUTC_DateTime
print("\n\nLes valeurs NaN de notre flights_df :\n")
print(flights_df.isna().sum())
flights_df = flights_df.dropna(subset=['Arrival_ActualTimeUTC_DateTime'])

print("\nSuppression des valeurs 'Arrival = NaN'\n")

# Récupérer les coordonnées longitude/latitude des aéroports depuis airports_df
def get_coordinates(airport_code, airports_df):
    airport = airports_df[airports_df['iata_code'] == airport_code]
    if not airport.empty:
        latitude = airport.iloc[0]['latitude_deg']
        longitude = airport.iloc[0]['longitude_deg']
        return latitude, longitude
    else:
        return None, None

# Je convertis la date flights en 'YYYY-MM-DD'
flights_df['Arrival_ActualTimeUTC_Date'] = pd.to_datetime(flights_df['Arrival_ActualTimeUTC_DateTime']).dt.date

weather_results = []
url = "https://api.open-meteo.com/v1/forecast"

print("\nRécupération des données en cours...\n")

for _, flight in flights_df.iterrows():
    arrival_airport_code = flight['Arrival_AirportCode']
    arrival_date = pd.to_datetime(flight['Arrival_ActualTimeUTC_DateTime']).date()
    latitude, longitude = get_coordinates(arrival_airport_code, airports_df)
    
    if latitude is not None and longitude is not None:
        formatted_date = arrival_date.strftime('%Y-%m-%d')
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max", "apparent_temperature_min", "sunrise", "sunset", "daylight_duration", "sunshine_duration", "uv_index_max", "uv_index_clear_sky_max", "precipitation_sum", "rain_sum", "showers_sum", "snowfall_sum", "precipitation_hours", "precipitation_probability_max", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration"],
	        "timezone": "Europe/London",
            "start_date": formatted_date,
            "end_date": formatted_date
        }
        
        try:
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]
            
            daily = response.Daily()
            daily_data = {
                "date": [formatted_date],  # Utilisation de la date d'arrivée pour toutes les données
                "weather_code": daily.Variables(0).ValuesAsNumpy(),
                "temperature_2m_max": daily.Variables(1).ValuesAsNumpy(),
                "temperature_2m_min": daily.Variables(2).ValuesAsNumpy(),
                "precipitation_sum": daily.Variables(3).ValuesAsNumpy(),
                "rain_sum": daily.Variables(4).ValuesAsNumpy(),
                "snowfall_sum": daily.Variables(5).ValuesAsNumpy(),
                "precipitation_probability_max": daily.Variables(6).ValuesAsNumpy(),
                "wind_speed_10m_max": daily.Variables(7).ValuesAsNumpy(),
                "wind_gusts_10m_max": daily.Variables(8).ValuesAsNumpy(),
                "wind_direction_10m_dominant": daily.Variables(9).ValuesAsNumpy(),
                "apparent_temperature_max": daily.Variables(3).ValuesAsNumpy(),
                "apparent_temperature_min": daily.Variables(4).ValuesAsNumpy(),
                "sunrise": daily.Variables(5).ValuesAsNumpy(),
                "sunset": daily.Variables(6).ValuesAsNumpy(),
                "daylight_duration": daily.Variables(7).ValuesAsNumpy(),
                "sunshine_duration": daily.Variables(8).ValuesAsNumpy(),
                "uv_index_max": daily.Variables(9).ValuesAsNumpy(),
                "uv_index_clear_sky_max": daily.Variables(10).ValuesAsNumpy(),
                "precipitation_hours": daily.Variables(15).ValuesAsNumpy(),
                "showers_sum": daily.Variables(13).ValuesAsNumpy(),
                "shortwave_radiation_sum": daily.Variables(20).ValuesAsNumpy(),
                "et0_fao_evapotranspiration": daily.Variables(21).ValuesAsNumpy(),
                "arrival_airport_code": arrival_airport_code                      
            }
            weather_results.append(pd.DataFrame(data=daily_data))
        except Exception as e:
            print(f"\nYA UN BUG POUR CELUI LA : {arrival_airport_code}. Error: {e}\n")
        
                ## ANCIENNE VERSION :
            #     responses = openmeteo.weather_api(url, params=params)
            #     response = responses[0]
                
            #     daily = response.Daily()
            #     daily_data = {
            #         # "date": pd.date_range(
            #         #     start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            #         #     end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            #         #     freq=pd.Timedelta(seconds=daily.Interval()),
            #         #     inclusive="left"
            #         # ),
            #         "date": [formatted_date],
            #         "weather_code": daily.Variables(0).ValuesAsNumpy(),
            #         "temperature_2m_max": daily.Variables(1).ValuesAsNumpy(),
            #         "temperature_2m_min": daily.Variables(2).ValuesAsNumpy(),
            #         "precipitation_sum": daily.Variables(3).ValuesAsNumpy(),
            #         "rain_sum": daily.Variables(4).ValuesAsNumpy(),
            #         "snowfall_sum": daily.Variables(5).ValuesAsNumpy(),
            #         "precipitation_probability_max": daily.Variables(6).ValuesAsNumpy(),
            #         "wind_speed_10m_max": daily.Variables(7).ValuesAsNumpy(),
            #         "wind_gusts_10m_max": daily.Variables(8).ValuesAsNumpy(),
            #         "wind_direction_10m_dominant": daily.Variables(9).ValuesAsNumpy(),
            #         "arrival_airport_code": arrival_airport_code
            #     }
            #     weather_results.append(pd.DataFrame(data = daily_data))
    
    
    else:
        print(f"\nCA A PAS MARCHE POUR L'AEROPORT {arrival_airport_code}\n")

#############
### ENREGISTREMENT FICHIER DYNAMIQUE
#############
weather_df = pd.concat(weather_results, ignore_index=True)
print(weather_df.head())
weather_df.to_csv(f"/home/sanou/DST-Airlines/data/1_raw/weather_test_{date_traitement}.csv")

print("\nEnregistrement des données weather_CSV terminé.")



