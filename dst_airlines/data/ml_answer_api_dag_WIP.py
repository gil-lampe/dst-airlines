import os
from datetime import datetime
import pandas as pd
from joblib import load
import openmeteo_requests
import requests_cache
from retry_requests import retry
from typing import List
import logging
import time
import re
from typing import List, Dict
from openmeteo_requests.Client import OpenMeteoRequestsError

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

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

def get_weather_data(input_airportcode: str, input_date: str, airports_df: pd.DataFrame):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
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
    
    latitude, longitude = get_coordinates(input_airportcode, airports_df)
    if latitude is None or longitude is None:
        print(f"Coordonnées non trouvées pour l'aéroport {input_airportcode}")
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(variables),
        "timezone": "Europe/London",
        # "forecast_days": 1,
        # "past_days":1,
        "start_date": input_date.date().strftime('%Y-%m-%d'),
        "end_date": input_date.date().strftime('%Y-%m-%d'),
        "start_hour": input_date,
        "end_hour": input_date
    }   

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()

    hourly_data = {
        "Flight_DateTime": input_date,
        "Airport_Code": input_airportcode,
        "Latitude": latitude,
        "Longitude": longitude,
    }
    for i, var in enumerate(variables):
        hourly_data[var] = hourly.Variables(i).ValuesAsNumpy()
    
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    weather_data.append(hourly_dataframe)
    return hourly_dataframe

# TODO : date : scheduled-arrival-date, airportcode : arrival
def predict_delay(input_date: str, input_airportcode: str, airport_df: pd.DataFrame = None):
    
    if len(input_airportcode) != 3:
        raise ValueError("Attention, le code aéroport doit contenir 3 caractères.")
    
    if airport_df is None:
        airport_df = pd.read_csv("/app/clean_data/airport_names.csv")
    
    model_path = "/app/clean_data/best_model.pickle"
    model = load(model_path)
    
    input_date = pd.to_datetime(input_date).floor('h').tz_localize(None)
    weather_df = get_weather_data([input_airportcode], [input_date], airport_df)
    ###
    ### REFAIRE LE DF A L'IDENTIQUE QUE POUR LE TRAIN
    ###
    if weather_df.empty:
        raise ValueError(f'Pas de données météo disponibles pour l\'aéroport {input_airportcode}.')
    
    features_predict = weather_df.drop(columns=['Flight_DateTime', 'Latitude', 'Longitude'])
    prediction = model.predict(features_predict)
    return float(prediction[0])
    
    

