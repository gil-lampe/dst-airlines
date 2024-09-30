import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from typing import List
import logging
import os
import time
import json
from dst_airlines import utils
import re
from typing import List, Dict
from openmeteo_requests.Client import OpenMeteoRequestsError

URL = os.getenv("URL_API_OPEN_METEO")

logger = logging.getLogger(__name__)

# Récupérer les coordonnées longitude/latitude des aéroports depuis airports_df
# def get_coordinates(airport_code, airports_df):
#     ''' Get latitude, longitude from airports_df with flights_df AirportCode
    
#     Args :
#     airport_code (str) : 3 letters to indicate which Airport is chosen
#     airports_df (df) : /home/sanou/DST-Airlines/data/4_external/airport_names.csv
    
#     Returns :
#     latitude (float) : latitude of the Airport
#     longitude (float) : longitude of the Airport
#     '''
#     airport = airports_df[airports_df['iata_code'] == airport_code]
#     if not airport.empty:
#         latitude = airport.iloc[0]['latitude_deg']
#         longitude = airport.iloc[0]['longitude_deg']
#         return latitude, longitude
#     else:
#         return None, None


def fetch_weather_data(airport_codes: List[str], latitudes: List[str], longitudes: List[str], times: List[str]):
    
    if (len(airport_codes) != len(latitudes)) or (len(airport_codes) != len(times)) or (len(airport_codes) != len(longitudes)):
        logger.error(error_message := f"Lengths of provided lists are not equals : {len(airport_codes) = } | {len(latitudes) = } | {len(longitudes) = } | {len(times) = }")
        raise ValueError(error_message)
    
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Variables = éléments de l'api Meteo que l'on veut mettre en avant dans notre database
    variables = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability",
                "precipitation", "rain", "showers", "snowfall", "snow_depth", "weather_code", "pressure_msl", "surface_pressure", 
                "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", 
                "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", 
                "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", 
                "wind_gusts_10m", "temperature_80m", "temperature_120m", "temperature_180m", "soil_temperature_0cm", 
                "soil_temperature_6cm", "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", 
                "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm"]

    hourly_data = {"Flight_DateTime": times,
                   "Airport_Code": airport_codes,
                   "Latitude": latitudes,
                   "Longitude": longitudes}

    for var in variables:
        hourly_data[var] = []

    # Les paramètres de la requête API

    timezones = ["GMT" for i in range(len(times))]

    # TODO: réduire la taille des lat & log pour pouvoir envoyer plus de coordonnées
    page_length = 50 # Nombre maximum de coordonnées qu'on peut donner avant d'atteindre la taille limite de l'URL
    page_nb = len(times) // page_length + 1

    for page in range(page_nb):

        page_start = page * page_length
        page_end = (page + 1) * page_length
        logger.debug(f"{page_start = } - {page_end = }")

        latitudes_page = latitudes[page_start:page_end]
        longitudes_page = longitudes[page_start:page_end]
        timezones_page = timezones[page_start:page_end]
        times_page = times[page_start:page_end]

        params = {"latitude": latitudes_page,
                "longitude": longitudes_page,
                "hourly": ",".join(variables),
                "timezone": timezones_page,
                "start_hour": times_page,
                "end_hour": times_page}
        
        try:
            responses = openmeteo.weather_api(URL, params=params)
        except Exception as e:
            logger.error(f"Error in open-meteo API query: {e}")
            if isinstance(e, OpenMeteoRequestsError):
                str_err = str(e).replace("'", '"').replace("True", "true").replace("False", "false")
                error_reason = json.loads(str_err)["reason"]    
                if error_reason == "Minutely API request limit exceeded. Please try again in one minute.":
                    logger.info("Minutely API request limit exceeded, sleep for one minute.")
                    time.sleep(60)
                    responses = openmeteo.weather_api(URL, params=params)
                elif error_reason == "Hourly API request limit exceeded. Please try again in the next hour.":
                    logger.info("Minutely API request limit exceeded, sleep for one minute.")
                    time.sleep(3600)
                    responses = openmeteo.weather_api(URL, params=params)
                else:
                    raise e
            else:
                raise e


        logger.debug(f"{len(times_page) = } vs. {len(responses) = }")

        for response in responses:
            hourly = response.Hourly()

            for j, var in enumerate(variables):
                hourly_data[var].append(hourly.Variables(j).ValuesAsNumpy().item())
    
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    return hourly_dataframe


def download_weather_data_for_existing_flights() -> None:
    """Generate weather data for all existing flights files that do not already exist
    For this method to work: 
    - a flight files must be stored in "data/2_interim/flights" of the project root
    - a folder "data/2_interim/weather_hourly" must exist
    - existing weather data must be stored in the previously mentionned "weather_hourly" folder
    - flights files and weather files must have a "YYYY-MM-DD"-formatted date in its name, e.g., "AA_hourly_weather_2024-09-01.csv" 
    """
    def create_dict_file_date(files: List[str]) -> Dict[str, str]:
        """create a dictionary from a list of strings containing a YYYY-MM-DD formatted date in it with the extracted date as a key and the string as a value

        Args:
            files (List[str]): List of file names containing a YYYY-MM-DD formatted date in them

        Returns:
            Dict[str, str]: Dictionary with the extracted date as key and the full string (name of the file) as value
        """
        date_pattern = r"\d{4}-\d{2}-\d{2}"
        dict_date_file = {}

        for file in files:
            match = re.search(date_pattern, file)
            if match:
                date = match.group()
                dict_date_file[date] = file
        return dict_date_file

    # Récupération de tous les fichiers CSV de données de vols 
    flights_folder = utils.build_data_storage_path(file_name="", data_stage="interim", folder="flights")
    flights_files = utils.get_files_in_folder(flights_folder)
    flights_files_csv = [file for file in flights_files if ".csv" in file]

    # Récupération de tous les fichiers de météo
    weather_folder = utils.build_data_storage_path(file_name="", data_stage="interim", folder="weather_hourly")
    weather_files = utils.get_files_in_folder(weather_folder)

    # Génération de dictionnaires clé = date / valeur = nom du fichier 
    flights_date_file = create_dict_file_date(flights_files_csv)
    weather_date_file = create_dict_file_date(weather_files)

    # Récupération du fichier contenant les coordonnées des aéroports
    airport_data_path = utils.build_data_storage_path("airport_names.csv", "external", "")
    df_airports = pd.read_csv(airport_data_path)

    # Pour chaque fichiers de données de vols...
    for date, file in flights_date_file.items():
        # ... Vérification qu'il n'existe pas déjà un fichier météo associé
        if date not in weather_date_file:
            # Récupération des données de vols
            flight_data_path = utils.build_data_storage_path(file, "interim", "flights")
            df_flights = pd.read_csv(flight_data_path)

            # Jointure avec les données contenant les coordonnées des aéroports, formatage des dates, suppression des vols vers des aéroports sans coordonnée
            df_flights_coord = df_flights.merge(right=df_airports[["iata_code", "latitude_deg", "longitude_deg"]], left_on="Arrival_AirportCode", right_on="iata_code", how="left")
            df_flights_coord["Arrival_ScheduledTimeUTC_DateTime"] = pd.to_datetime(df_flights_coord["Arrival_ScheduledTimeUTC_DateTime"])
            df_flights_coord["Arrival_ScheduledTimeUTC_DateTime"] = df_flights_coord["Arrival_ScheduledTimeUTC_DateTime"].apply(lambda row: row.strftime("%Y-%m-%dT%H:%M")) 
            df_flights_coord = df_flights_coord.dropna(axis=0, how="any", subset=["Arrival_AirportCode", "latitude_deg", "longitude_deg", "Arrival_ScheduledTimeUTC_DateTime"])
            
            # Collecte des données de météo
            logger.info(f"Generating {file = } for the {date = }.")
            df_weather = fetch_weather_data(df_flights_coord["Arrival_AirportCode"].tolist(),
                                            df_flights_coord["latitude_deg"].tolist(),
                                            df_flights_coord["longitude_deg"].tolist(),
                                            df_flights_coord["Arrival_ScheduledTimeUTC_DateTime"].tolist())
            logger.info(f"Generation of the {file = } finalized.")
            
            # Enregistrement des données météo
            weather_file_name = f"AA_hourly_weather_{date}.csv"
            weather_path = os.path.join(weather_folder, weather_file_name)
            df_weather.to_csv(weather_path, index=False)