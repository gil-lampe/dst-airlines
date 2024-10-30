from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

import pandas as pd
from joblib import load  # Attention : utilise joblib pour charger un modèle pickled
from logging import getLogger
import os

from dst_airlines.data import lufthansa_api_flights, open_meteo_api_weather_hourly, airports
from dst_airlines.database import mysql
from dst_airlines import utils
from dst_airlines.modeling import prepare_data, predict


logger = getLogger(__name__)

sql_user = os.getenv("MYSQL_USER")
sql_password = os.getenv("MYSQL_ROOT_PASSWORD")
sql_host = os.getenv("MYSQL_HOST")
sql_port = int(os.getenv("MYSQL_PORT"))
sql_database = os.getenv("MYSQL_DATABASE")

client_id=os.getenv("CLIENT_ID")
client_secret=os.getenv("CLIENT_SECRET")


@dag(
    dag_id= 'dst_airlines_predict_delay',
    # schedule_interval=timedelta(minutes=1),
    tags=['DST-airlines', 'prediction'],
    start_date=days_ago(0),
    catchup=False
)
def taskflow():
    """
    Taskflow to: 
        1. Retrieve flight information from the Lufthansa API based on user input.
        2. Fetch weather forecasts for the flight's arrival airport.
        3. Predict potential flight delays using the prepared data from flights and weather forecasts.
    """
    @task()
    def get_flights(**kwargs):
        """
        Retrieve flight information from the Lufthansa API based on user input parameters.
        
        The function extracts the arrival airport IATA code and scheduled departure time 
        from the DAG run's configuration. It then builds an API request to fetch current 
        flight data and structures the received flight information into a DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing the structured flight information.
        """
        conf = kwargs.get('dag_run').conf
        arrival_airport_iata = conf.get('arrival_iata_code')
        departure_datetime_local = conf.get('scheduled_departure_local_time')

        departure_date_local = departure_datetime_local[0:10]

        public_ip = utils.get_public_ip_address()

        api_token = utils.get_lh_api_token()

        headers = utils.build_lh_api_headers(api_token, public_ip)

        url = lufthansa_api_flights.build_flight_url(departure_date_local=departure_date_local, departure_airport_iata = "FRA", arrival_airport_iata=arrival_airport_iata)

        flights = lufthansa_api_flights.fetch_flights(url, headers)

        logger.info(f"{flights}")

        flights_structured = lufthansa_api_flights.structure_flights(flights, "flightstatus")

        logger.info(f"{flights_structured}")

        flight = lufthansa_api_flights.get_flight_via_departing_time(flights_structured, departure_datetime_local)

        logger.info(f"{flight}")

        flight_df = pd.DataFrame([utils.flatten(flight)])

        return flight_df


    @task()
    def get_weather_forecasts(flight_df: pd.DataFrame):
        """
        Fetch weather forecasts for the flight's arrival airport.
        
        The function extracts the arrival airport IATA code and the scheduled arrival time 
        from the provided flight DataFrame. It retrieves the airport's geographical coordinates 
        from the database and then requests hourly weather data using the Open Meteo API.

        Args:
            flight_df (pd.DataFrame): DataFrame containing flight information.

        Returns:
            pd.DataFrame: A DataFrame containing the weather forecast data for the arrival airport.
        """
        arrival_airport_iata = flight_df.loc[0, "Arrival_AirportCode"]
        departure_datetime_utc = flight_df.loc[0, "Arrival_ScheduledTimeUTC_DateTime"].replace("Z", "")

        airport_df = mysql.get_tables(["airports"], sql_user, sql_password, sql_host, sql_port, sql_database)[0]

        latitude, longitude = airports.get_coordinates(arrival_airport_iata, airport_df)

        weather_forecast_df = open_meteo_api_weather_hourly.fetch_weather_data([arrival_airport_iata], [latitude], [longitude], [departure_datetime_utc], manage_hourly_limit=False)

        return weather_forecast_df


    @task()
    def predict_delay(flight_df: pd.DataFrame, weather_forecast_df: pd.DataFrame, **kwargs):
        """
        Predict potential flight delays using the prepared flight and weather data.
        
        This function prepares the features from the flight DataFrame and merges them 
        with the weather forecasts. It loads a pre-trained machine learning model and 
        generates a delay prediction based on the prepared features. The predicted 
        delay is then pushed to XCom for further use.

        Args:
            flight_df (pd.DataFrame): DataFrame containing structured flight information.
            weather_forecast_df (pd.DataFrame): DataFrame containing weather forecast data.
        """       
        flights = prepare_data.preprare_features_from_flights(flight_df)

        flights_weather_forecasts = prepare_data.merge_flights_and_weather(flights, weather_forecast_df)
        features = prepare_data.select_feature_columns(flights_weather_forecasts)

        model = load('/opt/airflow/best_model.pickle')

        prediction = predict.generate_prediction(model, features)

        logger.info(f'Delay predicted by the model: {prediction = }min.')
        
        ti = kwargs['ti']
        ti.xcom_push(key='prediction', value=prediction)

    task1_flights = get_flights()
    task2_weather_forecasts = get_weather_forecasts(task1_flights)
    task3 = predict_delay(task1_flights, task2_weather_forecasts)

taskflow()