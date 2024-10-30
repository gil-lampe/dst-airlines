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
    dag_id= "predict_delay", #'dst_airlines_predict_delay',
    # schedule_interval=timedelta(minutes=1),
    tags=['DST-airlines', 'prediction'],
    start_date=days_ago(0),
    catchup=False
)
def taskflow():
    """
    Tasklfow to: 
        1. xxx
    """

    @task()
    def get_flights(**kwargs):
            
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

        arrival_airport_iata = flight_df.loc[0, "Arrival_AirportCode"]
        departure_datetime_utc = flight_df.loc[0, "Arrival_ScheduledTimeUTC_DateTime"].replace("Z", "")

        airport_df = mysql.get_tables(["airports"], sql_user, sql_password, sql_host, sql_port, sql_database)[0]

        latitude, longitude = airports.get_coordinates(arrival_airport_iata, airport_df)

        weather_forecast_df = open_meteo_api_weather_hourly.fetch_weather_data([arrival_airport_iata], [latitude], [longitude], [departure_datetime_utc], manage_hourly_limit=False)

        return weather_forecast_df


    @task()
    def predict_delay(flight_df: pd.DataFrame, weather_forecast_df: pd.DataFrame, **kwargs):
        
        flights = prepare_data.preprare_features_from_flights(flight_df)

        flights_weather_forecasts = prepare_data.merge_flights_and_weather(flights, weather_forecast_df)
        features = prepare_data.select_feature_columns(flights_weather_forecasts)

        model = load('/opt/airflow/best_model.pickle')

        prediction = predict.generate_prediction(model, features)[0]

        logger.info(f'Delay predicted by the model: {prediction = }min.')
        
        ti = kwargs['ti']
        ti.xcom_push(key='prediction', value=prediction)

    task1_flights = get_flights()
    task2_weather_forecasts = get_weather_forecasts(task1_flights)
    task3 = predict_delay(task1_flights, task2_weather_forecasts)

taskflow()