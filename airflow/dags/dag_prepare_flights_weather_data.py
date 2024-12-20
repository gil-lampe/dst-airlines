from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
import pandas as pd
from dst_airlines.data import lufthansa_api_flights
from dst_airlines.data import open_meteo_api_weather_hourly
from dst_airlines.database import mongodb
from dst_airlines.database import mysql
from dst_airlines import utils
import logging
import sqlalchemy
import os

logger = logging.getLogger(__name__)

airport_iata = "FRA"

# Lufthansa
client_id=os.getenv("CLIENT_ID")
client_secret=os.getenv("CLIENT_SECRET")

mongodb_host = os.getenv("MONGODB_HOST")
mongodb_port = int(os.getenv("MONGODB_PORT"))
mongodb_username = os.getenv("MONGODB_USER")
mongodb_password = os.getenv("MONGODB_ROOT_PASSWORD")
mongodb_db_name = os.getenv("MONGODB_DATABASE")
collection_name = os.getenv("MONGODB_COLLECTION")

sql_user = os.getenv("MYSQL_USER")
sql_password = os.getenv("MYSQL_ROOT_PASSWORD")
sql_host = os.getenv("MYSQL_HOST")
sql_port = int(os.getenv("MYSQL_PORT"))
sql_database = os.getenv("MYSQL_DATABASE")

filtered_cols = [
     "Departure_AirportCode", 
    #  "Departure_ScheduledTimeLocal_DateTime", 
     "Departure_ScheduledTimeUTC_DateTime", 
    #  "Departure_ActualTimeLocal_DateTime", 
    #  "Departure_ActualTimeUTC_DateTime", 
    #  "Departure_TimeStatus_Code", 
    #  "Departure_TimeStatus_Definition", 
    #  "Departure_Terminal_Name", 
    #  "Departure_Terminal_Gate", 
     "Arrival_AirportCode", 
    #  "Arrival_ScheduledTimeLocal_DateTime", 
     "Arrival_ScheduledTimeUTC_DateTime", 
    #  "Arrival_ActualTimeLocal_DateTime", 
     "Arrival_ActualTimeUTC_DateTime", 
    #  "Arrival_TimeStatus_Code", 
    #  "Arrival_TimeStatus_Definition", 
    #  "Arrival_Terminal_Name", 
    #  "MarketingCarrier_AirlineID", 
    #  "MarketingCarrier_FlightNumber", 
    #  "OperatingCarrier_AirlineID", 
    #  "OperatingCarrier_FlightNumber", 
    #  "Equipment_AircraftCode", 
    #  "Equipment_AircraftRegistration", 
    #  "FlightStatus_Code", 
    #  "FlightStatus_Definition", 
    #  "ServiceType", 
    #  "Arrival_Terminal_Gate", 
    #  "Arrival_EstimatedTimeLocal_DateTime", 
    #  "Arrival_EstimatedTimeUTC_DateTime", 
    #  "Departure_EstimatedTimeLocal_DateTime", 
    #  "Departure_EstimatedTimeUTC_DateTime"
     ]


@dag(
    dag_id='dst_airlines_prepare_flights_weather_data',
    # schedule_interval=timedelta(minutes=1),
    tags=['DST-airlines', 'data preparation'],
    start_date=days_ago(0),
    catchup=False
)
def taskflow():
    """
    Define a DAG for fetching flight data, structuring it, and fetching weather forecasts for flights.
    The DAG consists of three tasks:
        1. Collect and store raw flights data in MongoDB.
        2. Structure and store flights data in MySQL.
        3. Collect and store weather data for the flights in MySQL.
    """
    @task()
    def collect_store_raw_flights_in_mongodb(prev_task=None):
        """
        Fetch flight data from the Lufthansa API and store it in MongoDB.

        Args:
            prev_task: Previous task dependency (optional).

        Returns:
            None
        """
        public_ip = utils.get_public_ip_address()

        api_token = utils.get_lh_api_token(client_id=client_id, client_secret=client_secret)
        print(api_token)
        headers = utils.build_lh_api_headers(api_token, public_ip)
        
        flights = lufthansa_api_flights.fetch_departing_flights_till_midnight(airport_iata=airport_iata, headers=headers)

        flights_collection = mongodb.get_collection_from_mongodb(mongodb_username, mongodb_password, collection_name = "FlightStatusResource", mongodb_db_name = mongodb_db_name, mongodb_host = mongodb_host, mongodb_port = mongodb_port)

        logger.info(f"Starting the storage of the data into the MongoDB collection.")
        mongodb.add_flight_dict(flights, flights_collection)
        logger.info(f"Storage of the data into the MongoDB collection finalized.")


    @task()
    def structure_store_flights_in_mysql(prev_task=None):
        """
        Retrieve raw flights data from MongoDB, structure it, and store it in a MySQL database.

        Args:
            prev_task: Previous task dependency (optional).

        Returns:
            None
        """
        flights_collection = mongodb.get_collection_from_mongodb(mongodb_username, mongodb_password, collection_name = "FlightStatusResource", mongodb_db_name = mongodb_db_name, mongodb_host = mongodb_host, mongodb_port = mongodb_port)

        raw_flights = flights_collection.find()

        structured_flights = lufthansa_api_flights.extract_flights_from_resources(raw_flights)

        flatten_flights = utils.flatten_list_of_dict(structured_flights)

        filtered_flights = flatten_flights[filtered_cols]

        table_name = "flights"
        mysql.upload_data_in_mysql(data=filtered_flights, table_name=table_name, sql_user=sql_user, sql_password=sql_password, sql_host=sql_host, sql_port=sql_port, sql_database=sql_database)


    @task()
    def collect_store_weather_in_mysql(prev_task=None):
        """
        Retrieve flight data from MySQL, fetch corresponding weather data from Open Meteo API, and store it in MySQL.

        Args:
            prev_task: Previous task dependency (optional).

        Returns:
            None
        """
        connection_string = f"mysql+pymysql://{sql_user}:{sql_password}@{sql_host}:{sql_port}/{sql_database}"
        engine = sqlalchemy.create_engine(connection_string)

        df_flights = pd.read_sql_table(table_name="flights", con=engine)
        df_airports = pd.read_sql_table(table_name="airports", con=engine)

        df_flights_coord = open_meteo_api_weather_hourly.prepare_flights_for_arrival_weather(df_flights=df_flights, df_airports=df_airports)
        df_weather_arr = open_meteo_api_weather_hourly.fetch_weather_data(
            df_flights_coord["Arrival_AirportCode"].tolist(),
            df_flights_coord["latitude_deg"].tolist(),
            df_flights_coord["longitude_deg"].tolist(),
            df_flights_coord["Arrival_ScheduledTimeUTC_DateTime"].tolist()
        )

        table_name = "weather_forecasts"
        mysql.upload_data_in_mysql(data=df_weather_arr, table_name=table_name, sql_user=sql_user, sql_password=sql_password, sql_host=sql_host, sql_port=sql_port, sql_database=sql_database)

    task1 = collect_store_raw_flights_in_mongodb()
    task2 = structure_store_flights_in_mysql(task1)
    task3 = collect_store_weather_in_mysql(task2)

taskflow()