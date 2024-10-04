from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from dst_airlines.data import airports
from dst_airlines.database import mysql
from dst_airlines import utils
import logging
import os



logger = logging.getLogger(__name__)

utils.load_env_variables()



sql_user = "root"
sql_password = os.getenv("MYSQL_ROOT_PASSWORD")
sql_host = "mysql-db"
sql_port = "3306"

airport_file_path = "/app/raw_files/airport_names.csv"

@dag(
    dag_id='dst_airlines_prepare_airports_data',
    # schedule_interval=timedelta(minutes=1),
    tags=['DST-airlines', 'data preparation'],
    start_date=days_ago(0),
    catchup=False
)
def taskflow():
    @task()
    def collect_structure_store_airports_in_mysql():

        airports_df = airports.generate_clean_airport_data(airport_file_path=airport_file_path)

        table_name = "airports"

        logger.info(f"Starting the insertion of airports data into {table_name = }.")
        mysql.upload_data_in_mysql(data=airports_df, table=table_name, sql_user=sql_user, sql_password=sql_password, sql_host=sql_host, sql_port=sql_port)

    collect_structure_store_airports_in_mysql()

taskflow()





