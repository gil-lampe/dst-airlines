from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from dst_airlines.data import airports
from dst_airlines.database import mysql
import logging


logger = logging.getLogger(__name__)

sql_user = ""
sql_password = ""

airport_file_path = ""

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
        mysql.upload_data_in_mysql(data=airports_df, table="airports", sql_user=sql_user, sql_password=sql_password)

    collect_structure_store_airports_in_mysql()

taskflow()





