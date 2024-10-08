from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from dst_airlines.data import airports
from dst_airlines.database import mysql
import logging
from sqlalchemy import create_engine, text
import os


logger = logging.getLogger(__name__)

sql_user = os.getenv("MYSQL_USER")
sql_password = os.getenv("MYSQL_ROOT_PASSWORD")
sql_host = os.getenv("MYSQL_HOST")
sql_port = int(os.getenv("MYSQL_PORT"))
sql_database = os.getenv("MYSQL_DATABASE")

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
        # Créez une connexion au serveur MySQL sans spécifier de base de données
        engine = create_engine(f'mysql+pymysql://{sql_user}:{sql_password}@{sql_host}:{sql_port}')

        # Ouvrez une session
        with engine.connect() as connection:
            # Vérifiez si la base de données existe déjà
            result = connection.execute(text(f"SHOW DATABASES LIKE '{sql_database}';"))
            exists = result.fetchone()

            # Si la base de données n'existe pas, créez-la
            if not exists:
                connection.execute(text(f"CREATE DATABASE {sql_database};"))
                logger.info(f"Database '{sql_database}' created successfully.")
            else:
                logger.info(f"Database '{sql_database}' already exists.")

        logger.info(f"Starting the collection and structuration of airport data from the {airport_file_path = }")
        airports_df = airports.generate_clean_airport_data(airport_file_path=airport_file_path)

        table_name = "airports"

        logger.info(f"Starting the insertion of airports data into the MySQL {table_name = }.")
        mysql.upload_data_in_mysql(data=airports_df, sql_database=sql_database, table=table_name, sql_user=sql_user, sql_password=sql_password, sql_host=sql_host, sql_port=sql_port)
        logger.info(f"Insertion of the airports data into {table_name = } finalized.")

    collect_structure_store_airports_in_mysql()

taskflow()





