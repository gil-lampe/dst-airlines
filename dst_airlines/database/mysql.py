from sqlalchemy import create_engine, inspect
from logging import getLogger
import pandas as pd


logger = getLogger(__name__)


def get_tables(table_names: list[str], sql_user: str, sql_password: str, sql_host: str="localhost", sql_port: str="3306", sql_database: str="DST_AIRLINES") -> list[pd.DataFrame]:
    """Get tables based on the provided table_names list from the MySQL database whose connection details are provided

    Args:
        table_names (list[str]): Name of the tables to retrive from the MySQL database
        sql_user (str): Username to be used to connect to the MySQL database
        sql_password (str): Password
        sql_host (str, optional): MySQL host to use to connect. Defaults to "localhost".
        sql_port (str, optional): MySQL port to use to connect. Defaults to "3306".
        sql_database (str, optional): MySQL database name to which to connect. Defaults to "DST_AIRLINES".

    Returns:
        list[pd.DataFrame]: Collected dataframes from the MySQL database
    """
    logger.info(f"Initiating data download form the {table_names = }.")

    connection_string = f"mysql+pymysql://{sql_user}:{sql_password}@{sql_host}:{sql_port}/{sql_database}"
    engine = create_engine(connection_string)

    dataframes = [pd.read_sql_table(table_name=table_name, con=engine) for table_name in table_names]

    logger.info(f"Data download form the {table_names = } finalized.")
    return dataframes


def upload_data_in_mysql(data: pd.DataFrame | pd.Series, table_name: str, sql_user: str, sql_password: str, insert_existing_rows: bool=False, if_exists: str="append", sql_host: str="localhost", sql_port: str="3306", sql_database: str="DST_AIRLINES") -> None:
    """Upload provided data into the named table from the MySQL database whose detailed are provided, 
    will either add only new rows of the data into the table if it exists or create the table and insert data into it if it does not already exist

    Args:
        data (pd.DataFrame | pd.Series): Data to be inserted into the MySQL table
        table (str): Name of the MySQL table
        sql_user (str): Username to be used to connect to the MySQL database
    
        sql_password (str): Password
        insert_existing_row (bool, optional): Insert rows which already exist in the database (True / False). Defaults to "False" - i.e., already existing rows are not inserted.
        if_exists (str, optional): Method to use if the table already exists, see `DataFrame.to_sql()` for more details. Defaults to "append".
        sql_host (str, optional): MySQL host to use to connect. Defaults to "localhost".
        sql_port (str, optional): MySQL port to use to connect. Defaults to "3306".
        sql_database (str, optional): MySQL database name to which to connect. Defaults to "DST_AIRLINES".
    """
    logger.info(f"Initiating data upload into the the {table_name = }.")

    # Création de la connexion avec la base de données MySQL
    connection_string = f"mysql+pymysql://{sql_user}:{sql_password}@{sql_host}:{sql_port}/{sql_database}"
    engine = create_engine(connection_string)

    # Récupération du nom des tables, s'il y en a
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    # Conversion en DataFrame si les données sont de type Series
    if isinstance(data, pd.Series):
        new_data = data.to_frame('0')
        new_data.columns.astype(str)
    else:
        new_data = data.copy()

    # Si la table existe, ajout des nouvelles lignes uniquement, sinon création de la table et ajout des données
    if table_name in table_names:
        logger.info(f"{table_name = } is found in the {sql_database = }, appending new rows only into the table.")
        
        if not insert_existing_rows: 
            # Récupération des données existantes
            existing_data = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)

            # Sélection des nouvelles données à ajouter uniquement
            new_data = new_data.merge(existing_data, on=list(new_data.columns), how='left', indicator=True)
            new_data = new_data[new_data['_merge'] == 'left_only'].drop(columns=['_merge'])
        
    else:
        logger.info(f"{table_name = } not found in the {sql_database = }, creating the table {table_name = } and inserting data into it.")
    
    new_data_row_number = new_data.shape[0]
    number_rows_appended = new_data.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)

    logger.info(f"New rows inserted in the {table_name = }, ({number_rows_appended = } vs. {new_data_row_number = }).")