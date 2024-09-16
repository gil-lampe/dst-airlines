from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pymongo.collection import Collection
import os
import logging
from .. import utils
from typing import List
import json

logger = logging.getLogger(__name__)

def create_users(database_name: str, user_list: List[str], role: str = "readWrite") -> None:

    ROOT_USERNAME = os.getenv("MONGO_INITDB_ROOT_USERNAME")
    ROOT_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")

    client = MongoClient(
        host = "localhost",
        port = 27017,
        username = ROOT_USERNAME,
        password = ROOT_PASSWORD
    )

    db_admin = client["admin"]

    for user in user_list:
        user = user.upper()
        print(f"{user = }")
        user_username = os.getenv(f"{user}_USERNAME")
        user_password = os.getenv(f"{user}_PASSWORD")

        try:
            db_admin.command("createUser", user_username,
                             pwd = user_password,
                             roles = [{"role": role, "db": database_name}])
            logger.info(f"User '{user_username}' created in the database '{database_name}' with '{role}' role.")
        except OperationFailure as e:
            if e.code == 51003: # Code associé à l'erreur "l'utilisateur existe déjà"
                logger.info(f"User '{user_username}' already exist in the database '{database_name}'.")
            else:
                raise e


def add_flights_data(mongo_client: MongoClient, db_name: str, collection_name: str, force_test_all: bool=False):
    flights_db = mongo_client[db_name]

    if collection_name in flights_db.list_collection_names():
        flights_collection = flights_db[collection_name]
    else:
        flights_collection = flights_db.create_collection(collection_name)
        logger.info(f"'{collection_name}' collection created in '{db_name}' database.")

    raw_path = utils.build_data_storage_path("", data_stage="raw")
    raw_files = utils.get_files_in_folder(raw_path)
    flights_files = [flight_file for flight_file in raw_files if ("dep_flights" in flight_file and "OLD" not in flight_file)]

    existence_count = 0

    for flights_file in flights_files:
        flights_path = os.path.join(raw_path, flights_file)
        
        with open(flights_path, "r") as file:
            flights = json.load(file)

        flight_data = flights["data"]
        flight_status_resources = [data["FlightStatusResource"] for data in flight_data]

        for flight_status_resource in flight_status_resources:
            existence_test = flights_collection.find_one(flight_status_resource)

            if existence_test == None:
                flights_collection.insert_one(flight_status_resource)
            else:
                existence_count +=1
            
        if existence_count >= 5 and not force_test_all:
            logger.info(f"At least 5 documents of '{flights_path}' already exist in the '{collection_name}' collection of the '{db_name}' database, moved to the next file.")
            existence_count = 0
            continue
        
        logger.info(f"FlightStatusResource documents of '{flights_path}' added in the '{collection_name}' collection of the '{db_name}' database.")
