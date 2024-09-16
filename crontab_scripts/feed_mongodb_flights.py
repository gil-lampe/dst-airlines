from dst_airlines.database import mongodb
from dst_airlines import utils
from pymongo import MongoClient
import os

utils.setup_logging()

def main():
    CURRENT_USER = os.getenv("CURRENT_USER")

    USER_USERNAME = os.getenv(f"{CURRENT_USER}_USERNAME")
    USER_PASSWORD = os.getenv(f"{CURRENT_USER}_PASSWORD")

    client = MongoClient(
            host = "localhost",
            port = 27017,
            username = USER_USERNAME,
            password = USER_PASSWORD
        )

    mongodb.add_flights_data(client, db_name="DST-Airlines", collection_name="FlightStatusResource")

if __name__ == "__main__":
    main()