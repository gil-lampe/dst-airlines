from FlightRadar24 import FlightRadar24API
from pprint import pprint
from datetime import datetime
import json

fr_api = FlightRadar24API()

# flights = fr_api.get_flights(airline="AFR")
# flight = flights[0]

# print(flight)

# flight_details = fr_api.get_flight_details(flight)
# flight.set_flight_details(flight_details)

# print("Flying to:", flight.destination_airport_name)

def get_airport_data(fr_api, code="", airport=None):
    if code == "" and airport is None:
        raise ValueError("One of the two inputs (code or airport) \
                         should be provided.")
    elif code != "" and airport is not None:
        raise ValueError("Only one of the two inputs (code or airport) \
                         should be provided.")
    
    if airport is None:
        target_airport = fr_api.get_airport(code=code)
    else:
        target_airport = airport

    airport_details = fr_api.get_airport_details(target_airport.icao)
    target_airport.set_airport_details(airport_details)

    # Get generic info
    airport_data = {}
    airport_data["name"] = target_airport.name
    airport_data["icao"] = target_airport.icao
    airport_data["iata"] = target_airport.iata
    airport_data["country"] = target_airport.country
    airport_data["country_code"] = target_airport.country_code
    airport_data["city"] = target_airport.city

    airport_data["timezone"] = {"name": target_airport.timezone_name,
                                "offset": target_airport.timezone_offset,
                                "offset_hours": target_airport.timezone_timezone_offset_hours,
                                "abbr": target_airport.timezone_abbr,
                                "abbr_name": target_airport.timezone_abbr_name}
    

    # Get weather info
    airport_data["weather"] = target_airport.weather

    # Get arrivals info
    airport_data["arrivals"] = {}
    airport_data["arrivals"]["total_items"] = target_airport.arrivals["item"]["total"]
    airport_data["arrivals"]["timestamp"] = target_airport.arrivals["timestamp"]
    airport_data["arrivals"]["data"] = []

    max_page = target_airport.arrivals["page"]["total"]
    i = 1
    while i <= max_page:
        airport_details = fr_api.get_airport_details(target_airport.icao, page=i)
        target_airport.set_airport_details(airport_details)
        airport_data["arrivals"]["data"].append(target_airport.arrivals) 

        max_page = target_airport.arrivals["page"]["total"]
        i += 1

    if len(airport_data["arrivals"]["data"]) != airport_data["arrivals"]["total_items"]:
        raise AssertionError("There is a difference between the number of flights retrieved and the number communicated by the API.")

    return airport_data

# airport_arrivals = get_airport_arrivals(fr_api, code="CDG")

# print(len(airport_arrivals["data"]))


# target_airport = fr_api.get_airport(code="CDG")

# pprint(target_airport.basic)


# for i in range(len(airport_arrivals)):
#     print(f"Time of the page n°{i + 1} is:", datetime.fromtimestamp(airport_arrivals[i]["timestamp"]))

# print("\n-Weather-\n")

# pprint(airport.weather)

# print("\n-Time:", datetime.fromtimestamp(airport.arrivals["timestamp"]))

# airport_file = "test_airport.json"

# flights_file = "test_flights.json"

# data_storage_path = "raw/"

# with open(data_storage_path + "disruptions.json", 'w') as file:
#     json.dump(fr_api.get_airport_disruptions(), file, indent=4, ensure_ascii=False)

# # with open(data_storage_path + airport_file, 'w') as file:
#     json.dump(airport_arrivals, file, indent=4, ensure_ascii=False)