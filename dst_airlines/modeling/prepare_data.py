from logging import getLogger
import pandas as pd


logger = getLogger(__name__)


def prepare_flights_for_training(flights: pd.DataFrame) -> pd.DataFrame:
    """Prepare Lufthansa flight data for model training by droping rows where information required for target computation is missing

    Args:
        flights (pd.DataFrame): flight data from the Lufhtansa API

    Returns:
        pd.DataFrame: Cleaned version of the flight data
    """
    logger.info(f"Starting the preparation of flights data for model training.")
    flights_clean = flights.copy()

    # Suppression des lignes de vols n'ayant pas d'heures d'arrivée prévue ou réelle
    flights_clean = flights_clean.dropna(how='any', subset=['Arrival_ActualTimeUTC_DateTime', 'Arrival_ScheduledTimeUTC_DateTime'])

    logger.info(f"Preparation of flights data for model training finalized.")
    return flights_clean


def preprare_features_from_flights(flights: pd.DataFrame) -> pd.DataFrame:
    """Prepare features from Lufthansa flight data by removing UTC time indicator ('Z') from dates (required for merging with weather forecasts)

    Args:
        flights (pd.DataFrame): flight data from the Lufhtansa API 

    Returns:
        pd.DataFrame: Cleaned version of the flight data
    """
    logger.info(f"Starting the flights data feature engineering.")
       
    flights_clean = flights.copy()

    datetime_cols = [
        "Departure_ScheduledTimeUTC_DateTime",
        "Arrival_ScheduledTimeUTC_DateTime"
    ]

    # Suppression du symbole UTC "Z" (toutes les dates sont en UTC et celles des prédictions météo n'ont pas cet élément)
    flights_clean[datetime_cols] = flights_clean[datetime_cols].map(lambda date: date.replace('Z', ''))

    logger.info(f"Flights data feature engineering finalized.")
    return flights_clean


def merge_flights_and_weather(cleaned_flights: pd.DataFrame, weather_forecasts: pd.DataFrame) -> pd.DataFrame:
    """Merge Lufthansa flights data with Open Meteo hourly weather forecasts

    Args:
        flights (pd.DataFrame): Cleaned version of Lufthansa flight data (dates should not have UTC marker 'Z' in it)
        weather_forecasts (pd.DataFrame): Open Meteo weather forecast data

    Returns:
        pd.DataFrame: Merged DataFrame with fligths and weather forecasts
    """
    logger.info(f"Starting the merging of flight and weather forecast data.")

    flights_weather_forecasts = pd.merge(cleaned_flights, weather_forecasts,
                                         left_on=['Arrival_AirportCode', 'Arrival_ScheduledTimeUTC_DateTime'],
                                         right_on=['Airport_Code', 'Flight_DateTime'],
                                         how="inner")

    logger.info(f"Merging of flight and weather forecast data finalized.")
    return flights_weather_forecasts


def select_feature_columns(flights_weather_forecasts: pd.DataFrame) -> pd.DataFrame:
    """Select the only important columns from the flight - weather forecast merged data

    Args:
        flights_weather_forecasts (pd.DataFrame): Flight - weather forecast merged data

    Returns:
        pd.DataFrame: Cleaned features
    """
    logger.info(f"Starting the selection of important columns from merged flight and weather forecast data.")
    
    feature_cols = [
        'temperature_2m',
        'relative_humidity_2m',
        'dew_point_2m',
        'apparent_temperature',
        'precipitation_probability',
        'precipitation',
        'rain',
        'showers',
        'snowfall',
        'snow_depth',
        'weather_code',
        'pressure_msl',
        'surface_pressure',
        'cloud_cover',
        'cloud_cover_low',
        'cloud_cover_mid',
        'cloud_cover_high',
        'visibility',
        'evapotranspiration',
        'et0_fao_evapotranspiration',
        'vapour_pressure_deficit',
        'wind_speed_10m',
        'wind_speed_80m',
        'wind_speed_120m',
        'wind_speed_180m',
        'wind_direction_10m',
        'wind_direction_80m',
        'wind_direction_120m',
        'wind_direction_180m',
        'wind_gusts_10m',
        'temperature_80m',
        'temperature_120m',
        'temperature_180m',
        'soil_temperature_0cm',
        'soil_temperature_6cm',
        'soil_temperature_18cm',
        'soil_temperature_54cm',
        'soil_moisture_0_to_1cm',
        'soil_moisture_1_to_3cm',
        'soil_moisture_3_to_9cm',
        'soil_moisture_9_to_27cm',
        'soil_moisture_27_to_81cm'
    ]
    
    features = flights_weather_forecasts[feature_cols]

    logger.info(f"Selection of important columns from merged flight and weather forecast data finalized.")
    return features


def compute_target_delay_in_minutes(flights_weather_forecasts: pd.DataFrame) -> pd.Series:
    """Compute the delay in minutes between two series of times (delay = actual_time - scheduled_time)

    Args:
        flights_weather_forecasts (pd.DataFrame): Lufthansa flights union-joined with weather forecasts

    Returns:
        pd.Series: Delay in minutes between the actual and the initially scheduled time
    """
    logger.info(f"Starting the computation of delay.")

    scheduled_time_dt = pd.to_datetime(flights_weather_forecasts["Arrival_ScheduledTimeUTC_DateTime"], utc=True)
    actual_time_dt = pd.to_datetime(flights_weather_forecasts["Arrival_ActualTimeUTC_DateTime"], utc=True)

    target = (actual_time_dt - scheduled_time_dt).dt.total_seconds() / 60

    logger.info(f"Computation of delay finalized.")
    return target