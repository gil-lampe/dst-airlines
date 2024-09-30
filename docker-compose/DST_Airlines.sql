CREATE DATABASE IF NOT EXISTS DST_AIRLINES;
USE DST_AIRLINES;

CREATE TABLE IF NOT EXISTS Airports (
  -- id SERIAL PRIMARY KEY,                    -- ID auto-incrémenté comme clé primaire
  airport_code VARCHAR(3) NOT NULL UNIQUE,  -- Code IATA d'aéroport unique // sauf si on nettoie
  latitude FLOAT NOT NULL,       
  longitude FLOAT NOT NULL,       
  name VARCHAR(60) NOT NULL        
);

CREATE TABLE IF NOT EXISTS Flights (
  flight_id SERIAL PRIMARY KEY,                    -- ID de vol auto-incrémenté comme clé primaire
  departure_airport_code VARCHAR(3) NOT NULL,      -- Code IATA de l'aéroport de départ
  departure_scheduled_time_local TIMESTAMP,        -- Heure de départ prévue (locale)
  departure_scheduled_time_utc TIMESTAMP,          -- Heure de départ prévue (UTC)
  departure_actual_time_local TIMESTAMP,           -- Heure de départ réelle (locale)
  departure_actual_time_utc TIMESTAMP,             -- Heure de départ réelle (UTC)
  departure_time_status_code VARCHAR(2),           -- Code du statut du temps de départ (ex: "OT")
  departure_time_status_definition VARCHAR(30),    -- Définition du statut (ex: "Flight On Time")
  departure_terminal_name VARCHAR(10),             -- Nom du terminal de départ
  departure_terminal_gate VARCHAR(10),             -- Porte du terminal de départ
  arrival_airport_code VARCHAR(3) NOT NULL,        -- Code IATA de l'aéroport d'arrivée
  arrival_scheduled_time_local TIMESTAMP,          -- Heure d'arrivée prévue (locale)
  arrival_scheduled_time_utc TIMESTAMP,            -- Heure d'arrivée prévue (UTC)
  arrival_actual_time_local TIMESTAMP,             -- Heure d'arrivée réelle (locale)
  arrival_actual_time_utc TIMESTAMP,               -- Heure d'arrivée réelle (UTC)
  arrival_time_status_code VARCHAR(2),             -- Code du statut du temps d'arrivée (ex: "OT")
  arrival_time_status_definition VARCHAR(30),      -- Définition du statut (ex: "Flight On Time")
  arrival_terminal_name VARCHAR(10),               -- Nom du terminal d'arrivée
  marketing_carrier_airline_id VARCHAR(10),        -- ID de la compagnie aérienne commerciale
  marketing_carrier_flight_number VARCHAR(10),     -- Numéro de vol de la compagnie commerciale
  operating_carrier_airline_id VARCHAR(10),        -- ID de la compagnie aérienne opérante
  operating_carrier_flight_number VARCHAR(10),     -- Numéro de vol de la compagnie opérante
  equipment_aircraft_code VARCHAR(10),             -- Code de l'avion
  equipment_aircraft_registration VARCHAR(10),     -- Immatriculation de l'avion
  flight_status_code VARCHAR(2),                   -- Code de statut du vol (ex: "LD" pour landed)
  flight_status_definition VARCHAR(30),            -- Définition du statut du vol
  service_type VARCHAR(30),                        -- Type de service (ex: "Passenger")
  arrival_estimated_time_local TIMESTAMP,          -- Heure estimée d'arrivée (locale)
  arrival_estimated_time_utc TIMESTAMP,            -- Heure estimée d'arrivée (UTC)
  arrival_terminal_gate VARCHAR(10),               -- Porte du terminal d'arrivée
  departure_estimated_time_local TIMESTAMP,        -- Heure estimée de départ (locale)
  departure_estimated_time_utc TIMESTAMP           -- Heure estimée de départ (UTC)
);

CREATE TABLE IF NOT EXISTS Weather (
  weather_id SERIAL PRIMARY KEY,                  -- ID météo auto-incrémenté comme clé primaire
  date DATE NOT NULL,                             -- Date de la météo
  weather_code FLOAT NOT NULL,                    -- Code météo
  temperature_2m_max FLOAT NOT NULL,              -- Température maximale à 2m
  temperature_2m_min FLOAT NOT NULL,              -- Température minimale à 2m
  precipitation_sum FLOAT NOT NULL,               -- Somme des précipitations
  rain_sum FLOAT NOT NULL,                        -- Somme des pluies
  snowfall_sum FLOAT NOT NULL,                    -- Somme des chutes de neige
  precipitation_probability_max FLOAT NOT NULL,   -- Probabilité maximale de précipitations
  wind_speed_10m_max FLOAT NOT NULL,              -- Vitesse maximale du vent à 10m
  wind_gusts_10m_max FLOAT NOT NULL,              -- Rafales de vent maximales à 10m
  wind_direction_10m_dominant FLOAT NOT NULL,     -- Direction dominante du vent à 10m
  apparent_temperature_max FLOAT NOT NULL,        -- Température ressentie maximale
  apparent_temperature_min FLOAT NOT NULL,        -- Température ressentie minimale
  sunrise TIME NOT NULL,                          -- Heure du lever du soleil
  sunset TIME NOT NULL,                           -- Heure du coucher du soleil
  daylight_duration FLOAT NOT NULL,               -- Durée d'ensoleillement
  sunshine_duration FLOAT NOT NULL,               -- Durée totale d'ensoleillement
  uv_index_max FLOAT NOT NULL,                    -- Indice UV maximal
  uv_index_clear_sky_max FLOAT NOT NULL,          -- Indice UV maximal par ciel dégagé
  precipitation_hours FLOAT NOT NULL,             -- Heures de précipitations
  showers_sum FLOAT NOT NULL,                     -- Somme des averses
  shortwave_radiation_sum FLOAT NOT NULL,         -- Somme de la radiation à ondes courtes
  et0_fao_evapotranspiration FLOAT NOT NULL,      -- Evapotranspiration ET0 FAO
  arrival_airport_code VARCHAR(3) NOT NULL,       -- Code IATA de l'aéroport d'arrivée (référence à Airports)
  
  CONSTRAINT fk_arrival_airport FOREIGN KEY (arrival_airport_code) REFERENCES Airports(airport_code) -- Clé étrangère liée à la table Airports
);

CREATE TABLE IF NOT EXISTS Airlines (
  airline_id VARCHAR(3) PRIMARY KEY,               -- ID de la compagnie aérienne (AirlineID)
  airline_id_icao VARCHAR(3) NOT NULL,             -- Code ICAO de la compagnie aérienne (AirlineID_ICAO)
  name VARCHAR(100) NOT NULL                      -- Nom de la compagnie aérienne (ex: "Amber Air")
);

CREATE TABLE IF NOT EXISTS Aircrafts (
    AircraftCode INT PRIMARY KEY,          -- Code unique pour chaque type d'avion
    Names VARCHAR(100) NOT NULL,                   -- Détails du nom, stockés en format JSON
    AirlineEquipCode VARCHAR(10) NOT NULL  -- Code d'équipement de la compagnie aérienne
);

