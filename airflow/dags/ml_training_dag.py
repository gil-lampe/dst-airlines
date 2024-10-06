import requests
import json
import os
import pandas as pd
import pymysql
import sqlalchemy
from datetime import datetime

from sklearn.impute import SimpleImputer
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
  
def prepair_data_to_ml():
    """
    Prépare les données en effectuant un merge entre les vols et la météo,
    puis nettoie et encode les variables catégorielles.
    """
    sql_user = "root"
    sql_password = "rootpassword123"
    sql_host = "mysql-db"
    sql_port = "3306"
    sql_database = "DST_AIRLINES"

    connection_string = f"mysql+pymysql://{sql_user}:{sql_password}@{sql_host}:{sql_port}/{sql_database}"
    engine = sqlalchemy.create_engine(connection_string)

    flights_df = pd.read_sql_table(table_name="flights", con=engine)
    weather_df = pd.read_sql_table(table_name="weather_forecasts", con=engine)

    cols_to_drop = [
        'Departure_ScheduledTimeLocal_DateTime',
        # 'Departure_ScheduledTimeUTC_DateTime',
        'Departure_ActualTimeLocal_DateTime',
        'Departure_ActualTimeUTC_DateTime',
        # 'Departure_TimeStatus_Code', ##
        'Departure_TimeStatus_Definition',
        'Arrival_ScheduledTimeLocal_DateTime',
        # 'Arrival_ScheduledTimeUTC_DateTime',
        'Arrival_ActualTimeLocal_DateTime',
        # 'Arrival_ActualTimeUTC_DateTime',
        'Arrival_EstimatedTimeLocal_DateTime',
        'Arrival_EstimatedTimeUTC_DateTime',
        # 'Departure_EstimatedTimeLocal_DateTime', ##
        # 'Departure_EstimatedTimeUTC_DateTime', ##
        # 'Flight_DateTime',
        # 'Flight_DateTime_Hour',
        'Departure_Terminal_Name',
        'Departure_Terminal_Gate',
        'Arrival_Terminal_Name',
        'Arrival_Terminal_Gate',
        'ServiceType',
        'Departure_AirportCode',
        # 'Arrival_AirportCode',
        'MarketingCarrier_AirlineID',
        'MarketingCarrier_FlightNumber',
        'OperatingCarrier_AirlineID',
        'OperatingCarrier_FlightNumber',
        'Equipment_AircraftCode',
        'Equipment_AircraftRegistration',
        'FlightStatus_Code',
        # 'Airport_Code',
        # 'Latitude',
        # 'Longitude',
        # Valeurs status = inutiles car nous cherchons à déterminer le retard, chiffré
        'FlightStatus_Definition',
        'Arrival_TimeStatus_Definition',
        'FlightStatus_Definition'
    ]

    flights_df = flights_df.drop(cols_to_drop, axis=1)
    flights_df = flights_df.dropna(subset=['Arrival_ActualTimeUTC_DateTime'])

    ### ETL flights_df
    # Convertir en format datetime avec fuseau horaire (UTC si les données sont en UTC)
    flights_df['Arrival_ScheduledTimeUTC_DateTime'] = pd.to_datetime(flights_df['Arrival_ScheduledTimeUTC_DateTime'], utc=True)
    flights_df['Arrival_ActualTimeUTC_DateTime'] = pd.to_datetime(flights_df['Arrival_ActualTimeUTC_DateTime'], utc=True)
    # Calculer le délai avant toute modification de format de date
    flights_df['Delay_minutes'] = (flights_df['Arrival_ActualTimeUTC_DateTime'] - flights_df['Arrival_ScheduledTimeUTC_DateTime']).dt.total_seconds() / 60
    # Convertir ensuite les dates au format souhaité YYYY-mm-ddTHH-MM
    flights_df['Arrival_ScheduledTimeUTC_DateTime'] = flights_df['Arrival_ScheduledTimeUTC_DateTime'].dt.strftime('%Y-%m-%dT%H')#-%M')
    flights_df['Arrival_ActualTimeUTC_DateTime'] = flights_df['Arrival_ActualTimeUTC_DateTime'].dt.strftime('%Y-%m-%dT%H')#-%M')

    ### ETL weather_df
    # Convertir en format datetime et appliquer le fuseau horaire UTC
    weather_df['Flight_DateTime'] = pd.to_datetime(weather_df['Flight_DateTime']).dt.tz_localize('UTC')
    # Convertir au format souhaité YYYY-mm-ddTHH-MM
    weather_df['Flight_DateTime'] = weather_df['Flight_DateTime'].dt.strftime('%Y-%m-%dT%H')#:%MZ')

    ### MERGE
    flights_df = flights_df.rename(str, axis="columns")
    weather_df = weather_df.rename(str, axis="columns")
    df = pd.merge(flights_df, weather_df,
                    left_on=['Arrival_AirportCode', 'Arrival_ScheduledTimeUTC_DateTime'],
                    right_on=['Airport_Code', 'Flight_DateTime'],
                    how="left")

    new_cols_drop = [
        'Departure_ScheduledTimeUTC_DateTime',
        'Departure_TimeStatus_Code',
        'Arrival_AirportCode',
        'Arrival_ScheduledTimeUTC_DateTime',
        'Arrival_ActualTimeUTC_DateTime',
        'Arrival_TimeStatus_Code',
        # 'Delay_minutes',
        'Flight_DateTime',
        'Airport_Code',
        'Latitude',
        'Longitude',
        # 'temperature_2m',
        # 'relative_humidity_2m',
        # 'dew_point_2m',
        # 'apparent_temperature',
        # 'precipitation_probability',
        # 'precipitation',
        # 'rain',
        # 'showers',
        # 'snowfall',
        # 'snow_depth',
        # 'weather_code',
        # 'pressure_msl',
        # 'surface_pressure',
        # 'cloud_cover',
        # 'cloud_cover_low',
        # 'cloud_cover_mid',
        # 'cloud_cover_high',
        # 'visibility',
        # 'evapotranspiration',
        # 'et0_fao_evapotranspiration',
        # 'vapour_pressure_deficit',
        # 'wind_speed_10m',
        # 'wind_speed_80m',
        # 'wind_speed_120m',
        # 'wind_speed_180m',
        # 'wind_direction_10m',
        # 'wind_direction_80m',
        # 'wind_direction_120m',
        # 'wind_direction_180m',
        # 'wind_gusts_10m',
        # 'temperature_80m',
        # 'temperature_120m',
        # 'temperature_180m',
        # 'soil_temperature_0cm',
        # 'soil_temperature_6cm',
        # 'soil_temperature_18cm',
        # 'soil_temperature_54cm',
        # 'soil_moisture_0_to_1cm',
        # 'soil_moisture_1_to_3cm',
        # 'soil_moisture_3_to_9cm',
        # 'soil_moisture_9_to_27cm',
        # 'soil_moisture_27_to_81cm'
    ]

    df = df.drop(columns=new_cols_drop, axis=1)
    df = df.drop_duplicates(subset=['Delay_minutes', 'temperature_2m'])
    # df = df.dropna(subset=['temperature_2m'])

    df = pd.get_dummies(df)

    df = df.dropna()

    features = df.drop(['Delay_minutes'], axis=1)
    features.columns = features.columns.astype(str)

    target = df['Delay_minutes']
    
    return features, target

def compute_model_score(model, X, y):
    """
    Calcule le score d'un modèle donné à l'aide de la validation croisée.

    Args:
        model (sklean): le modèle de machine learning utilisé
        X (pd.DataFrame): les variables explicatives
        y (pd.DataFrame): la variable cible (target) à prédire

    Returns:
        float: le score moyen (plus la valeur est proche de 0, meilleur est le modèle)
    """
    print(X.head())
    X.to_csv(f'/app/clean_data/X{model}.csv', index=False)
    print("\n\n\n\n")
    print(y.head())
    y.to_csv(f'/app/clean_data/y{model}.csv', index=False)
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()
    return model_score

def train_and_save_model(model, X, y, path_to_model='./app/model.pckl'):
    """
    Entraîne un modèle de machine learning sur les données fournies et sauvegarde le modèle entraîné.
    

    Args:
        model (sklearn): le modèle de machine learning utilisé
        X (pd.DataFrame): les variables explicatives
        y (pd.DataFrame): la variable cible (target) à prédire
        path_to_model (str, optional): le chemin du fichier de sauvegarde. Defaults to './app/model.pckl'.
    """
    model.fit(X, y)
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)

def train_model(model_name, **kwargs):
    """
    Entraine un modèle de machine learning spécifié par son nom et sauvegarde le modèle dans un fichier.pickle.

    Args:
        model_name (str): le nom du modèle à entraîner cf énoncé.
        **kwargs : arguments supplémentaires, notamment "ti", utilisé pour accéder à XCom.
    """
    X, y = prepair_data_to_ml()
    
    if model_name == 'LinearRegression':
        model = LinearRegression()
    elif model_name == 'DecisionTreeRegressor':
        model = DecisionTreeRegressor()
    elif model_name == 'RandomForestRegressor':
        model = RandomForestRegressor()
    else:
        raise ValueError(f'Unknown model name: {model_name}')
    
    score = compute_model_score(model, X, y)
    path_to_model = f'/app/clean_data/{model_name}_model.pickle'
    train_and_save_model(model, X, y, path_to_model)
    kwargs['ti'].xcom_push(key=f'{model_name}', value=score)

def select_best_model(**kwargs):
    """
    Sélectionne le meilleur modèle basé sur les scores calculés et sauvegarde le modèle.
    """
    ti = kwargs['ti']
    score_lr = ti.xcom_pull(key='LinearRegression')
    score_dtr = ti.xcom_pull(key='DecisionTreeRegressor')
    score_rfr = ti.xcom_pull(key='RandomForestRegressor')
    
    X, y = prepair_data_to_ml()
    best_score = max(score_lr, score_dtr, score_rfr)
    
    if best_score == score_lr:
        print(f'LinearRegression selected with score : {score_lr}')
        train_and_save_model(
            LinearRegression(),
            X, y,
            '/app/clean_data/best_model.pickle'
        )
    if best_score == score_dtr:
        print(f'DecisionTreeRegressor selected with score : {score_lr}')
        train_and_save_model(
            DecisionTreeRegressor(),
            X, y,
            '/app/clean_data/best_model.pickle'
        )
    else:
        print(f'RandomForestRegressor selected with score : {score_lr}')
        train_and_save_model(
            RandomForestRegressor(),
            X, y,
            '/app/clean_data/best_model.pickle'
        )


with DAG(
    dag_id='model_training_dag',
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1)
    },
    tags=['training', 'regression', 'models'],
    catchup=False,
) as dag_1:
    
    with TaskGroup('training_tasks') as training_tasks:
        train_lr = PythonOperator(
            task_id='train_linear_regression',
            python_callable=train_model,
            op_kwargs={'model_name': 'LinearRegression'}
        )
        
        train_dtr = PythonOperator(
            task_id='train_decision_tree_regressor',
            python_callable=train_model,
            op_kwargs={'model_name': 'DecisionTreeRegressor'}
        )
        
        train_rfr = PythonOperator(
            task_id='train_random_forest_regressor',
            python_callable=train_model,
            op_kwargs={'model_name': 'RandomForestRegressor'}
        )
        
    select_best_model_task = PythonOperator(
        task_id='select_best_model',
        python_callable=select_best_model,
        provide_context=True
    )

training_tasks >> select_best_model_task