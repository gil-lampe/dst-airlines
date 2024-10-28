from airflow.utils.dates import days_ago
from airflow.decorators import dag, task, task_group
from dst_airlines.modeling import prepare_data
from dst_airlines.modeling import train_model
from dst_airlines.database import mysql
from logging import getLogger
import os


logger = getLogger(__name__)

sql_user = os.getenv("MYSQL_USER")
sql_password = os.getenv("MYSQL_ROOT_PASSWORD")
sql_host = os.getenv("MYSQL_HOST")
sql_port = int(os.getenv("MYSQL_PORT"))
sql_database = os.getenv("MYSQL_DATABASE")


@dag(
    dag_id='dst_airlines_train_ml_model',
    # schedule_interval=timedelta(minutes=1),
    tags=['DST-airlines', 'ml training'],
    start_date=days_ago(0),
    catchup=False
)
def taskflow():
    """
    Tasklfow to: 
        1. Prepare Lufthansa flight and Open Meteo weather forecast data
        2. Compute score for 3 regressions models
        3. Select the best of the 3 models
        4. Train the best model and store it 
    """


    @task()
    def prepare_train_data():
        """Prepare Lufthansa flight and Open Meteo weather forecast data to make them ready for ML model training.
        """

        flights, weather_forecasts = mysql.get_tables(["flights", "weather_forecasts"], sql_user, sql_password, sql_host, sql_port, sql_database)

        flights = prepare_data.prepare_flights_for_training(flights)
        flights = prepare_data.preprare_features_from_flights(flights)

        flights_weather_forecasts = prepare_data.merge_flights_and_weather(flights, weather_forecasts)

        features = prepare_data.select_feature_columns(flights_weather_forecasts)
        mysql.upload_data_in_mysql(data=features, table_name="features", sql_user=sql_user, sql_password=sql_password, sql_host=sql_host, sql_port=sql_port, sql_database=sql_database)

        target = prepare_data.compute_target_delay_in_minutes(flights_weather_forecasts)
        mysql.upload_data_in_mysql(data=target, table_name="target", sql_user=sql_user, sql_password=sql_password, sql_host=sql_host, sql_port=sql_port, sql_database=sql_database)


    @task()
    def compute_model_score(model_name: str, prev_task: None=None) -> float:
        """Compute model socre by downloading features and target from MySQL database

        Args:
            model_name (str): Name of the model
            prev_task (None, optional): Placeholder to ensure task connection. Defaults to None.

        Returns:
            float: Score of the model
        """

        features, target = mysql.get_tables(["features", "target"], sql_user, sql_password, sql_host, sql_port, sql_database)

        model_score = train_model.compute_model_score(model_name, features, target)

        return model_score


    @task_group()
    def compute_model_scores(model_names: list[str], prev_task: None=None) -> dict[float]:
        """Compute score for 3 differents regression models (Linear, DecisionTree and RandomForest)

        Args:
            prev_task (None, optional): Placeholder to ensure task connection. Defaults to None.

        Returns:
            dict[float]: Computed scores  ["LinearRegression", "DecisionTreeRegressor", "RandomForestRegressor"]
        """
        scores = {}

        for model_name in model_names:
            scores[model_name] = compute_model_score.override(task_id=f"compute_{model_name}_score")(model_name, prev_task=prev_task)
        # score_lr = compute_model_score.override(task_id="compute_LinearRegression_score")("LinearRegression", prev_task=prev_task)
        # score_dtr = compute_model_score.override(task_id="compute_DecisionTreeRegressor_score")("DecisionTreeRegressor", prev_task=prev_task)
        # score_rfr = compute_model_score.override(task_id="compute_RandomForestRegressor_score")("RandomForestRegressor", prev_task=prev_task)

        return scores
    # {
    #         "score_lr": score_lr,
    #         "score_dtr": score_dtr,
    #         "score_rfr": score_rfr
    #     }


    @task()
    def select_best_model(scores: dict[str]) -> str:
        """Select the best model based on provided scores

        Args:
            scores (dict[str]): Score of the models associated to their name {model_name: model_score}

        Returns:
            str: Name of the best model
        """

        best_model_name = train_model.select_best_model(scores)

        return best_model_name


    @task()
    def train_store_model(model_name: str):
        """Train and store the model

        Args:
            model_name (str): Name of the model to be stored
        """

        features, target = mysql.get_tables(["features", "target"], sql_user, sql_password, sql_host, sql_port, sql_database)

        model_storage_path = '/opt/airflow/best_model.pickle'

        train_model.train_store_model(model_name, features, target, model_storage_path)

    # Workflow
    task1 = prepare_train_data()
    task2_scores = compute_model_scores(["LinearRegression", "DecisionTreeRegressor", "RandomForestRegressor"], prev_task=task1)
    task3_best_model = select_best_model(score_lr=task2_scores["score_lr"], score_dtr=task2_scores["score_dtr"], score_rfr=task2_scores["score_rfr"])
    task4 = train_store_model(task3_best_model)

taskflow()