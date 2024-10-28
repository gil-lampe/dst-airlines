from logging import getLogger
import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.base import BaseEstimator
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

logger = getLogger(__name__)


def _get_model_from_name(model_name: str) -> BaseEstimator:
    """Get the sklearn model corresponding to the provided name (only works for LinearRegression, DecisionTreeRegressor and RandomForestRegressor)

    Args:
        model_name (str): Name of the model

    Raises:
        ValueError: Raised if the name of the model is not either LinearRegression, DecisionTreeRegressor or RandomForestRegressor

    Returns:
        BaseEstimator: Corresponding sklearn model 
    """
    match model_name:
        case "LinearRegression":
            model = LinearRegression()
        case "DecisionTreeRegressor":
            model = DecisionTreeRegressor()
        case "RandomForestRegressor":
            model = RandomForestRegressor()
        case _:
            logger.error(msg := f'Unknown model name: {model_name}')
            raise ValueError(msg)

    return model    


def compute_model_score(model_name: str, features: pd.DataFrame, target: pd.Series, scoring_method: str = 'neg_mean_squared_error') -> float:
    """Compute the score for the score of the model_name following the selected scoring_method using features and target as datasets

    Args:
        model_name (str): Name of the model
        features (pd.DataFrame): Feature dataset
        target (pd.Series): target dataset
        scoring_method (str, optional): Method to be used for scoring (computed thrice and averaged). Defaults to 'neg_mean_squared_error'.

    Returns:
        float: Score of the model based on features and target
    """
    logger.info(f"Starting the score computation for {model_name = } using the {scoring_method = }.")

    model = _get_model_from_name(model_name)

    cross_validation = cross_val_score(
        model,
        features,
        target,
        cv=3,
        scoring=scoring_method)

    model_score = cross_validation.mean()

    logger.info(f"Score computation for {model_name = } using the {scoring_method = } finalized.")
    return model_score


def train_store_model(model_name: str, features: pd.DataFrame, target: pd.Series, model_file_path: str):
    """
    Train the model_name on the provided features and target dataset and save the resulting model in model_storage_path.

    Args:
        model_name (str): Name of the model
        features (pd.DataFrame): The feature matrix
        target (pd.Series): The target variable
        model_storage_path (str, optional): The file path to save the trained model
    """
    logger.info(f"Starting {model_name = } fitting.")

    model = _get_model_from_name(model_name)

    model.fit(features, target)

    logger.info(f"{model_name = } fitting finalized, starting to store it in {model_file_path = }.")

    # NOTE: Ideally, the model would be stored into a persistant storage (S3 or others)
    dump(model, model_file_path)

    logger.info(f"{model_name = } stored at {model_file_path = }.")


def select_best_model(scores: dict[float]) -> str:
    """Select the best performing machine learning model based on cross-validated scores

    Args:
        scores (dict[float]): Scores of the machine learning models

    Raises:
        ValueError: Raised if there is an issue in matching the best score with related scores 

    Returns:
        str: Name of the best model
    """
    logger.info(f"Starting the selection of the best model based on its score.")

    best_model_name = max(scores, key=scores.get)
    
    # if best_score == score_lr:
    #     logger.info(f'LinearRegression selected with score : {score_lr}')
    #     best_model_name = "LinearRegression"
    # elif best_score == score_dtr:
    #     logger.info(f'DecisionTreeRegressor selected with score : {score_dtr}')
    #     best_model_name = "DecisionTreeRegressor"
    # elif best_score == score_rfr:
    #     logger.info(f'RandomForestRegressor selected with score : {score_rfr}')
    #     best_model_name = "RandomForestRegressor"
    # else:
    #     logger.error(msg := f'Unable to match best score with model scores: {best_score = } | {score_lr = } | {score_dtr = } | {score_rfr = }')
    #     raise ValueError(msg)        

    logger.info(f"Selection of the best model based on its score finalized ({best_model_name = } with a score = {scores[best_model_name]}).")
    return best_model_name

