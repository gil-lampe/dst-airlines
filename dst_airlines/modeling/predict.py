from sklearn.base import BaseEstimator
from logging import getLogger
import pandas as pd
from numpy import ndarray

logger = getLogger(__name__)

def generate_prediction(model: BaseEstimator, features: pd.DataFrame) -> ndarray:
    """Generate a prediction from the inputted features based on the provided model

    Args:
        model (BaseEstimator): sklean trained model
        features (pd.DataFrame): Features

    Returns:
        ndarray: Predictions
    """
    logger.info(f"Starting the prediction from the provided model.")
    prediction = model.predict(features)[0]

    logger.info(f"Prediction from the provided model finalized.")
    return prediction