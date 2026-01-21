import ast
import os
import uuid
import joblib
import numpy as np
from typing import List, Tuple, Dict, Any
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from agentscope.service.mcp_manager import MCPSessionHandler, sync_exec
from mcp.server import FastMCP
from mcp import Tool
from mcp.types import TextContent
from agentscope.service import ServiceExecStatus, ServiceResponse

# Initialize the FastMCP server instance
mcp = FastMCP()

# Define the directory where trained models will be saved
MODEL_DIR = 'models'

# Create the model directory if it doesn't already exist
if not os.path.exists(MODEL_DIR):
    os.makedirs(MODEL_DIR)


def _gen_model_id():
    return "aaa" + str(uuid.uuid4()) + "aaa"


@mcp.tool()
def train_linear_regression(features: List[Tuple], labels: List[Tuple]) -> str:
    """
    Train a Linear Regression model using the provided features and labels.
    The trained model is saved locally, and a unique model identifier is returned.

    Args:
        features (List[Tuple]): A list of feature tuples for training.
        labels (List[Tuple]): A list of target values in singleton tuples like [(y1,), (y2,), ...].

    Returns:
        str: A unique identifier for the saved model.
    """
    X = np.array(features)
    y = np.array([label[0] for label in labels])  # Flatten labels from [(y1,), (y2,)] to [y1, y2]

    model = LinearRegression()
    model.fit(X, y)

    model_id = _gen_model_id()
    model_path = os.path.join(MODEL_DIR, f'{model_id}.joblib')
    joblib.dump(model, model_path)

    return model_id


@mcp.tool()
def train_decision_tree(features: List[Tuple], labels: List[Tuple]) -> str:
    """
    Train a Decision Tree Regressor using the provided features and labels.
    The trained model is saved locally, and a unique model identifier is returned.

    Args:
        features (List[Tuple]): A list of feature tuples for training.
        labels (List[Tuple]): A list of target values in singleton tuples.

    Returns:
        str: A unique identifier for the saved model.
    """
    X = np.array(features)
    y = np.array([label[0] for label in labels])

    model = DecisionTreeRegressor(random_state=42)
    model.fit(X, y)

    model_id = _gen_model_id()
    model_path = os.path.join(MODEL_DIR, f'{model_id}.joblib')
    joblib.dump(model, model_path)

    return model_id


@mcp.tool()
def train_random_forest(features: List[Tuple], labels: List[Tuple]) -> str:
    """
    Train a Random Forest Regressor using the provided features and labels.
    The trained model is saved locally, and a unique model identifier is returned.

    Args:
        features (List[Tuple]): A list of feature tuples for training.
        labels (List[Tuple]): A list of target values in singleton tuples.

    Returns:
        str: A unique identifier for the saved model.
    """
    X = np.array(features)
    y = np.array([label[0] for label in labels])

    model = RandomForestRegressor(n_estimators=3, random_state=42)
    model.fit(X, y)

    model_id = _gen_model_id()
    model_path = os.path.join(MODEL_DIR, f'{model_id}.joblib')
    joblib.dump(model, model_path)

    return model_id


@mcp.tool()
def train_svm(features: List[Tuple], labels: List[Tuple]) -> str:
    """
    Train a Support Vector Machine (SVM) regressor using the provided features and labels.
    The trained model is saved locally, and a unique model identifier is returned.

    Args:
        features (List[Tuple]): A list of feature tuples for training.
        labels (List[Tuple]): A list of target values in singleton tuples.

    Returns:
        str: A unique identifier for the saved model.
    """
    X = np.array(features)
    y = np.array([label[0] for label in labels])

    kernel = "linear"  # Supported kernels are 'linear', 'poly', 'rbf', 'sigmoid'. Defaults to 'rbf'.

    model = SVR(kernel=kernel, C=0.1, epsilon=0.2, max_iter=1000)
    model.fit(X, y)

    model_id = _gen_model_id()
    model_path = os.path.join(MODEL_DIR, f'{model_id}.joblib')
    joblib.dump(model, model_path)

    return model_id


@mcp.tool()
def model_predict(model_id: str, features: List[Tuple]) -> List[Tuple]:
    """
    Make predictions using a previously trained model identified by model_id.
    The function loads the model from disk, performs prediction on the provided features,
    and returns the prediction results as a list of singleton tuples.

    Args:
        model_id (str): The unique identifier of the model to be used for prediction.
        features (List[Tuple]): A list of feature tuples for which predictions are to be made.

    Returns:
        List[float]: A list of predicted values wrapped in singleton tuples like [(y1,), (y2,)].

    Raises:
        FileNotFoundError: If the model with the specified model_id does not exist.
    """
    model_path = os.path.join(MODEL_DIR, f'{model_id}.joblib')

    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model with ID {model_id} does not exist.")

    model = joblib.load(model_path)
    X = np.array(features)
    predictions = model.predict(X)

    # Wrap each prediction into a singleton tuple
    return f"<predict>{str(predictions.tolist())}</predict>"


# Start the FastMCP server to listen for incoming tool requests and handle them accordingly
mcp.run()
