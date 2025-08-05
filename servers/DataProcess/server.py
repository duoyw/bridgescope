from typing import List, Tuple, Any
import numpy as np
from mcp.server import FastMCP
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import mcp.types as types

mcp = FastMCP()


def format_response(res: Any) -> List[types.TextContent]:
    return [types.TextContent(type="text", text=str(res))]


def round_tuple(t: Tuple[float, ...], precision: int = 3) -> Tuple[float, ...]:
    """
    Helper function to round each float in a tuple to a specified number of decimal places.

    Args:
        t (Tuple[float, ...]): Input tuple of floats.
        precision (int): Number of decimal places to round to.

    Returns:
        Tuple[float, ...]: Rounded tuple.
    """
    return tuple(round(x, precision) for x in t)


@mcp.tool()
def min_max_scaling(data: List[Tuple[float, ...]]) -> List[Tuple[float, ...]]:
    """
    Perform Min-Max Scaling on the input data and round results to 3 decimal places.

    Args:
        data (List[Tuple[float, ...]]): Input data where each tuple contains feature values.

    Returns:
        List[Tuple[float, ...]]: Scaled data with values rounded to 3 decimal places.
    """
    X = np.array(data, dtype=float)
    scaler = MinMaxScaler(feature_range=(0, 1))
    X_scaled = scaler.fit_transform(X)
    # Convert to list of tuples and round each value to 3 decimal places
    scaled_data = [round_tuple(tuple(float(x) for x in row), 3) for row in X_scaled]
    return format_response(scaled_data)


@mcp.tool()
def z_score_scaling(data: List[Tuple[float, ...]]) -> List[Tuple[float, ...]]:
    """
    Perform Z-Score Standardization on the input data and round results to 3 decimal places.

    Args:
        data (List[Tuple[float, ...]]): Input data where each tuple contains feature values.

    Returns:
        List[Tuple[float, ...]]: Standardized data with values rounded to 3 decimal places.
    """
    X = np.array(data, dtype=float)
    scaler = StandardScaler()
    X_standardized = scaler.fit_transform(X)
    # Convert to list of tuples and round each value to 3 decimal places
    standardized_data = [round_tuple(tuple(float(x) for x in row), 3) for row in X_standardized]
    return format_response(standardized_data)


mcp.run()
