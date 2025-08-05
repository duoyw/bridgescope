import asyncio
import json
from typing import Any, Callable, List, Dict

def load_mcp_config(config):
    with open(config["config_path"]) as f:
        conf = json.load(f)
    return conf[config["config_name"]]


def load_dataset(data_path: str) -> List[Dict[str, Any]]:
    """
    Loads the dataset from the specified path.

    Args:
        data_path (str): Path to the data file.

    Returns:
        List[Dict[str, Any]]: The loaded dataset.
    """
    with open(data_path, 'r') as file:
        dataset = json.load(file)
    return dataset

def sync_exec(func: Callable, *args: Any, **kwargs: Any) -> Any:
    """
    Execute a function synchronously.

    Args:
        func (Callable): The asynchronous function to execute.
        *args (Any): Positional arguments to pass to the function.
        **kwargs (Any): Keyword arguments to pass to the function.

    Returns:
        Any: The result of the function execution.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError as e:
        # If there is no event loop in the current context, create one
        if "no current event loop" in str(e):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        else:
            raise

    result = loop.run_until_complete(func(*args, **kwargs))
    return result
