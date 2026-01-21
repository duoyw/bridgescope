import asyncio

from typing import Callable, Any

def load_prompt(filename):
    with open(filename, 'r') as f:
        return f.read()

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
