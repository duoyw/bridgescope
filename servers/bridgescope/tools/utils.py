import mcp.types as types
from typing import Any, Dict, List
from collections import defaultdict

from sentence_transformers import SentenceTransformer

import mcp_context
from mcp_context import MCPContext
from mcp_constants import response_type

from db_adapters.base_adapter import BaseAdapter


def format_response(res: Any) -> response_type:
    """
    Format a response as a list of TextContent objects.

    :param res: The response content to be formatted.
    :return: A list containing a single TextContent object with the response as text.
    """
    return [types.TextContent(type="text", text=str(res))]


def get_db_adapter() -> BaseAdapter:
    """
    Get the database adapter from the global context.

    :return: Database adapter instance.
    :raises RuntimeError: If connection to database is not established.
    """
    db_adapter = get_context_attribute("db_adapter")
    if db_adapter is None:
        raise RuntimeError("Connection to database not established.")
    return db_adapter


def get_semantic_model() -> SentenceTransformer:
    """
    Get the semantic model from the global context.

    :return: SentenceTransformer instance.
    :raises RuntimeError: If the model is not initialized.
    """
    semantic_model = get_context_attribute("semantic_model")
    if semantic_model is None:
        raise RuntimeError("Semantic model not initialized")
    return semantic_model


def get_context_attribute(attribute_name: str) -> Any:
    """
    Get the value of a specific attribute from the global MCP context.

    :param attribute_name: The name of the attribute to retrieve from the context.
    :return: The value of the specified attribute.
    :raises RuntimeError: If the context is not initialized.
    :raises AttributeError: If the specified attribute does not exist in the context.

    Example:
        # Get user privileges
        privileges = get_context_attribute('user_privilege')

        # Get database adapter
        db_adapter = get_context_attribute('db_adapter')

        # Get whitelist
        whitelist = get_context_attribute('white_object_list')
    """
    ctx: MCPContext = mcp_context.context
    if ctx is None:
        raise RuntimeError("MCP context is not initialized.")

    if not hasattr(ctx, attribute_name):
        raise AttributeError(f"Context does not have attribute: {attribute_name}")

    return getattr(ctx, attribute_name)


def reformat_privilege(
    privilege: Dict[str, Dict[str, List[str]]],
) -> Dict[str, Dict[str, List[str]]]:
    """
    Reformat privilege dictionary from operation-centric to object-centric structure.

    :param privilege: A nested dictionary with structure:
                     {operation: {object_type: [object_names]}}
    :return: Reformatted dictionary with structure:
             {object_type: {object_name: [operations]}}

    Example:
        Input: {
            'SELECT': {'TABLE': ['public.users', 'public.orders']},
            'INSERT': {'TABLE': ['public.users']}
        }
        Output: {
            'TABLE': {
                'users': ['SELECT', 'INSERT'],
                'orders': ['SELECT']
            }
        }
    """
    type_2_obj_2_priv = defaultdict(lambda: defaultdict(list))

    for operation, obj_types in privilege.items():
        for obj_type, objects in obj_types.items():
            for obj in objects:
                # Remove 'public.' prefix if present
                clean_obj_name = obj[7:] if obj.startswith("public.") else obj
                type_2_obj_2_priv[obj_type][clean_obj_name].append(operation)

    return type_2_obj_2_priv
