from tools.utils import get_db_adapter, format_response, response_type
from mcp_constants import default_res
from mcp_context import mcp


@mcp.tool(description="Begin a transaction")
async def begin() -> response_type:
    """
    Begin a new transaction.

    :return: A formatted response for beginning the transaction.
    """
    db_adapter = get_db_adapter()

    await db_adapter.begin()
    return format_response(default_res)

@mcp.tool(description="Commit current transaction")
async def commit() -> response_type:
    """
    Commit the current transaction.

    :return: A formatted response for committing the transaction.
    """
    db_adapter = get_db_adapter()

    await db_adapter.commit()
    return format_response(default_res)


@mcp.tool(description="Rollback current transaction")
async def rollback() -> response_type:
    """
    Rollback the current transaction.

    :return: A formatted response for rolling back the transaction.
    """
    db_adapter = get_db_adapter()

    await db_adapter.rollback()
    return format_response(default_res)