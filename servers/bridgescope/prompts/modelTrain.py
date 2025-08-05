from mcp import GetPromptResult
from mcp.types import PromptMessage, TextContent

from mcp_context import mcp


@mcp.prompt()
def model_train_prompt(question: str) -> GetPromptResult:
    """Convert a natural language question into an executable SQL.

    Args:
        question (str): The natural language question or request.
    """

    prompt = f"""
    Please help me complete my task: {question}.
    Notes:
    1. When training the model, there is no need to normalize the labels. Please use the original label data directly.
    2. Before generating any SQL query, you must understand the structure of the relevant tables. If uncertain, use tools to inspect the database first;
    """

    return GetPromptResult(
        messages=[
            PromptMessage(
                role="user",
                content=TextContent(type="text", text=prompt),
            )
        ],
    )


@mcp.prompt()
def model_train_proxy_prompt(question: str) -> GetPromptResult:
    """Convert a natural language question into an executable SQL.

    Args:
        question (str): The natural language question or request.
    """

    prompt = f"""
    Please help me complete my task: {question}.
    Notes:
    1. When training the model, there is no need to normalize the labels. Please use the original label data directly.
    2. Before generating any SQL query, you must understand the structure of the relevant tables. If uncertain, use tools to inspect the database first;
    3. The Proxy tool is designed to directly pass the output of one tool as input to another. When parameter dependencies exist between tools, use the Proxy tool (which typically coordinates two or more tool calls). Since the data volume is large, avoid direct data retrieval via tool (e.g., SELECT); instead, strictly use the Proxy tool for data transfer.    """

    return GetPromptResult(
        messages=[
            PromptMessage(
                role="user",
                content=TextContent(type="text", text=prompt),
            )
        ],
    )

