import ast

from mcp.server import FastMCP
from mcp import Tool
from mcp.types import TextContent
from typing import Dict, Any, List, Tuple
from mcp_client import MCPClient, ServiceResponse, ServiceExecStatus, sync_exec
mcp = FastMCP()


def literal_eval(target):
    try:
        return ast.literal_eval(target)
    except SyntaxError:
        return target


@mcp.tool()
async def proxy(target_tool: str, tool_args: Dict[str, Any], server_config: Dict[str, Any]) -> Any:
    """
    Proxy to execute the specified tool and handle parameter dependencies between tools.

    **Purpose**:
    - Simplify complex tool invocation workflows by automatically handling parameter dependencies.
    - Execute multiple tools where one tool's parameters depend on the output of another.

    **Parameters**:
    - `target_tool` (str): The name of the target tool to be executed.
    - `tool_args` (Dict[str, Any]): Dictionary of arguments passed to the target tool.
      - Arguments can be direct values like strings or numbers.
      - Arguments can also be a dictionary containing the `__tool__` key to specify a tool that must be executed first to obtain the value of this parameter.
        - Each tool call must include an `identifier` field to ensure unique identification.
        - Can include a `__transform__` field specifying a transformation (using a lambda expression) on the tool result.
      - Arguments can also be a dictionary containing the `__ref__` key to reference results from previous tool calls.
        - Can include a `__transform__` field specifying a transformation (using a lambda expression) on the referenced result.
    - `server_config` (Dict[str, Any]): Configuration information for the server, such as host address and port, used to initialize client connections.

    **Returns**:
    - Any: The result after executing the target tool.

    **Use Cases**:
    - When one tool requires the output of another tool as its input argument.
    - For example, during model training, the output of a data preprocessing tool is used as input for the training tool.

    **Usage Example**:

    *Simple Two-Level Example*:
    Suppose you have two tools:
    1. `SELECT`: Executes a query from the database, returning `List[tuple]`.
    2. `line_plotter`: Responsible for drawing line charts, requiring two array parameters `x` and `y`.

    You can use the `proxy` tool to automatically handle the `x` and `y` parameters required by `line_plotter`. These parameters both come from the output of the same `SELECT` tool, with `x` extracting the first column and `y` referencing the second column.

    ```python
    result = await proxy(
        target_tool="line_plotter",
        tool_args={
            "x": {
                "__tool__": "SELECT",
                "args": {
                    "query": "SELECT timestamp, sales_amount FROM sales_data"
                },
                "identifier": "select_sales_data",
                "__transform__": "lambda data: [row[0] for row in data]"
            },
            "y": {
                "__ref__": "select_sales_data",
                "__transform__": "lambda data: [row[1] for row in data]"
            }
        },
         server_config={
            "mcpServers": {
                "Server1": {specific information},
                "Server2": {specific information}
            }
        }
    )
    ```

    *Simple Three-Level Example*:
    Suppose you have Three tools:
    1. `SELECT`: Executes a query from the database, returning `List[tuple]`.
    2. `data_processor`: Processes the data, such as smoothing or filtering.
    3. `line_plotter`: Responsible for drawing line charts, requiring two array parameters `x` and `y`.

    result = await proxy(
        target_tool="line_plotter",
        tool_args={
            "x": {
                "__tool__": "data_processor",
                "args": {
                    "raw_data": {
                        "__tool__": "SELECT",
                        "args": {
                            "query": "SELECT timestamp, temperature FROM sensor_readings"
                        },
                        "identifier": "sensor_data_query",
                        "__transform__": "lambda data: [row[0] for row in data]"
                    },
                    "method": "smooth",
                    "window_size": 5
                },
                "identifier": "processed_x_data"
            },
            "y": {
                "__tool__": "data_processor",
                "args": {
                    "raw_data": {
                        "__ref__": "sensor_data_query",
                        "__transform__": "lambda data: [row[1] for row in data]"
                    },
                    "method": "filter_outliers",
                    "threshold": 3
                },
                "identifier": "processed_y_data"
            }
        },
        server_config={
            "mcpServers": {
                "Server1": { /* specific information */ },
                "Server2": { /* specific information */ }
            }
        }
    )
    """
    # Create and initialize MCP session clients
    mcp_clients = []
    for name, config in server_config["mcpServers"].items():
        if name != 'proxy':
            mcp_clients.append(MCPClient(name, config))

    # Define internal helper function with mapping from tools to clients and result cache
    async def _proxy(
            target_tool: str,
            tool_args: Dict[str, Any],
            tool_to_client: Dict[str, Tuple[Tool, MCPClient]],
            results_cache: Dict[str, Any],
    ) -> Any:
        # Check if the target tool exists
        if target_tool not in tool_to_client:
            raise RuntimeError(f"Tool '{target_tool}' was not found in the available tools list.")

        # Get the client providing the target tool
        mcp_tool: Tool = tool_to_client[target_tool][0]
        client: MCPClient = tool_to_client[target_tool][1]

        # Process tool arguments, supporting nested tool calls and references
        processed_args = {}
        for arg_key, arg_value in tool_args.items():
            if isinstance(arg_value, dict):
                if "__tool__" in arg_value:
                    # Tool call
                    identifier = arg_value.get("identifier")
                    if not identifier:
                        raise RuntimeError(f"Missing 'identifier' field in tool call: {arg_value}")

                    # Check uniqueness of identifier
                    if identifier in results_cache:
                        raise RuntimeError(
                            f"The identifier '{identifier}' already exists. Please ensure each tool call has a unique identifier.")

                    # Get tool name and arguments
                    nested_tool = arg_value["__tool__"]
                    nested_args = arg_value.get("args", {})
                    transform_expr = arg_value.get("__transform__")

                    # Recursively call _proxy to handle nested tool
                    nested_result = await _proxy(nested_tool, nested_args, tool_to_client, results_cache)

                    # Store raw result in cache
                    results_cache[identifier] = nested_result

                    # Apply transformation (if any)
                    if transform_expr:
                        try:
                            # Safely evaluate lambda expression
                            # Note: Executing lambda expressions from untrusted input poses security risks
                            # Ensure inputs are trusted or use safer parsing methods
                            transform_func = eval(transform_expr, {"__builtins__": {}})
                            transformed_result = transform_func(nested_result)
                        except Exception as e:
                            raise RuntimeError(
                                f"Failed to execute transformation expression: {transform_expr}. Error: {e}")
                    else:
                        transformed_result = nested_result

                    # Assign processed value
                    processed_args[arg_key] = transformed_result

                elif "__ref__" in arg_value:
                    # Reference
                    ref_identifier = arg_value["__ref__"]
                    if ref_identifier not in results_cache:
                        raise RuntimeError(f"Referenced identifier '{ref_identifier}' not found.")

                    ref_result = results_cache[ref_identifier]

                    # Apply transformation (if any)
                    transform_expr = arg_value.get("__transform__")
                    if transform_expr:
                        try:
                            transform_func = eval(transform_expr, {"__builtins__": {}})
                            transformed_ref = transform_func(ref_result)
                        except Exception as e:
                            raise RuntimeError(
                                f"Failed to execute transformation expression: {transform_expr}. Error: {e}")
                    else:
                        transformed_ref = ref_result

                    # Assign processed value
                    processed_args[arg_key] = transformed_ref

                else:
                    # Other dictionaries, assign directly
                    processed_args[arg_key] = arg_value
            else:
                # Direct assignment
                processed_args[arg_key] = arg_value

        # Execute the target tool and get result
        result: ServiceResponse = await client.execute_tool(target_tool, **processed_args)
        return _parser_result(result)

    # Build mapping from tool names to clients
    tool_2_client = await fetch_all_tools(mcp_clients)

    # Result cache based on identifier
    results_cache: Dict[str, Any] = {}

    result = await _proxy(target_tool, tool_args, tool_2_client, results_cache)

    for client in mcp_clients:
        sync_exec(client.cleanup)

    return result


def _parser_result(result: ServiceResponse) -> Any:
    """
    Parse the content of ServiceResponse to extract actual data.
    Raise an exception if the response status indicates failure.

    **Parameters**:
    - `result` (ServiceResponse): The response result from tool execution.

    **Returns**:
    - Any: Parsed actual data.

    **Exceptions**:
    - `RuntimeError`: If tool execution fails or the content format is incorrect.
    """
    if result.status == ServiceExecStatus.ERROR:
        raise RuntimeError(f"Tool execution failed: {result.content}")
    # Use ast.literal_eval to parse string into Python data type
    assert isinstance(result.content, list) and len(
        result.content) == 1, "The content of the tool execution result should be a list."
    if isinstance(result.content[0], dict):
        return literal_eval(result.content[0]["text"])
    elif isinstance(result.content[0], TextContent):
        return literal_eval(result.content[0].text)
    else:
        raise RuntimeError(
            f"Unable to parse tool execution result: {result.content}. "
            "Ensure the returned content is a parsable string or dictionary."
        )


async def fetch_all_tools(mcp_clients: List[MCPClient]) -> Dict[str, Tuple[Tool, MCPClient]]:
    """
    Fetch available tools from all MCP clients and build a mapping from tool names to (tool, client).

    **Parameters**:
    - `mcp_clients` (List[MCPClient]): List of MCP session handlers.

    **Returns**:
    - Dict[str, Tuple[Tool, MCPClient]]: Mapping from tool names to (tool, client).

    **Exceptions**:
    - `RuntimeError`: If the same tool is available in multiple clients.
    """
    tool_2_client: Dict[str, Tuple[Tool, MCPClient]] = {}

    # Get tool lists from all clients
    all_tools_lists: List[List[Tool]] = [
        sync_exec(client.list_tools) for client in mcp_clients
    ]

    # Build mapping from tool names to clients
    for client, tools in zip(mcp_clients, all_tools_lists):
        for tool in tools:
            if tool.name in tool_2_client:
                raise RuntimeError(
                    f"Tool '{tool.name}' is available in multiple clients: "
                    f"'{tool_2_client[tool.name][0].name}' and '{client.name}'."
                )
            tool_2_client[tool.name] = (tool, client)

    return tool_2_client


def main():
    mcp.run(transport='stdio')

if __name__ == "__main__":
    main()
