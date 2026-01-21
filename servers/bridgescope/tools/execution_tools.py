from mcp_context import global_privilege_operations, mcp

from tools.utils import (
    format_response,
    response_type,
    get_db_adapter,
    get_context_attribute,
)
from tools.sql_checker import SQLChecker


async def execute_sql_by_action(sql, action: str | None = None) -> response_type:
    """
    Executing SQL of a specific action type.

    :param sql: The SQL statement to execute
    :param action: The action type (SELECT, INSERT, UPDATE, DELETE, etc.). If None, any SQL type is allowed.
    :return: A formatted response containing the query results or affected row count
    """
    db_adapter = get_db_adapter()

    if action is not None:
        if action not in global_privilege_operations:
            raise RuntimeError("SQL function not supported.")

    # Pre-execution security checks
    checker = SQLChecker(sql)
    if action is not None and not checker.check_operation_match(action):
        raise RuntimeError("SQL and tool function mismatch.")

    if not checker.check_privilege():
        raise RuntimeError("SQL exceeds user privilege.")

    if not checker.check_object_acl():
        raise RuntimeError("SQL violates user-configured ACL.")

    # Execute query
    rows = await db_adapter.execute_query(sql)
    if isinstance(rows, list):
        return format_response(list([r for r in rows]))
    else:
        return format_response(f"{rows} rows affected.")


def create_tool(action=None):
    """
    Create a tool function for executing SQL of a specific action type.

    :param action: The SQL action type to restrict this tool to. If None, creates a generic tool
    :return: An async function that performs SQL execution with the specified action constraint
    """

    async def tool(sql):
        return await execute_sql_by_action(sql, action=action)

    return tool


def build_sql_exec_tools():
    """
    Build and register SQL execution tools with MCP based on user privileges and configuration.
    """

    if get_context_attribute("disable_fine_gran_tool"):
        # Single generic EXECUTE tool
        mcp.add_tool(
            create_tool(),
            name="execute",
            description=_common_sql_exe_param_prompt_with_single(),
        )
    else:
        # Fine-grained tools for each allowed operation
        user_privilege = get_context_attribute("user_privilege")
        if user_privilege:
            privileged_operations = [
                o for o in global_privilege_operations if o in user_privilege.keys()
            ]

            # Apply white list filter if configured
            white_tool_list = get_context_attribute("white_tool_list")
            if white_tool_list:
                privileged_operations = [
                    o for o in privileged_operations if o in white_tool_list
                ]
            else:
                # Apply black list filter if configured
                black_tool_list = get_context_attribute("black_tool_list")
                if black_tool_list:
                    privileged_operations = [
                        o for o in privileged_operations if o not in black_tool_list
                    ]

            for action in privileged_operations:
                mcp.add_tool(
                    create_tool(action),
                    name=action.lower(),
                    description=_common_sql_exe_param_prompt(action),
                )
    

def _common_sql_exe_param_prompt(action):
    """
    Generate description for action-specific SQL execution tools.

    :param action: The SQL action type
    :return: Formatted tool description
    """
    return f"""
Execute a `{action}` SQL statement
    - sql (str): The {action} SQL. Other operations is not allowed"""


def _common_sql_exe_param_prompt_with_single():
    """
    Generate description for generic SQL execution tool.

    :return: Formatted tool description
    """
    return f"""
Execute any SQL statement
    - sql (str): The SQL statement to run. This SQL string can contain a wide variety of database operations, including but not limited to SELECT, DELETE, UPDATE, INSERT, and other valid SQL commands that interact with the database. It allows for flexible execution of queries tailored to different use cases such as retrieving data, modifying records, or managing database structure.
    The execute tool is designed to run only a single command at a time. If you need to perform multiple operations, such as transactions, they must be executed separately. For example, the following calling is not supported: execute("BEGIN; Query; COMMIT;")
"""
