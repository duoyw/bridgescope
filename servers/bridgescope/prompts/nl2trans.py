from mcp import GetPromptResult
from mcp.types import PromptMessage, TextContent

from mcp_context import mcp


@mcp.prompt()
def nl2trans(task: str, knowledge: str | None = None) -> GetPromptResult:
    """Convert a natural language task into a database operation.

    Args:
        task (str): The natural language question or task.
        knowledge (str): External knowledge for finishing the task.
    """

    prompt = f"""
Act as a database expert, generate and execute SQL statements to answer the following question or accomplish the given task:

### Task Description:
{task}. If the task fails, all database changes should be rolled back to ensure transactional atomicity.

### Reference Knowledge:
{knowledge}

--

### Core Principles:
 1. **Before generating any SQL query, gather sufficient information to ensure the statement is valid and accurate**
    - Make sure you have sufficient permissions to access database operations;
    - Fully understand the structure of the relevant tables. If uncertain, use tools to inspect the database first;
    - If you need to filter or match specific values in a column, first check what actual values exist in that column. Do not guess or assume values;
 2. **Ensure the generated SQL is executable**: 
    - The SQL must be syntactically correct for **PostgreSQL**.
    - It must be compatible with the available execution tools.
 3. If the task requires an database permission(e.g., insert) that is **not supported by any available tool, or lack the permission for related resources(e.g. table) **, **abort immediately** and do not attempt any preparatory actions.
 4. **Once enough information is gathered, generate a single, complete SQL statement that directly fulfills the task and execute it as a whole**
"""

    return GetPromptResult(
        messages=[
            PromptMessage(
                role="user",
                content=TextContent(type="text", text=prompt),
            )
        ],
    )
