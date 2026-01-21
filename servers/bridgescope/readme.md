# BridgeScope: A Universal Toolkit for Bridging Large Language Models and Databases

## Overview

BridgeScope is a universal toolkit that bridges LLMs and databases towards greater usability, efficiency, and security guarantees. It aims to help general-purpose LLM agents (see below figure) handling data-related tasks more effectively and efficiently.

<div align="center">
<img src="figure/general_agent.png" width="350" loc="center">
</div>

### Current Challenges

#### 1. **Coarse-Grained Tooling**

Current database toolkits offer a single, straightforwardly implemented `execute_sql` tool for executing SQL queries, which leads to two major issues:

- **Security risks**: LLMs gain full user privileges, increasing the risks of accessing sensitive data or performing destructive operations, such as `DROP DATABASE` due to hallucinations and prompt injections. [See a recent incident.](https://news.ycombinator.com/item?id=44625119)

- **Cognitive overload**: One tool handling all database operations (queries and managing transactions, ...) increases the LLM's cognitive load and heightens their tool-selection errors.

#### 2. **Privilege-Unaware Planning**

The LLM agents lack intrinsic awareness of users' privileges in the database, and thereby may generate pipelines that exceed authorized permissions (e.g., querying unauthorized tables). Although the database engine will eventually reject such operations, generating infeasible plans incurs non-negligible overhead for the LLM, especially when privilege violations occur late in the task execution workflow.

#### 3. **Data Transmission Bottlenecks**

The current agent architecture, which relies on LLMs to exchange data between tools, is particularly problematic for data-intensive workflows because the LLM's limited context window can be quickly exhausted and cause task failures. Additionally, LLM hallucinations may introduce errors during transmission, further compromising the reliability of this paradigm. 

### How BridgeScope Solves These Challenges?

BridgeScope addresses these challenges through three core innovations:

#### **1. Fine-Grained Tooling.**
Modularizes SQL operations into fine-grained tools for **context retrieval**, **CRUD execution**, and **ACID-compliant transaction management**, enabling more precise and LLM-friendly control over database functionalities.

#### **2. Multi-level Security Guarantees**

Customizes tool implementations based on both **database-side user privileges** and **user-side security policies**. Both prompts the LLM planning within authorized and secure operational boundaries and employs rule-based verifications to strictly block unauthorized access and risky operations. 

#### **3. Direct Data Routing**

Introduces proxy-based mechanisms that enables seamless inter-tool data transmission, eliminating LLM transmission bottlenecks. See [MCP-Proxy-Execute](../ProxyServer/README.md) for details.


## Core Functionalities and MCP Interfaces

BridgeScope supports the below four categories of database functionalities, each implemented with a dedicated set of tools.


### 🔍 **Context Retrieval**

Retrieve task-related context as essential background for SQL generation by the LLM. Key features: 

- **Adaptive Schema Retrieval**: Automatically switches between full database schema and hierarchical object-level retrieval based on database size. 

- **Secure Awareness**: Schema results are annotated with user's privileges, and only database objects complying with the user's configured ACL are exposed to the LLM, providing the LLM with clear awareness of access boundaries and guiding it toward compliant operations and reducing hallucinated restricted access.

- **Column Exemplar Search**: AI-powered semantically similar column exemplar search for accurate predicate formulation. 


#### **Tools**:

- **`get_schema`**: Retrieve database schema 
  - **Small databases** (≤ a user-specified threshold, default 200): Returns complete schema (tables, views, columns, keys, and indexes) as SQL DDL with privilege annotations
    ```sql
    -- Access: True, Permissions: SELECT
    CREATE TABLE brand_A_sales (
        date DATE PRIMARY KEY,
        item_id INT NOT NULL,
        amount DECIMAL(10,2) -- Permissions: UPDATE
        FOREIGN KEY (item_id) REFERENCES brand_A_items(id)
    );
    CREATE INDEX idx_sales_date ON brand_A_sales(date);
    
    -- Access: False  
    CREATE TABLE brand_B_sales (...);
    
    -- Access: Partial columns
    CREATE TABLE user_profiles (...);
    ```
  
  - **Large databases** (> threshold): Returns top-level objects including tables and views in JSON format
    ```json
    {
      "TABLE": [
        {"Name": "orders", "Access": true, "Permissions": ["SELECT", "INSERT"]},
        {"Name": "customers", "Access": "Partial columns"},
        {"Name": "admin_logs", "Access": false}
      ],
      "VIEW": [
        {"Name": "sales_summary", "Access": true, "Permissions": "all"}
      ]
    }
    ```

- **`get_object`**: Retrieve detailed information about a specific database object
  - **Parameters**: 
    - `object_type` (str): Object type - "TABLE" or "VIEW"
    - `object_name` (str): Name of the database object
  - **Returns**: Detailed object structure as SQL DDL. 
 
- **`search_relative_column_values`**: Find semantically similar values in database columns
  - **Parameters**: `column_2_value` (Dict[str, Any]) - Mapping of "table.column" to target values
  - **Returns**: Top-5 most similar values for each table.column
  - **Example Input**: 
    ```json
    {
      "products.name": "wireless headphones",
      "categories.description": "audio devices"
    }
    ```
  - **Example Output**:
    ```json
    {
      "name": ["wireless headphones", "bluetooth earphones", "noise-canceling headset", "earbuds", "audio headphones"],
      "description": ["audio devices", "sound equipment", "speaker systems", "headphones", "amplifiers"]
    }
    ```


### ⚡ **SQL Execution**
Execute a SQL statement. Key features: 

- **Fine-Grained Action Control**: Separate tools for exclusively handling SELECT, INSERT, UPDATE, DELETE operations. 
- **Flexible Tool Configuration**: Expose tools aligned with both the database-side user privilege and the user-side configured ACL to give the LLM an inherent understanding of its operational boundaries and discourage attempts at unauthorized or risky operations.
- **Security Guarantee**: Both privilege and user ACL violations are checked upon tool execution, which not only intercepts unauthorized operations, reducing the burden on the database, but also complements the database's native security mechanisms by additionally blocking dangerous actions specified by users.

#### **Tools**:

Current BridgeScope offers the below tools for handling CRUD SQL executions:
- **`select`**: Execute SELECT queries for data retrieval
- **`insert`**: Execute INSERT statements for data creation  
- **`update`**: Execute UPDATE statements for data modification
- **`delete`**: Execute DELETE statements for data removal
- **Parameters** (for each tool): `sql` (str) - The SQL statement to execute (must match tool type)
- **Returns** (for each tool): Queried data or #rows affected

Or a single generic tool if explicitly configured (with `--disable_fine_gran_tool`):

- **`execute`**: Execute any SQL statement
- **Parameters**: `sql` (str) - Any valid SQL statement
- **Returns** (for each tool): Queried data or #rows affected

### 🔒 **Transaction Control**

Follows traditional database transaction controls and provides distinct begin, commit, and rollback tools to manage transactions. The ACID properties of transactions are guaranteed inherently by the database engine. 

**Tools** (configured with `--enable_trans`):
- **`begin`**: Start a new database transaction
- **`commit`**: Commit the current transaction  
- **`rollback`**: Roll back the current transaction
- **Returns**: Confirmation messages for each operation

### **🔄 Data Transmission**

Direct data transmission between tools without LLM involvement. For details on the mechanism and tools, refer to [MCP-Proxy-Execute](../ProxyServer/README.md). This tool is applicable for any data-intensive transfer scenarios and, as such, has been isolated into a dedicated MCP server.

### 📝 **Prompts**

BridgeScope provides specialized prompts for data-related scenarios to guide LLMs in performing database operations safely and effectively.

#### `nl2all`
A general-purpose prompt that guides the LLM to understand natural language requests and convert them into appropriate database operations. It emphasizes context gathering, generating executable SQL statements, SQL-tool alignment, and early termination for infeasible tasks. 

- **Parameters**: 
  - `task` (str): The natural language question or task to be accomplished
  - `knowledge` (str): External knowledge or context information to help complete the task


#### `nl2trans`
Similar to `nl2all` but additionally prompts the LLM to handle database modifications with proper transaction controls.

- **Parameters**: The same to `nl2all`

#### `model_train_proxy_prompt`
Guide NL2ML tasks with proxy-based data transfer, particularly useful for large-scale data transmissions.

- **Parameters**: 
  - `question` (str): The natural language question or request related to model training


## Quick Integration  

### Usage in Desktop Applications (Claude, VSCode, Cursor, etc.)

Add the following configuration to your MCP server settings:

```json
{
  "mcpServers": {
    "BridgeScope": {
      "command": "python",
      "args": ["/path/to/server.py", "--dsn", "postgresql://user:password@host:port/database"]
    }
  }
}
```

## Configuration Parameters

### Database Connection

#### `--dsn` (str, optional)
Database connection string in DSN format. If provided, overrides individual connection parameters.
```
postgresql://username:password@host:port/database
```

#### Individual Connection Parameters
- `--usr` (str): Username for database authentication
- `--pwd` (str): Password for the database user  
- `--host` (str): Hostname of the database server
- `--port` (str): Port number for the database server
- `--db` (str): Database name
- `--type` (str): Database type (default: "postgresql")

### Server Configuration

#### `--transport` (str)
Transport for MCP communication.
- **Options**: "stdio" (default), "sse"


#### `--sse_host` (str, optional)
Host address for SSE transport.
- **Default**: "localhost"
- **Required when**: Using `--transport sse`

#### `--sse_port` (str, optional)
Port number for SSE transport.
- **Default**: 10800
- **Required when**: Using `--transport sse`


#### `--n` (int)
Threshold for adaptive schema retrieval (default: 200).
- **Below threshold**: `get_schema` tool returns complete database schema
- **Above threshold**: `get_schema` tool returns only names of top-level objects.

#### `--mp` (str)
Path to semantic model for similar column value retrieval.

- **Default**: If not provided, use the default [paraphrase-MiniLM-L3-v2 model](/resources/paraphrase-MiniLM-L3-v2)

#### `--persist` (flag)
Always persist database changes immediately. Use with caution! 
- **Default**: Disabled (readonly mode with automatic rollback)

### Security and Access Control

#### Tool Configuration
- `--disable_fine_gran_tool` (flag): Disable fine-grained SQL tools, expose only generic `execute` tool
  - **Default**: False
- `--disable_tool_priv` (flag): Disable privilege annotations in database schema output
  - **Default**: False

#### Object Access Control Lists (ACL)
- `--wo` (str): Whitelist of accessible database objects (raw str or file path)
- `--bo` (str): Blacklist of forbidden database objects (raw str or file path)
- **Format:** JSON format with two supported structures:

  **Simple list format:**
  ```json
  {
    "TABLE": ["orders", "customers", "products"],
    "VIEW": ["sales_summary", "user_profiles"]
  }
  ```

  **Detailed format with column-level control:**
  ```json
  {
    "TABLE": {
      "orders": {
        "COLUMN": ["id", "customer_id", "total"]
      },
      "customers": {
        "COLUMN": ["id", "name", "email"]
      }
    }
  }
  ```

#### Tool Access Control Lists
- `--wt` (str): Whitelist of permitted tools (raw str or file path)  
- `--bt` (str): Blacklist of forbidden tools (raw str or file path)
- **Format:** Comma-separated tool names:
  ```
  select,insert,update
  ```

  Or as a JSON array:
  ```json
  ["select", "insert", "update"]
  ```

### Transaction Management

#### `--disable_trans` (flag)
Disable transaction control tools (`begin`, `commit`, `rollback`) and 
`nl2trans` prompt.
- **Default**: Disabled


## Extending BridgeScope

### 1. Adding New Database Adapters

An implementation for **PostgreSQL** has been included currently. To support more databases: 

1. Choose the appropriate base class:
   - Inherit from `BaseAdapter` in `db_adapters/base_adapter.py` for completely custom database implementations
   - Inherit from `SQLAlchemyAdapter` in `db_adapters/sqlalchemy_adapter.py` for databases already supported by SQLAlchemy
2. Implement required methods for your database type
3. Register the adapter in `db_adapters/registry.py`
4. Add connection string parsing in `db_adapters/db_config.py`

### 2. Custom Security Policies

Extend the ACL parser in `acl_parser.py` to support more diverse and fine-grained access controls. To implement custom security policies:

1. Modify the `ACLParser` class to handle your custom access control rules
2. Add new verification logic for your specific security configurations to SQL tools

### 3. Additional Tools

Add new tools by:

1. Creating tool functions in `tools/`
2. Registering with the MCP server using `@mcp.tool()` decorator
3. Adding necessary security checks and privilege validation:
   - Implement user privilege verification
   - Add ACL compliance checks
   - Validate input parameters for security risks
   - Include proper error handling for unauthorized access attempts

## Contributions

We welcome contributions! Please submit PRs or issues on GitHub.

## Contact

For questions, issues, or collaboration opportunities:

📧 **Email**: lianggui.wlg@alibaba-inc.com  
