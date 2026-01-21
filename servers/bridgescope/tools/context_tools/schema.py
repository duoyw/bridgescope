import json
from collections import defaultdict
from typing import Dict, Any, Optional

from sqlalchemy.exc import NoSuchTableError

from mcp_constants import top_level_obj_types, obj_type_mapping
from mcp_context import mcp
from db_adapters.db_constants import DB_OBJ_TYPE_ENUM, DB_PRIV_ENUM

from tools.utils import (
    response_type,
    format_response,
    reformat_privilege,
    get_db_adapter,
    get_context_attribute,
)


async def get_database_schema() -> response_type:
    """
    Retrieve the complete database schema.

    :return: A formatted response containing the database schema as SQL DDL statements
    """

    db_adapter = get_db_adapter()

    # Retrieve raw database schema from the database
    db_schema = await db_adapter.get_database_schema()

    # Filter schema by user ACL (first level only for the current version)
    schema_filtered = filter_top_level(db_schema)
    if schema_filtered:
        return format_response(schema_format(schema_filtered))
    else:
        return format_response("No objects can be accessed with current ACL")


async def get_top_level_objects() -> response_type:
    """
    Retrieve the names and types of top-level database objects (tables, views, etc.).

    :return: A formatted response containing a dictionary of object types mapped to
             their respective object names
    """
    db_adapter = get_db_adapter()

    # Retrieve top-level database objects (tables, views, etc.)
    objs = await db_adapter.get_top_level_objects()

    # Filter objects by user ACL
    obj_filtered = filter_top_level(objs)
    if obj_filtered:
        return format_response(object_format(obj_filtered))
    else:
        return format_response("No objects can be accessed with current ACL")


async def get_object_details(object_type: str, object_name: str) -> response_type:
    """
    Retrieve detailed information about a specific database object.

    :param object_type: The type of the database object (e.g., "TABLE")
    :param object_name: The name of the database object to inspect
    :return: A formatted response containing the object's detailed structure as SQL DDL
    """

    db_adapter = get_db_adapter()

    # Check if the requested object type is supported
    if object_type in top_level_obj_types:
        if filter_single(object_type, object_name):

            if object_type in [DB_OBJ_TYPE_ENUM.TABLE, DB_OBJ_TYPE_ENUM.VIEW]:
                # Retrieve table details and format as SQL DDL
                try:
                    details = await db_adapter.get_table_details(object_name)
                except NoSuchTableError:
                    raise RuntimeError(
                    f"{object_type} '{object_name}' not found"
                )
                except Exception:
                    raise

            else:
                raise RuntimeError(
                    f"Cannot retrieve details for {object_type} objects"
                )

            # Filter columns
            details[DB_OBJ_TYPE_ENUM.COL] = filter_columns(object_name, details[DB_OBJ_TYPE_ENUM.COL])

            details = table_schema_format(object_name, details)
            return format_response(details)

        else:
            return format_response(f"{object_type} {object_name} cannot be accessed with current ACL")
    else:
        raise RuntimeError(
            f"Query details for '{object_type}' object is not supported. Supported types: {', '.join(top_level_obj_types)}."
        )


def filter_single(obj_type, obj_name):
    """
    Filter a single object based on user ACL.

    :param obj_type: The type of the database object
    :param obj_name: The name of the database object
    :return: True if the object should be allowed, False if it should be filtered out
    """
    white_object_dict = get_context_attribute("white_object_dict")
    black_object_dict = get_context_attribute("black_object_dict")
    obj_type_ref = obj_type_mapping[obj_type] if obj_type in obj_type_mapping else obj_type

    if white_object_dict:
        # If whitelist exists, only allow objects that are explicitly listed
        if obj_type_ref in white_object_dict:
            return obj_name in white_object_dict[obj_type_ref]
        else:
            return False

    elif black_object_dict:
        # If blacklist exists, deny objects that are explicitly listed
        if obj_type_ref in black_object_dict:
            return obj_name not in black_object_dict[obj_type_ref] or isinstance(black_object_dict[obj_type_ref], dict)
        else:
            return True

    else:
        # No filtering configured, allow access
        return True


def filter_columns(table_name, columns):
    """
    Filter columns by user ACL

    :param columns: List of columns objects
    :return: Filtered list containing only accessible columns
    """

    if not columns:
        return

    white_object_dict = get_context_attribute("white_object_dict")
    black_object_dict = get_context_attribute("black_object_dict")

    if white_object_dict and DB_OBJ_TYPE_ENUM.TABLE in white_object_dict:
        if isinstance(white_object_dict[DB_OBJ_TYPE_ENUM.TABLE], dict) and table_name in white_object_dict[DB_OBJ_TYPE_ENUM.TABLE]:
            white_col_list = white_object_dict[DB_OBJ_TYPE_ENUM.TABLE][table_name].get(DB_OBJ_TYPE_ENUM.COL)
            if white_col_list:
                return [col for col in columns if col['name'] in white_col_list]
    elif black_object_dict and DB_OBJ_TYPE_ENUM.TABLE in black_object_dict:
        if isinstance(black_object_dict[DB_OBJ_TYPE_ENUM.TABLE], dict) and table_name in black_object_dict[DB_OBJ_TYPE_ENUM.TABLE]:
            black_col_list = black_object_dict[DB_OBJ_TYPE_ENUM.TABLE][table_name].get(DB_OBJ_TYPE_ENUM.COL)
            if black_col_list:
                return [col for col in columns if col['name'] not in black_col_list]

    return columns


def filter_top_level(obj_dict):
    """
    Filter top-level database objects based on user ACL.

    :param obj_dict: Dictionary of database object
    :return: Filtered dictionary containing only accessible objects
    """
    filtered_top_level_objs = defaultdict()

    white_object_dict = get_context_attribute("white_object_dict")
    black_object_dict = get_context_attribute("black_object_dict")

    if white_object_dict:
        # If whitelist exists, only allow objects that are explicitly listed
        for obj_type, objs in obj_dict.items():
            obj_type_ref = obj_type_mapping[obj_type] if obj_type in obj_type_mapping else obj_type
            if obj_type_ref in white_object_dict:
                if isinstance(objs, list):
                    filtered_objs = [
                        o for o in objs if o in white_object_dict[obj_type_ref]
                    ]
                else:
                    filtered_objs = {
                        k: v
                        for k, v in objs.items()
                        if k in white_object_dict[obj_type_ref]
                    }
                if filtered_objs:
                    filtered_top_level_objs[obj_type] = filtered_objs
        return filtered_top_level_objs

    elif black_object_dict:
        # If blacklist exists, deny objects that are explicitly listed
        for obj_type, objs in obj_dict.items():
            obj_type_ref = obj_type_mapping[obj_type] if obj_type in obj_type_mapping else obj_type
            if obj_type_ref in black_object_dict and isinstance(black_object_dict[obj_type_ref], list):
                filtered_objs = [
                        o for o in objs if o not in black_object_dict[obj_type_ref]
                    ]

                if filtered_objs:
                    filtered_top_level_objs[obj_type] = filtered_objs
            else:
                filtered_top_level_objs[obj_type] = objs
        return filtered_top_level_objs
    else:
        # No filtering configured, allow all access
        return obj_dict


def count_objects(schema) -> int:
    """
    Count the total number of columns in the database schema as an indicator of #objects.

    :param schema: The database schema
    :return: Total count of all columns
    """
    total_columns = 0

    # Check if schema has COLUMN key and it's a list
    if DB_OBJ_TYPE_ENUM.COL in schema and isinstance(
        schema[DB_OBJ_TYPE_ENUM.COL], list
    ):
        total_columns += len(schema[DB_OBJ_TYPE_ENUM.COL])

    # Recursively check nested dictionaries for COLUMN keys
    for key, value in schema.items():
        if isinstance(value, dict):
            total_columns += count_objects(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    total_columns += count_objects(item)

    return total_columns


async def build_context_retrieval_tool() -> Optional[str]:
    """
    Build and register context retrieval tools.

    :return: None if successful, error message string if fails
    """
    db_adapter = get_db_adapter()
    try:
        db_schema = await db_adapter.get_database_schema()
    except Exception as e:
        return str(e)

    adaptive_schema_threshold = get_context_attribute("adaptive_schema_threshold")
    n_objects = count_objects(db_schema)

    if n_objects <= adaptive_schema_threshold:
        # A single get_schema tool returns the entire database schema
        mcp.add_tool(
            get_database_schema, name="get_schema", description=get_schema_prompt()
        )
    else:
        mcp.add_tool(
            get_top_level_objects, name="get_schema", description=get_schema_prompt()
        )
        mcp.add_tool(
            get_object_details, name="get_object", description=get_object_prompt()
        )

    return None


def object_format(json_input: Dict[str, Any]) -> str:
    """
    Format object presentation as JSON with optional privilege information.

    :param json_input: The dictionary of top-level objects
    :return: The formatted JSON-style object representation
    """
    enable_privilege = not get_context_attribute("disable_privilege_annotation")

    # Add privilege information as comments if enabled
    if enable_privilege:
        obj_2_priv = None
        ref_tables = None

        user_privilege = get_context_attribute("user_privilege")
        if user_privilege:
            obj_2_priv = reformat_privilege(user_privilege)

            # check partial access for tables/views only
            col_2_priv = obj_2_priv.get(DB_OBJ_TYPE_ENUM.COL)
            if col_2_priv:
                ref_tables = [col.split('.')[0] for col in col_2_priv.keys()]

        formatted_objs = defaultdict(list)
        for obj_type, objs in json_input.items():
            obj_type_ref = obj_type_mapping[obj_type] if obj_type in obj_type_mapping else obj_type
            for obj in objs:
                partial_access = False
                if obj_2_priv:
                    if obj_type_ref in obj_2_priv and obj in obj_2_priv[obj_type_ref]:
                        privs = [priv for priv in DB_PRIV_ENUM if priv in obj_2_priv[obj_type_ref][obj]]
                        formatted_objs[obj_type].append(
                            {
                                "Name": obj,
                                "Access": True,
                                "Permissions": "all" if len(privs) == len(DB_PRIV_ENUM.__members__) else privs
                            }
                        )
                        partial_access = True

                    elif obj_type_ref == DB_OBJ_TYPE_ENUM.TABLE and ref_tables and obj in ref_tables:
                        formatted_objs[obj_type].append(
                            {
                                "Name": obj,
                                "Access": "Partial columns",
                            }
                        )
                        partial_access = True

                if not partial_access:
                    formatted_objs[obj_type].append(
                        {
                            "Name": obj,
                            "Access": False,
                        }
                    )

        return json.dumps(dict(formatted_objs))

    else:
        return json.dumps(json_input)


def schema_format(json_input: Dict[str, Any]) -> str:
    """
    Format schema dictionary as SQL DDL with optional privilege annotations.

    :param json_input: The schema dictionary
    :return: The formatted schema
    """
    result = []
    enable_privilege = not get_context_attribute("disable_privilege_annotation")

    table_2_priv = None
    col_2_priv = None
    if enable_privilege:
        user_privilege = get_context_attribute("user_privilege")

        reformat_priv = reformat_privilege(user_privilege)
        table_2_priv = reformat_priv[DB_OBJ_TYPE_ENUM.TABLE]
        col_2_priv = reformat_priv[DB_OBJ_TYPE_ENUM.COL]

    for table_name, table_data in json_input[DB_OBJ_TYPE_ENUM.TABLE].items():
        # Filter columns
        table_data[DB_OBJ_TYPE_ENUM.COL] = filter_columns(table_name, table_data[DB_OBJ_TYPE_ENUM.COL])
        result.append(table_schema_format(table_name, table_data, table_2_priv, col_2_priv))

    return "\n\n".join(result)


def table_schema_format(
    table_name, table_info: Dict[str, Any], table_2_priv=None, col_2_priv=None
) -> str:
    """
    Format table information as SQL DDL with optional privilege annotations.

    :param table_name: The name of the table to format
    :param table_info: Dictionary containing table structure information including columns, keys, and indexes
    :param table_2_priv: Optional dictionary mapping table names to their privileges
    :param col_2_priv: Optional dictionary mapping column names to their privileges
    :return: Formatted SQL DDL string for the table
    """

    enable_privilege = not get_context_attribute("disable_privilege_annotation")
    lines = []

    # Add privilege information as comments if enabled
    if enable_privilege:
        user_privilege = get_context_attribute("user_privilege")
        if user_privilege:
            reformat_priv = reformat_privilege(user_privilege)
            if not table_2_priv:
                table_2_priv = reformat_priv.get(DB_OBJ_TYPE_ENUM.TABLE)
            if not col_2_priv:
                col_2_priv = reformat_priv.get(DB_OBJ_TYPE_ENUM.COL)

        partial_access = False
        if table_2_priv and table_name in table_2_priv:
            privs = [priv for priv in DB_PRIV_ENUM if priv in table_2_priv[table_name]]
            if len(privs) == len(DB_PRIV_ENUM.__members__):
                lines.append(f"-- Access: True, Permissions: all")
            else:
                lines.append(f"-- Access: True, Permissions: {', '.join(privs)}")

            partial_access = True

        elif col_2_priv:
            for col in col_2_priv.keys():
                if col.split('.')[0] == table_name:
                    lines.append(f"-- Access: Partial columns")
                    partial_access = True
                    break

        if not partial_access:
            lines.append(f"-- Access: False")

    # Start table definition
    lines.append(f"CREATE TABLE {table_name} (")

    # Process columns
    if DB_OBJ_TYPE_ENUM.COL in table_info and table_info[DB_OBJ_TYPE_ENUM.COL]:
        column_definitions = []
        for col in table_info[DB_OBJ_TYPE_ENUM.COL]:
            col_identifier = f"{table_name}.{col['name']}"

            if not isinstance(col, dict) or "name" not in col or "type" not in col:
                continue  # Skip invalid column definitions

            col_def = f"{col['name']} {col['type']}"

            # Add NOT NULL constraint
            if "nullable" in col and not col["nullable"]:
                col_def += " NOT NULL"

            if enable_privilege and col_2_priv and col_identifier in col_2_priv:
                col_def += f" -- Permissions: {', '.join(col_2_priv[col_identifier])}"

            column_definitions.append("    " + col_def)

        lines.extend(column_definitions)

    # Add primary key constraint
    if DB_OBJ_TYPE_ENUM.PK in table_info and table_info[DB_OBJ_TYPE_ENUM.PK]:
        pk_columns = table_info[DB_OBJ_TYPE_ENUM.PK]
        if isinstance(pk_columns, list) and pk_columns:
            lines.append("    PRIMARY KEY (" + ", ".join(pk_columns) + ")")

    # Add foreign key constraints
    if DB_OBJ_TYPE_ENUM.FK in table_info and table_info[DB_OBJ_TYPE_ENUM.FK]:
        for fk in table_info[DB_OBJ_TYPE_ENUM.FK]:
            if isinstance(fk, dict) and all(
                key in fk for key in ["local_column", "remote_table", "remote_column"]
            ):
                fk_line = f"    FOREIGN KEY ({fk['local_column']}) REFERENCES {fk['remote_table']}({fk['remote_column']})"
                lines.append(fk_line)

    lines.append(");")

    # Add indexes
    if DB_OBJ_TYPE_ENUM.INDEX in table_info and table_info[DB_OBJ_TYPE_ENUM.INDEX]:
        for idx in table_info[DB_OBJ_TYPE_ENUM.INDEX]:
            if isinstance(idx, dict) and "name" in idx and "columns" in idx:
                unique_keyword = "UNIQUE " if idx.get("unique", False) else ""
                columns_str = (
                    ", ".join(idx["columns"])
                    if isinstance(idx["columns"], list)
                    else str(idx["columns"])
                )
                index_line = f"CREATE {unique_keyword}INDEX {idx['name']} ON {table_name}({columns_str});"
                lines.append(index_line)

    return "\n".join(lines)


def get_schema_prompt():
    """
    Generate the prompt description for the get_schema tool.

    :return: Prompt for the get_schema tool
    """
    return f"""Retrieve the database schemas"""


def get_object_prompt():
    """
    Generate the prompt description for the get_object tool.

    :return: Prompt for the get_object tool
    """
    return f"""
Retrieve the details of a given object
    - object_type (str): The type of the queried object, e.g., "TABLE"
    - object_name (str): The name of the queried object
"""
