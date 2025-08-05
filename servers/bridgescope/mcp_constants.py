import mcp.types as types
from typing import List

from db_adapters.db_constants import DB_OBJ_TYPE_ENUM

# Response format for each tool
response_type = List[types.TextContent]
default_res = "Done"

# Default threshold for adaptive schema retrieval
schema_scale_threshold = 200

# Types of top-level object supported
top_level_obj_types = [DB_OBJ_TYPE_ENUM.TABLE, DB_OBJ_TYPE_ENUM.VIEW]

# A dictionary mapping object types to equivalent types that share the same
# processing logic in the database-side.
obj_type_mapping = {
    DB_OBJ_TYPE_ENUM.VIEW: DB_OBJ_TYPE_ENUM.TABLE
}