import os
import json
from enum import Enum

from mcp_constants import top_level_obj_types, obj_type_mapping
from mcp_context import global_privilege_operations


class ACLParseError(Exception):
    """Custom exception for ACL parsing errors."""

    pass


class ACLType(Enum):
    """
    Target of access controls
    """

    TOOL = "tool"
    OBJECT = "object"


class ACLParser:
    @staticmethod
    def parse(acl_str, acl_type=ACLType.TOOL):
        """
        Parse an ACL string or file content into structured ACL data.

        :param acl_str: A string representing the ACL. It can be:
                        - For tool ACL: a comma-separated string like "action1,action2,..."
                        - For object ACL: a JSON string like:
                          {
                              "TABLE": {
                                  "table_name": {
                                      "columns": [col1, col2, ...] # most-inner level object, use list
                                  }
                              }
                          }
                        - Or a file path pointing to a file containing the above formats.
        :param acl_type: An ACLType enum indicating whether the ACL is for a tool or an object.
                         Defaults to ACLType.TOOL.
        :return: A list of operations (for tool ACL) or a dictionary (for object ACL).
        :raises ACLParseError: If the input is invalid or cannot be parsed.
        """
        if not acl_str:
            return [] if acl_type == ACLType.TOOL else {}

        # Handle file path input
        if os.path.exists(acl_str):
            try:
                with open(acl_str, "r", encoding="utf-8") as f:
                    acl_str = f.read().strip()
            except Exception as e:
                raise ACLParseError(
                    f"Failed to read ACL file '{acl_str}': {str(e)}"
                ) from e

        if not acl_str:
            return [] if acl_type == ACLType.TOOL else {}

        try:
            if acl_type == ACLType.TOOL:
                # Parse tool ACL
                if acl_str[0] == "[":
                    acl_str = acl_str[1:-1]

                acl_list = [
                    item.strip().strip('"')
                    for item in acl_str.split(",")
                    if item.strip()
                ]
                # Filter valid operations
                valid_operations = [
                    op for op in acl_list if op in global_privilege_operations
                ]
                return valid_operations
            else:
                # Parse object ACL
                acl_dict = json.loads(acl_str)
                if not isinstance(acl_dict, dict):
                    raise ACLParseError("Object ACL must be a JSON object")

                # Validate structure - support both nested dict and simple list formats
                for obj_type, obj_content in acl_dict.items():
                    if obj_type.upper() not in top_level_obj_types:
                        raise ACLParseError(f"Unsupported object type: {obj_type}")

                    # Support two formats:
                    # 1. Simple list format: {"TABLE": ["table1", "table2"]}
                    # 2. Nested dict format: {"TABLE": {"table1": {"COLUMN": []}}}
                    if isinstance(obj_content, list):
                        # Simple list format - validate all items are strings
                        for item in obj_content:
                            if not isinstance(item, str):
                                raise ACLParseError(
                                    f"All items in '{obj_type}' list must be strings"
                                )
                    elif isinstance(obj_content, dict):
                        # Nested dict format - validate second level
                        for obj_name, obj_details in obj_content.items():
                            if not isinstance(obj_details, dict):
                                raise ACLParseError(
                                    f"Details for '{obj_type} {obj_name}' must be a dictionary"
                                )
                    else:
                        raise ACLParseError(
                            f"Content for '{obj_type}' must be either a list or a dictionary"
                        )

                # Merge equivalent objects
                for obj_type, ref_obj_type in obj_type_mapping.items():
                    if obj_type in acl_dict:
                        obj_content = acl_dict[obj_type]
                        if ref_obj_type in acl_dict:
                            if isinstance(obj_content, list):
                                acl_dict[ref_obj_type].extend(obj_content)
                            else:
                                acl_dict[ref_obj_type].update(obj_content)

                        else:
                            acl_dict[ref_obj_type] = obj_content

                return {
                    obj_type: obj_content
                    for obj_type, obj_content in acl_dict.items()
                    if obj_type not in obj_type_mapping
                }

        except json.JSONDecodeError as e:
            raise ACLParseError(f"Invalid JSON format: {str(e)}") from e
        except Exception as e:
            if isinstance(e, ACLParseError):
                raise
            raise ACLParseError(
                f"Failed to parse {acl_type.value} ACL: {str(e)}"
            ) from e
