from typing import Optional, Dict, List, Any
from mcp.server.fastmcp import FastMCP
from sentence_transformers import SentenceTransformer

from db_adapters.base_adapter import BaseAdapter
from db_adapters.db_constants import DB_PRIV_ENUM

# Initialize FastMCP
mcp = FastMCP("MCP-DB-Universal")

class MCPContext:
    """
        Context for MCP server runtime state.
    """
    def __init__(self, db_adapter, semantic_model, adaptive_schema_threshold, user_privilege, disable_privilege_annotation, disable_fine_gran_tool, white_object_dict, black_object_dict, white_tool_list, black_tool_list):
        """
        Initialize context

        :param db_adapter: Database adapter instance for executing queries.
        :param semantic_model: Semantic model for similar value retrieval.
        :param user_privilege: User privilege dictionary fetched from the database.
        :param disable_privilege_annotation: Disable privilege annotation in database schema.
        :param disable_fine_gran_tool: Disable tool modularization for SQL execution.
        :param white_object_dict: Dictionary of whitelisted database objects.
        :param black_object_dict: Dictionary of blacklisted database objects.
        :param white_tool_list: Whitelisted tool names.
        :param black_tool_list: Blacklisted tool names.
        """
        # Database-related
        self.db_adapter: BaseAdapter = db_adapter
        
        # Server-related
        self.semantic_model: Optional[SentenceTransformer] = semantic_model
        self.adaptive_schema_threshold = adaptive_schema_threshold
        self.disable_privilege_annotation = disable_privilege_annotation
        self.disable_fine_gran_tool = disable_fine_gran_tool
        
        # Security-related
        self.user_privilege = user_privilege
        self.white_object_dict = white_object_dict
        self.black_object_dict = black_object_dict
        self.white_tool_list = white_tool_list
        self.black_tool_list = black_tool_list
        
        

# Global context instance (to be initialized when starting server)
context: Optional[MCPContext] = None

# Global supported SQL operations
global_privilege_operations = DB_PRIV_ENUM
