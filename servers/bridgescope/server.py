import os
import signal
import sys
import argparse
import asyncio

from sentence_transformers import SentenceTransformer

from loguru import logger

# Database adapters
import db_adapters.pg_adapter
from db_adapters.db_config import DBConfig
from db_adapters.db_exception import DatabaseError
from db_adapters.registry import get_adapter_instance

# MCP constants and modules
from acl_parser import ACLParser, ACLType, ACLParseError
from mcp_context import mcp, MCPContext
from mcp_constants import schema_scale_threshold
import mcp_context

# Tools and prompt templates
from tools.context_tools.schema import build_context_retrieval_tool
from tools.execution_tools import build_sql_exec_tools
from tools.utils import get_context_attribute


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments for MCP server configuration.
    """
    parser = argparse.ArgumentParser(description="Start the MCP Server.")

    ##### Server settings #####

    # Transport type
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse"],
        help="Transport type: stdio (default) or sse",
        default="stdio",
    )

    parser.add_argument(
        "--sse_host",
        type=str,
        help="Host for sse transportation (default: localhost)",
        default="localhost"
    )

    parser.add_argument(
        "--sse_port",
        type=int,
        help="Port for sse transportation (default: 10800)",
        default=10800
    )

    # Disable privilege annotations
    parser.add_argument(
        "--disable_tool_priv",
        action="store_true",
        help="Disable privilege annotations in database schema",
        default=False,
    )

    # Disable fine-grained SQL execution tools
    parser.add_argument(
        "--disable_fine_gran_tool",
        action="store_true",
        help="Disable fine-grained tools (only offers the 'execute_sql' tool).",
        default=False,
    )

    # Disable transaction control
    parser.add_argument(
        "--disable_trans",
        action="store_true",
        help="Disable transaction management tools and prompts.",
        default=False,
    )

    # Semantic model path
    parser.add_argument(
        "--mp", type=str, help="Path of the semantic model for similar value retrieval."
    )

    ##### Database settings #####
    # for db connection
    parser.add_argument(
        "--dsn",
        type=str,
        help="DSN for database connection. If provided, overrides individual connection arguments (--usr, --pwd, --host, --port, --db, --type).",
    )
    parser.add_argument("--usr", type=str, help="Username for database authentication.")
    parser.add_argument("--pwd", type=str, help="Password for the database user.")
    parser.add_argument("--host", type=str, help="Hostname of the database server.")
    parser.add_argument("--port", type=str, help="Port number for the database server.")
    parser.add_argument("--db", type=str, help="Database name.")
    parser.add_argument(
        "--type",
        type=str,
        help="Type of the database, e.g., 'postgresql'.",
        default="postgresql",
    )

    parser.add_argument(
        "--persist",
        action="store_true",
        help="Always persist database changes immediately (risky!).",
        default=False,
    )

    # Threshold for adaptive schema retrieval
    parser.add_argument(
        "--n",
        type=int,
        help=f"Threshold for adaptive schema retrieval. Default is f{schema_scale_threshold}.",
        default=schema_scale_threshold,
    )

    # user security policy
    parser.add_argument(
        "--wo", type=str, help="Whitelist of accessible database objects."
    )
    parser.add_argument("--wt", type=str, help="Whitelist of permitted tools.")
    parser.add_argument(
        "--bo", type=str, help="Blacklist of forbidden database objects."
    )
    parser.add_argument("--bt", type=str, help="Blacklist of forbidden tools.")

    args = parser.parse_args()
    if not args.dsn:
        required_fields = ["usr", "pwd", "host", "port", "type", "db"]
        for field in required_fields:
            if not getattr(args, field):
                parser.error(
                    f"Missing required argument: --{field} or specifying dsn directly."
                )

    if args.transport == 'sse' and not (args.sse_host and args.sse_port):
        parser.error(
            "Both host and port must be specified for the sse transportation mode."
        )

    return args


async def init_global_server_context(args):
    """
    Initialize server context
    """

    # prepare database adapter
    db_config = DBConfig(readonly=not args.persist)
    if args.dsn:
        # If --dsn is provided, use it directly and ignore other parameters
        db_config.build_from_dsn(args.dsn)
    else:
        db_config.build(args.type, args.host, args.port, args.usr, args.pwd, args.db)

    db_adapter = get_adapter_instance(db_config)
    try:
        await db_adapter.connect()
        logger.info("Initiate database connection")
    except DatabaseError as e:
        logger.error(
            f"Could not connect to database: {str(e)}.",
        )
        sys.exit(1)

    # prepare semantic model
    semantic_model_path = getattr(args, "mp")
    semantic_model = None
    try:
        if not semantic_model_path or not os.path.exists(semantic_model_path):
            logger.info(
                f"Invalid path for the semantic model. Use `paraphrase-MiniLM-L3-v2` by default.",
            )
            current_dir = os.path.dirname(os.path.abspath(__file__))
            semantic_model_path = os.path.join(current_dir,"resources/paraphrase-MiniLM-L3-v2")

        semantic_model = SentenceTransformer(semantic_model_path)
    except Exception as e:
        logger.warning(
            f"Could not initialize the semantic model for value retrieval: {str(e)}. The search_relative_column_values tool is disabled.",
        )

    schema_threshold = getattr(args, "n")

    try:
        user_privilege = await db_adapter.get_user_privileges()
    except DatabaseError as e:
        logger.error(
            f"Could not retrieval user privilege: {str(e)}.",
        )
        sys.exit(1)

    disable_privilege_annotation = getattr(args, "disable_tool_priv")
    disable_fine_gran_tool = getattr(args, "disable_fine_gran_tool")

    try:
        white_object_dict = ACLParser.parse(getattr(args, "wo"), ACLType.OBJECT)
        black_object_dict = ACLParser.parse(getattr(args, "bo"), ACLType.OBJECT)

        white_tool_list = ACLParser.parse(getattr(args, "wt"), ACLType.TOOL)
        black_tool_list = ACLParser.parse(getattr(args, "bt"), ACLType.TOOL)
    except ACLParseError as e:
        logger.error(f"Failed to parse ACL configuration: {str(e)}")
        sys.exit(1)

    ctx = MCPContext(
        db_adapter,
        semantic_model,
        schema_threshold,
        user_privilege,
        disable_privilege_annotation,
        disable_fine_gran_tool,
        white_object_dict,
        black_object_dict,
        white_tool_list,
        black_tool_list,
    )

    return ctx


async def run_server():
    """
    Main asynchronous server routine.
    """
    ## Initialize server context
    args = parse_args()
    cxt = await init_global_server_context(args)
    mcp_context.context = cxt

    ## Initialize tools and prompts
    import prompts.modelTrain

    # context retrieval tools
    error = await build_context_retrieval_tool()
    if error:
        logger.error(f"Could not initialize schema retrieval tool: {error}.")
        sys.exit(1)

    if cxt.semantic_model:
        import tools.context_tools.column_value

    # SQL execution tools
    build_sql_exec_tools()

    # transaction management tools
    if getattr(args, "disable_trans"):
        import prompts.nl2all
    else:
        import tools.transaction_tools
        import prompts.nl2trans

    ## Shutdown handling
    shutdown_initiated = False

    async def shutdown(received_signal=None):
        return
        """
        Shutdown handler for the MCP server.
        
        Args:
            received_signal: The signal that triggered the shutdown
        """
        nonlocal shutdown_initiated

        # Prevent multiple shutdown attempts
        if shutdown_initiated:
            logger.warning("Shutdown already initiated. Forcing immediate exit.")
            os._exit(1)

        shutdown_initiated = True

        if received_signal:
            logger.info(f"Received exit signal {received_signal.name}, starting shutdown.")

        # Close database connections
        try:
            db_adapter = get_context_attribute("db_adapter")
            if db_adapter:
                await db_adapter.close()
                logger.info("Database connection closed successfully")
        except Exception as e:
            logger.error(f"Error during database cleanup: {e}.")

        # Exit with appropriate status code
        os._exit(128 + received_signal.value if received_signal else 0)

    # Register signal handlers for graceful shutdown
    try:
        loop = asyncio.get_running_loop()
        shutdown_signals = (signal.SIGTERM, signal.SIGINT)
        
        for sig in shutdown_signals:
            loop.add_signal_handler(
                sig, 
                lambda s=sig: asyncio.create_task(shutdown(s))
            )
        
    except NotImplementedError:
        # Signal handling is not supported on Windows
        logger.warning("Signal handling not supported on this platform (Windows).")
    except Exception as e:
        logger.error(f"Failed to register signal handlers: {e}.")

    # Run the server with the selected transport
    if args.transport == "stdio":
        await mcp.run_stdio_async()
    else:
        mcp.settings.host = args.sse_host
        mcp.settings.port = args.sse_port
        await mcp.run_sse_async()


if __name__ == "__main__":
    asyncio.run(run_server())
