from collections import defaultdict
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from db_adapters.db_config import DBConfig
from db_adapters.db_constants import DB_ENUM, DB_OBJ_TYPE_ENUM
from db_adapters.db_exception import DatabaseError, DatabaseConnectionError
from db_adapters.registry import register
from db_adapters.sqlalchemy_adapter import SqlAlchemyAdapter


@register(DB_ENUM.PG)
class PostgresAdapter(SqlAlchemyAdapter):
    """
    PostgreSQL-specific implementation of the SqlAlchemyAdapter.
    """
    def __init__(
        self,
        config: DBConfig,
        args: Dict[str, Any] = None
    ):
        """
        Initialize PostgreSQL adapter.

        :param config: Database configuration instance
        :param args: Additional engine arguments
        """
        super().__init__(config, args)

    async def get_user_privileges(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Query the current user's privileges in the PostgreSQL database.

        :return: Dict mapping privilege types to object types and their object lists.
                Structure: {
                    'SELECT': {
                        'TABLE': ['public.table1', 'public.table2'],
                        'COLUMN': ['public.table3.col1', 'public.table3.col2']
                    },
                    ...
                }
        :rtype: Dict[str, Dict[str, List[str]]]
        :raises DatabaseConnectionError: If database is not connected
        :raises DatabaseError: If query user privilege fails
        """
        if not self.session_factory:
            raise DatabaseConnectionError()

        try:
            async with self.session_factory() as session:

                # Get the current user from the session
                result = await session.execute(text("SELECT current_user;"))
                user_result = result.fetchone()

                if not user_result or not user_result[0]:
                    raise DatabaseError("Failed to retrieve current user.")
                current_user = user_result[0]

                # Query all table/column privileges in one query for efficiency
                privilege_query = text("""
                                SELECT 
                                    grantee,
                                    object_type,
                                    privilege_type,
                                    table_schema || '.' || table_name AS table_with_schema,
                                    column_name
                                FROM (
                                    -- Table privileges
                                    SELECT grantee, 'TABLE'::text AS object_type, privilege_type, table_schema, table_name, NULL AS column_name
                                    FROM information_schema.role_table_grants
                                    WHERE grantee = :user

                                    UNION ALL

                                    -- Column privileges 
                                    SELECT grantee, 'COLUMN'::text AS object_type, privilege_type, table_schema, table_name, column_name
                                    FROM information_schema.column_privileges
                                    WHERE grantee = :user
                                ) AS all_perms
                                WHERE grantee IS NOT NULL;
                            """)

                results = await session.execute(privilege_query, {"user": current_user})
                privilege_rows = results.fetchall()

                # Organize results by privilege type and object type
                privileges_dict = defaultdict(lambda: defaultdict(list))

                for row in privilege_rows:
                    grantee, object_type, privilege_type, table_with_schema, column_name = row
                    if not grantee or not privilege_type:
                        continue

                    # Handle table-level privileges
                    if object_type == DB_OBJ_TYPE_ENUM.TABLE:
                        if table_with_schema not in privileges_dict[privilege_type][object_type]:
                            privileges_dict[privilege_type][object_type].append(table_with_schema)

                    # Handle column-level privileges
                    elif object_type == DB_OBJ_TYPE_ENUM.COL:
                        # Skip column privilege if table-level privilege already exists
                        if privilege_type in privileges_dict and DB_OBJ_TYPE_ENUM.TABLE in privileges_dict[privilege_type] and \
                            table_with_schema in privileges_dict[privilege_type][DB_OBJ_TYPE_ENUM.TABLE]:
                            continue

                        column_identifier = f"{table_with_schema}.{column_name}"
                        if column_identifier not in privileges_dict[privilege_type][object_type]:
                            privileges_dict[privilege_type][object_type].append(column_identifier)

                # Clean up empty privilege categories
                cleaned_privileges = {}
                for privilege_type, objects in privileges_dict.items():
                    cleaned_objects = {
                        object_type: object_list
                        for object_type, object_list in objects.items()
                        if object_list
                    }
                    if cleaned_objects:
                        cleaned_privileges[privilege_type] = cleaned_objects

                return cleaned_privileges

        except SQLAlchemyError as e:
            raise DatabaseError(f"Failed to fetch PostgreSQL user privileges: {e}") from e
        except Exception as e:
            raise DatabaseError(f"Unexpected error while fetching user privileges: {e}") from e