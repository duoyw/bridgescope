import sqlglot
from collections import defaultdict

from db_adapters.db_constants import DB_OBJ_TYPE_ENUM
from tools.utils import get_context_attribute


class SQLChecker:
    """
    SQL statement validator for operation and privilege checks.
    """

    def __init__(self, sql: str):
        """
        Initialize SQL checker with SQL statement and expected action.

        :param sql: The SQL statement to validate
        :raises RuntimeError: If SQL format is invalid or cannot be parsed
        """
        self.sql = sql
        self.sql_type = None
        self.permissions = None

        try:
            self._parsed = sqlglot.parse_one(self.sql)
        except Exception as e:
            raise RuntimeError(f"Invalid SQL format: {str(e)}")

        # Parse sql type
        sql_type = (
            self._parsed.key.upper()
            if hasattr(self._parsed, "key")
            else getattr(self._parsed, "token_type", None)
        )
        if hasattr(sql_type, "value"):
            sql_type = sql_type.value.upper()

        self.sql_type = sql_type

    def check_operation_match(self, action) -> bool:
        """
        Verify if the SQL statement matches the specified action type.

        :param action: The expected action type (e.g., "SELECT", "UPDATE", "INSERT", "DELETE")
        :return: True if the SQL operation matches the expected action, False otherwise
        """
        return self.sql_type == action.upper()

    def check_privilege(self) -> bool:
        """
        Verify that all database objects accessed by the SQL have required privileges.

        :return: True if all required privileges are granted, False otherwise
        """
        if not self.permissions:
            self._determine_permissions()

        # Get user privileges from context
        privilege_dict = get_context_attribute("user_privilege")

        # Group required permissions:
        reformat_permissions = defaultdict(lambda: defaultdict(list))
        for obj_type, required_perms in self.permissions.items():
            for obj_name, perms in required_perms.items():
                for perm in perms:
                    reformat_permissions[perm][obj_type].append(obj_name)

        # Check permission type
        for perm, objs in reformat_permissions.items():
            if perm not in privilege_dict:
                return False

            tables = objs[DB_OBJ_TYPE_ENUM.TABLE]
            columns = objs[DB_OBJ_TYPE_ENUM.COL]

            table_2_col = self._match_col_2_table(tables, columns)

            for table in tables:
                table_name = f'public.{table}' if not table.startswith('public') else table
                if DB_OBJ_TYPE_ENUM.TABLE not in privilege_dict[perm] or table_name not in privilege_dict[perm][DB_OBJ_TYPE_ENUM.TABLE]:
                    # No global table permission, check columns
                    for col in table_2_col[table]:
                        col_name = f'{table_name}.{col}'
                        if DB_OBJ_TYPE_ENUM.COL not in privilege_dict[perm] or (col_name not in privilege_dict[perm][DB_OBJ_TYPE_ENUM.COL] and \
                            col_name.lower() not in privilege_dict[perm][DB_OBJ_TYPE_ENUM.COL]):
                            return False

            return True

    def check_object_acl(self) -> bool:
        """
        Verify if accessed database objects are allowed by access control lists.

        :return: True if all accessed objects are allowed, False otherwise
        """
        if not self.permissions:
            self._determine_permissions()

        whitelist = get_context_attribute("white_object_dict") or {}
        blacklist = get_context_attribute("black_object_dict") or {}

        tables = list(self.permissions[DB_OBJ_TYPE_ENUM.TABLE].keys())
        columns = list(self.permissions[DB_OBJ_TYPE_ENUM.COL].keys())

        table_2_col = self._match_col_2_table(tables, columns)

        if whitelist:
            for table in tables:
                if table not in whitelist[DB_OBJ_TYPE_ENUM.TABLE]:
                    return False

                if isinstance(whitelist[DB_OBJ_TYPE_ENUM.TABLE], dict):
                    for col in table_2_col[table]:
                        if col not in whitelist[DB_OBJ_TYPE_ENUM.TABLE][table][DB_OBJ_TYPE_ENUM.COL] and \
                            col.lower() not in whitelist[DB_OBJ_TYPE_ENUM.TABLE][table][DB_OBJ_TYPE_ENUM.COL]:
                            return False
        elif blacklist:
            for table in tables:
                if isinstance(blacklist[DB_OBJ_TYPE_ENUM.TABLE], list) and table in blacklist[DB_OBJ_TYPE_ENUM.TABLE]:
                    return False
                elif isinstance(blacklist[DB_OBJ_TYPE_ENUM.TABLE], dict) and table in blacklist[DB_OBJ_TYPE_ENUM.TABLE]:
                    for col in table_2_col[table]:
                        if col in blacklist[DB_OBJ_TYPE_ENUM.TABLE][table][DB_OBJ_TYPE_ENUM.COL] or col.lower() in blacklist[DB_OBJ_TYPE_ENUM.TABLE][table][DB_OBJ_TYPE_ENUM.COL]:
                            return False

        return True

    def _extract_tables_and_columns(self):
        """
        Parse the SQL statement and extract all accessed tables and columns.

        :return: A tuple (tables, columns) where:
                - tables: set of table names accessed in the SQL
                - columns: set of column names accessed (including qualified names like "table.column")
                - alias_to_table: dictionary mapping table alias to table names
        """
        tables = set()
        columns = set()

        # Build alias to table name mapping
        alias_to_table = {}
        
        # Extract all tables from the parsed SQL and build alias mapping
        for table in self._parsed.find_all(sqlglot.exp.Table):
            table_name = table.name
            tables.add(table_name)
            
            # If table has an alias, map alias to original table name
            if table.alias:
                alias_to_table[table.alias] = table_name

        # Extract all columns from the parsed SQL
        for column in self._parsed.find_all(sqlglot.exp.Column):
            if column.table:
                # Map alias back to original table name if needed
                table_name = alias_to_table.get(column.table, column.table)
                # Qualified column name (e.g., "users.id")
                columns.add(f"{table_name}.{column.name}")
            else:
                # Unqualified column name (e.g., "id")
                columns.add(column.name)

        # For INSERT statements, also extract column names from the column list
        # (e.g., INSERT INTO table (col1, col2) VALUES ...)
        if (
            self.sql_type == "INSERT"
            and hasattr(self._parsed, "this")
            and hasattr(self._parsed.this, "expressions")
        ):
            for expr in self._parsed.this.expressions:
                if hasattr(expr, "name"):
                    columns.add(expr.name)

        return tables, columns, alias_to_table

    def _determine_permissions(self):
        """
        Analyze the SQL statement to determine required permissions for tables and columns.
        """
        table_permissions = {}
        column_permissions = {}

        tables, columns, alias_to_table = self._extract_tables_and_columns()

        # Get the SQL operation type
        sql_type = self.sql_type

        # Determine required permissions based on SQL operation type
        if sql_type == "SELECT":
            # SELECT operations need SELECT permission on all accessed tables and columns
            for table in tables:
                table_permissions[table] = {"SELECT"}
            for column in columns:
                column_permissions[column] = {"SELECT"}

        elif sql_type == "INSERT":
            # INSERT operations need INSERT permission on target table and columns
            target_table = self._get_insert_target_table()
            target_columns = self._get_insert_target_columns()

            # Target table needs INSERT permission
            if target_table:
                table_permissions[target_table] = {"INSERT"}

            # Target columns need INSERT permission
            for column in target_columns:
                column_permissions[column] = {"INSERT"}

            # Other tables accessed in subqueries need SELECT permission
            for table in tables:
                if table != target_table:
                    table_permissions[table] = table_permissions.get(table, set()) | {
                        "SELECT"
                    }

            # Other columns accessed in subqueries need SELECT permission
            for column in columns:
                if column not in target_columns:
                    column_permissions[column] = column_permissions.get(
                        column, set()
                    ) | {"SELECT"}

        elif sql_type == "UPDATE":
            # UPDATE operations need UPDATE permission on target table and modified columns
            target_table = self._get_update_target_table()
            modified_columns = self._get_update_target_columns()
            condition_columns = self._get_where_condition_columns(alias_to_table)

            # Target table needs UPDATE permission
            if target_table:
                table_permissions[target_table] = {"UPDATE"}

            # Modified columns need UPDATE permission
            for column in modified_columns:
                column_permissions[column] = {"UPDATE"}

            # Columns in WHERE clause need SELECT permission
            for column in condition_columns:
                column_permissions[column] = column_permissions.get(column, set()) | {
                    "SELECT"
                }

            # Other tables accessed need SELECT permission
            for table in tables:
                if table != target_table:
                    table_permissions[table] = table_permissions.get(table, set()) | {
                        "SELECT"
                    }

        elif sql_type == "DELETE":
            # DELETE operations need DELETE permission on target table
            target_table = self._get_delete_target_table()
            condition_columns = self._get_where_condition_columns(alias_to_table)

            # Target table needs DELETE permission
            if target_table:
                table_permissions[target_table] = {"DELETE"}

            # Columns in WHERE clause need SELECT permission
            for column in condition_columns:
                column_permissions[column] = {"SELECT"}

            # Other tables accessed need SELECT permission
            for table in tables:
                if table != target_table:
                    table_permissions[table] = table_permissions.get(table, set()) | {
                        "SELECT"
                    }

        self.permissions = {
            DB_OBJ_TYPE_ENUM.TABLE: table_permissions,
            DB_OBJ_TYPE_ENUM.COL: column_permissions,
        }

    def _get_insert_target_table(self):
        """Get the target table for INSERT operation."""

        # Try to get table from args.this first
        if hasattr(self._parsed, "this") and hasattr(self._parsed.this, "this"):
            return self._parsed.this.this.name

        return None

    def _get_insert_target_columns(self):
        """Get the target columns for INSERT operation."""
        columns = set()
        if hasattr(self._parsed, "this") and hasattr(self._parsed.this, "expressions"):
            for expr in self._parsed.this.expressions:
                if hasattr(expr, "name"):
                    columns.add(expr.name)
        return columns

    def _get_update_target_table(self):
        """Get the target table for UPDATE operation."""
        if hasattr(self._parsed, "this") and hasattr(self._parsed.this, "name"):
            return self._parsed.this.name
        return None

    def _get_update_target_columns(self):
        """Get the columns being modified in UPDATE operation."""
        columns = set()
        if hasattr(self._parsed, "expressions"):
            for expr in self._parsed.expressions:
                if hasattr(expr, "this") and hasattr(expr.this, "name"):
                    columns.add(expr.this.name)
        return columns

    def _get_where_condition_columns(self, alias_to_table):
        """
        Get the columns used in UPDATE WHERE clause.

        :param alias_to_table: dictionary mapping table alias to table names
        :return columns: set of column names in where clauses
        """
        columns = set()
        if hasattr(self._parsed, "args"):
            where_clause = self._parsed.args.get("where")

            # If where is a function/property, call it to get the actual clause
            if where_clause:
                for column in where_clause.find_all(sqlglot.exp.Column):
                    if column.table:
                        table_name = alias_to_table.get(column.table, column.table)
                        columns.add(f"{table_name}.{column.name}")
                    else:
                        columns.add(column.name)
        return columns

    def _get_delete_target_table(self):
        """Get the target table for DELETE operation."""
        if hasattr(self._parsed, "this") and hasattr(self._parsed.this, "name"):
            return self._parsed.this.name
        return None

    def _match_col_2_table(self, tables, columns):
        """ Match columns to each table """
        table_2_col = defaultdict(list)
        for col in columns:
            try:
                if len(tables) == 1:
                    table_2_col[tables[0]].append(col)
                else:
                    table, col = col.rsplit('.', 1)
                    table_2_col[table].append(col)
            except Exception:
                pass

        return dict(table_2_col)