from sqlalchemy import inspect

from benchmark.nl2trans_sql.core.utils import sync_exec


class DatabaseSchemaInspector:
    def __init__(self, db_adapter):
        self.tables = dict()
        sync_exec(self.build_db_schema, db_adapter)

    def get_table_names(self):
        return list(self.tables.keys())

    def get_column_names(self, table_name):
        return [col["name"] for col in self.tables[table_name.lower()]["columns"]]

    def is_primary_key(self, table, column):
        return column in self.tables[table]["primary_keys"]

    def get_unique_column(self, table):
        table_feat = self.tables[table.lower()]
        if table_feat["primary_keys"]:
            return table_feat["primary_keys"][0]

        elif table_feat["indexes"]:
            for idx in table_feat["indexes"]:
                if 'unique' in idx and idx['unique']:
                    return idx['columns'][0]

        return None

    def get_column_type(self, table, column):
        column_feat = self.tables[table.lower()]["columns"]
        for col in column_feat:
            if col["name"] == column.lower():
                return col["type"]

    async def build_db_schema(self, adapter):
        async with adapter.engine.connect() as conn:
            await conn.run_sync(self.check_db_schema)

    def check_db_schema(self, sync_conn):
        inspector = inspect(sync_conn)

        table_names = inspector.get_table_names()
        for table_name in table_names:
            table_info = {
                "columns": [],
                "primary_keys": [],
                "foreign_keys": [],
                "indexes": []
            }

            # Columns
            columns = inspector.get_columns(table_name)
            for col in columns:
                table_info["columns"].append({
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col["nullable"]
                })

            # Primary Keys
            pk_constraint = inspector.get_pk_constraint(table_name)
            if pk_constraint and 'constrained_columns' in pk_constraint:
                table_info["primary_keys"] = pk_constraint['constrained_columns']

            # Foreign Keys
            fk_constraints = inspector.get_foreign_keys(table_name)
            for fk in fk_constraints:
                referred_table = fk["referred_table"]
                local_col = fk["constrained_columns"][0]
                remote_col = fk["referred_columns"][0]
                table_info["foreign_keys"].append({
                    "local_column": local_col,
                    "remote_table": referred_table,
                    "remote_column": remote_col
                })

            # Indexes
            indexes = inspector.get_indexes(table_name)
            for idx in indexes:
                table_info["indexes"].append({
                    "name": idx["name"],
                    "columns": idx["column_names"],
                    "unique": idx["unique"]
                })

            self.tables[table_name] = table_info