from typing import Optional, List

from loguru import logger

from agents.utils import sync_exec
from benchmark.nl2trans_sql.core.schema_inspector import DatabaseSchemaInspector
from benchmark.nl2trans_sql.core.sql_info import SQL
from db_adapters.db_config import DBConfig
from db_adapters.pg_adapter import PostgresAdapter


class NL2SQLEvaluator:
    def __init__(self, db_url: str = 'postgresql://postgres:postgres@localhost:5432/{}', dialect="postgres"):
        self.db_2_adapter = dict()
        self.db_2_schema = dict()
        self.db_url = db_url
        self.dialect = dialect

    def __del__(self):
        for adapter in self.db_2_adapter.values():
            sync_exec(adapter.close)

    def get_db_adapter(self, db):
        if db not in self.db_2_adapter:
            adapter = PostgresAdapter(DBConfig(self.db_url.format(db)))
            sync_exec(adapter.connect)
            self.db_2_adapter[db] = adapter

        return self.db_2_adapter[db]

    def get_db_schema(self, db):
        if db not in self.db_2_adapter:
            adapter = self.get_db_adapter(db)
            self.db_2_schema[db] = DatabaseSchemaInspector(adapter)

        return self.db_2_schema[db]

    def evaluate(self, db, sql1, sql2, gt=None):
        return {'sql_str_equal': self.are_sql_strings_equal(sql1, sql2),
                'sql_equivalent': self.are_sql_equivalent(db, sql1, gt if gt else sql2)}

    @staticmethod
    def are_sql_strings_equal(sql1: str, sql2: str) -> bool:
        normalized_sql1 = sql1.strip(';').strip().lower()
        normalized_sql2 = sql2.strip(';').strip().lower()
        return normalized_sql1 == normalized_sql2

    def are_sql_equivalent(self, db, sql: str, base: str|dict) -> bool:
        try:
            if isinstance(base, str):
                return self.are_result_equal(db, sql, base)
            else:
                parsed_sql = SQL(sql, self.get_db_schema(db))
                dec = parsed_sql.decompose()
                ck = 'values' if base['type'] in ['insert', 'INSERT'] else 'condition'
                mk = [k for k in base.keys() if k != ck]
                for k in mk:
                    if k not in dec:
                        return False
                    bk = str(tuple(base[k])).lower()
                    dk = str(tuple(dec[k])).lower()
                    if bk != dk:
                        return False

                return self.are_result_equal(db, dec[ck], base[ck]) if ck in dec else False

        except Exception as e:
            logger.info(f"Error in evaluating sql equivalence: {str(e)}")

        return False

    def execute_sql(self, db, sql: str) -> Optional[List]:
        adapter = self.get_db_adapter(db)
        try:
            results = sync_exec(adapter.execute_query, sql)
            return results
        except Exception as e:
            print(f"Evaluation error: {str(e)}")
            return None

    def are_result_equal(self, db, sql1, sql2):
        res1 = self.execute_sql(db, sql1)
        res2 = self.execute_sql(db, sql2)

        print("executing")
        print(sql1)
        print(sql2)

        if res1 is None or res2 is None:
            return False

        return set(res1) == set(res2)
