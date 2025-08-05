import json
import os
import unittest
import random

from benchmark.nl2trans_sql.core.nl2trans_sql import NL2TransSql
from benchmark.nl2trans_sql.core.privilege_builder import PrivilegeBuilder
from benchmark.nl2trans_sql.core.utils import sync_exec
from db_adapters.db_config import DBConfig
from db_adapters.pg_adapter import PostgresAdapter


class TestExtBench(unittest.TestCase):
    def setUp(self):
        self.db_path = "postgresql://{}:{}@localhost:5432/{}"
        self.base_bench = "bench_with_result_size.json"
        self.new_bench_dir = "new_bench/"
        self.n_generate=50

    def test_gen_insert(self):
        random.seed(42)
        bench_ext = NL2TransSql(self.base_bench, self.new_bench_dir, n_generate=self.n_generate)
        bench_ext.create_oltp_bench("insert") # 1226 4h

    def test_gen_delete(self):
        random.seed(43)
        bench_ext = NL2TransSql(self.base_bench, self.new_bench_dir, n_generate=self.n_generate)
        bench_ext.create_oltp_bench("delete") # 478 1h47min

    def test_gen_update(self):
        random.seed(44)
        bench_ext = NL2TransSql(self.base_bench, self.new_bench_dir, n_generate=self.n_generate)
        bench_ext.create_oltp_bench("update") # 916 2h50min

    def test_gen_select(self):
        random.seed(45)
        bench_ext = NL2TransSql(self.base_bench, self.new_bench_dir, n_generate=150)
        bench_ext.create_oltp_bench("select")

    def test_build_user_privilege(self):
        pb = PrivilegeBuilder(self.new_bench_dir, n_generate={
            'select': 150,
            'update': 50,
            'delete': 50,
            'insert': 50
        })
        pb.build_user_priv_file()
        pb.create_user_privilege()
        # pb.clear_user_privilege()

    def test_task_syntax_corr(self):
        tasks = []
        readonly_adapter = dict()
        postgres_adapter = dict()

        def _get_bench_file(operation):
            return os.path.join(self.new_bench_dir,
                         f"{operation}_bench{'_{}'.format(self.n_generate) if self.n_generate != -1 else ''}.json")

        for operation in ['select', 'insert', 'delete', 'update']:
            with open(_get_bench_file(operation), 'r') as f:
                tasks.extend(json.load(f))

        # initialize all db conn instances
        dbs = set([task['db_id'] for task in tasks])
        for db in dbs:
            conn = PostgresAdapter(DBConfig(self.db_path.format('postgres', 'postgres', db)))
            sync_exec(conn.connect)
            readonly_adapter[db] = conn

            p_conn = PostgresAdapter(DBConfig(self.db_path.format('postgres', 'postgres', db)), readonly=False)
            sync_exec(p_conn.connect)
            postgres_adapter[db] = p_conn


        def _try_exec_sql_commit(db, sql):
            try:
                sync_exec(postgres_adapter[db].execute_query, sql)
            except Exception as e:
                print(f'Executing sql ``{sql}`` error: {str(e)}')

        def _try_exec_sql_wo_commit(db, sql):
            try:
                result = sync_exec(readonly_adapter[db].execute_query, sql)

            except Exception as e:
                print(f'Executing sql ``{sql}`` error: {str(e)}')

        for task in tasks:
            db = task.get('db_id')
            pg_sql = task.get('pg_sql')
            pre_sql = task.get('pre_pg_sql')
            post_sql = task.get('post_pg_sql')

            print(task['question_id'], pg_sql)
            if not pg_sql.startswith("INSERT"):
                continue

            if pre_sql:
                _try_exec_sql_commit(db, pre_sql)

            _try_exec_sql_wo_commit(db, pg_sql)

            if post_sql:
                _try_exec_sql_commit(db, post_sql)

        for conn in readonly_adapter.values():
            sync_exec(conn.close)

        for conn in postgres_adapter.values():
            sync_exec(conn.close)

    # 1. ✅
    # delete 会有 ForeignKeyViolationError
    # update for UniqueViolationError, asyncpg.exceptions.GroupingError (置换)
    # insert
    #  '
    #           CREATE TABLE AveragePostsByOldestUsers (
    #                   AVG(PostId) DECIMAL
    #           );
    #           '
    # 2. insert 的权限
    # 3. schema 的实现 ✅