import json
import os.path

import sqlglot

from sqlglot import exp
from collections import defaultdict

from benchmark.nl2trans_sql.core.schema_inspector import DatabaseSchemaInspector
from benchmark.nl2trans_sql.core.sql_info import SQL
from benchmark.nl2trans_sql.core.utils import sync_exec
from db_adapters.db_config import DBConfig
from db_adapters.pg_adapter import PostgresAdapter


class PrivilegeBuilder:
    def __init__(self, bench_dir, n_generate):
        self.bench_dir = bench_dir

        self.n_generate = n_generate

        self.db_path = 'postgresql://postgres:postgres@localhost:5432/{}'

        self.db_2_conn = dict()
        self.db_2_schema = dict()

        self.user_prefix = ''

        self.tasks = []
        for operation in ['select', 'insert', 'delete', 'update']:
            with open(self.get_bench_file(operation), 'r') as f:
                self.tasks.extend(json.load(f))

        # initialize all db conn instances
        self.dbs = set([task['db_id'] for task in self.tasks])
        self.dbs.add('postgres')

        for db in self.dbs:
            conn = PostgresAdapter(DBConfig(self.db_path.format(db)), readonly=False, n_isolation_level=1)
            sync_exec(conn.connect)
            self.db_2_conn[db] = conn
            self.db_2_schema[db] = DatabaseSchemaInspector(conn)

    def __del__(self):
        for conn in self.db_2_conn.values():
            sync_exec(conn.close)

    def get_bench_file(self, operation):
        n_sample = self.n_generate if isinstance(self.n_generate, int) else self.n_generate[operation]
        return os.path.join(self.bench_dir, f"{operation}_bench{'_{}'.format(n_sample) if n_sample!=-1 else ''}.json")

    def get_user_2_privilege_file(self):
        if isinstance(self.n_generate, dict):
            identifier = '_'.join([str(self.n_generate[op]) for op in ['select', 'update', 'delete', 'insert']])
        else:
            identifier = self.n_generate
        return os.path.join(self.bench_dir, f'bench{'_{}'.format(identifier) if identifier!=-1 else ''}_user_2_table_privilege_config.json')

    def get_task_2_user_file(self, operation):
        n_sample = self.n_generate if isinstance(self.n_generate, int) else self.n_generate[operation]
        return os.path.join(self.bench_dir, f"{operation}_bench{'_{}'.format(n_sample) if n_sample!=-1 else ''}_user_priv.json")

    def build_user_priv_file(self):
        db_privilege_2_user = defaultdict(dict)
        task_type_2_id_2_user = defaultdict(dict)

        for task in self.tasks:
            db = task['db_id']
            task_user = dict()

            try:
                parsed_sql = SQL(task['pg_sql'], self.db_2_schema[db])
                operation = parsed_sql.to_sql().split()[0].lower()
            except sqlglot.errors.ParseError as e:
                print(f"Error parsing SQL: {e}")
                continue

            db_tables = list(self.db_2_schema[db].tables.keys())

            all_tables = parsed_sql.extract_all_tables()
            operate_tables = parsed_sql.extract_operate_tables()
            operate_tables = operate_tables if isinstance(operate_tables, list) else [operate_tables]

            reference_table = set(all_tables) - set(operate_tables)
            others_table = set(db_tables) - set(all_tables)

            operate_table_only_priv = tuple([tuple(sorted(set(operate_tables))), tuple(['SELECT', 'UPDATE', 'INSERT', 'DELETE'])])
            reference_table_only_priv = tuple([tuple(sorted(reference_table)), tuple(['SELECT', 'UPDATE', 'INSERT', 'DELETE'])])
            other_table_only_priv = tuple([tuple(sorted(others_table)[:2]), tuple(['SELECT', 'UPDATE', 'INSERT', 'DELETE'])])

            # overall positive user and readonly user
            if db not in db_privilege_2_user:
                db_privilege_2_user[db][tuple(
                    [tuple(sorted(db_tables)),
                     tuple(['SELECT', 'UPDATE', 'INSERT', 'DELETE'])])] = self.user_prefix + 'superuser'

                db_privilege_2_user[db][tuple(
                    [tuple(sorted(db_tables)), tuple(['SELECT'])])] = self.user_prefix + 'readonly_user'

            is_insert = isinstance(parsed_sql.parsed_sql, exp.Insert)

            full_priv_user = self.user_prefix + 'superuser'
            read_only_user = self.user_prefix + 'readonly_user'

            task_user['full_priv_user'] = [full_priv_user, tuple([tuple(sorted(set(operate_tables))), tuple(['SELECT', 'UPDATE', 'INSERT', 'DELETE'])])] if is_insert else full_priv_user
            task_user['read_only_user'] = [read_only_user, tuple([tuple(sorted(set(operate_tables))), tuple(['SELECT'])])] if is_insert else read_only_user

            print(task['pg_sql'])

            for priv, user_label in zip([operate_table_only_priv, reference_table_only_priv, other_table_only_priv],
                                  ['operate_table_only_user', 'reference_table_only_user', 'other_table_only_user']):

                if not priv[0]:
                    continue

                if isinstance(parsed_sql.parsed_sql, exp.Insert) and user_label == 'operate_table_only_user':
                    task_user[user_label] = ('tmp_user', priv)
                    continue

                if priv in db_privilege_2_user[db]:
                    user = db_privilege_2_user[db][priv]
                else:
                    user = f'{self.user_prefix}user_{len(db_privilege_2_user[db])-1}'
                    db_privilege_2_user[db][priv] = user

                task_user[user_label] = user

            task_type_2_id_2_user[operation][task["question_id"]] = task_user

        with open(self.get_user_2_privilege_file(), "w") as f:
            json.dump(
                {db: {user: priv for priv, user in db_privilege_2_user[db].items()} for db in
                 db_privilege_2_user.keys()},
                f, indent=4)

        for operation, task_id_2_user in task_type_2_id_2_user.items():
            with open(self.get_task_2_user_file(operation), "w") as f:
                json.dump(task_id_2_user, f, indent=4)

    def create_user_privilege(self):
        if not os.path.exists(self.get_user_2_privilege_file()):
            self.build_user_priv_file()

        with open(self.get_user_2_privilege_file(), 'r') as f:
            db_2_user_priv = json.load(f)

        users = set([u for db in db_2_user_priv for u in db_2_user_priv[db].keys()])
        users.add('tmp_user')

        # check user exists
        try:
            db_conn = self.db_2_conn['postgres']
            for user in users:
                check_user_sql = f"SELECT 1 FROM pg_roles WHERE rolname = '{user}';"
                result =  sync_exec(db_conn.execute_query, check_user_sql)

                if result:
                    print(f"User '{user}' already exists. Skipping creation.")
                else:
                    create_user_sql = f"CREATE USER {user} WITH PASSWORD '{user}';"
                    sync_exec(db_conn.execute_query, create_user_sql)
                    print(f"User '{user}' created successfully.")

        except Exception as e:
            print(f"An error occurred in checking user: {e}")

        for db, user_2_priv in db_2_user_priv.items():
            db_conn = self.db_2_conn[db]
            print(f"Connected to database: {db}.")

            try:
                for user, table_priv in user_2_priv.items():
                    if isinstance(table_priv[0], list):
                        # multiple privilege
                        tables, priv = table_priv
                        priv_text = ','.join(priv)

                    else:
                        # select only
                        tables = table_priv
                        priv_text = 'SELECT'

                    for table in tables:
                        grant_select_sql = f"GRANT {priv_text} ON TABLE public.{table} TO {user};"
                        sync_exec(db_conn.execute_query, grant_select_sql)
                        print(f"Granted {priv_text} on table '{table}' in database '{db}' to user '{user}'.")

            except Exception as e:
                print(f"An error occurred when granting privilege: {e}")

    def clear_user_privilege(self):
        if not os.path.exists(self.get_user_2_privilege_file()):
            print('User privilege file is not generated yet.')
            return

        with open(self.get_user_2_privilege_file(), 'r') as f:
            db_2_user_priv = json.load(f)

        users = set([u for db in db_2_user_priv for u in db_2_user_priv[db].keys()])
        users.add('tmp_user')

        # check user exist
        db_conn = self.db_2_conn['postgres']
        existing_users = []
        try:
            for user in users:
                check_sql = f"SELECT 1 FROM pg_roles WHERE rolname = '{user}';"
                exists = sync_exec(db_conn.execute_query, check_sql)
                if not exists:
                    print(f"User '{user}' does not exist.")
                else:
                    existing_users.append(user)

        except Exception as e:
            print(f"An error occurred when checking users: {e}")

        # revoke user privilege
        for db, user_2_priv in db_2_user_priv.items():
            db_conn = self.db_2_conn[db]
            print(f"Connected to database: {db}.")

            try:
                for user, tables in user_2_priv.items():
                    if user not in existing_users:
                        continue

                    if isinstance(tables[0], list):
                        tables = tables[0]

                    for table in tables:
                        revoke_sql = f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {user};"
                        sync_exec(db_conn.execute_query, revoke_sql)
                        print(f"Revoke all privilege on table '{table}' in database '{db}' from user '{user}'.")

            except Exception as e:
                print(f"An error occurred when revoking privilege: {e}")

        # delete user
        db_conn = self.db_2_conn['postgres']
        try:
            for user in existing_users:
                drop_user_sql = f"DROP USER {user};"
                sync_exec(db_conn.execute_query, drop_user_sql)

                print(f"User '{user}' deleted successfully.")

        except Exception as e:
            print(f"An error occurred when droping users: {e}")
