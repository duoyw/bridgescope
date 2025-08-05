import json
import os.path
import re
from collections import defaultdict
from random import shuffle

from typing import List, Any


from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlglot import exp
from sqlglot import parse_one

from benchmark.nl2trans_sql.core.schema_inspector import DatabaseSchemaInspector
from benchmark.nl2trans_sql.core.sql2text import Sql2Text
from benchmark.nl2trans_sql.core.sql_info import SQL
from benchmark.nl2trans_sql.core.table_builder import SchemaBuilder
from benchmark.nl2trans_sql.core.utils import sync_exec
from db_adapters.db_config import DBConfig
from db_adapters.pg_adapter import PostgresAdapter


class NL2TransSql:
    def __init__(self, benchmark_file, new_benchmark_path, n_generate):
        self.benchmark_file = benchmark_file  # benchmark_file #
        self.new_benchmark_path = new_benchmark_path

        self.db_path = 'postgresql://postgres:postgres@localhost:5432/{}'
        self.db_2_conn = dict()
        self.db_2_schema = dict()
        self.dialect = 'postgres'

        self.n_generate = n_generate

        self.sql2text = Sql2Text()

        with open(self.benchmark_file, 'r') as f:
            self.tasks = json.load(f)

        shuffle(self.tasks)

        # initialize all db conn instances
        for task in self.tasks:
            db = task['db_id']
            if db not in self.db_2_conn:
                conn = PostgresAdapter(DBConfig(self.db_path.format(db)), n_isolation_level=1)
                sync_exec(conn.connect)
                self.db_2_conn[db] = conn

        # pre-retrieve database schema
        for db, conn in self.db_2_conn.items():
            self.db_2_schema[db] = DatabaseSchemaInspector(conn)

    def __del__(self):
        for conn in self.db_2_conn.values():
            sync_exec(conn.close)

    def create_oltp_bench(self, operation, generate_question=True):
        operation_2_to_task = {
            "select": self.to_select_task,
            "insert": self.to_insert_task,
            "delete": self.to_delete_task,
            "update": self.to_update_task,
        }

        self.tasks = [task for task in self.tasks if 'error' not in task and task['result_size']]
        print(f"Total {len(self.tasks)} tasks.")

        new_tasks = []
        for task in self.tasks:
            if 'None' in task['result'] or task['db_id'] == 'formula_1':
                continue

            new_task = operation_2_to_task[operation](task, generate_question)
            if new_task:
                new_tasks.append(new_task)
                if len(new_tasks) == self.n_generate:
                    break

        with open(os.path.join(self.new_benchmark_path, f"{operation}_bench{'_{}'.format(self.n_generate) if self.n_generate!=-1 else ''}.json"), 'w') as f:
            json.dump(new_tasks, f, indent=4)

        print(f"Total {len(new_tasks)} new {operation} tasks generated.")

    def to_select_task(self, task, generate_question=True):
        qid, sql_statement, db = task['question_id'], task['pg_sql'], task['db_id']
        try:
            parsed_sql = SQL(sql_statement, self.db_2_schema[db])
            if parsed_sql.has_pure_field_sel_sql() and not any([isinstance(s, exp.Subquery) for s in parsed_sql.source]):
                return task
        except Exception as e:
            print(f"Skipping task {qid} with sql {sql_statement} for {str(e)}")

        return None

    def to_insert_task(self, task, generate_question=True):
        new_task = task.copy()
        qid, sql_statement, db = task['question_id'], task['pg_sql'], task['db_id']
        try:
            parsed_sql = SQL(sql_statement, self.db_2_schema[db])
        except Exception as e:
            print(f"Skipping task {qid} with sql {sql_statement} for {str(e)}")
            return

        try:
            logger.info(f"Processing task {qid} with sql {sql_statement}")
            _pre_sql, insert_sql, post_sql = self.to_insert(db, parsed_sql, new_task['question'])
        except Exception as e:
            print(f"Skipping task {qid} with sql {sql_statement} for {str(e)}")
            return None

        self.rename_base(new_task)
        new_task['pg_sql'] = insert_sql
        new_task['pre_pg_sql'] = _pre_sql
        new_task['post_pg_sql'] = post_sql

        # build gt
        parsed_trans_sql = SQL(insert_sql, db_schema=self.db_2_schema[db])
        new_task['gt'] = parsed_trans_sql.decompose()

        # try condition
        self.get_sql_result(db, new_task['gt']['values'])

        # build question
        if generate_question:
            self.sql2text.build_one(new_task)
        return new_task

    def to_update_task(self, task, generate_question=True):
        new_task = task.copy()
        qid, sql_statement, db = task['question_id'], task['pg_sql'], task['db_id']
        try:
            parsed_sql = SQL(sql_statement, self.db_2_schema[db])
        except Exception as e:
            print(f"Skipping task {qid} with sql {sql_statement} for {str(e)}")
            return

        if not self.check_sql(parsed_sql):
            print(f"Skipping task {qid} with sql {sql_statement} for invalid format")
            return None

        if parsed_sql.is_featuring_any(['order', 'limit', 'group', 'offset']):
            print(f"Skipping task {qid} with sql {sql_statement} for invalid format")
            return

        if parsed_sql.has_pure_field_sel_sql():
            print(f"Processing task {qid} with sql {sql_statement}")
            try:
                update_sql = self.to_update_direct(db, parsed_sql)
            except RuntimeError as e:
                logger.info(f"Skipping task {qid} with sql {sql_statement} for {str(e)}")
                return None

        elif parsed_sql.is_expr_sel_sql():
            try:
                print(f"Processing task {qid} with sql {sql_statement}")
                update_sql = self.to_update_perm(db, parsed_sql)
            except RuntimeError as e:
                print(f"Skipping task {qid} with sql {sql_statement} for {str(e)}")
                return None
        else:
            print(f"Skipping task {qid} with sql {sql_statement}")
            return None

        # check_int_constraint
        if self.check_violate_integrity_constraint(db, update_sql):
            print(f"Skipping task {qid} with sql {sql_statement} for violating integrity constraint.")
            return None

        self.rename_base(new_task)
        new_task['pg_sql'] = update_sql

        # build gt
        parsed_trans_sql = SQL(update_sql, db_schema=self.db_2_schema[db])
        new_task['gt'] = parsed_trans_sql.decompose()

        # try condition
        self.get_sql_result(db, new_task['gt']['condition'])

        # build question
        if generate_question:
            self.sql2text.build_one(new_task)
        return new_task

    def to_delete_task(self, task, generate_question=True):
        new_task = task.copy()
        qid, sql_statement, db = task['question_id'], task['pg_sql'], task['db_id']
        try:
            parsed_sql = SQL(sql_statement, self.db_2_schema[db])
        except Exception as e:
            print(f"Skipping task {qid} with sql {sql_statement} for {str(e)}")
            return

        try:
            if (parsed_sql.is_featuring_any(['order', 'limit', 'group', 'offset']) or
                    (not parsed_sql.where and not parsed_sql.join_conditions)):
                print(f"Skipping task {qid} with sql {sql_statement} for invalid format")
                return

            print(f"Processing task {qid} with sql {sql_statement}")
            delete_sql = self.to_delete_direct(db, parsed_sql)
            print(delete_sql)
        except Exception as e:
            print(f"Skipping task {qid} with sql {sql_statement} for {str(e)}")
            return

        if self.check_violate_integrity_constraint(db, delete_sql):
            print(f"Skipping task {qid} with sql {sql_statement} for violating integrity constraint.")
            return None

        self.rename_base(new_task)
        new_task['pg_sql'] = delete_sql

        # build gt
        parsed_trans_sql = SQL(delete_sql, db_schema=self.db_2_schema[db])
        new_task['gt'] = parsed_trans_sql.decompose()

        # try condition
        self.get_sql_result(db, new_task['gt']['condition'])

        # build question
        if generate_question:
            self.sql2text.build_one(new_task)

        return new_task


    def to_insert(self, db, parsed_sql: SQL, question):
        db_schema = self.db_2_schema[db]
        result = self.get_sql_result(db, parsed_sql.to_sql())
        sb = SchemaBuilder()

        if parsed_sql.has_pure_field_sel_sql():
            insert_table, _, _ = sb.build(question, parsed_sql.to_sql(), result[:3])
            insert_columns = []
            insert_column_types = []
            for insert_column in parsed_sql.select:
                column_name = insert_column.name
                table_name = parsed_sql.get_column_table(insert_column).name
                column_type = db_schema.get_column_type(table_name, column_name)
                insert_columns.append(column_name)
                insert_column_types.append(column_type)
        else:
            insert_table, insert_columns, insert_column_types = sb.build(question, parsed_sql.to_sql(), result[:3])

        _pre_sql = self.build_create_sql(insert_table, insert_columns, insert_column_types)
        _insert_sql = self.build_insert_sql(insert_table, insert_columns, parsed_sql)
        _post_sql = self.build_drop_sql(insert_table)

        return _pre_sql, _insert_sql, _post_sql

    def to_update_direct(self, db, parsed_sql: SQL):
        db_schema = self.db_2_schema[db]

        cand_cols = parsed_sql.select
        cand_table_2_cols = defaultdict(list)

        for col in cand_cols:
            try:
                table = parsed_sql.get_column_table(col)
                if db_schema.is_primary_key(table.name, col.name):  # 不更新主键
                    continue
            except KeyError:
                # from is not a simple table
                continue

            cand_table_2_cols[table].append(col)

        if not cand_table_2_cols:
            raise RuntimeError('Primary key should not be updated.')

        update_table = list(cand_table_2_cols.keys())[0]
        update_cols = cand_table_2_cols[update_table]

        result = self.get_sql_result(db, parsed_sql.to_sql())
        sets = []
        for col, old_value in zip(update_cols, result[0]):
            col_type = self.db_2_schema[db].get_column_type(update_table.name, col.name)
            new_value = self.get_replaceable_value(db, update_table.name, col.name, old_value)
            if col_type in ['TEXT', 'TIMESTAMP']:
                new_value = f"\'{new_value}\'"
            sets.append((col, new_value))

        return self.build_update_sql(parsed_sql, update_table, sets)


    def to_update_perm(self, db, parsed_sql:SQL):
        # get permutable condition
        field_where_conditions = parsed_sql.get_pure_field_cond_sql()

        if not len(field_where_conditions):
            raise RuntimeError('No update where to permutate')

        for new_update in field_where_conditions:
            # build updates
            update_col, old_value = new_update.left, new_update.right
            update_table = parsed_sql.get_column_table(update_col)
            update_col_type = self.db_2_schema[db].get_column_type(update_table.name, update_col.name)

            new_value = self.get_replaceable_value(db, update_table.name, update_col.name, old_value)
            if new_value is None:
                continue

            if update_col_type in ['TEXT', 'TIMESTAMP']:
                new_value = f"\'{new_value}\'"
            sets = [(update_col, new_value)]

            # build new condition
            new_where_conditions = []
            old_selects = parsed_sql.select
            search_result = self.get_sql_result(db, parsed_sql.to_sql())[0]
            for old_sel, value in zip(old_selects, search_result):
                new_where_conditions.append(exp.EQ(
                    this=old_sel,
                    expression=exp.Literal(this=str(search_result[0]), is_string=not isinstance(search_result[0], (float, int)))
                ))
            condition_map = {
                new_update: new_where_conditions
            }

            return self.build_update_sql(parsed_sql, update_table, sets, condition_map)

        raise RuntimeError('No replacement value')

    def to_delete_direct(self, db, parsed_sql: SQL):
        cand_cols = parsed_sql.select
        cand_table_2_cols = defaultdict(list)

        for col in cand_cols:
            table = parsed_sql.get_column_table(col)
            cand_table_2_cols[table].append(col)

        if not cand_table_2_cols:
            return None  # another random column

        delete_table = list(cand_table_2_cols.keys())[0]
        return self.build_delete_sql(parsed_sql, delete_table)

    def build_create_sql(self, create_table, column_name, column_type):
        # 构造 CREATE TABLE 语句
        create_sql_lines = []
        for col, col_type in zip(column_name, column_type):
            create_sql_lines.append(f"    {col} {str(col_type)}")

        create_sql = f"""
          CREATE TABLE {create_table} (
              {',\n'.join(create_sql_lines)}
          );
          """

        return create_sql

    def build_insert_sql(self, insert_table, insert_columns, values):
        f_it = insert_table.name if isinstance(insert_table, exp.Table) else insert_table
        f_ics = ', '.join(insert_columns)

        # build value
        if isinstance(values, SQL):
            f_val = values.to_sql()
        else:
            f_val = 'VALUES ' + self.row_2_str(values)

        insert_sql = ' '.join([obj for obj in [
            f"INSERT INTO {f_it} ({f_ics})",
            f"{f_val}",
        ] if obj])

        return insert_sql

    def row_2_str(self, row):
        return '(' + ', '.join("'" + str(item) + "'" if isinstance(item, str) else str(item) for item in row) + ')'

    def build_drop_sql(self, drop_table):
        return f"""DROP TABLE {drop_table};"""

    def format_str_name(self, name):
        return name if ' ' not in name else f'\"{name}\"'

    def format_str_val(self, val):
        if isinstance(val, str):
            if not (val.startswith("'") and val.endswith("'")):
                return f"'{val.replace("'", '"')}'"
            else:
                return f"'{val[1:-1].replace("'", '"')}'"
        return val

    def build_update_sql(self, base_sql:SQL, update_table, sets, condition_map=None):
        f_ut = self.format_str_name(update_table.name)
        f_sets = ', '.join([f'{self.format_str_name(ss[0].name)} = {self.format_str_val(ss[1])}' for ss in sets])

        old_source = base_sql.source # old source 中没有子查询
        sts = self.to_sql([t for t in old_source if t != update_table])
        f_st = ', '.join(sts) if isinstance(sts, list) else sts

        f_jc = ' AND '.join(self.to_sql(base_sql.join_conditions))
        f_wc = self.to_sql(base_sql.where) if base_sql.where else ''
        if condition_map is not None:
            for old_cond, new_cond in condition_map.items():
                f_wc = f_wc.replace(self.to_sql(old_cond), 'AND'.join(self.to_sql(new_cond)))
        f_o = [base_sql.group, base_sql.order, base_sql.limit, base_sql.offset]

        if f_jc and f_wc:
            _suffix = "{} AND {}{}".format(f'{f_jc}', f_wc,
                                      ' ' + ' '.join([obj.sql(dialect=self.dialect) for obj in [s_obj for s_obj in f_o] if obj is not None]))
        else:
            _suffix = "{}{}{}".format(f'{f_jc}', f_wc,
                                           ' ' + ' '.join(
                                               [obj.sql(dialect=self.dialect) for obj in [s_obj for s_obj in f_o] if
                                                obj is not None]))

        # replace alias for update table
        if update_table.name in base_sql.table_name_2_alias:
            alias = base_sql.table_name_2_alias[update_table.name]
            _suffix = re.sub(r'\b' + re.escape(alias) + r'\.', update_table.name + '.', _suffix)

        update_sql = ' '.join([obj for obj in [
            f"UPDATE {f_ut}",
            f"SET {f_sets}",
            f"FROM {f_st}" if f_st else '',
            f"WHERE {_suffix}",
        ] if obj])
        return update_sql

    def build_delete_sql(self, base_sql:SQL, delete_table, condition_map=None):
        f_ut = self.format_str_name(delete_table.name)

        old_source = base_sql.source # old source 中没有子查询
        sts = self.to_sql([t for t in old_source if t != delete_table])
        f_st = ', '.join(sts) if isinstance(sts, list) else sts

        f_jc = ' AND '.join(self.to_sql(base_sql.join_conditions))
        f_wc = self.to_sql(base_sql.where) if base_sql.where else ''
        if condition_map is not None:
            for old_cond, new_cond in condition_map.items():
                f_wc.replace(self.to_sql(old_cond), 'AND'.join(self.to_sql(new_cond)))
        f_o = [base_sql.group, base_sql.order, base_sql.limit, base_sql.offset]

        if f_jc and f_wc:
            _suffix = "{} AND {}{}".format(f'{f_jc}', f_wc,
                                           ' ' + ' '.join(
                                               [obj.sql(dialect=self.dialect) for obj in [s_obj for s_obj in f_o] if
                                                obj is not None]))
        else:
            _suffix = "{}{}{}".format(f'{f_jc}', f_wc,
                                      ' ' + ' '.join(
                                          [obj.sql(dialect=self.dialect) for obj in [s_obj for s_obj in f_o] if
                                           obj is not None]))

        # replace alias for delete table
        if delete_table.name in base_sql.table_name_2_alias:
            alias = base_sql.table_name_2_alias[delete_table.name]
            _suffix = re.sub(r'\b' + re.escape(alias) + r'\.', delete_table.name + '.', _suffix)

        delete_sql = ' '.join([obj for obj in [
            f"DELETE FROM {f_ut}",
            f"USING {f_st}" if f_st else '',
            f"WHERE {_suffix}",
        ] if obj])

        return delete_sql

    def to_sql(self, expr):
        if isinstance(expr, list):
            return [e.sql(dialect=self.dialect) for e in expr]
        else:
            return expr.sql(dialect=self.dialect)

    @staticmethod
    def rename_base(task):
        task['base_pg_sql'] = task['pg_sql']
        task['base_question'] = task['question']
        task['base_evidence'] = task['evidence']

    def get_replaceable_value(self, db, table_name, column_name, old_value=None):
        if isinstance(old_value, exp.Literal):
            old_value = old_value.sql

        values = self.get_all_values(db, table_name, column_name)
        if len(values) > 1:
            shuffle(values)
            for val in values:
                if val != old_value:
                    return val
        return None

    def get_all_values(self, db, table_name, column_name) -> List[Any]:
        table_name = f'"{table_name}"' if ' ' in table_name else table_name
        column_name = f'"{column_name}"' if ' ' in column_name else column_name

        sql = f"""SELECT DISTINCT {column_name} FROM {table_name};"""
        raw_result = sync_exec(self.db_2_conn[db].execute_query, sql)
        return [row[0] for row in raw_result]

    def get_sql_result(self, db, sql):
        return sync_exec(self.db_2_conn[db].execute_query, sql)

    @staticmethod
    def check_sql(sql: SQL):
        # 非 groupby
        # 非 count
        # 表达式 select 非 limit
        # pure select 或者 pure condition
        return not sql.is_group_by_sql() and \
                not sql.is_counting_sql() and \
                (sql.has_pure_field_sel_sql() or not sql.is_limit_sql())

    def check_violate_integrity_constraint(self, db, sql):
        try:
            sync_exec(self.db_2_conn[db].execute_query, sql)
            return False
        except IntegrityError:
            return True
        except Exception:
            return True

    def inspect_bench_result_size(self):
        for idx, task in enumerate(self.tasks):
            if idx % 20 == 0:
                print(f"processing {idx}... ")

            sql = task['SQL']
            db = task['db_id']

            # Parse the SQL statement using the specified dialect (SQLite by default)
            parsed_sql = parse_one(sql, dialect="sqlite")
            # postgre-version sql
            pg_sql = parsed_sql.sql(dialect="postgres")
            task['pg_sql'] = pg_sql

            try:
                result = sync_exec(self.db_2_conn[db].execute_query, sql)
                task['result_size'] = len(result)
                task['result'] = '####'.join([str(tuple(res)) for res in result])
            except Exception as e:
                task['error'] = str(e)

        with open('../bench_with_result_size.json', 'w') as f:
            json.dump(self.tasks, f, indent=4)
