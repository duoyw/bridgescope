import asyncio
import json
import os
import argparse
import sys

sys.path.append("../")
sys.path.append("../servers/bridgescope")

from loguru import logger

from agents.prompt_agent import RunPromptAgent
from agents.utils import sync_exec
from data_recorder import DataRecorder
from evaluator.nl2sql_evaluator import NL2SQLEvaluator
from servers.bridgescope.db_adapters.db_config import DBConfig
from servers.bridgescope.db_adapters.pg_adapter import PostgresAdapter
from test_bird_base import TestBirdWrite


class TestBirdNlp2Sql(TestBirdWrite):
    def setUp(self) -> None:
        super().setUp()

        self.bench_dir = "../benchmark/nl2trans_sql/new_bench/"
        self.bench_scale = 50
        self.select_bench_scale = 150
        self.bench_op = 'insert'

        self.evaluator = NL2SQLEvaluator()
        self.postgres_adapter = dict()

        self.test_version = 0
        self.num_workers = 1
        self.n_samples = 1
        self.llm = "gpt-4o"
        # self.llm = 'qwen-max'
        # self.llm = 'claude-3-7'
        # self.llm = 'claude-4'
        self.n_repeats = 1

        self.prompt_name = 'nl2all'
        self.agent_args = {
            "exclude_tools": ["close_db_conn", "begin", "commit", "rollback"],
            "keep_parameter": False
        }

        self.server_config_2_test = {
            'w_tool_desc': self._evaluate_tool_desc,
            'w_tool_desc_single': self._evaluate_single_tool,
            'wo_tool_desc': self._evaluate_no_tool_desc,
            'wo_tool_desc_single': self._evaluate_no_tool_desc_single_tool,
            'wo_tool_desc_execute': self._evaluate_no_tool_desc_execute_tool
        }

        self.user_types = ["full_priv_user",
                           "read_only_user",
                           "operate_table_only_user",
                           "reference_table_only_user",
                           "other_table_only_user"]

        self.server_types = ['w_tool_desc',
                             'w_tool_desc_single',
                             'wo_tool_desc',
                             'wo_tool_desc_single',
                             'wo_tool_desc_execute'
                             ]

    def _test_evaluate_insert(self, test_st, test_ut):
        self.bench_op = 'insert'
        self.output_csv_dir += f'{self.bench_op}/'
        self.num_workers = 1
        self.server_config_2_test[test_st](test_ut)

    def _test_evaluate_delete(self, test_st, test_ut):
        self.bench_op = 'delete'
        self.output_csv_dir += f'{self.bench_op}/'
        self.server_config_2_test[test_st](test_ut)

    def _test_evaluate_update(self, test_st, test_ut):
        self.bench_op = 'update'
        self.output_csv_dir += f'{self.bench_op}/'
        self.server_config_2_test[test_st](test_ut)

    def _test_evaluate_select(self, test_st, test_ut):
        self.bench_op = 'select'
        self.output_csv_dir += f'{self.bench_op}/'
        self.server_config_2_test[test_st](test_ut)

    def _evaluate_tool_desc(self, user_type):
        self.task_recorder = DataRecorder()
        self.server_config["config_name"] = "fine_gran_tool_with_priv_desc"
        tasks = self._load_task()
        self._evaluate(tasks, user_type=user_type, desc="w_tool_desc")

    def _evaluate_no_tool_desc(self, user_type):
        self.task_recorder = DataRecorder()
        self.server_config["config_name"] = "fine_gran_tool_without_tool_priv_desc"
        tasks = self._load_task()
        self._evaluate(tasks, user_type=user_type, desc="wo_tool_desc")

    def _evaluate_single_tool(self, user_type):
        self.task_recorder = DataRecorder()
        self.server_config["config_name"] = "single_exec_tool_with_priv_desc"
        tasks = self._load_task()
        self._evaluate(tasks, user_type=user_type, desc="w_tool_desc_single")

    def _evaluate_no_tool_desc_single_tool(self, user_type):
        self.task_recorder = DataRecorder()
        self.agent_args["exclude_tools"] += ["begin", "commit", "rollback"]
        self.server_config["config_name"] = "single_exec_tool_without_priv_desc"
        tasks = self._load_task()
        self._evaluate(tasks, user_type=user_type, desc="wo_tool_desc_single")

    def _evaluate_no_tool_desc_execute_tool(self, user_type):
        self.agent_args["exclude_tools"] += ["get_schema", "search_relative_column_values"]
        # self.agent_args["exclude_tools"] += ["get_schema"]
        self.task_recorder = DataRecorder()
        self.server_config["config_name"] = "single_exec_tool_without_priv_desc"
        tasks = self._load_task()
        self._evaluate(tasks, user_type=user_type, desc="wo_tool_desc_execute")

    def _test_clean_connections(self):

        self.db_config = DBConfig('postgresql://postgres:postgres@localhost:5432/california_schools')
        self.db_adapter = PostgresAdapter(self.db_config)

        async def _func():
            await self.db_adapter.connect()
            rows = await self.db_adapter.execute_query("""SELECT pid
                                                          FROM pg_stat_activity
                                                          WHERE state = 'idle'
                                                            and (
                                                              usename = 'superuser'
                                                                  OR usename = 'readonly_user'
                                                                  OR usename ~ '^user_[0-9]+$');""")

            pids = [str(r[0]) for r in rows]
            await self.db_adapter.execute_query(f"""SELECT pg_terminate_backend(pid)
                                                   FROM pg_stat_activity
                                                   WHERE state = 'idle' and pid IN ({','.join(pids)});""")

            current_conn = await self.db_adapter.execute_query("""SELECT count(*)
                                                                  FROM pg_stat_activity;""")
            print(f'{current_conn[0][0]} connections remains.')

            await self.db_adapter.close()

        asyncio.run(_func())

    def _test_revoke_pre_sqls(self):
        tasks = self._load_task()

        for task in tasks:
            db = task["db_id"]
            post_sql = task.get('post_pg_sql')

            for user_type in self.user_types:
                if user_type in task["user"] and isinstance(task["user"][user_type], list):
                    user, (tables, priv) = task["user"][user_type]
                    for table in tables:
                        self.revoke_temporal_privilege(db, user, priv, table)

            self._try_exec_sql_commit(db, post_sql)

    def _get_identifier(self, user_type, desc):
        return f"v{self.test_version}_{self.llm}_{self.bench_op}_{user_type}_{desc}_{self.task_recorder.current_file[:-5].split('_', 1)[1]}"

    def grant_temporal_privilege(self, db, user, priv, table):
        priv_text = ','.join(priv)
        grant_select_sql = f"GRANT {priv_text} ON TABLE public.{table} TO {user};"
        self._try_exec_sql_commit(db, grant_select_sql)

    def revoke_temporal_privilege(self, db, user, priv, table):
        priv_text = ','.join(priv)
        revoke_select_sql = f"REVOKE {priv_text} ON TABLE public.{table} FROM {user};"
        self._try_exec_sql_commit(db, revoke_select_sql)

    def _try_exec_sql_commit(self, db, sql):
        try:
            sync_exec(self.get_postgres_adapter(db).execute_query, sql)
        except Exception as e:
            print(f'Executing sql ``{sql}`` error: {str(e)}')

    def _check_finishable(self, task, user_type):
        # "full_priv_user", "read_only_user", "operate_table_only_user", "reference_table_only_user", "other_table_only_user"]
        if user_type == "full_priv_user" or \
                (user_type == "read_only_user" and not task.get('gt')) or \
                (user_type == "operate_table_only_user" and not task["user"].get("reference_table_only_user")):
            return True

        return False

    def _process_user_db_tasks(self, task, user_type):
        db = task["db_id"]
        cat = self._check_finishable(task, user_type)

        pre_sql = task.get('pre_pg_sql')
        sql = task.get('pg_sql')
        post_sql = task.get('post_pg_sql')

        if pre_sql:
            self._try_exec_sql_commit(db, pre_sql)

        tables, priv = None, None
        if isinstance(task["user"][user_type], list):
            user, (tables, priv) = task["user"][user_type]
            for table in tables:
                self.grant_temporal_privilege(db, user, priv, table)

        else:
            user = task["user"][user_type]

        db_path = self.db_path.format(user, user, db)
        agent = self._get_agent(db_path)

        task_information = {
            'task_id': task['question_id'],
            'question': task['question'],
            'evidence': task['evidence'],
            'ground_truth_sql': task['pg_sql']
        }
        print("question: ", task['question'])

        try:
            # n_tries, response = agent(msg_task)
            response = agent.run_prompt(self.prompt_name, task=task['question'], knowledge=task['evidence'])
            memory, n_function_call, n_model_response = agent.get_process()

            task_information.update({
                'memory': memory,
                'n_tries': n_model_response,
                'n_func_call': n_function_call
            })

            if response in ['abort', 'exceed_maximum_tries']:
                task_information[response] = True

            else:
                task_information['predict_sql'] = response
                if cat:
                    try:
                        evaluate_result = self._agent_evaluate(db, response, sql, task, user_type)
                        print(evaluate_result)
                        task_information.update(evaluate_result)

                    except Exception as e:
                        task_information['evaluation_error'] = str(e)

        except Exception as e:
            logger.error(f'Evaluation error: {str(e)}')
            memory, _, _ = agent.get_process()
            task_information.update({
                'memory': memory,
                'agent_error': str(e),
            })

        logger.info(memory)
        if tables and priv:
            for table in tables:
                self.revoke_temporal_privilege(db, user, priv, table)

        if post_sql:
            self._try_exec_sql_commit(db, post_sql)

        self._record_task(task['question_id'], task_information)

        task_execution_flag = [
            task_information['task_id'],
            cat,
            'agent_error' in task_information,
            'exceed_maximum_tries' in task_information,
            'abort' in task_information,
            True if 'predict_sql' in task_information else False,
            'evaluation_error' in task_information,
            task_information['n_tries'] if 'n_tries' in task_information else 0,
            task_information['n_func_call'] if 'n_func_call' in task_information else 0,
            int(task_information.get('sql_str_equal', False)),  # sql_str_equal
            int(task_information.get('sql_equivalent', False)),
            0
        ]

        return task_execution_flag

    def _get_agent(self, db_path):
        agent = RunPromptAgent(db_path, self.server_config, model_config_name=self.llm, readonly=True,
                               args=self.agent_args)
        return agent

    def _agent_evaluate(self, db, response, sql, task, user_type):
        evaluate_result = self.evaluator.evaluate(db, response, sql,
                                                  task['gt'] if 'gt' in task else None)
        return evaluate_result

    def _load_task(self):
        with open(self.get_task_2_user_file(self.bench_op), 'r') as f:
            task_id_2_user = json.load(f)

        with open(self.get_bench_file(self.bench_op), 'r') as file:
            dataset = json.load(file)
            for task in dataset:
                task['user'] = task_id_2_user[str(task['question_id'])]

            dataset = dataset[:self.n_samples] if self.n_samples > 0 else dataset
            if self.n_repeats > 1:
                repeated_dataset = []
                for i in range(self.n_repeats):
                    modified = [{**task, 'question_id': f"{task['question_id']}_{i}"} for task in dataset]
                    repeated_dataset.extend(modified)
                return repeated_dataset
            else:
                return dataset

    def get_bench_file(self, operation):
        bench_scale = self.select_bench_scale if operation == 'select' else self.bench_scale
        return os.path.join(self.bench_dir,
                            f"{operation}_bench{'_{}'.format(bench_scale) if bench_scale != -1 else ''}.json")

    def get_task_2_user_file(self, operation):
        bench_scale = self.select_bench_scale if operation == 'select' else self.bench_scale
        return os.path.join(self.bench_dir,
                            f"{operation}_bench{'_{}'.format(bench_scale) if bench_scale != -1 else ''}_user_priv.json")

    def get_postgres_adapter(self, db):
        if db not in self.postgres_adapter:
            p_conn = PostgresAdapter(DBConfig(self.db_path.format('postgres', 'postgres', db), readonly=False))
            sync_exec(p_conn.connect)
            self.postgres_adapter[db] = p_conn

        return self.postgres_adapter[db]
