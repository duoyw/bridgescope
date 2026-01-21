import asyncio
import csv
import json
import os
import traceback
import unittest
import random
from concurrent.futures import ThreadPoolExecutor
from random import shuffle
from typing import Optional

import agentscope
import numpy as np
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from agents.prompt_agent import RunPromptAgent
from data_recorder import DataRecorder
from evaluator.nl2sql_evaluator import NL2SQLEvaluator
from servers.bridgescope.db_adapters.db_config import DBConfig
from servers.bridgescope.db_adapters.pg_adapter import PostgresAdapter


class TestBirdWrite(unittest.TestCase):
    def setUp(self) -> None:
        agentscope.init("model_config.json")

        self.output_csv_dir = "../experiment/nlp2sql/"
        # self.output_csv_dir = "../experiment/nlp2sql_t1/"
        # self.output_csv_dir = "../experiment/nlp2sql_ex_v1/"
        self.db_path = 'postgresql://{}:{}@localhost:5432/{}'  # +asyncpg
        self.num_workers = 1
        self.n_samples = 500
        self.test_version = '2'
        # self.llm = 'qwen-max'  # 'claude-3-7'
        self.llm = 'gpt-4o'  # 'claude-3-7'
        # self.llm = 'claude-3-7'
        self.n_repeats = 5
        self.coarse_eval_only = True
        self.prompt_name = 'nl2sql'

        self.server_config = {
            "config_path": f'mcp.json',
            "config_name": "fine_gran_tool_with_priv_desc"}

        self.task_recorder = DataRecorder()

        pd.set_option('display.float_format', '{:.2f}'.format)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', None)
        random.seed(42)

        # excel
        self.headers = [
            "task_id", "category", "total_tokens", "prompt_tokens", "completion_tokens", "agent_error",
            "exceed_maximum_tries", "abort", "finish",
            "evaluation_error", "n_tries", "n_func_call", "sql_str_equal",
            "sql_equivalent", "result_equal"
        ]

        # 重新拉 DB

        # self._clean_connections()

    def _test_clean_connections(self):

        self.db_config = DBConfig('postgresql://postgres:postgres@localhost:5432/california_schools')
        self.db_adapter = PostgresAdapter(self.db_config)

        async def _func():
            await self.db_adapter.connect()
            await self.db_adapter.execute_query("""SELECT pg_terminate_backend(pid)
                                                   FROM pg_stat_activity
                                                   WHERE state = 'idle';""")

            current_conn = await self.db_adapter.execute_query("""SELECT count(*)
                                                                  FROM pg_stat_activity;""")
            print(f'{current_conn[0][0]} connections remains.')

            await self.db_adapter.close()

        asyncio.run(_func())

    #### pos user
    # def test_evaluate_full_priv(self):
    #     self.server_config["config_name"] = "fine_gran_tool_with_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self._evaluate(tasks, user_type="pos_user", desc="post_sample, with tool desc")
    #
    # def test_evaluate_full_priv_no_tool_desc(self):
    #     self.server_config["config_name"] = "fine_gran_tool_without_tool_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self._evaluate(tasks, user_type="pos_user", desc="post_sample, without tool desc")
    #
    # def test_evaluate_full_priv_single_tool(self):
    #     self.server_config["config_name"] = "single_exec_tool_with_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self._evaluate(tasks, user_type="pos_user", desc="post_sample, with tool desc, single tool")
    #
    # def test_evaluate_full_priv_no_tool_desc_single_tool(self):
    #     self.server_config["config_name"] = "single_exec_tool_without_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self._evaluate(tasks, user_type="pos_user", desc="post_sample, without tool desc, single tool")
    #
    # #### semi_neg_user
    #
    # def test_evaluate_insufficient_priv_schema_view(self):
    #     self.server_config["config_name"] = "fine_gran_tool_with_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self._evaluate(tasks, user_type="semi_neg_user", desc="semi_neg_sample, with tool desc")
    #
    # def test_evaluate_insufficient_priv_schema_view_no_tool_desc(self):
    #     self.server_config["config_name"] = "fine_gran_tool_without_tool_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self.server_config_path = '/nas/ldd461759/code/mcp_db/MCP4DB/test/mcp_config_no_priv.json'
    #     self._evaluate(tasks, user_type="semi_neg_user", desc="semi_neg_sample, without tool desc")
    #
    # def test_evaluate_insufficient_priv_schema_view_single_tool(self):
    #     self.server_config["config_name"] = "single_exec_tool_with_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self._evaluate(tasks, user_type="semi_neg_user", desc="semi_neg_sample, with tool desc, single tool")
    #
    # def test_evaluate_insufficient_priv_schema_view_no_tool_desc_single_tool(self):
    #     self.server_config["config_name"] = "single_exec_tool_without_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self.server_config_path = '/nas/ldd461759/code/mcp_db/MCP4DB/test/mcp_config_no_priv.json'
    #     self._evaluate(tasks, user_type="semi_neg_user", desc="semi_neg_sample, without tool desc, single tool")
    #
    # #### neg_user
    #
    # def test_evaluate_insufficient_priv_no_schema_view(self):
    #     self.server_config["config_name"] = "fine_gran_tool_with_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self._evaluate(tasks, user_type="neg_user", desc="neg_sample, with tool desc")
    #
    # def test_evaluate_insufficient_priv_no_schema_view_no_tool_desc(self):
    #     self.server_config["config_name"] = "fine_gran_tool_without_tool_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self.server_config_path = '/nas/ldd461759/code/mcp_db/MCP4DB/test/mcp_config_no_priv.json'
    #     self._evaluate(tasks, user_type="neg_user", desc="neg_sample, without tool desc")
    #
    # def test_evaluate_insufficient_priv_no_schema_view_single_tool(self):
    #     self.server_config["config_name"] = "single_exec_tool_with_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self._evaluate(tasks, user_type="neg_user", desc="neg_sample, with tool desc, single tool")
    #
    # def test_evaluate_insufficient_priv_no_schema_view_no_tool_desc_single_tool(self):
    #     self.server_config["config_name"] = "single_exec_tool_without_priv_desc"
    #     tasks = self._load_task(self.data_path, self.n_samples)
    #     self.server_config_path = '/nas/ldd461759/code/mcp_db/MCP4DB/test/mcp_config_no_priv.json'
    #     self._evaluate(tasks, user_type="neg_user", desc="neg_sample, without tool desc, single tool")

    def _test_run(self):

        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import AsyncSession
        import asyncio

        def execute_sql_without_commit(sql: str):
            """
            执行 SQL 并立即回滚，防止真实修改数据库。

            参数:
                db_url (str): PostgreSQL 异步连接字符串
                sql (str): 要执行的 SQL 语句（INSERT/UPDATE/DELETE 等）
            """
            engine = create_async_engine(self.db_path.format('v2_superuser', 'v2_superuser', 'california_schools'))
            Session = sessionmaker(engine, class_=AsyncSession)

            async def _execute():
                try:
                    async with Session.begin() as session:
                        results = await session.execute(text(sql))
                        rows = results.fetchall()
                        return rows
                except Exception as e:
                    return str(e)

            return asyncio.run(_execute())

        with open("/home/ldd461759/code/mcp_db/test_db/mc_test_v2/origin_dev.json", 'r') as f:
            tasks = json.load(f)

        for task in tasks:
            print(task['question_id'], execute_sql_without_commit(task['SQL']))

    def _evaluate(self, tasks, user_type='pos', desc='evaluation'):
        last_total_tokens, last_prompt_tokens, last_completion_tokens = 0, 0, 0

        n_run, n_agent_error, n_exceed_maximum_tries, n_abort, n_finish, n_evaluation_error, n_sql_str_equal, n_sql_equivalent, n_result_equal = 0, 0, 0, 0, 0, 0, 0, 0, 0
        sum_n_func_call, sum_n_abort_func_call, sum_n_finish_func_call, sum_call_response_ratio = 0, 0, 0, 0.0

        summ = None
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            for task in tasks:
                futures.append(executor.submit(self._process_user_db_tasks, task, user_type))

            for future in tqdm(futures, total=len(futures), desc=desc):
                try:
                    result = future.result()
                    task_id, category, agent_error, exceed_maximum_tries, abort, finish, evaluation_error, n_tries, n_func_call, sql_str_equal, sql_equivalent, result_equal = result

                    # statistics tokens usage
                    usage = agentscope.print_llm_usage()
                    total_tokens = usage['text_and_embedding'][0]['total_tokens']
                    prompt_tokens = usage['text_and_embedding'][0]['prompt_tokens']
                    completion_tokens = usage['text_and_embedding'][0]['completion_tokens']

                    task_total_tokens = total_tokens - last_total_tokens
                    task_prompt_tokens = prompt_tokens - last_prompt_tokens
                    task_completion_tokens = completion_tokens - last_completion_tokens

                    last_total_tokens = total_tokens
                    last_prompt_tokens = prompt_tokens
                    last_completion_tokens = completion_tokens

                    # add token to results
                    result = result[0:2] + [task_total_tokens, task_prompt_tokens, task_completion_tokens] + result[2:]

                    file_path = os.path.join(self.output_csv_dir, self._get_identifier(user_type, desc) + ".csv")
                    # self._output_csv(result, file_path)

                    n_run += 1
                    n_agent_error += agent_error
                    n_exceed_maximum_tries += exceed_maximum_tries
                    n_abort += abort
                    n_finish += finish
                    n_evaluation_error += evaluation_error
                    n_sql_str_equal += sql_str_equal
                    n_sql_equivalent += sql_equivalent
                    n_result_equal += result_equal
                    sum_n_func_call += n_func_call
                    sum_n_abort_func_call += (n_func_call if abort else 0)
                    sum_n_finish_func_call += (n_func_call if finish else 0)
                    sum_call_response_ratio += n_func_call / n_tries if n_tries else 1

                    avg_func_call = round(sum_n_func_call / (n_run - n_agent_error), 2) if (
                            n_run - n_agent_error) else -1
                    avg_abort_func_call = round(sum_n_abort_func_call / n_abort, 2) if n_abort else -1
                    avg_finish_func_call = round(sum_n_finish_func_call / n_finish, 2) if n_finish else -1
                    avg_call_response_ratio = sum_call_response_ratio / n_run

                    summ = f"""
                        =========== {desc} ================
                        user:{user_type},
                        n_run : {n_run},
                        n_agent_error : {n_agent_error},
                        n_exceed_maximum_tries : {n_exceed_maximum_tries},
                        n_abort : {n_abort},
                        n_finish : {n_finish},
                        avg_tries : {avg_func_call},
                        avg_abort_tries : {avg_abort_func_call},
                        avg_finish_tries : {avg_finish_func_call},
                        avg_call_response_ratio : {avg_call_response_ratio}
                        n_evaluation_error : {n_evaluation_error},
                        n_sql_str_equal : {n_sql_str_equal},
                        n_sql_equivalent: {n_sql_equivalent},
                        n_result_equal : {n_result_equal}
                        =================================\n\n
                    """

                    print(summ)

                    self._record_task(f'{n_run}th summary',
                                      {
                                          "user": user_type,
                                          "n_run": n_run,
                                          "n_agent_error": n_agent_error,
                                          "n_exceed_maximum_tries": n_exceed_maximum_tries,
                                          "n_abort": n_abort,
                                          "n_finish": n_finish,
                                          "avg_tries": avg_func_call,
                                          "avg_abort_tries": avg_abort_func_call,
                                          "avg_finish_tries": avg_finish_func_call,
                                          "avg_call_response_ratio": avg_call_response_ratio,
                                          "n_evaluation_error": n_evaluation_error,
                                          "n_sql_str_equal": n_sql_str_equal,
                                          "n_sql_equivalent": n_sql_equivalent,
                                          "n_sql_result_equal": n_result_equal
                                      })



                except Exception as exc:
                    traceback.print_exc()
                    print(f"Task generated an exception: {exc}")

        print(f"log: {self.task_recorder.current_file}")

        with open(f"result/{self._get_identifier(user_type, desc)}.txt", 'w') as f:
            f.write(summ)
            f.write(f"log: {self.task_recorder.current_file}\n")

    def _get_identifier(self, user_type, desc):
        return f"v{self.test_version}_{self.llm}_{desc}_{self.task_recorder.current_file[:-5].split('_', 1)[1]}"

    def _output_csv(self, data, file_path):
        """
        将元组 data 写入 CSV 文件。
        如果文件不存在或为空，则写入 headers。
        """

        file_exists = os.path.isfile(file_path)
        is_empty = not file_exists or os.stat(file_path).st_size == 0

        with open(file_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 如果是空文件，先写入 header
            if is_empty:
                writer.writerow(self.headers)

            # 写入数据行
            writer.writerow(data)

    def _process_user_db_tasks(self, task, user_type):

        user = task[user_type]
        db = task["db_id"]

        db_path = self.db_path.format(user, user, db)
        # agent = NL2SQLAgent(db_path, self.server_config_path)
        # msg_task = Msg(
        #     name="user",
        #     content=task["question"],
        #     role="user",
        # )

        agent = RunPromptAgent(db_path, self.server_config, model_config_name=self.llm)

        task_information = {
            'task_id': task['question_id'],
            'question': task['question'],
            'ground_truth_sql': task['SQL'] if user_type == 'pos_user' else None,
        }

        try:
            # n_tries, response = agent(msg_task)
            response = agent.run_prompt(self.prompt_name, question=task['question'])
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

                if task_information['ground_truth_sql'] is not None:
                    task_information['predict_sql'] = response

                    try:
                        if self.coarse_eval_only:
                            evaluator = NL2SQLEvaluator()
                            evaluate_result = evaluator.coarse_gran_evaluate(response, task['SQL'])
                        else:
                            evaluator = NL2SQLEvaluator(db_path)
                            evaluate_result = evaluator.fine_gran_evaluate(response, task['SQL'])
                        print(evaluate_result)
                        task_information.update(evaluate_result)

                    except Exception as e:
                        task_information['evaluation_error'] = str(e)

        except Exception as e:
            memory, _, _ = agent.get_process()
            task_information.update({
                'memory': memory,
                'agent_error': str(e),
            })

        self._record_task(task['question_id'], task_information)

        task_execution_flag = [
            task_information['task_id'],
            1,
            'agent_error' in task_information,
            'exceed_maximum_tries' in task_information,
            'abort' in task_information,
            True if 'predict_sql' in task_information else False,
            'evaluation_error' in task_information,
            task_information['n_tries'] if 'n_tries' in task_information else 0,
            task_information['n_func_call'] if 'n_func_call' in task_information else 0,
            int(task_information.get('sql_str_equal', False)),
            int(task_information.get('sql_equivalent', False)),
            int(task_information.get('sql_result_equal', False))
        ]

        return task_execution_flag

    def _record_task(self, task_id, data):
        self.task_recorder.record(task_id, data)
        self.task_recorder.write(task_id)

    def _load_task(self, data_path: str, n_sample: Optional[int] = None):
        with open(data_path, 'r') as file:
            dataset = json.load(file)

        shuffle(dataset)
        dataset = dataset[:n_sample if n_sample is not None else len(dataset)]

        repeated_dataset = []
        for i in range(self.n_repeats):
            modified = [{**task, 'question_id': f"{task['question_id']}_{i}"} for task in dataset]
            repeated_dataset.extend(modified)

        return repeated_dataset

    def _test_extract_sql(self):
        log_files = ['record_20250525_230502.json']

        for log_file in log_files:
            with open(f'log/{log_file}', 'r') as f:
                log_json = json.load(f)

            with open(f'log/sql_from_{log_file}', 'w') as f:
                for task_id, task_log in log_json.items():
                    if 'predict_sql' not in task_log[0]:
                        continue

                    try:
                        f.write(f"{task_log[0]['ground_truth_sql']}\n")
                        f.write(f"{task_log[0]['predict_sql']}\n\n")
                    except Exception as e:
                        print(task_log[0])

    def _test_compare(self):
        log_files = ['record_20250526_230003.json', 'record_20250526_231419.json']
        for log_file in log_files:
            with open(f'log/{log_file}', 'r') as f:
                log_json = json.load(f)

                idx = []
                after_schema_try = []
                for task_id, task in log_json.items():
                    task = task[0]
                    id = -1
                    if 'summary' not in task_id and 'agent_error' not in task:
                        memories = task['memory']
                        for mem in memories:
                            if 'Execute function get_schema' in mem:
                                id = (int(mem[8: mem.find('}')]) - 1) // 2
                                idx.append(id)
                                break
                        if id != -1:
                            after_schema_try.append(task['n_tries'] + 1 - id)
                print(np.mean(idx), np.mean(after_schema_try))
