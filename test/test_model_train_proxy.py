import json
import sys
import traceback
import unittest
from typing import Optional

sys.path.append("../")
sys.path.append("../servers/bridgescope")
sys.path.append("../servers/ProxyServer")
import agentscope
from agentscope.agents import ReActAgent
from agentscope.message import Msg
from agentscope.service import ServiceToolkit
from agentscope.web.workstation.workflow import load_config

from agents.db_agent import load_mcp_config
from agents.model_train_agent import ModelTrainAgent
from evaluator.model_train_evaluator import ModelTrainEvaluator
from test_bird_base import TestBirdWrite


class TestProxy(TestBirdWrite):
    def setUp(self):
        super().setUp()
        self.output_csv_dir = "../experiment/proxy/"
        self.data_path = "../benchmark/proxy/benchmark.json"
        self.db_path = 'postgresql://postgres:postgres@localhost:5432/californiahousing'  # +asyncpg
        self.prompt_name = "model_train_proxy_prompt"
        self.num_workers = 1
        self.n_samples = 30
        # self.llm = 'qwen-max'  # 'claude-3-7'
        self.llm = 'gpt-4o'  # 'claude-3-7'
        # self.llm = 'claude-4'
        # self.llm = 'claude-3-7'
        self.test_version = "-proxy"
        self.server_config = {
            "config_path": 'mcp.json',
            "config_name": "model_train_proxy"
        }

    def test_evaluate(self):
        tasks = self._load_task(self.data_path, self.n_samples)
        self._evaluate(tasks, user_type="full_priv_user", desc="w_tool_desc")

    def _process_user_db_tasks(self, task, user_type):
        agent = ModelTrainAgent(self.db_path, self.server_config, model_config_name=self.llm)
        agent.memory._content[0].content += f"Server Configs: {agent.server_config}\n"

        question = task['task']
        print("question: ", question)

        task_information = {
            'task_id': task['question_id'],
            'question': question,
        }

        try:
            # n_tries, response = agent(msg_task)
            response = agent.run_prompt(self.prompt_name, question=question)
            memory, n_function_call, n_model_response = agent.get_process()

            task_information.update({
                'memory': memory,
                'n_tries': n_model_response,
                'n_func_call': n_function_call
            })

            if response in ['abort', 'exceed_maximum_tries']:
                task_information[response] = True

            else:
                task_information['model_id_and_predict'] = response

            try:
                evaluator = ModelTrainEvaluator()
                evaluate_result = evaluator.evaluate(agent, task, response)
                print(evaluate_result)
                task_information.update(evaluate_result)

            except Exception as e:
                traceback.print_exc()
                task_information['evaluation_error'] = str(e)

        except Exception as e:
            traceback.print_exc()
            memory, _, _ = agent.get_process()
            task_information.update({
                'memory': memory,
                'agent_error': str(e),
            })

        self._record_task(task['question_id'], task_information)

        task_execution_flag = [
            task_information['task_id'],
            task["category"],
            'agent_error' in task_information,
            'exceed_maximum_tries' in task_information,
            'abort' in task_information,
            True if 'model_id_and_predict' in task_information else False,
            'evaluation_error' in task_information,
            task_information['n_tries'] if 'n_tries' in task_information else 0,
            task_information['n_func_call'] if 'n_func_call' in task_information else 0,
            int(task_information.get('predict_equal', False)),
            0,
            0
        ]

        return task_execution_flag

    def _load_task(self, data_path: str, n_sample: Optional[int] = None):
        with open(data_path, 'r') as file:
            dataset = json.load(file)

        # todo: remove
        # dataset = dataset[20:]
        dataset = dataset[:n_sample if n_sample is not None else len(dataset)]

        for i, task in enumerate(dataset):
            task['question_id'] = i

        return dataset


if __name__ == '__main__':
    unittest.main()
