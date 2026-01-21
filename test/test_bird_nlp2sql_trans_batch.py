import sys
import traceback
import unittest

sys.path.append("../")

from agents.nlp2sql_trans_agent import NL2SQLTransAgent
from evaluator.nl2sql_trans_evaluator import NL2SQLTransEvaluator
from test_bird_nlp2sql_batch import TestBirdNlp2SqlBatch
from loguru import logger
from test_bird_nlp2sql import TestBirdNlp2Sql


class TestBirdNlp2SqlTransBatch(TestBirdNlp2Sql):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_types = [
            "full_priv_user",
            "read_only_user",
            "other_table_only_user",
        ]

        self.server_types = [
            'w_tool_desc',
            'wo_tool_desc_single'  # without get_schema
        ]

        self.ops = ["select", "insert", "delete", "update"]
        # self.ops = ["insert", "delete", "update"]
        # self.ops = ["insert"]
        self.conf = [(st, ut) for st in self.server_types for ut in self.user_types]
        self.op2test = {
            "insert": self._test_evaluate_insert,
            "delete": self._test_evaluate_delete,
            "update": self._test_evaluate_update,
            "select": self._test_evaluate_select
        }

    def setUp(self) -> None:
        super().setUp()
        self.output_csv_dir = "../experiment/trans/"
        self.num_workers = 5
        self.n_samples = -1
        self.llm = 'qwen-max'  # 'claude-3-7'
        # self.llm = 'gpt-4o'  # 'claude-3-7'
        # self.llm = 'claude-4'
        # self.llm = 'claude-3-7'

        self.test_version = "-trans"

        self.agent_args = {
            "exclude_tools": ["close_db_conn"],
            "keep_parameter": False
        }

        self.prompt_name = 'nl2trans'

    def generate_tests(self):
        for op in self.ops:
            for ut in self.user_types:
                for st in self.server_types:
                    def test_method(self, st=st, ut=ut, op=op):
                        test_func = self.op2test[op]
                        test_func(st, ut)

                    test_method.__name__ = f"test_{op}_{ut}_{st}"
                    setattr(TestBirdNlp2SqlTransBatch, test_method.__name__, test_method)

    def _get_agent(self, db_path):
        agent = NL2SQLTransAgent(db_path, self.server_config, model_config_name=self.llm, args=self.agent_args)
        return agent

    def _agent_evaluate(self, db, response, sql, task, user_type):
        evaluator = NL2SQLTransEvaluator()
        evaluate_result = evaluator.evaluate(response, user_type)
        return evaluate_result

    def _check_finishable(self, task, user_type):
        return True



TestBirdNlp2SqlTransBatch().generate_tests()

if __name__ == "__main__":
    unittest.main()
