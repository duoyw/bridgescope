import ast
import re
from time import perf_counter
from typing import Optional

import sqlglot
from agentscope.agents import AgentBase
from agentscope.message import ToolUseBlock
from sqlalchemy import Subquery
from sqlglot import exp

from agents.base_react_agent import BaseReActAgent
from agents.utils import sync_exec
from db_adapters.db_config import DBConfig
from db_adapters.pg_adapter import PostgresAdapter


class ModelTrainEvaluator:
    def __init__(self):
        """
        Initialize the evaluator with a PostgreSQL database connection.

        Parameters:
            db_url (str): SQLAlchemy-compatible PostgreSQL URL.
                          Example: "postgresql+psycopg2://user:password@localhost:5432/dbname"
        """

    def evaluate(self, agent: BaseReActAgent, task, model_response):
        model_id, agent_predict_value = model_response
        # assert model_id is not None

        if task["category"] == 3:
            assert agent_predict_value is not None

        if agent_predict_value is None:
            agent_predict_value = self._get_agent_predict_value(agent, model_id, task)

        gt_predict_value = task["predict"]

        evaluate_result = {
            "predict_equal": agent_predict_value == gt_predict_value,
        }
        return evaluate_result

    def _get_agent_predict_value(self, agent: BaseReActAgent, model_id, task):
        tool_call: ToolUseBlock = {
            "name": "model_predict",
            "input": {
                "model_id": model_id,
                "features": [tuple(0.5 for i in range(len(task["features"])))]
            },
            "type": "tool_use",
            "id": "1"
        }
        result = agent.service_toolkit.parse_and_call_func(tool_call, tools_api_mode=True)

        match = re.search(r"<predict>(.*)</predict>", result.content[0]["output"][0]["text"])

        data = ast.literal_eval(match.group(1))[0]
        return int(data)
