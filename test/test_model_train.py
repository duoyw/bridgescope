import json
import os
import sys
import traceback
sys.path.append("../")
sys.path.append("../servers/bridgescope")
sys.path.append("../servers/ProxyServer")
if 'TEAMCITY_VERSION' in os.environ:
    del os.environ['TEAMCITY_VERSION']
import unittest
from typing import Optional

import agentscope
from agentscope.agents import ReActAgent
from agentscope.message import Msg
from agentscope.service import ServiceToolkit
from agentscope.web.workstation.workflow import load_config

from agents.db_agent import load_mcp_config
from agents.model_train_agent import ModelTrainAgent
from evaluator.model_train_evaluator import ModelTrainEvaluator
from test_bird_base import TestBirdWrite
from test_model_train_proxy import TestProxy


class TestModelTrain(TestProxy):
    def setUp(self):
        super().setUp()
        self.data_path = "../benchmark/proxy/smallBenchmark.json"
        self.db_path = 'postgresql://postgres:postgres@localhost:5432/smallcaliforniahousing'  # +asyncpg
        self.prompt_name = "model_train_prompt"
        self.n_samples = 30
        self.test_version = "-modelTrain"
        self.server_config = {
            "config_path": 'mcp.json',
            "config_name": "model_train"
        }

    def test_evaluate(self):
        tasks = self._load_task(self.data_path, self.n_samples)
        self._evaluate(tasks, user_type="pos_user", desc="w_tool_desc")


if __name__ == '__main__':
    unittest.main()
