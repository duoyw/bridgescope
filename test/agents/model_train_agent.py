import ast
import re
from typing import Optional, Union, Sequence

from agentscope.message import Msg, ToolUseBlock

from agents.db_agent import DBAgent
from agents.prompt_agent import RunPromptAgent


class ModelTrainAgent(RunPromptAgent):

    def __init__(self, db_path, mcp_server_config_path, model_config_name, name='Friday'):
        super().__init__(db_path, mcp_server_config_path, name, model_config_name)
        self.model_id = None
        self.predict_value = None

    def _acting_post_handle(self, tool_call: ToolUseBlock, msg_execution):
        model_id = self._extract_model_id(msg_execution.content)
        if model_id is not None:
            self.model_id = model_id

        predict = self._extract_predict_value(tool_call, msg_execution)
        if predict is not None:
            self.predict_value = predict

    def _finish_response(self, memory):
        return self.model_id, self.predict_value

    def _extract_model_id(self, text):
        # 正则：匹配 <aaa [uuid] aaa> 格式
        match = re.search(r"aaa.*aaa", text)
        if match:
            return match.group(0)
        else:
            return None

    def _extract_predict_value(self, tool_call: ToolUseBlock, msg_execution):
        match = re.search(r"<predict>(.*)</predict>", msg_execution.content)
        if match:
            data = match.group(1)
            return int(ast.literal_eval(data)[0])
        else:
            return None
