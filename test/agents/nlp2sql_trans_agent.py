import ast
import re
from typing import Optional, Union, Sequence, Literal

from agentscope.message import Msg, ToolUseBlock

from agents.db_agent import DBAgent
from agents.prompt_agent import RunPromptAgent


class NL2SQLTransAgent(RunPromptAgent):

    def __init__(self, db_path, mcp_server_config_path, model_config_name, name='Friday', args=None):
        super().__init__(db_path, mcp_server_config_path, name, model_config_name, args=args)
        self.model_id = None
        self.predict_value = None

        # record all tools. we record the select/insert/... if it is an execute tool
        self.existed_trans = []

    def _acting_post_handle(self, tool_call: ToolUseBlock, msg_execution):
        self._handle_trans(tool_call, "begin")
        self._handle_trans(tool_call, "commit")
        self._handle_trans(tool_call, "rollback")

    def _handle_trans(self, tool_call: ToolUseBlock, target: Literal["begin", "commit", "rollback"]):
        if tool_call["name"].lower() == "execute":
            sql = tool_call["input"]["sql"].strip()
            if f"{target};" == sql.lower():
                self.existed_trans.append(target)
            if "select" in sql.lower():
                self.existed_trans.append("select")
            if "insert" in sql.lower():
                self.existed_trans.append("insert")
            if "update" in sql.lower():
                self.existed_trans.append("update")
            if "delete" in sql.lower():
                self.existed_trans.append("delete")
        elif tool_call["name"] == target:
            self.existed_trans.append(target.lower())

    def _finish_response(self, memory):
        return self.existed_trans

    def _abort_response(self, tool):
        return self.existed_trans
