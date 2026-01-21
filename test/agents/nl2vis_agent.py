from typing import Optional, Union, Sequence

from agentscope.message import Msg, ToolUseBlock

from agents.db_agent import DBAgent
from agents.prompt_agent import RunPromptAgent


class NL2VISAgent(RunPromptAgent):

    def __init__(self, db_path, mcp_server_config_path, model_config_name, name='Friday'):
        super().__init__(db_path, mcp_server_config_path, name, model_config_name)

    def _finish_response(self, memory):
        return self._extract_last_draw_tool(memory)

    def _extract_last_draw_tool(self, memory):
        for tool_call in self.tool_callings[::-1]:
            # assert isinstance(tool_call, ToolUseBlock), "Tool call should be a ToolUseBlock instance"
            tool_call: ToolUseBlock = tool_call
            if tool_call['name'] in ['create_line', 'create_bar', 'create_pie', "create_scatter", "proxy"]:
                return tool_call
        return None
