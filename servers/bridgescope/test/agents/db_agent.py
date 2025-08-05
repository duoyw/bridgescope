import json
import re
from contextlib import contextmanager
from typing import Optional, Union, Sequence, Dict

from abc import ABC
from agentscope.message import Msg, ToolUseBlock
from agentscope.service import ServiceToolkit
from loguru import logger
from shortuuid import uuid
from tenacity import retry

from agents.base_react_agent import BaseReActAgent


def load_mcp_config(config, repl: Dict):
    with open(config["config_path"]) as f:
        conf = f.read()
        for old, new in repl.items():
            conf = conf.replace(old, new)

    return json.loads(conf)[config["config_name"]]


class DBAgent(BaseReActAgent, ABC):

    def __init__(self, db_path, mcp_server_config_path, name='Friday', model_config_name="my-qwen-max", readonly=False,
                 args=None):

        self.server_config = load_mcp_config(mcp_server_config_path, {
            '<<db_dsn>>': db_path
        })

        toolkit = ServiceToolkit()
        toolkit.add_mcp_servers(self.server_config)

        if args is None:
            super().__init__(name=name, model_config_name=model_config_name, service_toolkit=toolkit)
        else:
            super().__init__(name=name, model_config_name=model_config_name, service_toolkit=toolkit, **args)

    def transaction_ctrl(self, op):
        if op not in ['begin', 'commit', 'rollback']:
            return

        tool_use = ToolUseBlock(
            type="tool_use",
            id=uuid,
            name=op,
            input={},
        )

        logger.info(f"trans: {op}")
        self.service_toolkit.parse_and_call_func(tool_use)

    @contextmanager
    def transaction_context(self):
        try:
            self.transaction_ctrl('begin')
            yield
        finally:
            self.transaction_ctrl('rollback')

    def reply(self, x: Optional[Union[Msg, Sequence[Msg]]] = None) -> str:
        with self.transaction_context():
            return self._reply(x)

    def _reply(self, x: Optional[Union[Msg, Sequence[Msg]]] = None) -> str:
        """The reply method of the agent."""
        self.memory.add(x)

        for n_tries in range(self.max_iters):
            # Step 1: Reasoning: decide what function to call
            print("begin reasoning...")
            tool_call = self._reasoning()
            print("end reasoning...")
            if tool_call is None:
                # Meet parsing error, skip acting to reason the parsing error,
                # which has been stored in memory
                continue

            # Step 2: Acting: execute the function accordingly
            print("begin acting...")
            memory_finish = self._acting(tool_call)
            print("end acting...")
            if memory_finish:
                tool, memory = memory_finish

                if tool == 'finish':
                    return self._finish_response(memory)
                elif tool == 'abort':
                    return self._abort_response(tool)

        # Generate a response when exceeding the maximum iterations
        return 'exceed_maximum_tries'

    def _finish_response(self, memory):
        return self._extract_last_sql(memory)

    def _abort_response(self, tool):
        return tool

    def _extract_last_sql(self, memory):

        def _parser(content):

            matches = re.finditer(
                self.parser.tagged_content_pattern,
                content,
                flags=re.DOTALL,
            )

            res = {}
            for match in matches:
                res[match.group("name")] = match.group("content")

            # Compose into a tool use block
            function_name: str = res["function"]
            input_ = {
                k: v
                for k, v in res.items()
                if k not in ["function", "thought"]
            }

            return function_name, input_

        for msg in memory._content[::-1]:
            if msg.role == 'assistant':
                func, args = _parser(msg.content)

                if func in ["SELECT", "UPDATE", "INSERT", "DELETE", "EXECUTE"]:
                    return args['sql']

        return None
