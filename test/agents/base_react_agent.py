# -*- coding: utf-8 -*-
"""An agent class that implements the ReAct algorithm. The agent will reason
and act iteratively to solve problems. More details can be found in the paper
https://arxiv.org/abs/2210.03629.
"""
import traceback
import re

from abc import abstractmethod
from typing import Optional, Union, Sequence, Any

from openai import APIStatusError
from shortuuid import uuid

from agentscope.exception import ResponseParsingError
from agentscope.agents import AgentBase
from agentscope.message import Msg, ToolUseBlock
from agentscope.service import (
    ServiceToolkit,
    ServiceResponse,
    ServiceExecStatus,
)

from agents.RegexTaggedContentParserV2 import RegexTaggedContentParserV2

INSTRUCTION_PROMPT = """## What You Should Do:
1. First, analyze the current situation, and determine your goal.
2. Then, check if your goal is already achieved. If so, call the "finish" tool and generate a response. Otherwise, think about how to achieve it with the help of provided tool functions.
3. Respond in the required format.
4. If your goal cannot be accomplished with given tool functions, immediately use the `abort` tool to stop further trying. **Do not attempt any additional or unrelated operations beyond the your goal.**.

## Note:
1. Fully understand the tool functions and their arguments before using them.
2. You should decide if you need to use the tool functions, if not then return an empty list in "tool" field.
3. Make sure the types and values of the arguments you provided to the tool functions are correct.
4. Don't take things for granted. For example, where you are, what's the time now, etc. You can try to use the tool functions to get information.
5. If the tool function execution fails, you should analyze the error and try to solve it.
6. It is prohibited to infer the execution result of the tool. 
"""  # noqa


class BaseReActAgent(AgentBase):
    """ A variant of the ReActAgent in AgentScope that implements the ReAct algorithm. Few feats:
        a. add an abort tool
        b. refined instruction
        c. enable getting memory & return memory when failed or abort.

    """

    def __init__(
            self,
            name: str,
            model_config_name: str,
            service_toolkit: ServiceToolkit,
            sys_prompt: str = "You're a helpful assistant named {name}.",
            max_iters: int = 10,
            verbose: bool = True,
            exclude_tools=None,
            keep_parameter=True
    ) -> None:
        """Initialize the ReAct agent with the given name, model config name
        and tools.

        Args:
            name (`str`):
                The name of the agent.
            sys_prompt (`str`):
                The system prompt of the agent.
            model_config_name (`str`):
                The name of the model config, which is used to load model from
                configuration.
            service_toolkit (`ServiceToolkit`):
                A `ServiceToolkit` object that contains the tool functions.
            max_iters (`int`, defaults to `10`):
                The maximum number of iterations of the reasoning-acting loops.
            verbose (`bool`, defaults to `True`):
                Whether to print the detailed information during reasoning and
                acting steps. If `False`, only the content in speak field will
                be print out.
        """
        super().__init__(
            name=name,
            sys_prompt=sys_prompt,
            model_config_name=model_config_name,
        )

        self.service_toolkit = service_toolkit
        self.exclude_tools = exclude_tools
        self.keep_parameter_desc = keep_parameter

        # Add `finish` function to the toolkit to allow agent to end
        # the reasoning-acting loop
        self.service_toolkit.add(self.finish)
        self.service_toolkit.add(self.abort)

        self.verbose = verbose
        self.max_iters = max_iters

        if not sys_prompt.endswith("\n"):
            sys_prompt = sys_prompt + "\n"

        self.sys_prompt = "\n".join(
            [
                # The brief intro of the role and target
                sys_prompt.format(name=self.name),
                # The instruction prompt for tools
                self.get_filtered_tool_instruction(),
                # The detailed instruction prompt for the agent
                INSTRUCTION_PROMPT,
            ],
        )

        # Put sys prompt into memory
        self.memory.add(Msg("system", self.sys_prompt, role="system"))
        self.tool_callings = []

        # Initialize a parser object to formulate the response from the model
        self.parser = RegexTaggedContentParserV2(
            format_instruction="""Respond with specific tags as outlined below:
<thought>{what you thought}</thought>
<function>{the function name you want to call}</function>
<{argument name}>{argument value}</{argument name}>
<{argument name}>{argument value}</{argument name}>
...""",  # noqa
            try_parse_json=True,
            required_keys=["thought", "function"],
        )

    def get_filtered_tool_instruction(self):
        """
            Reformat the tools instruction string with specified tools excluded and renumbered.
        """
        """
            Return the tools instruction string with specified tools excluded and their parameter lines optionally removed.

            :param exclude_tools: List of function names to exclude.
            :param keep_parameters: Whether to keep the parameter lines for excluded tools (default is False).
            :return: Filtered tool instructions string.
            """
        original_instruction = self.service_toolkit.tools_instruction

        if not original_instruction:
            return ""

        if not self.exclude_tools:
            return original_instruction

        lines = original_instruction.splitlines()
        result_lines = []
        in_tool_block = False
        current_tool_name = ""
        tool_count = 0

        instruction_templates = lines[:8]
        if not self.keep_parameter_desc:
            instruction_templates = instruction_templates[:4] + instruction_templates[-2:]

        result_lines.extend(instruction_templates)

        skip_nxt = False
        for line in lines[8:]:

            if skip_nxt:
                skip_nxt = False
                continue

            # 匹配工具标题行，如 "1. get_user_by_id: 根据ID获取用户信息"
            match = re.match(r"^\d+\.\s+(\w+):(\s+(.+))?", line)
            if match:
                tool_name = match.group(1)
                in_tool_block = True
                current_tool_name = tool_name

                if tool_name in self.exclude_tools:
                    continue

                tool_count += 1
                new_line = f"{tool_count}. {tool_name}: {match.group(2)}"
                result_lines.append(new_line)

                if tool_name in ['finish', 'abort']:
                    skip_nxt = True

            elif in_tool_block:
                if current_tool_name in self.exclude_tools:
                    continue

                param_match = re.match(r"^\t(\w+)\s+\((\w+)\):\s*(.+)$", line)
                if param_match:
                    # 参数行
                    if current_tool_name not in self.exclude_tools and self.keep_parameter_desc:
                        result_lines.append(line)
                    else:
                        continue
                else:
                    # 非参数行（可能是空行或其他内容），直接保留
                    if line and current_tool_name in ['finish', 'abort']:
                        result_lines.append(f'   {line}')
                    else:
                        result_lines.append(line)
            else:
                result_lines.append(line)

        return "\n".join(result_lines)

    @abstractmethod
    def reply(self, x: Optional[Union[Msg, Sequence[Msg]]] = None) -> Any:
        pass

    def _reasoning(self) -> Union[ToolUseBlock, None]:
        """The reasoning process of the agent.

        Returns:
            `Union[ToolUseBlock, None]`:
                Return `None` if no tool is used, otherwise return the tool use
                block.
        """
        # Assemble the prompt
        prompt = self.model.format(
            self.memory.get_memory(),
            # Hint LLM how to respond without putting hint message into memory
            Msg(
                "system",
                self.parser.format_instruction,
                role="system",
                echo=False  # self.verbose,
            ),
        )

        retry_count = 0
        max_retries = 10

        while retry_count <= max_retries:
            # Get the response from the model and print it out
            try:
                raw_response = self.model(prompt)
                if self.verbose:
                    self.speak("--------------------response start----------------------")
                    self.speak(raw_response.stream or raw_response.text)
                    self.speak("--------------------response end----------------------")

                # Try to parse the response into tool use block
                res = self.parser.parse(raw_response)
                # Compose into a tool use block
                function_name: str = res.parsed["function"]
                input_ = {
                    k: v
                    for k, v in res.parsed.items()
                    if k not in ["function", "thought"]
                }
                self.memory.add(Msg(self.name, raw_response.text, role="assistant"))

                return ToolUseBlock(
                    type="tool_use",
                    id=uuid,
                    name=function_name,
                    input=input_,
                )

            except ResponseParsingError as e:
                traceback.print_exc()
                retry_count += 1
                if retry_count > max_retries:
                    self.speak("Maximum retries reached. Giving up.")
                    self.memory.add(Msg(self.name, raw_response.text, role="assistant"))
                    self.memory.add(
                        Msg("system",
                            "Failed to generate valid tool use format after multiple attempts. Please abort task.",
                            "system",
                            echo=self.verbose))
                    return None
            except APIStatusError as e:
                self.speak(f"API error: {e}. Retrying...")
                retry_count += 1
        return None

    def _acting(self, tool_call: ToolUseBlock) -> Union[None, tuple]:
        """The acting process of the agent, which takes a tool use block as
        input, execute the function and return a message if the `finish`
        function is called.

        Args:
            tool_call (`ToolUseBlock`):
                The tool use block to be executed.

        Returns:
            `Union[None, Msg]`:
                Return `None` if the function is not `finish`, otherwise return
                a message to the user.
        """
        # The execution message, may be execution output or error information
        if tool_call["name"] == "proxy":
            tool_call["input"]["server_config"] = self.server_config

        is_trans, trans_op = self._is_trans_tool(tool_call)
        if is_trans:
            msg_execution = Msg(
                name=self.name,
                content=f"Execute function {trans_op}, result: done",
                role="user",
            )
        else:
            msg_execution = self.service_toolkit.parse_and_call_func(tool_call)

        if "train_" in tool_call["name"]:
            if "aaa" not in msg_execution.content:
                msg_execution = self.service_toolkit.parse_and_call_func(tool_call)
                raise RuntimeError(msg_execution.content)

        self.tool_callings.append(tool_call)

        self._acting_post_handle(tool_call, msg_execution)

        if msg_execution.role == "system":
            msg_execution.role = "user"
            msg_execution.name = "user"

        if len(msg_execution.content) > 10000:
            msg_execution.content = "Error: The result is too large. Please avoid generating excessively long outputs by Proxy tool."

        if self.verbose:
            self.speak(msg_execution)

        self.memory.add(msg_execution)

        if tool_call["name"] in ['abort', 'finish']:
            return tuple([tool_call["name"], self.memory])

        return None

    def _is_trans_tool(self, tool_call: ToolUseBlock):
        """Check if the tool call is a transition tool, which is used to
        transition the agent to a new state.

        Args:
            tool_call (`ToolUseBlock`):
                The tool use block to be checked.

        Returns:
            `bool`:
                Return `True` if the tool call is a transition tool, otherwise
                return `False`.
        """
        if tool_call["name"] in ["begin", "commit", "rollback"]:
            return True, tool_call["name"]

        if tool_call["name"].lower() == "execute":
            if "sql" in tool_call["input"]:
                sql = tool_call["input"]["sql"].strip()
                if sql.lower() in {"begin;", "commit;", "rollback;"}:
                    return True, sql
        return False, None

    def _acting_post_handle(self, tool_call, msg_execution):
        pass

    def _summarizing(self) -> Msg:
        """Generate a response when the agent fails to solve the problem in
        the maximum iterations."""
        hint_msg = Msg(
            "user",
            "You have failed to generate response within the maximum "
            "iterations. Now respond directly by summarizing the current "
            "situation.",
            role="user",
            echo=self.verbose,
        )

        # Generate a reply by summarizing the current situation
        prompt = self.model.format(
            self.memory.get_memory(),
            hint_msg,
        )
        res = self.model(prompt)
        self.speak(res.stream or res.text)
        res_msg = Msg(self.name, res.text, "assistant")
        return res_msg

    @staticmethod
    def finish(response: str) -> ServiceResponse:
        """Finish reasoning and generate a response to the user.
   **Arguments**
   - response (str): The response to the user.

   **Note**
   - The function won't be executed, actually."""

        # - sql (str): The SQL statement to run.
        #      Note:
        #          The function won't be executed, actually.
        #
        #      Args:
        #          response (`str`):
        #              The response to the user.
        #      """
        return ServiceResponse(
            status=ServiceExecStatus.SUCCESS,
            content=response,
        )

    @staticmethod
    def abort(response: str) -> ServiceResponse:
        """Abort reasoning and generate a response to the user.
   **Arguments**
   - response (str): The response to the user.

   **Note**
   - The function won't be executed, actually."""
        #
        # Note:
        #     The function won't be executed, actually.
        #
        # Args:
        #     response (`str`):
        #         The response to the user.
        # """
        return ServiceResponse(
            status=ServiceExecStatus.SUCCESS,
            content=response,
        )

    def get_process(self):
        msgs = []
        n_function_call, n_model_response = 0, 0
        for id, msg in enumerate(self.memory._content):
            if msg.role == 'assistant':
                n_model_response += 1
            if msg.role == 'user' and msg.content.startswith("1. Execute function "):
                n_function_call += 1

            msgs.append({
                'id': {id},
                'timestamp': {msg.timestamp},
                'role': {msg.role},
                'content': {msg.content}
            })

        return msgs, n_function_call, n_model_response
