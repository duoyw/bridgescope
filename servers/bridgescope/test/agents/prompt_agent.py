from agentscope.message import Msg

from agents.db_agent import DBAgent
from prompts.nl2all import nl2all
from prompts.nl2trans import nl2trans


prompts = {
    'nl2all': nl2all,
    "nl2trans": nl2trans,
    }


class RunPromptAgent(DBAgent):

    def __init__(self, db_path, mcp_server_config_path, name='Friday', model_config_name="my-qwen-max", readonly=False,
                 args=None):
        super().__init__(db_path, mcp_server_config_path, name, model_config_name, readonly, args)

    def run_prompt(self, name, **args):
        prompt = self.get_prompt(name, **args)
        return self.reply(Msg(
            name=prompt.role,
            content=prompt.content,
            role=prompt.role)
        )

    def get_prompt(self, name, **args):
        return prompts[name](**args).messages[0]
