from agents.db_agent import DBAgent

class NL2SQLAgent(DBAgent):

    sys_prompt  = r"""
        you are an expert database scientist named {name}. Given a user query, you need to generate a SQL query that can answer the user's question. 
        """

    def __init__(self, db_path, mcp_server_config_path, name='Friday', model_config_name="my-qwen-max"):
        super().__init__(db_path, mcp_server_config_path, name, model_config_name, system_prompt=self.sys_prompt)
