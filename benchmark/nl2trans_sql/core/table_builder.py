import re

from agentscope.agents import DialogAgent
import agentscope
from agentscope.message import Msg

from benchmark.nl2trans_sql.core.utils import load_prompt

agentscope.init(
    model_configs="/nas/ldd461759/code/mcp_db/MCP4DB/benchmark/nl2trans_sql/core/model_config.json"
)


class SchemaBuilder:
    model = 'qwen-max' #, 'gpt-4o'
    user_prompt = """
SQL: {}
    
Natural language description: {}

Example result: {}
    """
    def __init__(self):
        self.agent = DialogAgent(
            name='SchemaBuilder',
            model_config_name=self.model,
            sys_prompt=load_prompt('/nas/ldd461759/code/mcp_db/MCP4DB/benchmark/nl2trans_sql/core/prompts/generate_table_column_name_prompt.txt')
        )

    def build(self, sql, question, example_data):
        msg = Msg(name='user',
                  content=self.user_prompt.format(sql, question, example_data),
                  role='user')

        response = self.agent(msg).content
        return self._parse_result(response)

    def _parse_result(self, response):
        table_match = re.search(r'<Table>\s*(.*?)\s*</Table>', response)
        table = table_match.group(1) if table_match else None

        columns = []
        columns_type = []

        column_matches = re.findall(r'<Column>\s*(.*?)\s*,\s*(.*?)\s*</Column>', response)

        for column_name, column_type in column_matches:
            column_name = column_name.strip()
            column_type = column_type.strip()
            columns.append(column_name)
            columns_type.append(column_type)

        return table, columns, columns_type
