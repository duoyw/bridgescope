# An agentscope-version of Mixture-of-Agents https://github.com/togethercomputer/MoA
import json
import os
import re

import agentscope
from agentscope.agents import DialogAgent
from agentscope.message import Msg

from benchmark.nl2trans_sql.core.utils import load_prompt

agentscope.init(
    model_configs="/nas/ldd461759/code/mcp_db/MCP4DB/benchmark/nl2trans_sql/core/model_config.json"
)

class Sql2Text:
    reference_model = ['qwen-max', 'gpt-4o']
    aggregator_model = 'claude-3-7'
    n_retries = 3

    reference_user_prompt = """
SQL: {}

** Reference **
- SQL: {}
- Question: {}
- Evidence: {}
    """

    def build_all(self, base_dir, filename):
        output_file = os.path.join(base_dir, f"full_{filename}")
        task_file = os.path.join(base_dir, filename)
        with open(task_file, 'r') as f:
            tasks = json.load(f)

        for task in tasks:
            sel_sql = task['pg_sql']
            sel_q = task['question']
            evidence = task['evidence']
    
            trans_sql = task['trans_sql']
            user_prompt = self.reference_user_prompt.format(trans_sql, sel_sql, sel_q, evidence)
            
            question, evidence = self._gen_question(user_prompt)
            task['trans_question'] = question
            task['trans_evidence'] = task['evidence'] if evidence.lower() == 'yes' else ''

        with open(output_file, 'w') as f:
            json.dump(tasks, f, indent=4)

    def build_one(self, task):
        sel_sql = task['base_pg_sql']
        sel_q = task['base_question']
        evidence = task['base_evidence']

        trans_sql = task['pg_sql']
        user_prompt = self.reference_user_prompt.format(trans_sql, sel_sql, sel_q, evidence)

        question, evidence = self._gen_question(user_prompt)
        task['question'] = question
        task['evidence'] = task['evidence'] if evidence.lower() == 'yes' else ''

    def _init_reference_agent(self, model_name):
        return DialogAgent(
            name='NL2Trans_sql',
            model_config_name=model_name,
            sys_prompt=load_prompt('/nas/ldd461759/code/mcp_db/MCP4DB/benchmark/nl2trans_sql/core/prompts/sql2text_reference_system_prompt.txt')
        )

    def _parse_result(self, response):
        question_match = re.search(r'<Task>\s*(.*?)\s*</Task>', response)
        evidence_match = re.search(r'<Evidence>\s*(.*?)\s*</Evidence>', response)

        question = question_match.group(1) if question_match else None
        evidence = evidence_match.group(1) if evidence_match else None

        return question, evidence

    def _gen_question(self, user_prompt):
        for i in range(self.n_retries):
            try:
                # reference
                reference_agents = [self._init_reference_agent(model_name) for model_name in self.reference_model]
                user_msg = Msg(name="user", content=user_prompt, role="user")
                results = [ra(user_msg).content for ra in reference_agents]

                # aggregate
                aggregator = self._init_reference_agent(self.aggregator_model)
                sys_prompt = load_prompt(
                    '/nas/ldd461759/code/mcp_db/MCP4DB/benchmark/nl2trans_sql/core/prompts/sql2text_aggregate_system_prompt.txt') + "\n" + "\n".join(
                            [f"{i + 1}. {str(element)}" for i, element in enumerate(results)])

                sys_msg = Msg(name='system', content=sys_prompt, role="system")
                aggregator.memory.add(sys_msg)
                question, evidence = self._parse_result(aggregator(user_msg).content)
                return question, evidence

            except Exception:
                pass

        raise Exception

def load_nl2sql_task(base_path, filename):
    task_file = os.path.join(base_path, filename)
    with open(task_file, 'r') as f:
        tasks = json.load(f)
    return tasks
