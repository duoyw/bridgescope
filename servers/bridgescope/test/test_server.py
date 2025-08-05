import agentscope
from agents.prompt_agent import RunPromptAgent

agentscope.init(
    model_configs={
        "config_name": "my-qwen-max",
        "model_type": "dashscope_chat",
        "model_name": "qwen-max",
        "api_key": "sk-0JVspnhOLC",
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    },
)

db_path = 'postgresql://col_user_0:col_user_0@localhost:5432/california_schools'
agent = RunPromptAgent(db_path, {
            "config_path": f'/nas/ldd461759/code/mcp_db/MCP4DB/servers/bridgescope/test/mcp.json',
            "config_name": "fine_gran_tool_with_priv_desc"} # single_exec_tool_with_priv_desc
, model_config_name="my-qwen-max", args={
        "exclude_tools": ["close_db_conn", 'commit', 'begin', 'rollback'],
        "keep_parameter":False
                       })

# UPDATE frpm SET "District Name" = 'hahaha' WHERE "School Name"='FAME Public Charter'
# response = agent.run_prompt("nl2all", task="""
# """)
#
# print("response=", response)
#
# memory, n_function_call, n_model_response = agent.get_process()
# print(memory)
