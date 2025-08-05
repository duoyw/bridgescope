import json

from server import *


class mock_args:
    def __init__(self):
        self.dsn = ""
        self.mp = None
        self.n = None
        self.disable_tool_priv = False
        self.disable_fine_gran_tool = False
        self.wo = ''#'wo.txt'
        self.bo = ''#bo.txt'
        self.wt = ''#'wt.txt'
        self.bt = 'bt.txt'
        self.persist = False

class TestExecute:
    """Test schema retrievals."""

    def __init__(self):
        self.db = "california_schools"
        self.args = mock_args()

        self.type = "SELECT"
        self.sample = 150
        # self.type = "UPDATE"
        # self.sample = 50
        self.args = mock_args()
        self.bench, self.task_2_user = self.load_test_sql()

    def load_test_sql(self):
        with open(
                f"/nas/ldd461759/code/mcp_db/MCP-DB-Universal/benchmark/nl2trans_sql/new_bench/{self.type.lower()}_bench_{self.sample}.json",
                'r') as f:
            tasks = [task for task in json.load(f) if task["db_id"] == self.db]

        with open(
                f"/nas/ldd461759/code/mcp_db/MCP-DB-Universal/benchmark/nl2trans_sql/new_bench/{self.type.lower()}_bench_{self.sample}_user_priv.json",
                'r') as f:
            all_task_2_user = json.load(f)

        return tasks, {task["question_id"]: all_task_2_user[str(task["question_id"])] for task in tasks}

    async def prepare_mcp_context(self, user):
        self.args.dsn = "postgresql://{}:{}@localhost:5432/{}".format(user, user, self.db)
        mcp_context.context = await init_global_server_context(self.args)

    async def test(self, user_type):
        for task in self.bench:
            print(f"=== {user_type} === ")
            user = self.task_2_user[task["question_id"]][user_type]
            await self.prepare_mcp_context(user)
            build_sql_exec_tools()
            tools = await mcp.list_tools()
            for idx, tool in enumerate(tools):
                print(idx, tool)
            break

if __name__ == "__main__":
    test_instance = TestExecute()
    user_types = ["full_priv_user", "read_only_user", "operate_table_only_user",
                  "reference_table_only_user", "other_table_only_user"]
    asyncio.run(test_instance.test(user_types[0]))
