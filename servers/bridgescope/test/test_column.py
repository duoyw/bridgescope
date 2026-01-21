import json

from db_adapters.db_constants import DB_PRIV_ENUM
from server import *
from tools.context_tools.schema import get_database_schema, get_top_level_objects, get_object_details
from tools.execution_tools import execute_sql_by_action
from tools.utils import get_db_adapter


class mock_args:
    def __init__(self):
        self.dsn = ""
        self.mp = None
        self.n = None
        self.disable_tool_priv = False
        self.disable_fine_gran_tool = False
        self.wo = '' #'wo.txt'
        self.bo = ''#'bo.txt'
        self.wt = ''
        self.bt = ''
        self.persist = False

class TestExecute:
    """Test schema retrievals."""

    def __init__(self):
        self.db = "california_schools"
        self.type = "SELECT"
        self.sample = 150
        # self.type = "UPDATE"
        # self.sample = 50
        self.args = mock_args()
        self.bench, self.task_2_user = self.load_test_sql()

    def load_test_sql(self):
        with open(f"/nas/ldd461759/code/mcp_db/MCP-DB-Universal/benchmark/nl2trans_sql/new_bench/{self.type.lower()}_bench_{self.sample}.json", 'r') as f:
            tasks = [task for task in json.load(f) if task["db_id"] == self.db]

        with open(f"/nas/ldd461759/code/mcp_db/MCP-DB-Universal/benchmark/nl2trans_sql/new_bench/column_{self.type}_bench_user_priv.json", 'r') as f:
            all_task_2_user = json.load(f)

        return tasks, {task["question_id"]: all_task_2_user[str(task["question_id"])] for task in tasks}

    async def prepare_mcp_context(self, user):
        self.args.dsn = "postgresql://{}:{}@localhost:5432/{}".format(user, user, self.db)
        mcp_context.context = await init_global_server_context(self.args)

    async def test(self, user_type):
        for task in self.bench:
            user = self.task_2_user[task["question_id"]][user_type]
            if isinstance(user, list):
                user = user[0]

            await self.prepare_mcp_context(user)

            print("=== get schema ===")
            print(await get_database_schema())

            print("=== get top level objects ===")
            print(await get_top_level_objects())

            print("=== get object details ===")

            objects = [
                ("TABLE", "frpm"),
                ("TABLE", "schools"),
                ("COLUMN", "x")
            ]

            for type, obj_name in objects:
                try:
                    print(await get_object_details(type, obj_name))
                except Exception as e:
                    print(str(e))

            print("=== original SQL ===")
            print(task["pg_sql"])

            print("=== Type match ===")
            try:
                print(await execute_sql_by_action(task["pg_sql"], self.type))
            except Exception as e:
                print(e)

            print("=== Type mismatch ===")
            try:
                print(await execute_sql_by_action(task["pg_sql"], [a for a in DB_PRIV_ENUM if a != self.type][0]))
            except Exception as e:
                print(e)

            print("============\n")

            # await db_adapter.rollback()
            db_adapter = get_db_adapter()
            await db_adapter.close()


if __name__ == "__main__":
    test_instance = TestExecute()
    # user_type = "has_other_col_priv_user"
    user_type = "has_col_priv_user"
    asyncio.run(test_instance.test(user_type))
    # asyncio.run(test_instance.test_single(user_type))
