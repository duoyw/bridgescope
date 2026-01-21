import json

from db_adapters.db_constants import DB_PRIV_ENUM
from server import *
from tools.context_tools.schema import get_database_schema, get_object_details, get_top_level_objects
from tools.execution_tools import execute_sql_by_action
from tools.utils import get_db_adapter


class mock_args:
    def __init__(self):
        self.dsn = ""
        self.mp = None
        self.n = None
        self.disable_tool_priv = False
        self.disable_fine_gran_tool = False
        self.wo = 'wo.txt'
        self.bo = ''  # 'bo.txt'
        self.wt = ''  # 'wt.txt'
        self.bt = 'bt.txt'
        self.persist = False


class TestExecute:
    """Test schema retrievals."""

    def __init__(self):
        self.db = "california_schools_ext"
        self.args = mock_args()

        self.type = "SELECT"
        self.sample = 150
        # self.type = "UPDATE"
        # self.sample = 50
        self.args = mock_args()
        self.bench, self.task_2_user = self.load_test_sql()

    def load_test_sql(self):
        with open(
                f"/nas/ldd461759/code/mcp_db/MCP-DB-Universal/benchmark/nl2trans_sql/new_bench/view_select_bench_8.json",
                'r') as f:
            tasks = [task for task in json.load(f) if task["db_id"] == self.db]

        with open(
                f"/nas/ldd461759/code/mcp_db/MCP-DB-Universal/benchmark/nl2trans_sql/new_bench/view_select_bench_8_user_priv.json",
                'r') as f:
            all_task_2_user = json.load(f)

        return tasks, {task["question_id"]: all_task_2_user[str(task["question_id"])] for task in tasks}

    def test_to_test_sql(self):
        """从文件读取测试SQL查询"""
        sql_queries = []

        with open("view_query.sql", "r", encoding="utf-8") as f:
            content = f.read()

        # 按照空行分割不同的SQL查询块
        query_blocks = content.split('\n\n')

        query_id = 1
        for block in query_blocks:
            block = block.strip()
            if not block:
                continue

            lines = block.split('\n')
            comments = []
            sql_lines = []

            # 分离注释和SQL语句
            for line in lines:
                line = line.strip()
                if line.startswith('--'):
                    comments.append(line[2:].strip())
                elif line and not line.startswith('--'):
                    sql_lines.append(line)

            # 如果有SQL语句，则创建查询对象
            if sql_lines:
                sql_query = '\n'.join(sql_lines)

                # 提取用途和场景信息
                purpose = ""
                scenario = ""
                for comment in comments:
                    if comment.startswith('用途：'):
                        purpose = comment[3:].strip()
                    elif comment.startswith('场景：'):
                        scenario = comment[3:].strip()

                query_obj = {
                    "db_id": 'california_schools_ext',
                    "question_id": query_id,
                    "purpose": purpose,
                    "scenario": scenario,
                    "pg_sql": sql_query,
                    "comments": comments
                }

                sql_queries.append(query_obj)
                query_id += 1

        with open("../benchmark/nl2trans_sql/new_bench/view_select_bench_8.json", 'w') as f:
            json.dump(sql_queries, f, indent=4)

        return sql_queries

    async def prepare_mcp_context(self, user):
        self.args.dsn = "postgresql://{}:{}@localhost:5432/{}".format(user, user, self.db)
        mcp_context.context = await init_global_server_context(self.args)

    async def test_query(self, user_type):

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
                ("VIEW", "county_education_summary"),
                ("VIEW", "county_educat"),
                ("VIEW", "school_performance_summary"),
                ("COLUMN", "x")
            ]

            for type, obj_name in objects:
                try:
                    print(await get_object_details(type, obj_name))
                except Exception as e:
                    print(str(e))

            # await db_adapter.begin()

            print("=== original SQL ===")
            print(task["pg_sql"].replace('\n', ' '))

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
    user_types = ["full_priv_user", "read_only_user", "operate_table_only_user",
                  "reference_table_only_user", "other_table_only_user"]
    asyncio.run(test_instance.test_query("read_only_user"))
    # test_instance.test_to_test_sql()
