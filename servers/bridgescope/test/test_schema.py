import json

from tools.context_tools.schema import get_database_schema, get_top_level_objects, get_object_details
from server import *

class mock_args:
    def __init__(self):
        self.dsn = ""
        self.mp = None
        self.n = None
        self.disable_tool_priv = False
        self.disable_fine_gran_tool = False
        self.wo = ''#'wo.txt'
        self.bo = 'bo.txt'
        self.wt = ''
        self.bt = ''
        self.persist = False


class TestSchema:
    """Test schema retrievals."""

    def __init__(self):
        self.db = "california_schools"
        self.args = mock_args()
        self.user_privilege = self.load_user_privilege_config(self.db)

    def load_user_privilege_config(self, db):
        """Load user privilege configuration from JSON file."""
        config_path = os.path.join(os.path.dirname(__file__), 'user_2_table_privilege_config.json')
        with open(config_path, 'r') as f:
            return json.load(f)[db]

    async def prepare_mcp_context(self, user):
        self.args.dsn = "postgresql://{}:{}@localhost:5432/{}".format(user, user, self.db)
        mcp_context.context = await init_global_server_context(self.args)

    async def test_user(self):
        """Test schema operations for all users."""
        for user in self.user_privilege:
            print(f"\n=== Testing user: {user} ===")
            await self.prepare_mcp_context(user)
            print("=== get schema ===")
            res = await get_database_schema()
            print(res[0].text)

            print("=== get top level objects ===")
            res = await get_top_level_objects()
            print(res[0].text)

            print("=== get object details ===")
            res = await get_object_details("TABLE", "schools")
            print(res[0].text)

if __name__ == "__main__":
    test_instance = TestSchema()
    asyncio.run(test_instance.test_user())
