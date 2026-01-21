import json

from tools.context_tools.column_value import search_relative_column_values
from server import *
from tools.utils import get_db_adapter


class mock_args:
    def __init__(self):
        self.dsn = ""
        self.mp = ""
        self.n = None
        self.disable_tool_priv = False
        self.disable_fine_gran_tool = False
        self.wo = ''
        self.bo = ''#'bo.txt'
        self.wt = ''
        self.bt = ''
        self.persist = False

class TestColumnValue:
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

            db_adapter = get_db_adapter()
            await db_adapter.begin()

            print("=== get column value ===")
            try:
                print(await search_relative_column_values({
                    'frpm."Educational Option Type"': "Trad."}))
            except Exception as e:
                print(str(e))

            await db_adapter.rollback()
            await db_adapter.close()


if __name__ == "__main__":
    test_instance = TestColumnValue()
    asyncio.run(test_instance.test_user())
