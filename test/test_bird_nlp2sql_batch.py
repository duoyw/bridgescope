import sys
import unittest

sys.path.append("../")
sys.path.append("../servers/bridgescope")
sys.path.append("../servers/ProxyServer")
from test_bird_nlp2sql import TestBirdNlp2Sql


class TestBirdNlp2SqlBatch(TestBirdNlp2Sql):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_types = [
            "full_priv_user",
            # "read_only_user",
            # "other_table_only_user"
        ]

        self.server_types = [
            'w_tool_desc',
            # 'wo_tool_desc',  # ignore
            # 'w_tool_desc_single',
            'wo_tool_desc_single',  # baseline,
            # 'wo_tool_desc_execute'  # without get_schema
        ]

        # self.ops = ["select", "insert", "delete", "update"]
        # self.ops = ["insert", "delete", "update"]
        self.ops = ["insert"]
        self.conf = [(st, ut) for st in self.server_types for ut in self.user_types]
        self.op2test = {
            "insert": self._test_evaluate_insert,
            "delete": self._test_evaluate_delete,
            "update": self._test_evaluate_update,
            "select": self._test_evaluate_select
        }

    def generate_tests(self):
        for op in self.ops:
            for ut in self.user_types:
                for st in self.server_types:
                    def test_method(self, st=st, ut=ut, op=op):
                        test_func = self.op2test[op]
                        test_func(st, ut)

                    test_method.__name__ = f"test_{op}_{ut}_{st}"
                    setattr(TestBirdNlp2SqlBatch, test_method.__name__, test_method)


TestBirdNlp2SqlBatch().generate_tests()

if __name__ == "__main__":
    unittest.main()
