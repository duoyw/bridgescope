# test/evaluate_privilege.py
import argparse

from test_bird_nlp2sql import TestBirdNlp2Sql


def parse_arguments():
    parser = argparse.ArgumentParser(description='Test Bird NLP2SQL with configurable parameters')
    parser.add_argument('--user', type=str, required=True, choices=["administrator", "normal", "irrelevant"],
                        help='User type for testing')
    parser.add_argument('--llm', type=str, required=True, choices=["gpt-4o", "claude-4"],
                        help='LLM model to use')
    parser.add_argument('--n_samples', type=int, default=5, help='Number of samples to test (default: 5)')
    parser.add_argument('--algo', type=str, choices=["bridgescope", "pg-mcp"],
                        help='the algorithm to use for evaluation')
    parser.add_argument('--op', type=str, choices=["select", "insert", "delete", "update"],
                        help='the operator to use for evaluation')

    args = parser.parse_args()
    return args


def test_administrator_user(self):
    self.test_open_ex_base("full_priv_user")


def test_normal_user(self):
    self.test_open_ex_base("read_only_user")


def test_irrelevant_user(self):
    self.test_open_ex_base("other_table_only_user")


def test_open_ex_base(tester, ut):
    algo = args.get("algo")
    st = "w_tool_desc" if algo == "bridgescope" else "wo_tool_desc_single"
    operator = args.get("op")
    if operator == "insert":
        tester._test_evaluate_insert(st, ut)
    elif operator == "delete":
        tester._test_evaluate_delete(st, ut)
    elif operator == "update":
        tester._test_evaluate_update(st, ut)
    elif operator == "select":
        tester._test_evaluate_select(st, ut)


def run_privilege_tests():
    tester = TestBirdNlp2Sql()
    tester.setUp()
    tester.llm = args.get("llm")
    tester.n_samples = args.get("n_samples")

    user = args.get("user")
    if user == "administrator":
        test_open_ex_base(tester, "full_priv_user")
    elif user == "normal":
        test_open_ex_base(tester, "read_only_user")
    elif user == "irrelevant":
        test_open_ex_base(tester, "other_table_only_user")


args = vars(parse_arguments())

if __name__ == "__main__":
    run_privilege_tests()
