# test/evaluate_privilege.py
import argparse

from test_model_train import TestModelTrain
from test_model_train_proxy import TestProxy


def parse_arguments():
    parser = argparse.ArgumentParser(description='Test Effectiveness of Proxy')
    parser.add_argument('--llm', type=str, required=True, choices=["gpt-4o", "claude-4"],
                        help='LLM model to use')
    parser.add_argument('--n_samples', type=int, default=5, help='Number of samples to test (default: 5)')
    parser.add_argument('--algo', type=str, choices=["bridgescope", "pg-mcp-s"],
                        help='the algorithm to use for evaluation')

    args = parser.parse_args()
    return args


def main():
    algo = args.get("algo")
    if algo == "bridgescope":
        tester = TestProxy()
    elif algo == "pg-mcp-s":
        tester = TestModelTrain()
    else:
        raise ValueError("Unsupported algorithm specified. Use 'bridgescope' or 'pg-mcp-s'.")
    tester.setUp()
    tester.llm = args.get("llm")
    tester.n_samples = args.get("n_samples")
    tester.test_evaluate()


args = vars(parse_arguments())

if __name__ == "__main__":
    main()
