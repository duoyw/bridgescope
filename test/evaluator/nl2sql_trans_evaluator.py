import ast
import re
from time import perf_counter
from typing import Optional

import sqlglot
from agentscope.agents import AgentBase
from agentscope.message import ToolUseBlock
from sqlalchemy import Subquery
from sqlglot import exp

from agents.base_react_agent import BaseReActAgent
from agents.utils import sync_exec
from db_adapters.db_config import DBConfig
from db_adapters.pg_adapter import PostgresAdapter


class NL2SQLTransEvaluator:
    def __init__(self):
        """
        Initialize the evaluator with a PostgreSQL database connection.

        Parameters:
            db_url (str): SQLAlchemy-compatible PostgreSQL URL.
                          Example: "postgresql+psycopg2://user:password@localhost:5432/dbname"
        """

    def evaluate(self, model_response, user_type):
        existed_tran_ops = model_response

        satisfy_trans = False
        if user_type == "full_priv_user":
            if "insert" in existed_tran_ops or "delete" in existed_tran_ops or "update" in existed_tran_ops:
                # if "begin" in existed_tran_ops and "commit" in existed_tran_ops:
                if "begin" in existed_tran_ops:
                    satisfy_trans = True
            else:
                satisfy_trans = True
        elif user_type == "read_only_user" or user_type == "other_table_only_user":
            if "insert" in existed_tran_ops or "delete" in existed_tran_ops or "update" in existed_tran_ops:
                # if "begin" in existed_tran_ops and "rollback" in existed_tran_ops:
                if "begin" in existed_tran_ops:
                    satisfy_trans = True
            else:
                satisfy_trans = True
        else:
            raise Exception("Unknown user_type")

        evaluate_result = {
            "sql_str_equal": satisfy_trans,
        }
        return evaluate_result
