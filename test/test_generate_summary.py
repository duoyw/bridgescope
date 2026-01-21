import json
import os
import re

import pandas as pd

from collections import defaultdict
from numpy.ma.extras import average

from test_bird_nlp2sql import TestBirdNlp2Sql


class TestGenSummary(TestBirdNlp2Sql):
    def setUp(self) -> None:
        super().setUp()
        self.summary_file = "../experiment/nlp2sql/summary/summary.xlsx"
        self.target_dir = "../experiment/nlp2sql/"
        # self.summary_file = "../experiment/nlp2sql_t1/summary/summary.xlsx"
        # self.target_dir = "../experiment/nlp2sql_t1/"
        # self.summary_file = "../experiment/nlp2sql_ex/summary/summary.xlsx"
        # self.target_dir = "../experiment/nlp2sql_ex/"

        # self.summary_file = "../experiment/nlp2sql_ex_v1/summary/summary.xlsx"
        # self.target_dir = "../experiment/nlp2sql_ex_v1/"

        self.proxy_summary_file = "../experiment/proxy_ex/summary.xlsx"
        self.proxy_target_dir = "../experiment/proxy_ex/"
        self.is_proxy = False

        self.trans_summary_file = "../experiment/trans_ex/summary.xlsx"
        self.trans_target_dir = "../experiment/trans_ex/"
        # self.trans_summary_file = "../experiment/trans/summary.xlsx"
        # self.trans_target_dir = "../experiment/trans/"
        self.is_trans = False

        self.result_dir = "../test/result/"
        # self.models = ["gpt-4o", "claude-4", "qwen-max"]
        self.models = ["gpt-4o", "claude-4"]
        # self.models = ["claude-4"]
        # self.models = ["gpt-4o"]
        # self.models = ["qwen-max"]
        self.ops = ['update', 'select', 'delete', "insert"]
        self.exclude_server = []  # ['w_tool_desc_single', 'wo_tool_desc_single']

    def test_collect_proxy_result(self):
        self.is_proxy = True
        self.summary_file = self.proxy_summary_file
        self.target_dir = self.proxy_target_dir
        self.test_collect_result()

    def test_collect_trans_result(self):
        self.summary_file = self.trans_summary_file
        self.target_dir = self.trans_target_dir
        self.test_collect_result()

    def test_collect_result(self):
        overall_result = defaultdict(lambda: defaultdict(list))

        result_files = self.get_result_files()

        def gen_summary(base_dir, file):
            # parse configuration
            if self.is_proxy:
                v, llm, conf_log = file.strip()[:-4].split('_', 2)
                op = "proxy"
                ut = "full_priv_user"
                st = v
                conf, log_id = conf_log[:-16], conf_log[-15:]
            else:
                v, llm, op, conf_log = file.strip()[:-4].split('_', 3)
                conf, log_id = conf_log[:-16], conf_log[-15:]
                ut, st = self.find_user_server_type(conf)

            if st in self.exclude_server:
                return

            if llm not in self.models:
                return

            # compute summary
            overall_result[op]['llm'].append(llm)
            overall_result[op]['user'].append(ut)
            overall_result[op]['server'].append(st)
            overall_result[op]['log'].append(log_id)
            result = pd.read_csv(base_dir + file)

            def get_n_finish(agent_error, exceed_tries):
                return sum([not agent_error[i] and not exceed_tries[i] for i in range(len(agent_error))])

            def get_n_error_finish(has_priv, abort, finish):
                return sum(
                    [(has_priv[i] and abort[i]) or (not has_priv[i] and finish[i]) for i in range(len(has_priv))])

            def get_average_tries(type, requires_priv):
                has_priv = result['category']
                is_type = result[type]
                n_tries = result['n_tries']
                tries = [n_tries[i] for i in range(len(is_type)) if (is_type[i] and has_priv[i] == requires_priv)]
                return sum(tries) / len(tries) if tries else -1

            def get_ratio_over_finish(type):
                finish = result['finish']
                return round(sum(result[type]) / sum(finish), 2) if sum(finish) else -1

            def get_ratio_over_total(type):
                total = len(result['task_id'])
                return round(sum(result[type]) / total, 2) if total else -1

            q0 = result['total_tokens'].quantile(0.00)
            q99 = result['total_tokens'].quantile(0.99)
            total_tokens = sum(result['total_tokens'])
            # total_tokens = result[(result['total_tokens'] >= q0) & (result['total_tokens'] <= q99)][
            #     'total_tokens'].sum()
            metrics = {
                'total_tokens': total_tokens,
                "average_tokens": round(total_tokens / len(result['task_id']), 2),
                'n_fin/n_tasks': f"{get_n_finish(result['agent_error'], result['exceed_maximum_tries'])}/{len(result['task_id'])}",
                'n_has_priv': sum(result['category']),
                'n_err': get_n_error_finish(result["category"], result["abort"], result["finish"]),
                'n_abort': sum(result['abort']),
                'n_fin': sum(result['finish']),
                'n_corr_abort_tries': get_average_tries('abort', False),
                'n_corr_fin_tries': get_average_tries('finish', True),
                'n_err_abort_tries': get_average_tries('abort', True),
                'n_err_fin_tries': get_average_tries('finish', False),
                # 'n_func_call': average(result['n_func_call']),
                'n_func_call': result[result['n_func_call'] != 0]['n_func_call'].mean(),
                # 'n_sql_str_equal': f"{sum(result['sql_str_equal'])}({get_ratio_over_finish('sql_str_equal')})",
                'n_sql_str_equal': f"{sum(result['sql_str_equal'])}({get_ratio_over_total('sql_str_equal')})",
                # 'n_sql_equiv': f"{sum(result['sql_equivalent'])}({get_ratio_over_finish('sql_equivalent')})",
                'n_sql_equiv': f"{sum(result['sql_equivalent'])}({get_ratio_over_total('sql_equivalent')})",

                # errors
                'n_agent_error': sum(result['agent_error']),
                'n_exceed_tries': sum(result['exceed_maximum_tries']),
                'n_evaluation_error': average(result['evaluation_error']),
            }

            for metric, val in metrics.items():
                overall_result[op][metric].append(val)

        for base_dir, file in result_files:
            gen_summary(base_dir, file)

        with pd.ExcelWriter(self.summary_file, mode='w', engine='openpyxl') as writer:
            for op, res in overall_result.items():
                res_pd = pd.DataFrame(res)
                res_pd.sort_values(by=['user', 'server'], inplace=True)

                # 打印输出保持不变
                print(f"======== {op} ========")
                print(res_pd)

                res_pd.to_excel(writer, sheet_name=op, index=False)

    def test_token_ex_gpt4o(self):
        self.models = ["gpt-4o"]
        assert len(self.models) == 1
        self._print_brief_summary()

    def test_token_ex_claude(self):
        self.models = ["claude-4"]
        assert len(self.models) == 1
        self._print_brief_summary()

    def _print_brief_summary(self):
        """
        打印简要指标，包含每个操作的模型、用户类型、服务器类型、总token数、完成任务数/总任务数、SQL等效数量及比例
        """
        brief_result = defaultdict(list)
        exclude_server = []

        result_files = self.get_result_files()

        def gen_brief(base_dir, file):
            # 复用原 gen_summary 中的解析逻辑
            v, llm, op, conf_log = file.strip()[:-4].split('_', 3)
            conf, log_id = conf_log[:-16], conf_log[-15:]
            ut, st = self.find_user_server_type(conf)

            if st in exclude_server or llm not in self.models:
                return

            result = pd.read_csv(base_dir + file)

            # 计算简要指标
            n_finish = sum(~result['agent_error'] & ~result['exceed_maximum_tries'])
            n_tasks = len(result['task_id'])

            n_sql_equiv = sum(result['sql_equivalent'])
            ratio_sql_equiv = round(n_sql_equiv / n_finish, 2) if n_finish else -1

            total_tokens = result[(result['total_tokens'] >= result['total_tokens'].quantile(0.0)) &
                                  (result['total_tokens'] <= result['total_tokens'].quantile(0.99))][
                'total_tokens'].sum()

            # 存入结果
            brief_result[op].append({
                'llm': llm,
                'user': ut,
                'server': st,
                'total_tokens': total_tokens,
                'n_fin/n_tasks': f"{n_finish}/{n_tasks}",
                'n_sql_equiv': f"{n_sql_equiv}({ratio_sql_equiv})"
            })

        # 复用文件遍历逻辑
        for base_dir, file in result_files:
            gen_brief(base_dir, file)

        # 打印结果
        for op, res in brief_result.items():
            print(f"======== {op} ========")
            df = pd.DataFrame(res)
            df.sort_values(by=['user', 'server'], inplace=True)
            print(df.to_string(index=False))

        # === 合并逻辑：按 server 分组统计 Token Per Task 和 SQL Equiv Ratio ===

        # 初始化聚合字典：server -> 组合key -> tokens & sql_equiv & tasks
        combined_stats = defaultdict(lambda: defaultdict(
            lambda: {"total_tokens": 0, "total_sql_equiv": 0, "total_tasks": 0}
        ))

        # 遍历所有 brief_result 数据
        for op, entries in brief_result.items():
            for entry in entries:
                user = entry["user"]
                server = entry["server"]

                # 提取指标
                total_tokens = entry["total_tokens"]
                sql_equiv_str = entry["n_sql_equiv"]
                n_sql_equiv = int(sql_equiv_str.split("(")[0])  # 取括号前的整数部分
                n_tasks = int(entry["n_fin/n_tasks"].split("/")[1])

                # 定义组合键
                if user == "full_priv_user":
                    prefix = "F"
                elif user == "read_only_user":
                    prefix = "R"
                elif user == "other_table_only_user":
                    prefix = "O"
                else:
                    continue

                if op == "select":
                    suffix = "R"
                elif op in ["update", "delete", "insert"]:
                    suffix = "W"
                else:
                    continue

                key = f"{prefix}+{suffix}"

                # 累加数据
                combined_stats[server][key]["total_tokens"] += total_tokens
                combined_stats[server][key]["total_sql_equiv"] += n_sql_equiv
                combined_stats[server][key]["total_tasks"] += n_tasks

        # 构建 DataFrame 数据
        token_data = []
        sql_data = []

        for server, stats in combined_stats.items():
            token_row = {"server": server}
            sql_row = {"server": server}

            for key in ["F+R", "F+W", "R+R", "R+W", "O+R", "O+W"]:
                item = stats[key]
                if item["total_tasks"] > 0:
                    token_per_task = item["total_tokens"] / item["total_tasks"]
                    sql_equiv_ratio = item["total_sql_equiv"] / item["total_tasks"]
                    token_row[key] = round(token_per_task, 2)
                    sql_row[key] = round(sql_equiv_ratio, 2)
                else:
                    token_row[key] = None
                    sql_row[key] = None

            token_data.append(token_row)
            sql_data.append(sql_row)

        # 转换为 DataFrame 并排序
        cols = ["server", "F+R", "F+W", "R+R", "R+W", "O+R", "O+W"]

        token_df = pd.DataFrame(token_data, columns=cols)
        sql_df = pd.DataFrame(sql_data, columns=cols)

        token_df.sort_values(by="server", inplace=True)
        sql_df.sort_values(by="server", inplace=True)

        # 打印结果
        print("\n=== 按 server 分组的组合指标（Token Per Task）===")
        print(token_df.to_string(index=False))

        print("\n=== 按 server 分组的组合指标（SQL Equiv Ratio）===")
        print(sql_df.to_string(index=False))

    def get_result_files(self):
        result_files = []
        if self.is_proxy:
            op_base_dir = self.target_dir
            for root, dirs, files in os.walk(op_base_dir):
                for file in files:
                    if file.startswith("summary"):
                        continue
                    result_files.append((root, file))
        else:
            for op in self.ops:
                op_base_dir = self.target_dir + f'{op}/'
                for root, dirs, files in os.walk(op_base_dir):
                    for file in files:
                        result_files.append((root, file))
        return result_files

    def find_user_server_type(self, conf):
        for u in self.user_types:
            for s in self.server_types:

                if f'{u}_{s}' == conf:
                    return u, s
        return None

    def test_print_log(self):
        log_id = '0623_165951'

        with open(f'log/record_2025{log_id}.json', 'r') as f:
            res1 = json.load(f)

        res1 = {id: exec_info for id, exec_info in res1.items() if 'summary' not in id}
        print(json.dumps(res1, indent=4))
