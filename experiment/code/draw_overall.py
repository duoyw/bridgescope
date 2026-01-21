import math
import os.path
import re
import unittest
from collections import defaultdict

import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter

from experiment.code.common import get_plt, to_rgb_tuple, colors2legend_bar
from experiment.code.experiment_config import font_size, figure_base_path, user_label_mapping, algo_name_2_color, \
    algo_label_mapping


class DrawOverall(unittest.TestCase):
    def setUp(self) -> None:
        self.data_file_path = "../nlp2sql_ex/summary/summary.xlsx"
        self.metric = "n_func_call"
        self.llms = ["gpt-4o"]
        # self.users = ["full_priv_user", "read_only_user", "operate_table_only_user", "reference_table_only_user",
        #               "other_table_only_user"]
        self.users = ["full_priv_user", "read_only_user", "other_table_only_user"]
        self.ops = ["select", "insert", "update", "delete"]
        # self.algos = ["w_tool_desc", "wo_tool_desc_single", "w_tool_desc_single"]
        self.algos = ["w_tool_desc", "wo_tool_desc_single", "oracle"]

        self.user_2_oracle_perf = {
            "full_priv_user+r": 3,
            "full_priv_user+w": 3,
            "read_only_user+r": 3,
            "read_only_user+w": 1,
            "other_table_only_user+r": 2,
            "other_table_only_user+w": 2,
        }

    def test_draw_ylabel(self):
        """
        you need to run test_generate_summary for collecting data first.
        """
        llms = ["gpt-4o", "claude-4"]
        user_label_mapping = {'gpt-4o': 'GPT-4o', 'claude-4': 'Claude-4'}
        for llm in llms[:1]:
            user_2_algo_2_value = self._build_draw_data(llm)
            user_2_algo_2_value = {kw: val for kw, val in user_2_algo_2_value.items() if kw in ['full_priv_user+r','full_priv_user+w']}
            self.draw_group_bar_chart(user_2_algo_2_value, user_label_mapping[llm], "#LLM calls", f"y_label_permission",
                                      y_min=0.7, y_max=5.3 ,figsize=(4, 4),x_tick_size_gap=5, x_margin=0.1, y_ticks=[1,2,3,4,5])



    def test_draw_overall_full(self):
        """
        you need to run test_generate_summary for collecting data first.
        """
        llms = ["gpt-4o", "claude-4"]
        user_label_mapping = {'gpt-4o': 'GPT-4o', 'claude-4': 'Claude-4'}
        for llm in llms:
            user_2_algo_2_value = self._build_draw_data(llm)
            user_2_algo_2_value = {kw: val for kw, val in user_2_algo_2_value.items() if kw in ['full_priv_user+r','full_priv_user+w']}
            self.draw_group_bar_chart(user_2_algo_2_value, user_label_mapping[llm], "", f"{llm}_permission_full",
                                      y_min=0.7, y_max=5.3 ,figsize=(4, 4),x_tick_size_gap=5, x_margin=0.1,y_ticks=[1,2,3,4,5])


    def test_draw_overall_non_full(self):
        """
        you need to run test_generate_summary for collecting data first.
        """
        llms = ["gpt-4o", "claude-4"]
        user_label_mapping = {'gpt-4o': 'GPT-4o', 'claude-4': 'Claude-4'}
        for llm in llms:
            user_2_algo_2_value = self._build_draw_data(llm)
            user_2_algo_2_value = {kw: val for kw, val in user_2_algo_2_value.items() if kw not in ['read_only_user+r', 'full_priv_user+r','full_priv_user+w']}
            self.draw_group_bar_chart(user_2_algo_2_value, user_label_mapping[llm], "", f"{llm}_permission_non_full",
                                      y_min=0.7, y_max=5.3 ,figsize=(6, 4),x_tick_size_gap=5, x_margin=0.1, y_ticks=[1,2,3,4,5])



    def test_draw_overall_gpt(self):
        """
        you need to run test_generate_summary for collecting data first.
        """
        llm = "gpt-4o"
        user_2_algo_2_value = self._build_draw_data(llm)
        user_2_algo_2_value = {kw: val for kw, val in user_2_algo_2_value.items() if kw != 'read_only_user+r'}
        self.draw_group_bar_chart(user_2_algo_2_value, 'GPT-4o', "#LLM calls", f"{llm}_permission_overall",
                                  y_min=0.5, y_max=4)

    def test_draw_overall_claude(self):
        """
        you need to run test_generate_summary for collecting data first.
        """
        llm = "claude-4"
        user_2_algo_2_value = self._build_draw_data(llm)
        user_2_algo_2_value = {kw: val for kw, val in user_2_algo_2_value.items() if kw != 'read_only_user+r'}
        self.draw_group_bar_chart(user_2_algo_2_value, 'Claude-4', "#LLM calls", f"{llm}_permission_overall",
                                  y_min=0.5, y_max=6)


    def test_draw_legend(self):
        algos = self.algos
        colors = [algo_name_2_color[algo] for algo in algos]
        algo_labels = [algo_label_mapping[algo] for algo in algos]
        algo_labels[-1] = 'Best-Achievable'
        colors2legend_bar(colors, algo_labels, None, columnspacing=5.0, file_name="algo_bar_legend")

    def draw_group_bar_chart(self, user_2_algo_2_value, x_title, y_title, file_name,
                             y_min, y_max, show_x_title=False, figsize=(10, 5), show_legned=False, x_margin=0.0, x_tick_size_gap=0, y_ticks=None, y_range=None, user_label_mapping_ = None
                             ):
        """
        Draw a grouped bar chart using matplotlib.

        Parameters:
            user_2_algo_2_value (dict): A nested dict like:
                                        {
                                            'User1': {'algo1': val1, 'algo2': val2, ...},
                                            'User2': {...},
                                            ...
                                        }
            x_title (str): Title for the x-axis.
            y_title (str): Title for the y-axis.
            file_name (str): File path to save the figure.
            show_x_title (bool): Whether to show the x-axis title.
        """
        # Extract users and algorithms
        users = list(user_2_algo_2_value.keys())
        algos = list(next(iter(user_2_algo_2_value.values())).keys())

        n_users = len(users)
        n_algos = len(algos)

        # Set width of bar
        bar_width = (0.6 / n_algos) * 1

        index = np.arange(n_users)

        cur_font_size = font_size - 20

        # Plotting
        plt = get_plt()
        plt.figure(figsize=figsize)

        colors = [to_rgb_tuple(algo_name_2_color[algo]) for algo in algos]

        for i, algo in enumerate(algos):
            values = [user_2_algo_2_value[user].get(algo, 0) for user in users]
            bar_positions = index + i * bar_width
            plt.bar(bar_positions, values, width=bar_width * 0.9, label=algo_label_mapping[algo], color=colors[i])

        # Customize the plot
        plt.xlabel(x_title if show_x_title else '', fontsize=cur_font_size)
        if y_title:
            plt.ylabel(y_title, fontsize=cur_font_size)

        mapping = user_label_mapping_ if user_label_mapping_ else user_label_mapping
        x_labels = [mapping[u] for u in users]
        plt.xticks(index + bar_width, x_labels, size=cur_font_size-x_tick_size_gap)
        # plt.yticks(size=cur_font_size)

        if x_margin:
            plt.gca().set_xmargin(x_margin)

        if y_title and y_ticks:
            plt.yticks(y_ticks, y_ticks, size=cur_font_size)
        else:
            plt.yticks(y_ticks, [])

        plt.ylim(y_min, y_max)

        # No legend
        if show_legned:
            plt.legend(
                # bbox_to_anchor=(1, ),
                fontsize=cur_font_size - 20,
                # frameon=False
                # labelspacing=0,
                ncol=n_algos
            )
        # plt.legend()  # 注释掉不显示legend

        # Save the plot
        plt.title(x_title, fontsize = cur_font_size)
        plt.grid(axis='y', linestyle='--')

        plt.tight_layout()
        plt.savefig(file_name)
        plt.savefig(os.path.join(figure_base_path, f"{file_name}.pdf"), format="pdf")
        plt.show()
        plt.close()

    def _build_draw_data(self, llm):
        """
        构建绘图数据，将每个原始用户拆分为 user+r（读操作）和 user+w（写操作平均值）。

        Parameters:
            llm (str): 使用的LLM名称，如 'gpt-4o'

        Returns:
            dict: 格式为 {new_user: {algo: value}} 的字典
        """
        user_2_algo_2_value = {}

        # 读取 select 操作下的数据（对应 r）'
        select_data = None
        if "select" in self.ops:
            select_data = self._read_data(llm, "select", self.metric)  # 假设 metric 是 latency

        # 读取 insert/update/delete 操作下的数据并求平均（对应 w）
        write_values_per_op = []
        for op in ["delete", "insert", "update"]:
            data = self._read_data(llm, op, self.metric)
            if data:
                write_values_per_op.append(data)

        # 合并生成新的 user 类型
        for user in self.users:
            new_r = f"{user}+r"
            new_w = f"{user}+w"

            # 添加 read 数据
            if select_data and user in select_data:
                user_2_algo_2_value[new_r] = select_data[user]

            # 添加 write 平均数据
            write_values_for_user = {}
            for algo in self.algos:
                if algo == "oracle":
                    continue
                try:
                    values = [w_data[user][algo] for w_data in write_values_per_op]
                except:
                    raise RuntimeError
                assert len(values) == len(write_values_per_op)

                if self.metric == "n_sql_str_equal":
                    values = [float(re.search(r'\((.*?)\)$', value).group(1)) for value in values]
                    write_values_for_user[algo] = np.mean(values)
                else:
                    write_values_for_user[algo] = np.mean(values)

            user_2_algo_2_value[new_w] = write_values_for_user

        # 添加 Oracle 性能数据
        for user, perf in self.user_2_oracle_perf.items():
            if user not in user_2_algo_2_value:
                user_2_algo_2_value[user] = {}
            user_2_algo_2_value[user]["oracle"] = perf

        return user_2_algo_2_value

    def _read_data(self, llm, op, metric):
        df = pd.read_excel(self.data_file_path, sheet_name=op)
        user_2_algo_2_value = defaultdict(dict)

        for _, row in df.iterrows():
            user = row["user"]
            algo = row["server"]
            if row["llm"] == llm and algo in self.algos:
                # 确保每个用户+算法只有一行
                if user in user_2_algo_2_value and algo in user_2_algo_2_value[user]:
                    raise ValueError(f"Duplicate entry for user={user}, algo={algo}")

                user_2_algo_2_value[user][algo] = row[metric]
        return user_2_algo_2_value


if __name__ == '__main__':
    unittest.main()
