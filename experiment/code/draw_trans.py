import math
import os.path
import re
import unittest
from collections import defaultdict

import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter

from experiment.code.common import get_plt, to_rgb_tuple, colors2legend_bar
from experiment.code.draw_overall import DrawOverall
from experiment.code.draw_proxy import DrawProxy
from experiment.code.experiment_config import font_size, figure_base_path, user_label_mapping, algo_name_2_color, \
    algo_label_mapping


class DrawTrans(DrawOverall):
    def setUp(self) -> None:
        super().setUp()
        self.data_file_path = "../trans_ex/summary.xlsx"
        self.metric = "n_sql_str_equal"
        self.users = ["full_priv_user"] #,"read_only_user", "other_table_only_user"]
        self.ops = ["insert", "update", "delete"]
        self.algos = ["w_tool_desc", "wo_tool_desc_single"]
        self.user_2_oracle_perf = {
            "full_priv_user+w": 1.0,
            "read_only_user+w": 1.0,
            "other_table_only_user+w": 1.0,
        }

    def test_draw_together(self):
        summary = defaultdict(dict)
        llms = ['gpt-4o', 'claude-4']
        for idx, llm in enumerate(llms):
            user_2_algo_2_value = self._build_draw_data(llm)
            print(user_2_algo_2_value)
            summary[llm] = user_2_algo_2_value['full_priv_user+w']

        self.draw_group_bar_chart(summary, "", "Transaction \ntrigger ratio", f"trans_overall",
                                  y_min=2, y_max=6, figsize=(6.5, 3.5), show_legned=False, y_ticks=[0, 0.2, 0.4, 0.6, 0.8, 1], x_margin=0.1,
                                  y_range=[0, 1.1], user_label_mapping_={'gpt-4o':'GPT-4o', 'claude-4':'Claude-4'})


    def test_draw(self):
        llm = "gpt-4o"

        user_2_algo_2_value = self._build_draw_data(llm)
        self.draw_group_bar_chart(user_2_algo_2_value, "", "Transaction Trigger Ratio", f"{llm}_trans_overall",
                                  y_min=0, y_max=1.2, figsize=(8, 6), show_legned=True)

        print(user_2_algo_2_value)

    def test_draw_legend(self):
        algos = list(self._predefine_algo_2_value.keys()) + ["v-proxy", "v-modelTrain"]
        colors = [algo_name_2_color[algo] for algo in algos]
        algo_labels = [algo_label_mapping.get(algo, algo) for algo in algos]
        colors2legend_bar(colors, algo_labels, None, columnspacing=5.0, file_name="algo_bar_proxy_legend")


if __name__ == '__main__':
    unittest.main()
