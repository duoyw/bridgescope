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


class DrawSchema(DrawOverall):
    def setUp(self) -> None:
        super().setUp()
        self.data_file_path = "../nlp2sql_ex/summary/summary.xlsx"
        self.metric = "n_func_call"
        self.users = ["full_priv_user"]
        self.ops = ["select", "insert", "update", "delete"]
        self.algos = ["w_tool_desc", "wo_tool_desc_execute", "oracle"]
        self.user_2_oracle_perf = {
            "full_priv_user+r": 3,
            "full_priv_user+w": 3
        }

    def test_draw_schema_together(self):
        summary = defaultdict(dict)
        llms = ['gpt-4o', 'claude-4']
        for idx, llm in enumerate(llms):
            user_2_algo_2_value = self._build_draw_data(llm)
            print(user_2_algo_2_value)
            values = list(user_2_algo_2_value.values())
            summary[llm]['w_tool_desc'] = np.average([val['w_tool_desc'] for val in values])
            summary[llm]["wo_tool_desc_execute"] = np.average([val['wo_tool_desc_execute'] for val in values])
            summary[llm]['oracle'] = np.average([val['oracle'] for val in values])

            print(llm, (summary[llm]['w_tool_desc'] -  summary[llm]["wo_tool_desc_execute"])/ summary[llm]["wo_tool_desc_execute"])


        self.draw_group_bar_chart(summary, "", "#LLM calls", f"schema_overall",
                                  y_min=1, y_max=7, figsize=(6, 3.5 ), show_legned=False, y_ticks=[1, 3, 5, 7],
                                   x_margin=0.1, user_label_mapping_={'gpt-4o': 'GPT-4o', 'claude-4': 'Claude-4'})


    def test_draw_schema(self):
        llm = "gpt-4o"

        user_2_algo_2_value = self._build_draw_data(llm)
        self.draw_group_bar_chart(user_2_algo_2_value, "", "Mean Model Calls", f"{llm}_schema_overall",
                                  y_min=2, y_max=6, figsize=(8, 6), show_legned=True, y_ticks=[2, 3,4,5,6], y_range=[2, 6])

        print(user_2_algo_2_value)


    def test_draw_legend(self):
        algos = ['w_tool_desc', 'wo_tool_desc_execute', 'wo_tool_desc_single', 'oracle']#self.algos
        colors = [algo_name_2_color[algo] for algo in algos]
        algo_labels = [algo_label_mapping[algo] for algo in algos]
        algo_labels[-1] = 'Best-Achievable'
        colors2legend_bar(colors, algo_labels, None, columnspacing=5.0, file_name="algo_bar_legend_context_retrieval")


if __name__ == '__main__':
    unittest.main()
