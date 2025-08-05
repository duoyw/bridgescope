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
from experiment.code.experiment_config import font_size, figure_base_path, user_label_mapping, algo_name_2_color, \
    algo_label_mapping


class DrawProxy(DrawOverall):
    def setUp(self) -> None:
        self.data_file_path = "../proxy_ex/summary.xlsx"
        self.metric = "n_sql_str_equal"
        self._predefine_algo_2_value = {
            "PD-Oracle-Proxy": 1.0,
            "PD-Oracle": 1.0,
            "PD": 0.05,
            # "PG-Draw-Proxy-MCP": 2.0,
            # "PG-Draw-MCP On Tiny": 2.0,
        }

    def test_draw_overall_project1(self):
        llm = "gpt-4o"
        algo_2_value: dict = self._read_data(llm, None, self.metric)
        algo_2_value.update(self._predefine_algo_2_value)
        self.draw_bar_chart(algo_2_value, "", "Mean Model Calls", f"{llm}_proxy_overall", y_min=0, y_max=1.1)

    def test_draw_legend(self):
        algos = list(self._predefine_algo_2_value.keys()) + ["v-proxy", "v-modelTrain"]
        colors = [algo_name_2_color[algo] for algo in algos]
        algo_labels = [algo_label_mapping.get(algo, algo) for algo in algos]
        colors2legend_bar(colors, algo_labels, None, columnspacing=5.0, file_name="algo_bar_proxy_legend")

    def draw_bar_chart(self, algo_2_value, x_title, y_title, file_name, y_min=None, y_max=None, show_x_title=False):
        """
        Draw a simple bar chart using matplotlib.

        Parameters:
            algo_2_value (dict): A dict mapping algorithm names to values:
                                 {'algo1': val1, 'algo2': val2, ...}
            x_title (str): Title for the x-axis.
            y_title (str): Title for the y-axis.
            file_name (str): File path to save the figure.
            y_min (float): Minimum value for y-axis.
            y_max (float): Maximum value for y-axis.
            show_x_title (bool): Whether to show the x-axis title.
        """
        # Extract algorithms and values
        algos = list(algo_2_value.keys())
        values = list(algo_2_value.values())

        n_algos = len(algos)
        index = np.arange(n_algos)

        cur_font_size = font_size - 20  # 假设 font_size 是类中定义的变量

        # Plotting
        plt = get_plt()
        plt.figure(figsize=(8, 6))

        colors = [to_rgb_tuple(algo_name_2_color[algo]) for algo in algos]

        bar_width = 1.2 / n_algos
        # Draw bars
        plt.bar(index, values, width=bar_width, color=colors)

        # Customize the plot
        plt.xlabel(x_title if show_x_title else '', fontsize=cur_font_size)
        plt.ylabel(y_title, fontsize=cur_font_size)

        # Set x-ticks to algorithm names
        # plt.xticks(index, [algo_label_mapping.get(algo, algo) for algo in algos], size=cur_font_size)
        plt.xticks([])
        plt.yticks(size=cur_font_size)

        if y_min is not None and y_max is not None:
            plt.ylim(y_min, y_max)

        # Add grid
        plt.grid(axis='y', linestyle='--')

        # Layout and save
        plt.tight_layout()
        plt.savefig(file_name)
        plt.savefig(os.path.join(figure_base_path, f"{file_name}.pdf"), format="pdf")
        plt.show()
        plt.close()

    def _read_data(self, llm, op, metric):
        """
        algo: proxy, modelTrain
        """
        df = pd.read_excel(self.data_file_path)
        algo_2_value = defaultdict(dict)
        for _, row in df.iterrows():
            if row["llm"] == llm:
                algo = row["server"]
                if algo in algo_2_value:
                    raise ValueError(f"Duplicate {algo} in {algo_2_value[algo]}")
                value = row[metric]
                if metric == "n_sql_str_equal":
                    value = float(re.search(r'\((.*?)\)$', value).group(1))
                algo_2_value[algo] = value

        return algo_2_value


if __name__ == '__main__':
    unittest.main()
