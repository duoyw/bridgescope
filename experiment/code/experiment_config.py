import os.path

base_data_path = "../"
data_base_path = os.path.join(base_data_path, "data/all/")
figure_base_path = os.path.join(base_data_path, "figure/")

experiment_projects = ['citylife_anl_dev', "mkt_gpc_dev", "gd_aliyun_com", "nm_fin_dev", 'nm_ads_dev']

project_mapping = {
    experiment_projects[0]: "Project1",
    experiment_projects[2]: "Project2",
    experiment_projects[3]: "Project3",
    experiment_projects[4]: "Project4",
    experiment_projects[1]: "Project5",
}

my_algo = "BridgeScope"

user_label_mapping = {
    "full_priv_user+r": "(A, read)",
    "full_priv_user+w": "(A, write)",
    "read_only_user+r": "(N, read)",
    "read_only_user+w": "(N, write)",
    "other_table_only_user+r": "(I, read)",
    "other_table_only_user+w": "(I, write)",
}

algo_label_mapping = {
    "w_tool_desc": f"{my_algo}",
    "wo_tool_desc_single": "PG-MCP",
    "wo_tool_desc_execute": "PG-MCP$^-$",
    "oracle": "Oracle-LLM",
    "v-proxy": "PD-Proxy",
    "v-modelTrain": "PD-Tiny",
}

# 保证第一个是我们的算法，最后一个是最优算法,倒数第二个是数据库的算法
experiment_algos = ["TCNN-GRL", "GCN", "XGBoost", "Transformer", "MaxCompute", "Fastest Found"]
expected_error_algos = ["TCNN-GRL", "Perfect Model", "MaxComputer", "Oracle Model"]
system_load_algos = ["Fixed-Load (Used)", "No-Load", "Cluster-Load", "Before-Exec-Load", "Sample_5_Load", "MaxCompute",
                     "Achievable-Best"]

overall_file_path = os.path.join(data_base_path, "overall.xlsx")

font_size = 55

experiment_metric_label = "Average E2E Execution Cost"

algo_name_2_color = {
    # 橙色
    "w_tool_desc": "rgb(255,190,122)",
    "v-proxy": "rgb(255,190,122)",

    # 蓝色
    # "wo_tool_desc_single": "rgb(47,127,193)",
    "wo_tool_desc_single": "rgb(88,97,172)",
    # "wo_tool_desc_execute": "rgb(88,97,172)",
    "PD": "rgb(88,97,172)",
    # "PG-Draw-MCP": "rgb(202,198,88)",
    # "PG-Draw-MCP": "rgb(202,198,88)",

    # 灰色
    "v-modelTrain":"rgb(160,159,164)",
    "wo_tool_desc_execute": "rgb(167,171,208)",

    # 深红色
    "oracle": "rgb(147,75,67)",
    "PD-Oracle": "rgb(147,75,67)",

    # 红色
    "PD-Oracle-Proxy": "rgb(216,56,58)",

}

name_2_symbol = {
    "TCNN-GRL": "",
    "TCNN": "\\",
    "MaxComputer": "",
    "Fastest Found": "",
}
