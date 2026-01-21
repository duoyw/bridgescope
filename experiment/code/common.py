import os
import time

from matplotlib import pyplot as plt, font_manager

from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.lines import Line2D
from matplotlib.patches import Patch


from experiment_config import font_size, figure_base_path

plt.rcParams['pdf.fonttype'] = 42


def get_plt():
    path = 'Linux-Libertine.ttf'
    font_manager.fontManager.addfont(path)
    prop = font_manager.FontProperties(fname=path)
    plt.rcParams['font.family'] = prop.get_name()
    plt.rcParams['font.weight'] = 'bold'
    plt.rcParams['mathtext.default'] = 'regular'
    return plt


def to_rgb_tuple(color: str):
    return tuple([int(c) / 255 for c in color[4:-1].split(",")])



def _unify_layout(fig):
    fig.update_xaxes(showline=True, linewidth=1, linecolor='black', mirror=True)
    fig.update_yaxes(showline=True, linewidth=1, linecolor='black', mirror=True, gridcolor='lightgrey')
    fig['layout'].update(margin=dict(l=0, r=0, b=0, t=0))
    fig.update_layout(
        font=dict(
            # family="Arial Bold",
            family="Times New Roman",
            # family=font_path,
        )
    )


def capitalize(s: str):
    return s[0].upper() + s[1:]


def to_bold(values):
    return ["<b>{}</b>".format(x) for x in values]


def colors2legend(colors, names, lines, markers, file_name):
    # 创建一个空白的图形
    fig, ax = plt.subplots(figsize=(33, 5))

    # 添加需要的图例
    for i in range(len(colors)):
        ax.plot([], [], label=names[i], color=to_rgb_tuple(colors[i]), marker=markers[i])

    # ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=len(colors), facecolor='gray', edgecolor='black')
    legend = ax.legend(loc='upper center', bbox_to_anchor=(0.5, 0.9), ncol=len(colors), fontsize=font_size)
    ax.axis('off')
    legend_margin_width = 10
    legend.get_frame().set_linewidth(legend_margin_width)
    # 调节线的粗细和形状

    width = 15
    for i in range(len(colors)):
        legend.get_lines()[i].set_linewidth(width)
        legend.legendHandles[i].set_linestyle(lines[i])
    plt.show()
    pdf = PdfPages('draw_fig/{}.pdf'.format(file_name))
    # plt.savefig('draw_fig/{}.pdf'.format(file_name))
    pdf.savefig(fig)
    pdf.close()


#
#
# def colors2legend_bar(colors, names, hatches, file_name, handletextpad=1.0, columnspacing=1.0, handlelength=1.0):
#     # 创建一个空白的图形
#     fig = plt.figure(figsize=(58, 5))
#
#     # 添加需要的图例
#     for i in range(len(colors)):
#         hatch = "" if hatches is None else hatches[i]
#         plt.bar([0], [0], label=names[i], hatch=hatch, color=to_rgb_tuple(colors[i]))
#         # plt.bar([0], [0], label=names[i], color=to_rgb_tuple(colors[i]))
#
#     # ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=len(colors), facecolor='gray', edgecolor='black')
#     legend = plt.legend(loc='upper center', bbox_to_anchor=(0.5, 0.9), ncol=len(colors), fontsize=font_size + 10,
#                         handletextpad=handletextpad, columnspacing=columnspacing, handlelength=handlelength)
#
#     plt.gca().spines['top'].set_visible(False)
#     plt.gca().spines['right'].set_visible(False)
#     plt.gca().spines['bottom'].set_visible(False)
#     plt.gca().spines['left'].set_visible(False)
#     plt.axis('off')
#     legend_margin_width = 5
#     legend.get_frame().set_linewidth(legend_margin_width)
#     # width = 15
#     # for i in range(len(colors)):
#     #     legend.get_lines()[i].set_linewidth(width)
#     #     legend.legendHandles[i].set_linestyle(lines[i])
#     plt.show()
#     file_path = os.path.join(figure_base_path, f"{file_name}.pdf")
#     # pdf = PdfPages('draw_fig/{}.pdf'.format(file_name))
#     # pdf.savefig(fig)
#     # pdf.close()
#
#     plt.savefig(file_path)


def colors2legend_bar(colors, names, hatches, file_name, handletextpad=1.0, columnspacing=1, handlelength=2.0,
                      special_name="name"):
    """
    创建一个图例，其中名称为 special_name 的条目使用黑色虚线表示，其余条目使用彩色填充的条形图表示。

    参数：
    - colors: 列表，颜色列表。
    - names: 列表，名称列表。
    - hatches: 列表，图案列表，对应每个条形图的填充模式。
    - file_name: 字符串，保存图例的文件名（不带扩展名）。
    - handletextpad: float，图例标签与图例标识之间的间距。
    - columnspacing: float，图例列之间的间距。
    - handlelength: float，图例标识的长度。
    - special_name: 字符串，指定使用线条表示的名称。
    """
    # 创建一个空白的图形
    plt = get_plt()
    fig = plt.figure(figsize=(12, 2))# 调整图形大小以适应图例

    ax = fig.add_subplot(111)

    # 创建图例句柄和标签
    legend_handles = []
    legend_labels = []

    for i in range(len(colors)):
        current_name = names[i]
        color = colors[i]
        hatch = "" if hatches is None else hatches[i]

        if current_name == special_name:
            # 对于特殊名称，使用黑色虚线
            line = Line2D([0], [0], color='black', linestyle='--', linewidth=2)
            legend_handles.append(line)
        else:
            # 对于其他名称，使用彩色填充的条形图
            patch = Patch(facecolor=to_rgb_tuple(color), edgecolor='None', hatch=hatch)
            legend_handles.append(patch)

        legend_labels.append(current_name)

    # 创建图例
    legend = ax.legend(
        legend_handles,
        legend_labels,
        loc='center',
        ncol=len(colors),
        fontsize=10,  # 您可以根据需要调整字体大小
        handletextpad=handletextpad,
        # columnspacing=columnspacing,
        handlelength=handlelength,
        labelspacing=0.1
    )

    # 移除图形的所有边框和坐标轴
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.axis('off')

    # 设置图例边框宽度（如果需要）
    legend.get_frame().set_linewidth(1)  # 您可以根据需要调整边框宽度

    # 保存并显示图例
    file_path = os.path.join(figure_base_path, f"{file_name}.pdf")
    plt.tight_layout()
    plt.savefig(file_path, bbox_inches='tight', pad_inches=0)
    plt.show()


