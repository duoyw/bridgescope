from datetime import datetime

import json
import os
from collections import defaultdict, deque
import threading


class DataRecorder:
    def __init__(self, base_dir="log"):
        self.base_dir = base_dir
        self.data_cache = defaultdict(list)  # 缓存每个 task_id 的数据列表
        self.current_file = None  # 当前运行所使用的文件名
        self.written_tasks = set()  # 已经写入过的 task_id
        self.lock = threading.Lock()  # 线程安全锁（可选）

        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

        self._init_new_file()

    def _init_new_file(self):
        """生成一个唯一文件名，格式为 record_YYYYMMDD_HHMMSS.json"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_file = os.path.join(self.base_dir, f"record_{timestamp}.json")

        # 初始化空文件，写入 {}
        with open(self.current_file, 'w', encoding='utf-8') as f:
            f.write("{}")  # 初始是一个空对象

    def record(self, task_id, data_dict):
        """记录一个字典数据到指定的 task_id 中"""
        if not isinstance(data_dict, dict):
            raise ValueError("data_dict 必须是一个字典")

        self.data_cache[task_id].append(data_dict)

    def write(self, task_id=None):
        """
        将当前缓存的数据以「追加」方式写入 JSON 文件，确保格式正确。

        Args:
            task_id: 如果指定，则仅写入该 task_id 的数据；
                     否则跳过（不写入）
        """
        if task_id is None:
            print("必须指定 task_id 才能写入")
            return

        if task_id in self.written_tasks:
            print(f"task_id={task_id} 已写入，跳过")
            return

        if task_id not in self.data_cache or not self.data_cache[task_id]:
            print(f"没有可供写入的 {task_id} 数据")
            return

        data_to_write = {
            task_id: self.data_cache[task_id]
        }

        with self.lock:  # 加锁防止并发写入
            try:
                # 追加模式打开文件
                with open(self.current_file, 'rb+') as f:
                    f.seek(-1, os.SEEK_END)  # 移动到倒数第一个字符（即 '}'）

                    # 删除结尾的 '}'，准备追加新内容
                    f.truncate()

                    # 如果不是第一个 task，前面加逗号
                    if self.written_tasks:
                        f.write(b',')
                    else:
                        f.write(b'')  # 第一个 task 不需要逗号

                    # 写入新 task 的 JSON 字符串
                    json_bytes = self.format_dict_top_three_levels(data_to_write).encode('utf-8')

                    # 去掉开头的 '{' 和结尾的 '}'（因为整体是一个对象）
                    if len(json_bytes) > 2:
                        f.write(json_bytes[1:-1])  # 只写 key-value 部分

                    # 重新写入结尾的 '}'
                    f.write(b'}')

                # 标记该 task 已写入
                self.written_tasks.add(task_id)

                # print(f"任务 {task_id} 已追加写入到 {self.current_file}")

            except Exception as e:
                print(f"写入失败：{e}")
                # 出错后可能需要恢复文件内容，这里简化处理

    def format_dict_top_three_levels(self, d: dict) -> str:
        """
        将字典格式化为 JSON 字符串，但只格式化前两级。
        第三级及以下内容转换为字符串形式，避免因不可序列化抛出异常。
        """

        def _format(obj, depth=0):
            if depth >= 4:
                return repr(obj)

            if isinstance(obj, dict):
                result = {}
                for k, v in obj.items():
                    result[k] = _format(v, depth + 1)
                return result

            elif isinstance(obj, (list, tuple)):
                return [_format(item, depth + 1) for item in obj]

            else:
                return obj

        formatted_data = _format(d)
        return json.dumps(formatted_data, indent=2, ensure_ascii=False)


    def __del__(self):
        """确保对象销毁前写入所有剩余任务"""
        for task_id in list(self.data_cache.keys()):
            if task_id not in self.written_tasks:
                self.write(task_id)



