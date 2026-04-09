"""
@FileName: 00_quick_start.py
@Description: 快速入门 - 最简单的使用示例
@Author: HiPeng
@Time: 2026/4/9
"""

from neotask import TaskPool


def main():
    # 定义任务处理函数（同步函数也可以）
    def my_task(data):
        print(f"处理: {data}")
        return {"result": "done"}

    # 创建任务池并使用上下文管理器
    with TaskPool(executor=my_task) as pool:
        # 提交任务
        task_id = pool.submit({"name": "test"})
        print(f"任务ID: {task_id}")

        # 等待结果
        result = pool.wait_for_result(task_id)
        print(f"结果: {result}")

    print("完成!")


if __name__ == "__main__":
    main()