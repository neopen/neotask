"""
@FileName: 02_context_manager.py
@Description: 上下文管理器示例 - 自动启动和关闭
@Author: HiPeng
@Time: 2026/4/9
"""

import time
from neotask import TaskPool


async def my_task(data: dict) -> dict:
    """示例任务"""
    print(f"执行任务: {data['name']}")
    time.sleep(0.2)  # 模拟工作
    return {"status": "done", "name": data["name"]}


def main():
    # 使用上下文管理器，自动启动和关闭
    with TaskPool(executor=my_task) as pool:
        # 提交多个任务
        task_ids = []
        for i in range(5):
            task_id = pool.submit({"name": f"task_{i}"})
            task_ids.append(task_id)
            print(f"已提交: {task_id}")

        # 等待结果
        for task_id in task_ids:
            result = pool.wait_for_result(task_id)
            print(f"结果: {result}")

    print("任务池已自动关闭")


if __name__ == "__main__":
    main()