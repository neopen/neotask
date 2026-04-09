"""
@FileName: 02_context_manager.py
@Description: 上下文管理器示例 - 自动启动和关闭
@Author: HiPeng
@Time: 2026/4/9
"""

import time
import threading
from neotask import TaskPool


async def my_task(data: dict) -> dict:
    """示例任务"""
    task_name = data['name']
    # 使用锁避免 print 交错
    with print_lock:
        print(f"  ▶ 执行任务: {task_name}")
    time.sleep(0.2)
    return {"status": "done", "name": task_name}


# 用于同步打印的锁
print_lock = threading.Lock()


def main():
    print("=" * 50)
    print("上下文管理器示例")
    print("=" * 50)

    # 使用上下文管理器，自动启动和关闭
    with TaskPool(executor=my_task) as pool:
        print("\n提交任务中...")
        task_ids = []

        # 提交多个任务
        for i in range(5):
            task_id = pool.submit({"name": f"task_{i}"})
            task_ids.append(task_id)
            print(f"  ✓ 已提交: {task_id}")

        print("\n等待任务完成...\n")

        # 等待结果
        for task_id in task_ids:
            result = pool.wait_for_result(task_id)
            print(f"  📦 结果 [{task_id[:8]}...]: {result['status']}")

    print("\n✅ 任务池已自动关闭")


if __name__ == "__main__":
    main()