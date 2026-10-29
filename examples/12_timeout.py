"""
@FileName: 12_timeout.py
@Description: 任务超时示例 - task_timeout 触发超时失败及统计
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

import asyncio
import time
from datetime import datetime

from neotask import TaskPool, TaskPoolConfig


async def run_tasks(data: dict) -> dict:
    """按 kind 分流：fast 正常完成，slow 触发超时"""
    if data["kind"] == "fast":
        await asyncio.sleep(0.1)
    else:
        await asyncio.sleep(data.get("duration", 2))
    return {"status": "done", "task": data["name"]}


def main():
    print("=== 任务超时示例 ===\n")

    # task_timeout 是单任务执行时间上限（秒）；
    # 超时错误同样会走自动重试路径，这里 max_retries=0 便于观察一次失败
    config = TaskPoolConfig(
        worker_concurrency=2,
        task_timeout=0.5,
        max_retries=0
    )

    with TaskPool(executor=run_tasks, config=config) as pool:
        fast_id = pool.submit({"kind": "fast", "name": "fast"})
        slow_id = pool.submit({"kind": "slow", "name": "slow", "duration": 2})

        fast_result = pool.wait_for_result(fast_id, timeout=10)
        print(f"快速任务: {fast_result}")

        start = time.perf_counter()
        try:
            pool.wait_for_result(slow_id, timeout=10)
        except Exception as e:
            elapsed = time.perf_counter() - start
            print(f"慢速任务失败（从开始等待起 {elapsed:.2f}s）: {e}")

        result_info = pool.get_result(slow_id)
        if result_info and result_info["created_at"] and result_info["completed_at"]:
            created = datetime.fromisoformat(result_info["created_at"])
            completed = datetime.fromisoformat(result_info["completed_at"])
            lifetime = (completed - created).total_seconds()
            print(f"慢速任务从提交到被标记超时: {lifetime:.2f}s（task_timeout={config.task_timeout}s）")
        print(f"慢速任务状态: {result_info['status']}")
        print(f"慢速任务错误: {result_info['error']}")

        stats = pool.get_stats()
        print(f"\n统计: 完成={stats['completed']}, 失败={stats['failed']}")


if __name__ == "__main__":
    main()
