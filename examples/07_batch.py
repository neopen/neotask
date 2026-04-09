"""
@FileName: 05_batch.py
@Description: 批量任务示例 - 批量提交和监控大量任务
@Author: HiPeng
@Time: 2026/4/2 17:25
"""

import asyncio
import time
from neotask import TaskPool, TaskPoolConfig


async def batch_task(data: dict) -> dict:
    """批量处理任务"""
    await asyncio.sleep(0.05)  # 模拟快速处理
    return {
        "index": data["index"],
        "processed": True,
        "timestamp": time.time()
    }


async def main():
    pool = TaskPool(
        executor=batch_task,
        config=TaskPoolConfig(worker_concurrency=20)
    )

    try:
        batch_size = 100
        print(f"提交 {batch_size} 个任务...")

        start_time = time.time()
        task_ids = []

        # 批量提交
        for i in range(batch_size):
            task_id = await pool.submit_async({"index": i})
            task_ids.append(task_id)

        submit_time = time.time() - start_time
        print(f"提交完成，耗时: {submit_time:.2f}s")

        # 等待所有任务完成
        print("等待所有任务完成...")
        start_time = time.time()

        completed = 0
        failed = 0

        for task_id in task_ids:
            try:
                result = await pool.wait_for_result_async(task_id, timeout=10)
                if result.get("processed"):
                    completed += 1
            except Exception:
                failed += 1

        wait_time = time.time() - start_time

        print(f"\n结果统计:")
        print(f"  成功: {completed}")
        print(f"  失败: {failed}")
        print(f"  总耗时: {wait_time:.2f}s")
        print(f"  吞吐量: {batch_size / wait_time:.2f} 任务/秒")

    finally:
        pool.shutdown()


if __name__ == "__main__":
    asyncio.run(main())