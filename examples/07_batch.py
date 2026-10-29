"""
@FileName: 07_batch.py
@Description: 批量任务示例 - 批量提交、并发等待与吞吐量统计
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 17:25
"""

import asyncio
import time

from neotask import TaskPool, TaskPoolConfig


async def batch_task(data: dict) -> dict:
    """批量处理任务"""
    await asyncio.sleep(0.03)  # 模拟快速处理
    return {
        "index": data["index"],
        "processed": True,
        "timestamp": time.time()
    }


async def main():
    async with TaskPool(
        executor=batch_task,
        config=TaskPoolConfig(worker_concurrency=10)
    ) as pool:
        batch_size = 100
        print(f"提交 {batch_size} 个任务（并发 Worker=10）...")

        # 1. 批量提交（内部逐个入队，比手写循环更清晰）
        start_time = time.perf_counter()
        task_ids = await pool.submit_batch_async(
            [{"index": i} for i in range(batch_size)]
        )
        submit_time = time.perf_counter() - start_time
        print(f"提交完成: {len(task_ids)} 个任务, 耗时 {submit_time:.3f}s")

        # 2. 等待全部完成；失败任务以 {"error": ...} 返回
        print("等待所有任务完成...")
        start_time = time.perf_counter()
        results = await pool.wait_all_async(task_ids, timeout=60)
        wait_time = time.perf_counter() - start_time

        completed = sum(
            1 for r in results.values() if not (isinstance(r, dict) and "error" in r)
        )
        failed = batch_size - completed

        print("\n结果统计:")
        print(f"  成功: {completed}")
        print(f"  失败: {failed}")
        print(f"  总耗时: {wait_time:.2f}s")
        print(f"  吞吐量: {batch_size / wait_time:.2f} 任务/秒")

        # 3. 队列 / 任务统计
        stats = await pool.get_stats_async()
        print(
            f"\n池统计: 总={stats['total']}, 完成={stats['completed']}, "
            f"失败={stats['failed']}, 队列={stats['queue_size']}"
        )

        # 4. 抽样查看结果
        sample = results[task_ids[0]]
        print(f"示例结果: {sample}")


if __name__ == "__main__":
    asyncio.run(main())
