"""
@FileName: 01_simple.py
@Description: 简单使用示例 - 基础任务提交和等待
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 17:22
"""

import asyncio

from neotask import TaskPool, TaskPoolConfig


# 定义异步任务处理函数
async def process_data(data: dict) -> dict:
    """简单的数据处理函数"""
    print(f"处理数据: {data}")
    await asyncio.sleep(0.3)  # 模拟耗时操作
    return {"result": data["value"] * 2, "status": "success"}


async def main():
    async with TaskPool(
        executor=process_data,
        config=TaskPoolConfig(worker_concurrency=3, storage_type="memory")
    ) as pool:
        # 批量提交 5 个任务（异步 API）
        task_ids = []
        for i in range(5):
            task_id = await pool.submit_async({"value": i})
            task_ids.append(task_id)
            print(f"已提交: {task_id}")

        # 一次性等待全部完成；失败的任务以 {"error": ...} 形式返回，不会抛出
        results = await pool.wait_all_async(task_ids, timeout=30)
        for task_id, result in results.items():
            print(f"任务 {task_id} 结果: {result}")

        # 统计信息
        stats = await pool.get_stats_async()
        print(
            f"\n统计: 总数={stats['total']}, 完成={stats['completed']}, "
            f"失败={stats['failed']}, 队列={stats['queue_size']}"
        )
    # 退出上下文时自动调用 shutdown()


if __name__ == "__main__":
    asyncio.run(main())
