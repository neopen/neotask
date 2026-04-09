"""
@FileName: 01_simple.py
@Description: 简单使用示例 - 基础任务提交和等待
@Author: HiPeng
@Time: 2026/4/2 17:22
"""

import asyncio
from neotask import TaskPool, TaskPoolConfig


# 定义异步任务处理函数
async def process_data(data: dict) -> dict:
    """简单的数据处理函数"""
    print(f"处理数据: {data}")
    await asyncio.sleep(0.5)  # 模拟耗时操作
    return {"result": data["value"] * 2, "status": "success"}


async def main():
    with TaskPool(
        executor=process_data,
        config=TaskPoolConfig(worker_concurrency=3, storage_type="memory")
    ) as pool:
        tasks = []
        for i in range(5):
            task_id = await pool.submit_async({"value": i})
            tasks.append(task_id)
            print(f"已提交: {task_id}")

        for task_id in tasks:
            result = await pool.wait_for_result_async(task_id)
            print(f"任务 {task_id} 结果: {result}")
    # 退出上下文时自动调用 shutdown()


if __name__ == "__main__":
    asyncio.run(main())