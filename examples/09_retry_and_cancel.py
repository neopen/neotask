"""
@FileName: 09_retry_and_cancel.py
@Description: 重试和取消示例 - 演示任务重试机制和取消操作
@Author: HiPeng
@Time: 2026/4/9
"""

import asyncio
from neotask import TaskPool, TaskPoolConfig


# 计数器，用于演示重试
retry_counter = {"count": 0}


async def flaky_task(data: dict) -> dict:
    """可能失败的任务"""
    retry_counter["count"] += 1
    attempt = retry_counter["count"]

    print(f"第 {attempt} 次尝试执行任务 {data['id']}")

    if attempt < data.get("fail_until", 2):
        raise ValueError(f"模拟失败 (尝试 {attempt})")

    return {"status": "success", "attempts": attempt, "id": data["id"]}


async def long_task(data: dict) -> dict:
    """长任务，用于演示取消"""
    print(f"执行长任务 {data['id']}，预计 {data['duration']} 秒")
    await asyncio.sleep(data["duration"])
    return {"status": "completed", "id": data["id"]}


async def test_retry():
    """测试重试机制"""
    global retry_counter
    retry_counter = {"count": 0}

    print("\n=== 重试机制演示 ===")

    pool = TaskPool(
        executor=flaky_task,
        config=TaskPoolConfig(
            max_retries=3,      # 最多重试3次
            retry_delay=0.5,    # 重试间隔0.5秒
            worker_concurrency=1
        )
    )

    try:
        # 提交会失败2次的任务
        task_id = await pool.submit_async({"id": 1, "fail_until": 2})
        print(f"已提交任务: {task_id}")

        result = await pool.wait_for_result_async(task_id, timeout=10)
        print(f"最终结果: {result}")
        print(f"重试次数: {result['attempts'] - 1}")

    finally:
        pool.shutdown()


async def test_cancel():
    """测试取消任务"""
    print("\n=== 取消任务演示 ===")

    pool = TaskPool(executor=long_task)

    try:
        # 提交一个长任务
        task_id = await pool.submit_async({"id": 2, "duration": 10})
        print(f"已提交长任务: {task_id}")

        # 等待1秒后取消
        await asyncio.sleep(1)
        cancelled = await pool.cancel_async(task_id)
        print(f"取消任务: {'成功' if cancelled else '失败'}")

        # 检查状态
        status = await pool.get_status_async(task_id)
        print(f"任务状态: {status}")

    finally:
        pool.shutdown()


async def test_batch_cancel():
    """测试批量取消"""
    print("\n=== 批量取消演示 ===")

    pool = TaskPool(executor=long_task)

    try:
        # 提交多个任务
        task_ids = []
        for i in range(5):
            task_id = await pool.submit_async({"id": i, "duration": 5})
            task_ids.append(task_id)
            print(f"已提交任务 {i}: {task_id}")

        # 取消所有任务
        await asyncio.sleep(0.5)
        print("\n取消所有任务...")
        for task_id in task_ids:
            await pool.cancel_async(task_id)

        # 检查状态
        await asyncio.sleep(0.5)
        for task_id in task_ids:
            status = await pool.get_status_async(task_id)
            print(f"任务 {task_id}: {status}")

    finally:
        pool.shutdown()


async def main():
    await test_retry()
    await test_cancel()
    await test_batch_cancel()


if __name__ == "__main__":
    asyncio.run(main())