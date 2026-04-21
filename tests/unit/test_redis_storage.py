"""
@FileName: test_redis_simple.py
@Description: 简化的 Redis 测试，用于调试
@Author: HiPeng
@Time: 2026/4/21
"""

import asyncio
import os

import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig


async def simple_executor(data):
    """简单执行器"""
    print(f"Executing: {data}")
    return {"result": data}


@pytest.mark.asyncio
async def test_redis_single_task():
    """测试单个 Redis 任务"""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    config = TaskPoolConfig(storage_type="redis", redis_url=redis_url)

    pool = TaskPool(executor=simple_executor, config=config)
    pool.start()

    try:
        task_id = await pool.submit_async({"test": "single"})
        print(f"Submitted task: {task_id}")

        result = await pool.wait_for_result_async(task_id, timeout=10)
        print(f"Result: {result}")

        assert result["result"]["test"] == "single"
    finally:
        pool.shutdown()
        await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_redis_multiple_tasks_single_pool():
    """测试单个池多个任务"""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    config = TaskPoolConfig(storage_type="redis", redis_url=redis_url)

    pool = TaskPool(executor=simple_executor, config=config)
    pool.start()

    try:
        task_ids = []
        for i in range(10):
            task_id = await pool.submit_async({"id": i})
            task_ids.append(task_id)
            print(f"Submitted task {i}: {task_id}")

        success_count = 0
        for task_id in task_ids:
            try:
                result = await pool.wait_for_result_async(task_id, timeout=10)
                print(f"Task {task_id} completed: {result}")
                success_count += 1
            except Exception as e:
                print(f"Task {task_id} failed: {e}")

        print(f"Success rate: {success_count}/10")
        assert success_count == 10
    finally:
        pool.shutdown()
        await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_redis_two_pools_sequential():
    """测试两个池顺序使用（非并发）"""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    config = TaskPoolConfig(storage_type="redis", redis_url=redis_url)

    # 第一个池提交任务
    pool1 = TaskPool(executor=simple_executor, config=config)
    pool1.start()

    task_ids = []
    try:
        for i in range(5):
            task_id = await pool1.submit_async({"id": i, "pool": 1})
            task_ids.append(task_id)
            print(f"Pool1 submitted task {i}: {task_id}")
    finally:
        pool1.shutdown()
        await asyncio.sleep(0.5)

    # 第二个池消费任务
    pool2 = TaskPool(executor=simple_executor, config=config)
    pool2.start()

    try:
        success_count = 0
        for task_id in task_ids:
            try:
                result = await pool2.wait_for_result_async(task_id, timeout=10)
                print(f"Pool2 completed task {task_id}: {result}")
                success_count += 1
            except Exception as e:
                print(f"Pool2 failed task {task_id}: {e}")

        print(f"Success rate: {success_count}/5")
        assert success_count == 5
    finally:
        pool2.shutdown()
        await asyncio.sleep(0.5)
