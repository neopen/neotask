"""
@FileName: test_redis_storage.py
@Description: Redis 存储测试 - 修复事件循环问题
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import asyncio
import os
import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig


async def simple_executor(data):
    """简单执行器"""
    print(f"Executing: {data}")
    return {"result": data}


# ============================================================
# 方案1：使用同步方法（推荐用于简单测试）
# ============================================================

def test_redis_single_task_sync():
    """测试单个 Redis 任务 - 使用同步 API"""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    config = TaskPoolConfig(storage_type="redis", redis_url=redis_url)

    pool = TaskPool(executor=simple_executor, config=config)
    pool.start()

    try:
        task_id = pool.submit({"test": "single"})
        print(f"Submitted task: {task_id}")

        result = pool.wait_for_result(task_id, timeout=10)
        print(f"Result: {result}")

        assert result["result"]["test"] == "single"
    finally:
        pool.shutdown()


def test_redis_multiple_tasks_single_pool_sync():
    """测试单个池多个任务 - 使用同步 API"""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/7")
    config = TaskPoolConfig(storage_type="redis", redis_url=redis_url)

    pool = TaskPool(executor=simple_executor, config=config)
    pool.start()

    try:
        task_ids = []
        for i in range(10):
            task_id = pool.submit({"id": i})
            task_ids.append(task_id)
            print(f"Submitted task {i}: {task_id}")

        success_count = 0
        for task_id in task_ids:
            try:
                result = pool.wait_for_result(task_id, timeout=10)
                print(f"Task {task_id} completed: {result}")
                success_count += 1
            except Exception as e:
                print(f"Task {task_id} failed: {e}")

        print(f"Success rate: {success_count}/10")
        assert success_count == 10
    finally:
        pool.shutdown()


def test_redis_two_pools_sequential_sync():
    """测试两个池顺序使用（非并发）- 使用同步 API"""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    config = TaskPoolConfig(storage_type="redis", redis_url=redis_url)

    # 第一个池提交任务
    pool1 = TaskPool(executor=simple_executor, config=config)
    pool1.start()

    task_ids = []
    try:
        for i in range(5):
            task_id = pool1.submit({"id": i, "pool": 1})
            task_ids.append(task_id)
            print(f"Pool1 submitted task {i}: {task_id}")
    finally:
        pool1.shutdown()

    # 等待一下确保任务已持久化
    import time
    time.sleep(1)

    # 第二个池消费任务
    pool2 = TaskPool(executor=simple_executor, config=config)
    pool2.start()

    try:
        success_count = 0
        for task_id in task_ids:
            try:
                result = pool2.wait_for_result(task_id, timeout=10)
                print(f"Pool2 completed task {task_id}: {result}")
                success_count += 1
            except Exception as e:
                print(f"Pool2 failed task {task_id}: {e}")

        print(f"Success rate: {success_count}/5")
        assert success_count == 5
    finally:
        pool2.shutdown()


# ============================================================
# 方案2：使用异步方法 + 正确的事件循环管理
# ============================================================

@pytest.fixture
def event_loop():
    """创建独立的事件循环"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_redis_single_task_async():
    """测试单个 Redis 任务 - 异步 API"""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/7")
    config = TaskPoolConfig(storage_type="redis", redis_url=redis_url)

    pool = TaskPool(executor=simple_executor, config=config)
    pool.start()

    # 等待 pool 内部事件循环启动
    await asyncio.sleep(0.5)

    try:
        task_id = await pool.submit_async({"test": "single"})
        print(f"Submitted task: {task_id}")

        result = await pool.wait_for_result_async(task_id, timeout=10)
        print(f"Result: {result}")

        assert result["result"]["test"] == "single"
    finally:
        pool.shutdown()
        await asyncio.sleep(0.5)


# ============================================================
# 方案3：使用线程池执行测试（避免事件循环冲突）
# ============================================================

import concurrent.futures

def run_in_thread(func):
    """在线程中运行测试函数"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func)
        return future.result(timeout=60)


def test_redis_in_thread():
    """在线程中运行 Redis 测试"""
    run_in_thread(test_redis_single_task_sync)


# ============================================================
# 方案4：使用内存存储作为基线测试
# ============================================================

def test_memory_single_task_sync():
    """测试单个内存任务 - 验证基础功能"""
    config = TaskPoolConfig(storage_type="memory")

    pool = TaskPool(executor=simple_executor, config=config)
    pool.start()

    try:
        task_id = pool.submit({"test": "memory"})
        print(f"Submitted task: {task_id}")

        result = pool.wait_for_result(task_id, timeout=5)
        print(f"Result: {result}")

        assert result["result"]["test"] == "memory"
    finally:
        pool.shutdown()


def test_memory_multiple_tasks():
    """测试内存多个任务"""
    config = TaskPoolConfig(storage_type="memory", worker_concurrency=5)

    pool = TaskPool(executor=simple_executor, config=config)
    pool.start()

    try:
        task_ids = []
        for i in range(20):
            task_id = pool.submit({"id": i})
            task_ids.append(task_id)

        success_count = 0
        for task_id in task_ids:
            try:
                result = pool.wait_for_result(task_id, timeout=5)
                success_count += 1
            except Exception:
                pass

        print(f"Memory test success: {success_count}/20")
        assert success_count == 20
    finally:
        pool.shutdown()


# ============================================================
# 主入口
# ============================================================

if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("Redis Storage Test")
    print("=" * 60)

    # 检查 Redis 是否可用
    try:
        import redis
        r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))
        r.ping()
        print("✅ Redis connection successful")
        r.close()
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        print("\nPlease start Redis with:")
        print("  docker run -d -p 6379:6379 redis")
        print("\nRunning memory tests only...")

        # 只运行内存测试
        test_memory_single_task_sync()
        test_memory_multiple_tasks()
        sys.exit(0)

    # 运行 Redis 测试
    print("\n--- Running Redis single task test ---")
    test_redis_single_task_sync()

    print("\n--- Running Redis multiple tasks test ---")
    test_redis_multiple_tasks_single_pool_sync()

    print("\n--- Running Redis two pools test ---")
    test_redis_two_pools_sequential_sync()

    print("\n✅ All tests passed!")