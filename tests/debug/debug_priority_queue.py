"""
调试优先级队列 - 验证优先级排序
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import time
import asyncio
from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.models.task import TaskPriority


def test_priority():
    """测试优先级"""
    executed = []

    async def executor(data):
        executed.append(data["name"])
        return {"result": "done"}

    print("=" * 50)
    print("创建 TaskPool...")

    config = TaskPoolConfig(storage_type="memory", worker_concurrency=1)
    pool = TaskPool(executor, config)

    print("启动 TaskPool...")
    pool.start()
    time.sleep(0.5)

    print("提交任务...")
    print("  提交 LOW (priority=3)")
    pool.submit({"name": "LOW"}, priority=TaskPriority.LOW)

    print("  提交 NORMAL (priority=2)")
    pool.submit({"name": "NORMAL"}, priority=TaskPriority.NORMAL)

    print("  提交 HIGH (priority=1)")
    pool.submit({"name": "HIGH"}, priority=TaskPriority.HIGH)

    print("  提交 CRITICAL (priority=0)")
    pool.submit({"name": "CRITICAL"}, priority=TaskPriority.CRITICAL)

    print("等待任务执行...")
    time.sleep(2.0)

    print("关闭 TaskPool...")
    pool.shutdown()

    print(f"\n执行顺序: {executed}")
    print("=" * 50)

    if executed == ['CRITICAL', 'HIGH', 'NORMAL', 'LOW']:
        print("✅ 优先级队列工作正常!")
    else:
        print("❌ 优先级队列未生效 (FIFO 顺序)")

    return executed


def test_pool_priority():
    """测试优先级 - 使用 pause/resume 确保任务先入队"""
    executed = []

    async def executor(data):
        executed.append(data["name"])
        await asyncio.sleep(0.1)
        return {"result": "done"}

    config = TaskPoolConfig(storage_type="memory", worker_concurrency=1)
    pool = TaskPool(executor, config)
    pool.start()
    time.sleep(0.5)

    # 暂停 Worker，让所有任务先入队
    pool.pause()
    print("Paused worker, submitting tasks...")

    # 提交所有任务（按优先级从低到高）
    pool.submit({"name": "LOW"}, priority=TaskPriority.LOW)
    time.sleep(0.05)
    pool.submit({"name": "NORMAL"}, priority=TaskPriority.NORMAL)
    time.sleep(0.05)
    pool.submit({"name": "HIGH"}, priority=TaskPriority.HIGH)
    time.sleep(0.05)
    pool.submit({"name": "CRITICAL"}, priority=TaskPriority.CRITICAL)

    print("All tasks submitted, resuming worker...")
    # 恢复 Worker，队列中的任务将按优先级顺序执行
    pool.resume()

    # 等待所有任务完成
    time.sleep(1.5)

    pool.shutdown()

    print(f"Executed order: {executed}")
    return executed


if __name__ == "__main__":
    result = test_pool_priority()
    if result == ['CRITICAL', 'HIGH', 'NORMAL', 'LOW']:
        print("SUCCESS: Priority queue is working correctly")
    else:
        print("FAILED: Priority queue is not working")