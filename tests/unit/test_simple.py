"""
@FileName: test_simple.py
@Description: 
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/9 22:25
"""
from neotask import TaskScheduler, SchedulerConfig


async def test_task(data):
    print(f"执行: {data}")
    return {"ok": True}


def test_tt():
    scheduler = TaskScheduler(executor=test_task, config=SchedulerConfig.memory())
    print("1. 创建完成")
    scheduler.start()
    print("2. start() 完成")
    time.sleep(1)
    task_id = scheduler.submit_interval({"name": "test"}, interval_seconds=1, run_immediately=True)
    print(f"3. 提交任务: {task_id}")
    time.sleep(5)
    print("4. 完成")
    scheduler.shutdown()


"""
 快速 Redis 测试 - 使用同步 API
"""

import time
import os
from neotask.api.task_pool import TaskPool, TaskPoolConfig


def simple_executor(data):
    """同步执行器（注意：这里是同步函数）"""
    print(f"Executing: {data}")
    return {"result": data}


def main():
    """主函数"""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")

    print("=" * 60)
    print("Quick Redis Test")
    print("=" * 60)

    # 创建配置
    config = TaskPoolConfig(
        storage_type="redis",
        redis_url=redis_url,
        worker_concurrency=2  # 并发 worker 数
    )

    print(f"Config: storage={config.storage_type}, redis={config.redis_url}")

    # 创建任务池
    pool = TaskPool(executor=simple_executor, config=config)
    pool.start()

    try:
        # 提交单个任务
        print("\n1. Submitting single task...")
        task_id = pool.submit({"test": "hello"})
        print(f"   Task ID: {task_id}")

        # 等待结果
        result = pool.wait_for_result(task_id, timeout=10)
        print(f"   Result: {result}")

        # 批量提交任务
        print("\n2. Submitting batch tasks...")
        task_ids = []
        for i in range(5):
            task_id = pool.submit({"id": i, "message": f"task-{i}"})
            task_ids.append(task_id)
            print(f"   Submitted: {task_id}")

        # 等待所有任务完成
        print("\n3. Waiting for all tasks...")
        for task_id in task_ids:
            try:
                result = pool.wait_for_result(task_id, timeout=10)
                print(f"   Task {task_id} completed: {result}")
            except Exception as e:
                print(f"   Task {task_id} failed: {e}")

        # 获取统计
        print("\n4. Getting statistics...")
        stats = pool.get_stats()
        print(f"   Queue size: {stats.get('queue_size', 0)}")
        print(f"   Total completed: {stats.get('completed', 0)}")
        print(f"   Success rate: {stats.get('success_rate', 0)}")

        print("\n✅ All tasks completed successfully!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n5. Shutting down...")
        pool.shutdown()
        time.sleep(1)


if __name__ == "__main__":
    main()
