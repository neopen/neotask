"""
@FileName: test_e2e.py
@Description: 端到端集成测试
@Author: HiPeng
@Time: 2026/4/16
"""

import asyncio

import pytest

from neotask.api.task_pool import TaskPool
from tests.conftest import (
    echo_executor,
    skip_if_no_redis
)


class TestEndToEndWithRedis:
    """Redis 后端端到端测试"""

    @pytest.mark.asyncio
    @skip_if_no_redis
    async def test_redis_distributed_queue(self, redis_config):
        """测试 Redis 分布式队列"""
        print("\n=== Starting Redis distributed queue test ===")

        # 创建两个池共享 Redis
        pool1 = TaskPool(executor=echo_executor, config=redis_config)
        pool2 = TaskPool(executor=echo_executor, config=redis_config)

        pool1.start()
        pool2.start()

        print("Pools started")

        try:
            # 提交多个任务
            task_ids = []
            for i in range(10):
                task_id = await pool1.submit_async({"id": i})
                task_ids.append(task_id)
                print(f"Submitted task {i}: {task_id}")

            # 等待所有任务完成，收集详细结果
            results = {}
            failed_tasks = []

            for task_id in task_ids:
                try:
                    result = await pool1.wait_for_result_async(task_id, timeout=30)
                    results[task_id] = result
                    print(f"Task {task_id} completed successfully")
                except Exception as e:
                    failed_tasks.append((task_id, str(e)))
                    print(f"Task {task_id} failed: {e}")

            # 检查每个任务的状态
            print("\n=== Task Status Check ===")
            for task_id in task_ids:
                try:
                    status = await pool1.get_status_async(task_id)
                    task_data = await pool1.get_task_async(task_id)
                    print(f"Task {task_id}: status={status}, error={task_data.get('error') if task_data else 'N/A'}")
                except Exception as e:
                    print(f"Failed to get status for {task_id}: {e}")

            completed = len(results)
            failed = len(failed_tasks)

            print(f"\n=== Summary: {completed} completed, {failed} failed ===")

            assert completed == 10, f"Only {completed} tasks completed, {failed} failed"
        finally:
            pool1.shutdown()
            pool2.shutdown()
            await asyncio.sleep(0.5)


# 运行测试的入口
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
