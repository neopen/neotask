"""
@FileName: test_simple.py
@Description: 最简用法冒烟测试 - TaskPool 即时任务与 TaskScheduler 周期任务
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/9 22:25
"""

import time

import pytest

from neotask import TaskScheduler, SchedulerConfig
from neotask.api.task_pool import TaskPool, TaskPoolConfig


async def _executor(data):
    """测试执行器（私有命名，避免被 pytest 当作用例收集）"""
    return {"ok": True, "data": data}


@pytest.mark.asyncio
async def test_task_pool_submit_and_wait():
    """TaskPool 提交任务并等待结果"""
    pool = TaskPool(executor=_executor, config=TaskPoolConfig.memory())
    pool.start()

    try:
        task_id = await pool.submit_async({"name": "simple"})
        assert task_id is not None

        result = await pool.wait_for_result_async(task_id, timeout=10)
        assert result["ok"] is True
        assert result["data"] == {"name": "simple"}
    finally:
        pool.shutdown()


def test_task_scheduler_interval():
    """TaskScheduler 周期任务应至少执行两次"""
    scheduler = TaskScheduler(executor=_executor, config=SchedulerConfig.memory())
    scheduler.start()
    task_id = None

    try:
        task_id = scheduler.submit_interval(
            {"name": "test"},
            interval_seconds=0.2,
            run_immediately=True
        )
        assert task_id is not None

        # 事件驱动等待：轮询 run_count，避免固定 sleep 带来的时序抖动
        deadline = time.time() + 5
        run_count = 0
        while time.time() < deadline:
            task = scheduler.get_periodic_task(task_id)
            run_count = task["run_count"] if task else 0
            if run_count >= 2:
                break
            time.sleep(0.05)

        assert run_count >= 2, f"periodic task only ran {run_count} times"
    finally:
        if task_id:
            scheduler.cancel_periodic(task_id)
        scheduler.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
