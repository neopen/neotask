"""
@FileName: test_failure_recovery.py
@Description: 失败恢复集成测试 - 重试与最终失败语义
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/16 15:26
"""

import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.models.task import TaskStatus


@pytest.mark.asyncio
async def test_retry_eventually_succeeds():
    """前两次失败、第三次成功的任务应在重试后变为 SUCCESS"""
    attempts = {"count": 0}

    async def flaky_executor(data):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ValueError("transient failure")
        return {"result": "success", "attempts": attempts["count"]}

    config = TaskPoolConfig.memory()
    config.max_retries = 3
    config.retry_delay = 0.1

    pool = TaskPool(executor=flaky_executor, config=config)
    pool.start()

    try:
        task_id = await pool.submit_async({"data": "flaky"})
        result = await pool.wait_for_result_async(task_id, timeout=10)

        assert result["result"] == "success"
        assert result["attempts"] == 3

        task = await pool.get_task_async(task_id)
        assert task["status"] == TaskStatus.SUCCESS.value
    finally:
        pool.shutdown()


@pytest.mark.asyncio
async def test_failure_exhausts_retries():
    """始终失败的任务在耗尽重试次数后应变为 FAILED 并保留错误信息"""
    async def always_failing(data):
        raise ValueError("Task execution failed")

    config = TaskPoolConfig.memory()
    config.max_retries = 2
    config.retry_delay = 0.1

    pool = TaskPool(executor=always_failing, config=config)
    pool.start()

    try:
        task_id = await pool.submit_async({"data": "fail"})

        with pytest.raises(Exception):
            await pool.wait_for_result_async(task_id, timeout=10)

        task = await pool.get_task_async(task_id)
        assert task["status"] == TaskStatus.FAILED.value
        assert task["retry_count"] == config.max_retries
        assert "Task execution failed" in task["error"]
    finally:
        pool.shutdown()


@pytest.mark.asyncio
async def test_cancel_before_execution_prevents_retry():
    """任务在延迟阶段被取消后不应再进入执行/重试流程"""
    async def should_not_run(data):
        raise AssertionError("cancelled task should not be executed")

    config = TaskPoolConfig.memory()
    pool = TaskPool(executor=should_not_run, config=config)
    pool.start()

    try:
        task_id = await pool.submit_async({"data": "cancelled"}, delay=0.5)
        assert await pool.cancel_async(task_id) is True

        task = await pool.get_task_async(task_id)
        assert task["status"] == TaskStatus.CANCELLED.value
        assert task["retry_count"] == 0
    finally:
        pool.shutdown()
