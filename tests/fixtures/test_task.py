"""
@FileName: test_task.py
@Description: 任务相关 fixtures
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import socket
import os
import time
import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.models.task import Task, TaskPriority


@pytest.fixture
def sample_task():
    """示例任务"""
    return Task(
        task_id="test_task_001",
        data={"key": "value", "test": True},
        priority=TaskPriority.NORMAL
    )


@pytest.fixture
def sample_tasks():
    """多个示例任务"""
    return [
        Task(
            task_id=f"test_task_{i:03d}",
            data={"index": i, "data": f"task_{i}"},
            priority=TaskPriority(i % 4)
        )
        for i in range(10)
    ]


@pytest.fixture
def sample_task_data():
    """示例任务数据"""
    return [
        (f"task_{i}", i % 4, 0)  # (task_id, priority, delay)
        for i in range(20)
    ]


@pytest.fixture
def test_node_id():
    """生成测试节点 ID"""
    return f"test_node_{socket.gethostname()}_{os.getpid()}_{int(time.time() * 1000)}"


@pytest.fixture
def memory_task_pool():
    """内存后端 TaskPool（执行器固定返回 ``{"result": "success", "data": data}``）"""
    async def executor(data):
        return {"result": "success", "data": data}

    pool = TaskPool(executor=executor, config=TaskPoolConfig.memory())
    pool.start()
    yield pool
    pool.shutdown()


@pytest.fixture
def task_pool_with_fail_executor():
    """失败执行器 TaskPool（用于验证失败状态与异常透传）"""
    async def failing_executor(data):
        raise ValueError("Task execution failed")

    config = TaskPoolConfig.memory()
    # 用例只验证最终失败，重试次数压到最低以缩短执行时间
    config.max_retries = 1
    config.retry_delay = 0.1
    pool = TaskPool(executor=failing_executor, config=config)
    pool.start()
    yield pool
    pool.shutdown()