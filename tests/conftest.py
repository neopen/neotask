"""
@FileName: conftest.py
@Description: pytest 配置和共享 fixtures
@Author: HiPeng
@Time: 2026/4/21
"""

import asyncio
import os
import tempfile
from typing import Dict, Any

import pytest

from neotask.event.bus import EventBus
from neotask.models.task import Task, TaskPriority
from neotask.monitor.metrics import MetricsCollector
from neotask.queue.delayed_queue import DelayedQueue
from neotask.queue.priority_queue import PriorityQueue
from neotask.queue.scheduler import QueueScheduler
from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
from neotask.storage.sqlite import SQLiteTaskRepository, SQLiteQueueRepository


# ========== 异步支持 ==========

@pytest.fixture(scope="session")
def event_loop():
    """创建事件循环"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


# ========== 存储 Fixtures ==========

@pytest.fixture
def memory_task_repo():
    """内存任务存储"""
    return MemoryTaskRepository()


@pytest.fixture
def memory_queue_repo():
    """内存队列存储"""
    return MemoryQueueRepository()


@pytest.fixture
def sqlite_task_repo():
    """SQLite 任务存储（临时文件）"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    repo = SQLiteTaskRepository(db_path)
    yield repo

    # 清理
    if hasattr(repo, 'close'):
        try:
            asyncio.create_task(repo.close())
        except:
            pass
    os.unlink(db_path)


@pytest.fixture
def sqlite_queue_repo():
    """SQLite 队列存储（临时文件）"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    repo = SQLiteQueueRepository(db_path)
    yield repo

    os.unlink(db_path)


# ========== 队列 Fixtures ==========

@pytest.fixture
def priority_queue():
    """优先级队列"""
    return PriorityQueue()


@pytest.fixture
def priority_queue_with_repo(memory_queue_repo):
    """带存储的优先级队列"""
    return PriorityQueue(memory_queue_repo)


@pytest.fixture
def delayed_queue():
    """延迟队列"""
    return DelayedQueue()


@pytest.fixture
def queue_scheduler(memory_queue_repo):
    """队列调度器"""
    return QueueScheduler(memory_queue_repo, max_size=1000)


# ========== 事件总线 Fixtures ==========

@pytest.fixture
async def event_bus():
    """事件总线（异步 fixture）"""
    bus = EventBus()
    await bus.start()
    yield bus
    await bus.stop()


# ========== 指标收集 Fixtures ==========

@pytest.fixture
def metrics_collector():
    """指标收集器"""
    return MetricsCollector(window_size=100)


# ========== 测试任务 Fixtures ==========

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


# ========== 辅助函数 ==========

def create_test_executor():
    """创建测试执行器"""

    async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"result": "processed", "data": data}

    return executor


# ========== 参数化数据 ==========

@pytest.fixture(params=["memory", "sqlite"])
def storage_type(request):
    """参数化存储类型"""
    return request.param


@pytest.fixture
def task_repo_factory():
    """任务存储工厂"""

    def _create(storage_type: str):
        if storage_type == "memory":
            return MemoryTaskRepository()
        elif storage_type == "sqlite":
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
                db_path = f.name
            repo = SQLiteTaskRepository(db_path)
            repo._cleanup = lambda: os.unlink(db_path)
            return repo
        else:
            raise ValueError(f"Unknown storage type: {storage_type}")

    return _create


@pytest.fixture
def queue_repo_factory():
    """队列存储工厂"""

    def _create(storage_type: str):
        if storage_type == "memory":
            return MemoryQueueRepository()
        elif storage_type == "sqlite":
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
                db_path = f.name
            repo = SQLiteQueueRepository(db_path)
            repo._cleanup = lambda: os.unlink(db_path)
            return repo
        else:
            raise ValueError(f"Unknown storage type: {storage_type}")

    return _create
