"""
@FileName: conftest.py
@Description: pytest 配置和共享 fixtures
@Author: HiPeng
@Time: 2026/4/21
"""

import asyncio
import os
import sys
import tempfile
from typing import Dict, Any

import pytest
import redis.asyncio as redis

from neotask.event.bus import EventBus
from neotask.models.task import Task, TaskPriority
from neotask.monitor.metrics import MetricsCollector
from neotask.queue.delayed_queue import DelayedQueue
from neotask.queue.priority_queue import PriorityQueue
from neotask.queue.queue_scheduler import QueueScheduler
from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
from neotask.storage.sqlite import SQLiteTaskRepository, SQLiteQueueRepository

# Redis 配置
REDIS_URL = "redis://localhost:6379/11"  # 使用 db 0 用于测试
TEST_REDIS_URL = "redis://localhost:6379/12"  # 使用 db 1 避免冲突


# Windows 上解决事件循环关闭问题
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(scope="session")
def event_loop():
    """创建事件循环 - 修复 Windows 关闭问题"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    # 关闭前处理所有 pending 任务
    if not loop.is_closed():
        # 取消所有正在运行的任务
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        # 等待任务取消完成
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


@pytest.fixture(autouse=True)
def close_loop_after_test():
    """确保每个测试后正确关闭事件循环"""
    yield
    # 清理
    loop = asyncio.get_event_loop()
    if loop.is_running():
        # 如果循环正在运行，不要立即关闭
        pass


@pytest.fixture(scope="session")
async def redis_client():
    """Redis 客户端 fixture"""
    # 为每个测试创建新连接
    client = redis.from_url(TEST_REDIS_URL, decode_responses=True, max_connections=10)
    yield client
    # 清理测试数据
    await client.flushdb()
    await client.close()


@pytest.fixture(autouse=True)
async def clean_redis(redis_client):
    """每个测试后自动清理 Redis"""
    yield
    await redis_client.flushdb()


@pytest.fixture
def test_node_id():
    """生成测试节点 ID"""
    import socket
    import os
    import time
    return f"test_node_{socket.gethostname()}_{os.getpid()}_{int(time.time() * 1000)}"


@pytest.fixture
def mock_executor():
    """模拟任务执行器"""

    async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"result": "success", "data": data}

    return executor

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
