"""
@FileName: conftest.py
@Description: pytest 配置和共享 fixtures
@Author: HiPeng
@Time: 2026/4/16
"""

import asyncio
import os
import sys
from typing import Dict, Any, Optional

import pytest

# 确保可以导入 neotask
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.api.task_scheduler import TaskScheduler, SchedulerConfig


# ========== 异步支持 ==========

@pytest.fixture(scope="session")
def event_loop():
    """创建事件循环（session级别）"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    # 清理所有待处理的任务
    pending = asyncio.all_tasks(loop)
    for task in pending:
        task.cancel()
    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    loop.close()


# ========== 测试执行器 ==========

async def success_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """成功执行器"""
    return {"result": "success", "data": data}


async def fail_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """失败执行器"""
    raise ValueError("Task execution failed")


async def slow_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """慢速执行器"""
    delay = data.get("delay", 0.5)
    await asyncio.sleep(delay)
    return {"result": "slow_success", "data": data, "delay": delay}


async def echo_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """回显执行器"""
    return {"result": data}


# ========== 存储 fixtures ==========

@pytest.fixture
def memory_config() -> TaskPoolConfig:
    """内存存储配置"""
    return TaskPoolConfig(storage_type="memory")


@pytest.fixture
def sqlite_config() -> Optional[TaskPoolConfig]:
    """SQLite 存储配置"""
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    config = TaskPoolConfig(storage_type="sqlite", sqlite_path=db_path)
    yield config
    # 清理
    if os.path.exists(db_path):
        try:
            os.unlink(db_path)
        except PermissionError:
            pass


@pytest.fixture(scope="session")
def redis_url():
    """Redis URL - session级别，只创建一次"""
    return os.environ.get("REDIS_URL", "redis://localhost:6379/10")


@pytest.fixture
def redis_config(redis_url):
    """Redis 存储配置"""
    return TaskPoolConfig(storage_type="redis", redis_url=redis_url)


# ========== TaskPool fixtures ==========

@pytest.fixture
def memory_task_pool(memory_config):
    """内存 TaskPool"""
    pool = TaskPool(executor=success_executor, config=memory_config)
    pool.start()
    yield pool
    pool.shutdown()
    # 给时间让连接关闭
    import asyncio
    asyncio.sleep(0.1)


@pytest.fixture
def task_pool_with_fail_executor(memory_config):
    """使用失败执行器的 TaskPool"""
    pool = TaskPool(executor=fail_executor, config=memory_config)
    pool.start()
    yield pool
    pool.shutdown()


@pytest.fixture
def task_pool_with_slow_executor(memory_config):
    """使用慢速执行器的 TaskPool"""
    pool = TaskPool(executor=slow_executor, config=memory_config)
    pool.start()
    yield pool
    pool.shutdown()


# ========== TaskScheduler fixtures ==========

@pytest.fixture
def memory_scheduler(memory_config):
    """内存 TaskScheduler"""
    scheduler_config = SchedulerConfig.memory()
    scheduler = TaskScheduler(executor=success_executor, config=scheduler_config)
    scheduler.start()
    yield scheduler
    scheduler.shutdown()


# ========== 辅助函数 ==========
def has_redis():
    """检查 Redis 是否可用"""
    try:
        import redis

        client = redis.Redis.from_url("redis://localhost:6379", decode_responses=True)
        client.ping()
        client.close()
        return True
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return False

skip_if_no_redis = pytest.mark.skipif(
    not has_redis(),
    reason="Redis not available"
)
