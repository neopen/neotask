"""
@FileName: storage.py
@Description: 存储相关 fixtures
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import os
import tempfile
import pytest
import asyncio

from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
from neotask.storage.sqlite import SQLiteTaskRepository, SQLiteQueueRepository


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


@pytest.fixture(scope="function")
async def memory_storage():
    """内存存储 fixture"""
    return MemoryTaskRepository(), MemoryQueueRepository()


@pytest.fixture(scope="function")
async def sqlite_storage(tmp_path):
    """SQLite 存储 fixture"""
    db_path = tmp_path / "test.db"
    return SQLiteTaskRepository(str(db_path)), SQLiteQueueRepository(str(db_path))


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