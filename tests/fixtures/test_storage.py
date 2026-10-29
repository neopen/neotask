"""
@FileName: test_storage.py
@Description: 存储相关 fixtures
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import asyncio
import os
import tempfile

import pytest

from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
from neotask.storage.sqlite import SQLiteTaskRepository, SQLiteQueueRepository

# Windows 下 SQLite 文件句柄释放存在延迟，删除失败时短暂重试
_UNLINK_RETRIES = 10
_UNLINK_DELAY = 0.05


async def _close_repo(repo) -> None:
    """关闭仓储持有的连接（内存仓储没有 ``close``，直接跳过）。"""
    close = getattr(repo, "close", None)
    if close is not None:
        await close()


async def _close_and_remove_db(repo, db_path: str) -> None:
    """先关闭连接、再删除临时库文件，避免 Windows 下 ``WinError 32``。

    SQLite 运行时可能同时生成 ``-journal`` / ``-wal`` / ``-shm`` 边车文件，一并清理；
    边车文件删除失败不影响测试结论，主库文件在重试后仍被占用才会抛错。
    """
    await _close_repo(repo)

    for suffix in ("", "-journal", "-wal", "-shm"):
        path = db_path + suffix
        for attempt in range(_UNLINK_RETRIES):
            try:
                os.unlink(path)
                break
            except FileNotFoundError:
                break
            except PermissionError:
                if attempt == _UNLINK_RETRIES - 1:
                    if not suffix:
                        raise
                    break
                await asyncio.sleep(_UNLINK_DELAY)


@pytest.fixture
def memory_task_repo():
    """内存任务存储"""
    return MemoryTaskRepository()


@pytest.fixture
def memory_queue_repo():
    """内存队列存储"""
    return MemoryQueueRepository()


@pytest.fixture
async def sqlite_task_repo():
    """SQLite 任务存储（临时文件；用例结束后关闭连接并删除文件）"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    repo = SQLiteTaskRepository(db_path)
    try:
        yield repo
    finally:
        # 必须先关闭连接：Windows 下连接未关闭时删除文件会报 WinError 32
        await _close_and_remove_db(repo, db_path)


@pytest.fixture
async def sqlite_queue_repo():
    """SQLite 队列存储（临时文件；用例结束后关闭连接并删除文件）"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    repo = SQLiteQueueRepository(db_path)
    try:
        yield repo
    finally:
        await _close_and_remove_db(repo, db_path)


@pytest.fixture(scope="function")
async def memory_storage():
    """内存存储 fixture"""
    return MemoryTaskRepository(), MemoryQueueRepository()


@pytest.fixture(scope="function")
async def sqlite_storage(tmp_path):
    """SQLite 存储 fixture（任务 + 队列共用同一临时库；用例结束后关闭连接）"""
    db_path = tmp_path / "test.db"
    task_repo = SQLiteTaskRepository(str(db_path))
    queue_repo = SQLiteQueueRepository(str(db_path))
    try:
        yield task_repo, queue_repo
    finally:
        # 先关闭两个连接，避免留下未释放的 SQLite 句柄
        await _close_repo(task_repo)
        await _close_repo(queue_repo)


@pytest.fixture(params=["memory", "sqlite"])
def storage_type(request):
    """参数化存储类型"""
    return request.param


@pytest.fixture
async def task_repo_factory(tmp_path):
    """任务存储工厂。

    ``sqlite`` 场景为每次创建分配独立的临时库文件，并在用例结束后统一关闭
    由本工厂创建的所有仓储，避免连接泄漏（原先的 ``_cleanup`` 只删文件、不关连接）。
    """
    created = []
    counter = 0

    def _create(storage_type: str):
        nonlocal counter
        if storage_type == "memory":
            repo = MemoryTaskRepository()
        elif storage_type == "sqlite":
            counter += 1
            repo = SQLiteTaskRepository(str(tmp_path / f"task_{counter}.db"))
        else:
            raise ValueError(f"Unknown storage type: {storage_type}")
        created.append(repo)
        return repo

    try:
        yield _create
    finally:
        for repo in created:
            await _close_repo(repo)


@pytest.fixture
async def queue_repo_factory(tmp_path):
    """队列存储工厂（``sqlite`` 场景的临时库在用例结束后统一关闭）"""
    created = []
    counter = 0

    def _create(storage_type: str):
        nonlocal counter
        if storage_type == "memory":
            repo = MemoryQueueRepository()
        elif storage_type == "sqlite":
            counter += 1
            repo = SQLiteQueueRepository(str(tmp_path / f"queue_{counter}.db"))
        else:
            raise ValueError(f"Unknown storage type: {storage_type}")
        created.append(repo)
        return repo

    try:
        yield _create
    finally:
        for repo in created:
            await _close_repo(repo)
