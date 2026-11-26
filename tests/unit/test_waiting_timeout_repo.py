"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: test_waiting_timeout_repo.py
@Description: 等待超时索引测试 - list_expired_waiting 只返回到期任务，
             三后端在唤醒/回滚/删除后都要同步清理索引
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/25
"""

import os
import sqlite3

import pytest

from neotask.core.signal import SIGNAL_DATA_KEY
from neotask.models.task import Task, TaskStatus
from neotask.storage.memory import MemoryTaskRepository
from neotask.storage.redis import (
    _SIGNAL_TIMEOUT_KEY,
    _SIGNAL_TIMEOUT_READY_KEY,
    RedisTaskRepository,
)
from neotask.storage.sqlite import SQLiteTaskRepository

NOW = 1000.0


def _waiting(task_id: str, timeout_at=None, arrived: bool = False) -> Task:
    return Task(
        task_id=task_id,
        data={
            SIGNAL_DATA_KEY: {
                "signal": "go",
                "timeout_at": timeout_at,
                "arrived": arrived,
                "payload": "done" if arrived else None,
            }
        },
        status=TaskStatus.WAITING,
    )


def _redis_url() -> str:
    return os.environ.get("REDIS_TEST_URL", "redis://127.0.0.1:6379/2")


@pytest.fixture(params=["memory", "sqlite"])
async def repo(request, tmp_path):
    if request.param == "memory":
        yield MemoryTaskRepository()
        return

    repository = SQLiteTaskRepository(str(tmp_path / "timeout.db"))
    try:
        yield repository
    finally:
        await repository.close()


async def test_only_expired_waiting_tasks_are_listed(repo):
    """只返回已到期且仍在等待的任务，按到期时刻升序"""
    await repo.save(_waiting("late", NOW + 60))
    await repo.save(_waiting("expired-b", NOW - 10))
    await repo.save(_waiting("expired-a", NOW - 50))
    await repo.save(_waiting("no-timeout"))
    await repo.save(Task(task_id="pending", data={}))

    expired = await repo.list_expired_waiting(now=NOW)
    assert [task.task_id for task in expired] == ["expired-a", "expired-b"]

    first_page = await repo.list_expired_waiting(now=NOW, limit=1)
    assert [task.task_id for task in first_page] == ["expired-a"]
    second_page = await repo.list_expired_waiting(now=NOW, limit=1, offset=1)
    assert [task.task_id for task in second_page] == ["expired-b"]


async def test_signal_clears_timeout_entry(repo):
    """等待任务被唤醒后不再出现在到期集合；回滚后重新出现"""
    task = _waiting("wake-me", NOW - 1)
    await repo.save(task)
    assert len(await repo.list_expired_waiting(now=NOW)) == 1

    signalled = task.clone()
    signalled.data[SIGNAL_DATA_KEY]["arrived"] = True
    signalled.status = TaskStatus.PENDING
    signalled.node_id = ""
    assert await repo.save_fields_if_status(
        signalled, ("status", "data", "node_id"), TaskStatus.WAITING
    )
    assert await repo.list_expired_waiting(now=NOW) == []

    reverted = signalled.clone()
    reverted.data[SIGNAL_DATA_KEY]["arrived"] = False
    reverted.status = TaskStatus.WAITING
    assert await repo.save_fields_if_status(
        reverted, ("status", "data"), TaskStatus.PENDING
    )
    assert len(await repo.list_expired_waiting(now=NOW)) == 1


async def test_delete_clears_timeout_entry(repo):
    task = _waiting("to-delete", NOW - 1)
    await repo.save(task)
    await repo.delete(task.task_id)
    assert await repo.get(task.task_id) is None
    assert await repo.list_expired_waiting(now=NOW) == []


async def test_sqlite_backfills_legacy_waiting_rows(tmp_path):
    """升级前挂起的等待任务（列为 NULL）在重新打开时补索引"""
    path = str(tmp_path / "legacy.db")
    repo = SQLiteTaskRepository(path)
    await repo.save(_waiting("legacy", NOW - 1))
    await repo.close()

    # 模拟 v1.3.0 老库：等待任务只有 data，没有 signal_timeout_at
    conn = sqlite3.connect(path)
    try:
        conn.execute("UPDATE tasks SET signal_timeout_at = NULL")
        conn.commit()
    finally:
        conn.close()

    reopened = SQLiteTaskRepository(path)
    try:
        expired = await reopened.list_expired_waiting(now=NOW)
        assert [task.task_id for task in expired] == ["legacy"]
    finally:
        await reopened.close()


@pytest.mark.skipif(
    not os.environ.get("REDIS_TEST_URL") and not os.environ.get("NEOTASK_RUN_REDIS"),
    reason="set REDIS_TEST_URL or NEOTASK_RUN_REDIS to run Redis tests",
)
class TestRedisTimeoutIndex:
    async def test_index_follows_transitions_and_backfills(self):
        repo = RedisTaskRepository(_redis_url())
        task_id = "timeout-redis-1"
        try:
            await repo.delete(task_id)
            client = await repo._get_client()
            await client.zrem(_SIGNAL_TIMEOUT_KEY, task_id)
            await client.delete(_SIGNAL_TIMEOUT_READY_KEY)

            task = _waiting(task_id, NOW - 1)
            await repo.save(task)
            assert await client.zscore(_SIGNAL_TIMEOUT_KEY, task_id) == NOW - 1
            assert [t.task_id for t in await repo.list_expired_waiting(now=NOW)] == [task_id]

            signalled = task.clone()
            signalled.data[SIGNAL_DATA_KEY]["arrived"] = True
            signalled.status = TaskStatus.PENDING
            assert await repo.save_fields_if_status(
                signalled, ("status", "data"), TaskStatus.WAITING
            )
            assert await client.zscore(_SIGNAL_TIMEOUT_KEY, task_id) is None
            assert await repo.list_expired_waiting(now=NOW) == []

            # 回滚：重新进入 WAITING，索引按原 timeout_at 恢复
            reverted = signalled.clone()
            reverted.status = TaskStatus.WAITING
            assert await repo.save_fields_if_status(
                reverted, ("status", "data"), TaskStatus.PENDING
            )
            assert await client.zscore(_SIGNAL_TIMEOUT_KEY, task_id) == NOW - 1

            # 模拟升级前挂起的任务：成员缺失、标记缺失 → 首次扫描回填
            await client.zrem(_SIGNAL_TIMEOUT_KEY, task_id)
            await client.delete(_SIGNAL_TIMEOUT_READY_KEY)
            expired = await repo.list_expired_waiting(now=NOW)
            assert [t.task_id for t in expired] == [task_id]
            assert await client.zscore(_SIGNAL_TIMEOUT_KEY, task_id) == NOW - 1

            await repo.delete(task_id)
            assert await client.zscore(_SIGNAL_TIMEOUT_KEY, task_id) is None
        finally:
            await repo.delete(task_id)
            await repo.close()
