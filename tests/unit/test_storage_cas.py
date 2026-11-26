"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: test_storage_cas.py
@Description: 任务状态 CAS（save_fields_if_status）三后端一致性测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/25
"""

import asyncio
import os

import pytest

from neotask.common.constants import redis_key
from neotask.models.task import Task, TaskStatus
from neotask.storage.memory import MemoryTaskRepository
from neotask.storage.redis import RedisTaskRepository
from neotask.storage.sqlite import SQLiteTaskRepository

_RUNNING_FIELDS = ("status", "node_id", "started_at", "queue_wait_ms")


def _task(task_id: str = "cas-1", **kwargs) -> Task:
    return Task(task_id=task_id, data={"v": 1}, **kwargs)


def _started(task: Task, node_id: str) -> Task:
    clone = task.clone()
    clone.start(node_id)
    return clone


def _redis_url() -> str:
    return os.environ.get("REDIS_TEST_URL", "redis://127.0.0.1:6379/2")


async def _redis_available() -> bool:
    try:
        import redis.asyncio as aioredis

        client = aioredis.from_url(
            _redis_url(), decode_responses=True, socket_connect_timeout=2
        )
        try:
            await client.ping()
        finally:
            await client.close()
        return True
    except Exception:
        return False


@pytest.fixture(params=["memory", "sqlite"])
async def repo(request, tmp_path):
    """内置的原子实现后端：内存与 SQLite"""
    if request.param == "memory":
        yield MemoryTaskRepository()
        return

    repository = SQLiteTaskRepository(str(tmp_path / "cas.db"))
    try:
        yield repository
    finally:
        await repository.close()


async def test_only_one_concurrent_transition_wins(repo):
    """两个执行者同时 PENDING → RUNNING，只有一个成功"""
    task = _task()
    await repo.save(task)

    first = _started(task, "node-a")
    second = _started(task, "node-b")
    results = await asyncio.gather(
        repo.save_fields_if_status(first, _RUNNING_FIELDS, TaskStatus.PENDING),
        repo.save_fields_if_status(second, _RUNNING_FIELDS, TaskStatus.PENDING),
    )

    assert sorted(results) == [False, True]
    stored = await repo.get(task.task_id)
    assert stored.status == TaskStatus.RUNNING
    # node_id 必须来自赢家，不能出现两方字段混合
    assert stored.node_id in {"node-a", "node-b"}


async def test_stale_status_is_rejected_without_writing(repo):
    """状态已变化时 CAS 失败且不写入任何字段"""
    task = _task()
    await repo.save(task)
    running = _started(task, "node-a")
    assert await repo.save_fields_if_status(running, _RUNNING_FIELDS, TaskStatus.PENDING)

    stale = running.clone()
    stale.fail("boom")
    assert await repo.save_fields_if_status(
        stale, ("status", "error", "completed_at"), TaskStatus.PENDING
    ) is False

    stored = await repo.get(task.task_id)
    assert stored.status == TaskStatus.RUNNING
    assert stored.error is None


async def test_missing_task_returns_false(repo):
    """任务不存在时 CAS 返回 False"""
    task = _task(task_id="cas-missing")
    assert await repo.save_fields_if_status(task, ("status",), TaskStatus.PENDING) is False


@pytest.mark.skipif(
    not os.environ.get("REDIS_TEST_URL") and not os.environ.get("NEOTASK_RUN_REDIS"),
    reason="set REDIS_TEST_URL or NEOTASK_RUN_REDIS to run Redis tests",
)
class TestRedisCasIndexes:
    async def test_transitions_keep_status_and_expiry_indexes(self):
        """Redis CAS 需同步状态集合、父子索引与终态过期索引"""
        if not await _redis_available():
            pytest.skip("Redis not available")

        repo = RedisTaskRepository(_redis_url())
        task_id = "cas-redis-1"
        parent_id = "cas-parent-1"
        try:
            await repo.delete(task_id)
            client = await repo._get_client()
            await client.srem(redis_key(f"parent:{parent_id}"), task_id)

            task = _task(task_id, parent_task_id=parent_id)
            await repo.save(task)
            assert await client.sismember(redis_key("status:pending"), task_id)
            assert await client.zscore(repo._expiry_key, task_id) is None

            succeeded = task.clone()
            succeeded.complete({"ok": True})
            assert await repo.save_fields_if_status(
                succeeded,
                ("status", "completed_at", "result", "duration_ms"),
                TaskStatus.PENDING,
            )
            assert not await client.sismember(redis_key("status:pending"), task_id)
            assert await client.sismember(redis_key("status:success"), task_id)
            assert await client.sismember(redis_key(f"parent:{parent_id}"), task_id)
            assert await client.zscore(repo._expiry_key, task_id) is not None

            # 手动重试：终态 → PENDING，过期索引必须移除
            retried = succeeded.clone()
            retried.reset_for_retry()
            assert await repo.save_fields_if_status(
                retried, ("status", "completed_at", "result"), TaskStatus.SUCCESS
            )
            assert await client.sismember(redis_key("status:pending"), task_id)
            assert not await client.sismember(redis_key("status:success"), task_id)
            assert await client.zscore(repo._expiry_key, task_id) is None

            # 旧状态已过期，再次以 SUCCESS 为条件必须失败
            assert await repo.save_fields_if_status(
                retried, ("status",), TaskStatus.SUCCESS
            ) is False
        finally:
            await repo.delete(task_id)
            await repo.close()
