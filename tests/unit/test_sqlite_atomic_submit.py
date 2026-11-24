"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: test_sqlite_atomic_submit.py
@Description: SQLite「创建 + 入队」单事务快路径测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/22
"""

import asyncio

import pytest

from neotask.common.exceptions import TaskAlreadyExistsError
from neotask.models.task import Task, TaskPriority
from neotask.storage.sqlite import SQLiteQueueRepository, SQLiteTaskRepository


@pytest.fixture
async def repos(tmp_path):
    """共享同一库文件的任务/队列仓储（与 StorageFactory 的接线方式一致）"""
    db_path = str(tmp_path / "atomic.db")
    task_repo = SQLiteTaskRepository(db_path)
    queue_repo = SQLiteQueueRepository(db_path)
    await task_repo.ensure_ready()
    try:
        yield task_repo, queue_repo
    finally:
        await queue_repo.close()
        await task_repo.close()


def _make_task(task_id: str, priority: TaskPriority = TaskPriority.NORMAL) -> Task:
    return Task(task_id=task_id, data={"task_id": task_id}, priority=priority)


class TestSQLiteAtomicSubmit:
    """save_if_absent_and_enqueue 的事务语义"""

    @pytest.mark.asyncio
    async def test_creates_task_and_enqueues(self, repos):
        task_repo, queue_repo = repos
        task = _make_task("t-1", TaskPriority.HIGH)

        queued = await task_repo.save_if_absent_and_enqueue(task, max_size=10)

        assert queued is True
        assert await task_repo.get("t-1") is not None
        assert await queue_repo.size() == 1
        assert await queue_repo.peek(1) == ["t-1"]

    @pytest.mark.asyncio
    async def test_duplicate_raises_without_touching_queue(self, repos):
        task_repo, queue_repo = repos
        task = _make_task("t-1")
        assert await task_repo.save_if_absent_and_enqueue(task, max_size=10) is True

        with pytest.raises(TaskAlreadyExistsError):
            await task_repo.save_if_absent_and_enqueue(_make_task("t-1"), max_size=10)

        assert await queue_repo.size() == 1

    @pytest.mark.asyncio
    async def test_full_queue_keeps_task_but_skips_enqueue(self, repos):
        task_repo, queue_repo = repos
        assert await task_repo.save_if_absent_and_enqueue(
            _make_task("t-1"), max_size=1
        ) is True

        queued = await task_repo.save_if_absent_and_enqueue(
            _make_task("t-2"), max_size=1
        )

        assert queued is False
        stored = await task_repo.get("t-2")
        assert stored is not None
        assert stored.data["task_id"] == "t-2"
        assert await queue_repo.size() == 1
        assert await queue_repo.peek(10) == ["t-1"]

    @pytest.mark.asyncio
    async def test_concurrent_submits_respect_capacity(self, repos):
        """并发提交共用写事务：容量边界不会被两个协程同时越过"""
        task_repo, queue_repo = repos
        tasks = [_make_task(f"t-{index}") for index in range(20)]

        outcomes = await asyncio.gather(
            *(task_repo.save_if_absent_and_enqueue(task, max_size=8) for task in tasks)
        )

        assert outcomes.count(True) == 8
        assert outcomes.count(False) == 12
        assert await queue_repo.size() == 8
        # 未入队的任务同样已落库，由调用方标记失败
        for task in tasks:
            assert await task_repo.get(task.task_id) is not None

    @pytest.mark.asyncio
    async def test_priority_visible_through_queue_repo(self, repos):
        """事务写入的优先级与普通 push 的排序语义一致"""
        task_repo, queue_repo = repos
        await task_repo.save_if_absent_and_enqueue(
            _make_task("low", TaskPriority.LOW), max_size=10
        )
        await task_repo.save_if_absent_and_enqueue(
            _make_task("critical", TaskPriority.CRITICAL), max_size=10
        )

        # RETURNING 的行序不保证与子查询一致，这里只校验优先级筛选结果
        popped = await queue_repo.pop(1)
        assert popped == ["critical"]
        assert await queue_repo.peek(1) == ["low"]
