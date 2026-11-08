"""
@FileName: test_running_persist.py
@Description: RUNNING 状态延迟落库（执行侧写入优化）单元测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/8
"""

import asyncio

import pytest

from neotask.core.lifecycle import TaskLifecycleManager
from neotask.event.bus import EventBus
from neotask.models.task import TaskStatus
from neotask.storage.memory import MemoryTaskRepository


class CountingTaskRepository(MemoryTaskRepository):
    """记录每次字段级写入涉及哪些字段"""

    def __init__(self):
        super().__init__()
        self.saved_fields = []

    async def save_fields(self, task, fields):
        self.saved_fields.append(tuple(fields))
        return await super().save_fields(task, fields)


@pytest.fixture
async def event_bus():
    bus = EventBus()
    await bus.start()
    yield bus
    await bus.stop()


@pytest.mark.asyncio
async def test_default_writes_running_immediately(event_bus):
    """默认 delay=0：保持旧行为，start 立即写 RUNNING"""
    repo = CountingTaskRepository()
    manager = TaskLifecycleManager(repo, event_bus)

    task = await manager.create_task(data={"id": 1})
    await manager.start_task(task.task_id, "worker-1")

    assert repo.saved_fields == [("status", "node_id", "started_at")]
    stored = await repo.get(task.task_id)
    assert stored.status == TaskStatus.RUNNING
    assert stored.started_at is not None

    await manager.complete_task(task.task_id, {"ok": True})
    stored = await repo.get(task.task_id)
    assert stored.status == TaskStatus.SUCCESS
    assert ("started_at" in repo.saved_fields[-1])


@pytest.mark.asyncio
async def test_short_task_writes_terminal_state_only(event_bus):
    """窗口内完成：跳过 RUNNING 写入，终态写入补齐 started_at"""
    repo = CountingTaskRepository()
    manager = TaskLifecycleManager(repo, event_bus, running_persist_delay=0.3)

    task = await manager.create_task(data={"id": 2})
    await manager.start_task(task.task_id, "worker-1")
    await manager.complete_task(task.task_id, {"ok": True})

    # 只有终态一次写入
    assert len(repo.saved_fields) == 1
    assert "started_at" in repo.saved_fields[0]

    stored = await repo.get(task.task_id)
    assert stored.status == TaskStatus.SUCCESS
    assert stored.started_at is not None
    assert stored.result == {"ok": True}

    # 窗口结束后不应再出现迟到的 RUNNING 写入
    await asyncio.sleep(0.35)
    stored = await repo.get(task.task_id)
    assert stored.status == TaskStatus.SUCCESS


@pytest.mark.asyncio
async def test_long_task_persists_running_after_window(event_bus):
    """超出窗口仍在执行：RUNNING 状态在窗口到期后落库"""
    repo = CountingTaskRepository()
    manager = TaskLifecycleManager(repo, event_bus, running_persist_delay=0.2)

    task = await manager.create_task(data={"id": 3})
    await manager.start_task(task.task_id, "worker-1")

    await asyncio.sleep(0.3)
    stored = await repo.get(task.task_id)
    assert stored.status == TaskStatus.RUNNING
    assert stored.started_at is not None

    await manager.complete_task(task.task_id, {"ok": True})
    stored = await repo.get(task.task_id)
    assert stored.status == TaskStatus.SUCCESS


@pytest.mark.asyncio
async def test_failure_and_cancel_include_started_at(event_bus):
    """失败/取消的终态写入同样补齐 started_at"""
    repo = CountingTaskRepository()
    manager = TaskLifecycleManager(repo, event_bus, running_persist_delay=1.0)

    failed = await manager.create_task(data={"id": 4})
    await manager.start_task(failed.task_id, "worker-1")
    await manager.fail_task(failed.task_id, "boom")
    stored = await repo.get(failed.task_id)
    assert stored.status == TaskStatus.FAILED
    assert stored.started_at is not None

    cancelled = await manager.create_task(data={"id": 5})
    await manager.start_task(cancelled.task_id, "worker-1")
    await manager.cancel_task(cancelled.task_id)
    stored = await repo.get(cancelled.task_id)
    assert stored.status == TaskStatus.CANCELLED
    assert stored.started_at is not None


@pytest.mark.asyncio
async def test_flush_deferred_running_on_shutdown(event_bus):
    """关闭前把仍在窗口内的 RUNNING 状态立即落库"""
    repo = CountingTaskRepository()
    manager = TaskLifecycleManager(repo, event_bus, running_persist_delay=5.0)

    task = await manager.create_task(data={"id": 6})
    await manager.start_task(task.task_id, "worker-1")

    # 窗口未到，存储中仍是 PENDING
    stored = await repo.get(task.task_id)
    assert stored.status == TaskStatus.PENDING

    assert await manager.flush_deferred_running() == 1
    stored = await repo.get(task.task_id)
    assert stored.status == TaskStatus.RUNNING
    assert stored.started_at is not None
