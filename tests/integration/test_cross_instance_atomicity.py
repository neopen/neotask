"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: test_cross_instance_atomicity.py
@Description: 跨实例状态转换原子性测试 - 两个生命周期管理器共享同一 SQLite 库
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/25
"""

import asyncio
import time

import pytest

from neotask.common.constants import EventType
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.core.signal import SIGNAL_DATA_KEY, SignalWaiter
from neotask.event.bus import EventBus
from neotask.models.task import TaskStatus
from neotask.storage.sqlite import SQLiteTaskRepository


class _Node:
    """一个独立的生命周期管理器实例（模拟独立进程 / 节点）"""

    def __init__(self, name: str, db_path: str):
        self.name = name
        self.repo = SQLiteTaskRepository(db_path)
        self.bus = EventBus()
        self.lifecycle = TaskLifecycleManager(self.repo, self.bus, cache_enabled=True)
        self.events = []

    async def start(self) -> None:
        await self.bus.start()
        self.bus.subscribe_global(self._record)

    async def _record(self, event) -> None:
        self.events.append(event)

    def count(self, event_type: str) -> int:
        return sum(1 for event in self.events if event.event_type == event_type)

    async def close(self) -> None:
        await self.bus.stop()
        await self.repo.close()


async def _wait_total(
        nodes, event_type: str, expected: int, timeout: float = 2.0
) -> int:
    """等待事件总线把排队事件消费完，返回当前总计数"""
    deadline = time.monotonic() + timeout
    total = 0
    while time.monotonic() < deadline:
        total = sum(node.count(event_type) for node in nodes)
        if total >= expected:
            return total
        await asyncio.sleep(0.01)
    return total


@pytest.fixture
async def two_nodes(tmp_path):
    """两个共享同一 SQLite 库、各自持有独立缓存与事件总线的实例"""
    db_path = str(tmp_path / "shared.db")
    first = _Node("node-a", db_path)
    second = _Node("node-b", db_path)
    for node in (first, second):
        await node.start()
    try:
        yield first, second
    finally:
        for node in (first, second):
            await node.close()


async def test_stale_instance_cannot_suspend_finished_task(two_nodes):
    """A 已完成的任务，B 用陈旧的 RUNNING 缓存挂起必须失败（SUCCESS 不被覆盖）"""
    first, second = two_nodes

    task = await first.lifecycle.create_task(data={"job": "stale"})
    assert await first.lifecycle.start_task(task.task_id, "node-a")

    # B 读取后缓存 RUNNING，A 随后完成：B 的缓存到此已经过期
    assert (await second.lifecycle.get_task(task.task_id)).status == TaskStatus.RUNNING
    assert await first.lifecycle.complete_task(task.task_id, {"ok": True})

    assert await second.lifecycle.suspend_for_signal(task.task_id, "approve") is False

    stored = await second.lifecycle.get_task(task.task_id)
    assert stored.status == TaskStatus.SUCCESS
    assert stored.result == {"ok": True}
    # 失败方不得发布任何 waiting 事件
    assert await _wait_total([first, second], EventType.WAITING, 1, timeout=0.05) == 0


async def test_replay_picks_up_signal_from_other_instance(two_nodes):
    """重放时本实例缓存仍是旧 PENDING 副本：必须回查存储拿到另一实例写入的 payload

    真实执行链路上，提交实例可能一直缓存创建时的 PENDING 任务（没有任何
    ``__signal__``），而任务由另一实例启动、挂起并被唤醒。重放执行不能再挂起
    一次（那会覆盖已到达的信号），必须先回查存储消费 payload。
    """
    first, second = two_nodes

    task = await first.lifecycle.create_task(data={"job": "replay"})
    stale = await first.lifecycle.get_task(task.task_id)
    assert stale.status == TaskStatus.PENDING
    assert SIGNAL_DATA_KEY not in stale.data

    # 另一实例启动、挂起并写入信号
    assert await second.lifecycle.start_task(task.task_id, "node-b")
    assert await second.lifecycle.suspend_for_signal(task.task_id, "go")
    assert await second.lifecycle.signal_task(task.task_id, payload={"ok": 1}) is True

    consumed = []

    async def suspend(signal: str, timeout) -> bool:
        raise AssertionError("replay must not suspend again")

    async def consume(signal: str) -> bool:
        consumed.append(signal)
        return await first.lifecycle.consume_signal(task.task_id, signal)

    async def reload():
        return await first.lifecycle.get_task(task.task_id, refresh=True)

    waiter = SignalWaiter(stale, suspend, consume, reload=reload)
    assert await waiter.wait("go") == {"ok": 1}
    assert consumed == ["go"]

    stored = await second.repo.get(task.task_id)
    entry = stored.data[SIGNAL_DATA_KEY]
    assert entry["arrived"] is False
    assert entry["consumed"] is True
    assert entry["payload"] is None


async def test_signal_from_non_executing_instance_wakes_task(two_nodes):
    """未参与执行的实例也能 signal：缓存里的旧 PENDING 副本不得挡住唤醒"""
    first, second = two_nodes

    task = await first.lifecycle.create_task(data={"job": "signal-from-other"})
    assert (await first.lifecycle.get_task(task.task_id)).status == TaskStatus.PENDING

    assert await second.lifecycle.start_task(task.task_id, "node-b")
    assert await second.lifecycle.suspend_for_signal(task.task_id, "go")

    assert await first.lifecycle.signal_task(task.task_id, payload={"ok": 2}) is True

    stored = await second.repo.get(task.task_id)
    assert stored.status == TaskStatus.PENDING
    assert stored.data[SIGNAL_DATA_KEY]["arrived"] is True
    assert stored.data[SIGNAL_DATA_KEY]["payload"] == {"ok": 2}


async def test_timeout_scan_on_non_executing_instance_fails_task(two_nodes):
    """未参与执行的实例扫描到期任务：必须用存储快照而不是旧 PENDING 缓存"""
    first, second = two_nodes

    task = await first.lifecycle.create_task(data={"job": "timeout-from-other"})
    assert (await first.lifecycle.get_task(task.task_id)).status == TaskStatus.PENDING

    assert await second.lifecycle.start_task(task.task_id, "node-b")
    assert await second.lifecycle.suspend_for_signal(task.task_id, "go", timeout=0.01)
    await asyncio.sleep(0.02)

    assert await first.lifecycle.fail_expired_waiting(now=time.time() + 0.05) == 1

    stored = await second.repo.get(task.task_id)
    assert stored.status == TaskStatus.FAILED
    assert stored.failure_reason == "waiting_timeout"


async def test_stale_instance_cannot_fail_signalled_task(two_nodes):
    """A 已 signal 的任务，B 用陈旧的 WAITING 缓存判超时必须失败"""
    first, second = two_nodes

    task = await first.lifecycle.create_task(data={"job": "timeout"})
    assert await first.lifecycle.start_task(task.task_id, "node-a")
    assert await first.lifecycle.suspend_for_signal(task.task_id, "go", timeout=0.01)
    await asyncio.sleep(0.02)

    future = await first.lifecycle._future_manager.get(task.task_id)
    assert (await second.lifecycle.get_task(task.task_id)).status == TaskStatus.WAITING

    assert await first.lifecycle.signal_task(task.task_id, "payload") is True
    assert await second.lifecycle.fail_task(
        task.task_id,
        "Timed out waiting for signal 'go'",
        allow_waiting=True,
        failure_reason="waiting_timeout",
    ) is False

    stored = await second.lifecycle.get_task(task.task_id)
    assert stored.status == TaskStatus.PENDING
    assert future.is_completed is False
    assert second.count(EventType.FAILED) == 0


async def test_concurrent_signal_wakes_exactly_once(two_nodes):
    """两个实例同时 signal：只有一个生效，payload/事件都属于赢家"""
    first, second = two_nodes
    rounds = 20

    for index in range(rounds):
        task = await first.lifecycle.create_task(data={"round": index})
        assert await first.lifecycle.start_task(task.task_id, "node-a")
        assert await first.lifecycle.suspend_for_signal(task.task_id, "go")
        # 两边都缓存 WAITING
        await second.lifecycle.get_task(task.task_id)

        events_before = (
            first.count(EventType.SIGNAL_RECEIVED)
            + second.count(EventType.SIGNAL_RECEIVED)
        )
        results = await asyncio.gather(
            first.lifecycle.signal_task(task.task_id, payload=f"a{index}"),
            second.lifecycle.signal_task(task.task_id, payload=f"b{index}"),
        )

        assert sum(results) == 1
        stored = await second.repo.get(task.task_id)
        assert stored.status == TaskStatus.PENDING
        assert stored.data[SIGNAL_DATA_KEY]["arrived"] is True
        expected_payload = f"a{index}" if results[0] else f"b{index}"
        assert stored.data[SIGNAL_DATA_KEY]["payload"] == expected_payload

        # 事件经总线异步派发：等消费完成后再确认成功方只发布一次
        total = await _wait_total(
            [first, second], EventType.SIGNAL_RECEIVED, events_before + 1
        )
        await asyncio.sleep(0.02)
        assert total == events_before + 1
        assert (
            first.count(EventType.SIGNAL_RECEIVED)
            + second.count(EventType.SIGNAL_RECEIVED)
        ) == events_before + 1


async def test_timeout_and_signal_have_single_outcome(two_nodes):
    """超时扫描与 signal 并发：只有一方生效，future 与最终状态一致"""
    first, second = two_nodes
    rounds = 20

    for index in range(rounds):
        task = await first.lifecycle.create_task(data={"round": index})
        assert await first.lifecycle.start_task(task.task_id, "node-a")
        assert await first.lifecycle.suspend_for_signal(task.task_id, "go", timeout=0.01)
        await asyncio.sleep(0.02)

        future = await first.lifecycle._future_manager.get(task.task_id)
        signalled, failed = await asyncio.gather(
            second.lifecycle.signal_task(task.task_id, payload=f"go{index}"),
            first.lifecycle.fail_expired_waiting(now=time.time() + 0.05),
        )

        # 恰好一方生效：signal 成功则超时扫描失败，反之亦然
        assert bool(signalled) != bool(failed)
        stored = await second.repo.get(task.task_id)
        if failed:
            assert stored.status == TaskStatus.FAILED
            assert stored.failure_reason == "waiting_timeout"
            assert future.is_completed is True
        else:
            assert stored.status == TaskStatus.PENDING
            assert stored.data[SIGNAL_DATA_KEY]["arrived"] is True
            # 超时终结被 CAS 拒绝后，不能再用超时错误完成 future
            assert future.is_completed is False
