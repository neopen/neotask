"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: test_signal.py
@Description: v1.3 人工介入测试 - WAITING 状态、信号唤醒、等待超时、
             三后端持久化与事件顺序
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/25
"""

import asyncio
import json
import os
import time

import pytest

from neotask import (
    SIGNAL_DATA_KEY,
    EventType,
    TaskPool,
    TaskPoolConfig,
    TaskStatus,
    wait_for_signal,
)
from neotask.core.engine import EngineConfig, TaskEngine
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.event.bus import EventBus


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


async def _clean_redis_keys(*needles: str) -> None:
    import redis.asyncio as aioredis

    client = aioredis.from_url(_redis_url(), decode_responses=True)
    try:
        for needle in needles:
            keys = [key async for key in client.scan_iter(f"neotask:*{needle}*")]
            if keys:
                await client.delete(*keys)
    finally:
        await client.close()


def _wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


async def _await_until(awaitable_predicate, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if await awaitable_predicate():
            return True
        await asyncio.sleep(0.01)
    return False


def _status_of(pool: TaskPool, task_id: str):
    info = pool.get_task(task_id)
    return info["status"] if info else None


def _task_data(info) -> dict:
    data = info["data"]
    return json.loads(data) if isinstance(data, str) else data


async def _approval_handler(data):
    payload = await wait_for_signal("approval")
    return {"approved": payload}


# ========== 状态枚举 ==========


class TestTaskStatusEnum:
    def test_member_names_and_values(self):
        assert TaskStatus.__members__
        for name, member in TaskStatus.__members__.items():
            assert name == name.upper()
            assert member.value == member.value.lower()
        assert TaskStatus.WAITING.value == "waiting"

    def test_str_value_and_json_agree(self):
        status = TaskStatus.WAITING
        assert str(status) == "waiting"
        assert f"{status}" == "waiting"
        assert "{}".format(status) == "waiting"  # noqa: UP032 - 契约要求 format 输出一致
        assert json.dumps({"status": status}) == '{"status": "waiting"}'

    def test_from_value_accepts_all_forms(self):
        assert TaskStatus.from_value("waiting") is TaskStatus.WAITING
        assert TaskStatus.from_value("WAITING") is TaskStatus.WAITING
        assert TaskStatus.from_value(TaskStatus.WAITING) is TaskStatus.WAITING
        assert TaskStatus.from_value("  Waiting  ") is TaskStatus.WAITING
        assert TaskStatus.from_value(None) is TaskStatus.PENDING
        assert TaskStatus.from_value("unknown") is TaskStatus.PENDING
        for member in TaskStatus:
            assert TaskStatus.from_value(member.value) is member

    def test_waiting_is_not_terminal(self):
        assert not TaskStatus.WAITING.is_terminal()
        assert not TaskStatus.PENDING.is_terminal()
        assert TaskStatus.SUCCESS.is_terminal()
        assert TaskStatus.FAILED.is_terminal()
        assert TaskStatus.CANCELLED.is_terminal()


# ========== 信号 API 与上下文 ==========


class TestSignalContext:
    async def test_wait_outside_handler_raises(self):
        with pytest.raises(RuntimeError):
            await wait_for_signal("approval")

    async def test_signal_data_key_reserved(self):
        assert SIGNAL_DATA_KEY == "__signal__"


# ========== Memory 后端端到端 ==========


class TestSignalMemory:
    async def test_wait_then_signal_delivers_payload_and_events(self):
        events = []
        pool = TaskPool(executor=_approval_handler, config=TaskPoolConfig.memory())
        pool.event_bus.subscribe_global(
            lambda event: events.append((event.event_type, event.task_id, event.data))
        )
        try:
            pool.start()
            task_id = pool.submit({})
            assert _wait_until(lambda: _status_of(pool, task_id) == "waiting")

            info = pool.get_task(task_id)
            entry = _task_data(info)[SIGNAL_DATA_KEY]
            assert entry["signal"] == "approval"
            assert entry["arrived"] is False
            assert entry["timeout_at"] is None

            stats = pool.get_stats()
            assert stats["waiting"] == 1
            assert stats["waiting_count"] == 1
            assert pool.get_metrics()["waiting"] == 1

            assert pool.signal(task_id, {"by": "alice"}) is True
            assert pool.wait_for_result(task_id, timeout=10) == {
                "approved": {"by": "alice"}
            }
            assert pool.signal(task_id, "late") is False

            info = pool.get_task(task_id)
            entry = _task_data(info)[SIGNAL_DATA_KEY]
            assert entry["consumed"] is True
            assert entry["arrived"] is False
            assert entry["payload"] is None
            assert entry["sequence"] == 1

            types = [etype for etype, tid, _ in events if tid == task_id]
            waiting_index = types.index(EventType.WAITING)
            signal_index = types.index(EventType.SIGNAL_RECEIVED)
            started_after = types.index(EventType.STARTED, signal_index)
            completed_index = types.index(EventType.COMPLETED)
            assert waiting_index < signal_index < started_after < completed_index

            signal_event = next(
                data for etype, tid, data in events
                if tid == task_id and etype == EventType.SIGNAL_RECEIVED
            )
            assert signal_event == {"signal": "approval", "payload": {"by": "alice"}}

            final_stats = pool.get_stats()
            assert final_stats["waiting"] == 0
            assert final_stats["waiting_count"] == 0
            assert final_stats["completed"] == 1
        finally:
            pool.shutdown()

    async def test_signal_non_waiting_task_returns_false(self):
        async def quick(data):
            return "done"

        pool = TaskPool(executor=quick, config=TaskPoolConfig.memory())
        try:
            pool.start()
            task_id = pool.submit({})
            assert pool.wait_for_result(task_id, timeout=10) == "done"
            assert pool.signal(task_id, "payload") is False
        finally:
            pool.shutdown()

    async def test_signal_requeue_failure_reverts_counters(self):
        """signal 后入队失败：状态、计数与指标都退回 waiting"""
        pool = TaskPool(executor=_approval_handler, config=TaskPoolConfig.memory())
        events = []
        pool.event_bus.subscribe_global(
            lambda event: events.append((event.event_type, event.task_id))
        )
        try:
            pool.start()
            task_id = pool.submit({})
            assert _wait_until(lambda: _status_of(pool, task_id) == "waiting")
            assert _wait_until(lambda: pool.get_stats()["waiting_count"] == 1)

            async def deny_push(*args, **kwargs):
                return False

            original_push = pool._queue_scheduler.push
            pool._queue_scheduler.push = deny_push
            try:
                assert pool.signal(task_id, "go") is False
            finally:
                pool._queue_scheduler.push = original_push

            assert _status_of(pool, task_id) == "waiting"
            stats = pool.get_stats()
            assert stats["waiting_count"] == 1
            assert stats["pending_count"] == 0
            assert _wait_until(
                lambda: pool._metrics.get_summary()["waiting"] == 1
            )
            assert pool._metrics.get_summary()["pending"] == 0

            types = [etype for etype, tid in events if tid == task_id]
            signal_index = types.index(EventType.SIGNAL_RECEIVED)
            assert types.index(EventType.SIGNAL_REVERTED) > signal_index

            # 回滚后任务仍可被唤醒
            assert pool.signal(task_id, "later") is True
            assert pool.wait_for_result(task_id, timeout=10) == {"approved": "later"}
        finally:
            pool.shutdown()

    async def test_waiting_releases_worker_slot(self):
        async def work(data):
            if data.get("wait"):
                payload = await wait_for_signal("release")
                return {"released": payload}
            return {"quick": True}

        config = TaskPoolConfig.memory()
        config.worker_concurrency = 1
        pool = TaskPool(executor=work, config=config)
        try:
            pool.start()
            waiting_id = pool.submit({"wait": True})
            assert _wait_until(lambda: _status_of(pool, waiting_id) == "waiting")

            quick_id = pool.submit({"wait": False})
            assert pool.wait_for_result(quick_id, timeout=5) == {"quick": True}

            assert pool.signal(waiting_id, "go") is True
            assert pool.wait_for_result(waiting_id, timeout=10) == {"released": "go"}
        finally:
            pool.shutdown()

    async def test_cancel_waiting_then_signal_is_invalid(self):
        pool = TaskPool(executor=_approval_handler, config=TaskPoolConfig.memory())
        try:
            pool.start()
            task_id = pool.submit({})
            assert _wait_until(lambda: _status_of(pool, task_id) == "waiting")

            assert pool.cancel(task_id) is True
            assert _status_of(pool, task_id) == "cancelled"
            assert pool.signal(task_id, "too late") is False
            assert pool.get_stats()["waiting"] == 0
        finally:
            pool.shutdown()

    async def test_waiting_timeout_marks_failed(self):
        config = TaskPoolConfig.memory()
        config.waiting_timeout = 0.2
        config.waiting_scan_interval = 0  # 关闭后台扫描，手动触发
        pool = TaskPool(executor=_approval_handler, config=config)
        try:
            pool.start()
            task_id = pool.submit({})
            assert _wait_until(lambda: _status_of(pool, task_id) == "waiting")

            time.sleep(0.3)
            assert pool.cleanup_expired_waiting() == 1

            info = pool.get_task(task_id)
            assert info["status"] == "failed"
            assert info["failure_reason"] == "waiting_timeout"
            assert pool.signal(task_id, "late") is False
            assert pool.get_stats()["waiting"] == 0
        finally:
            pool.shutdown()

    async def test_explicit_signal_timeout_overrides_config(self):
        async def short_wait(data):
            return await wait_for_signal("approval", timeout=0.1)

        pool = TaskPool(
            executor=short_wait,
            config=TaskPoolConfig.memory(),
        )
        try:
            pool.start()
            task_id = pool.submit({})
            assert _wait_until(lambda: _status_of(pool, task_id) == "waiting")
            entry = _task_data(pool.get_task(task_id))[SIGNAL_DATA_KEY]
            assert entry["timeout_at"] is not None

            time.sleep(0.2)
            assert pool.cleanup_expired_waiting() == 1
            assert pool.get_task(task_id)["failure_reason"] == "waiting_timeout"
        finally:
            pool.shutdown()


# ========== 状态机 ==========


class TestSignalStateMachine:
    async def test_waiting_cannot_complete_directly(self, memory_task_repo):
        bus = EventBus()
        await bus.start()
        manager = TaskLifecycleManager(memory_task_repo, bus, cache_enabled=True)
        try:
            task = await manager.create_task(data={})
            assert await manager.start_task(task.task_id, "node-1")
            assert await manager.suspend_for_signal(task.task_id, "approval")

            assert await manager.complete_task(task.task_id, "done") is False
            current = await manager.get_task(task.task_id)
            assert current.status == TaskStatus.WAITING

            # signal 后重新变为 PENDING，可由 Worker 再次执行
            assert await manager.signal_task(task.task_id, "payload") is True
            current = await manager.get_task(task.task_id)
            assert current.status == TaskStatus.PENDING
            assert await manager.signal_task(task.task_id, "again") is False
        finally:
            await bus.stop()

    async def test_duplicate_signal_at_storage_level(self, memory_task_repo):
        bus = EventBus()
        await bus.start()
        manager = TaskLifecycleManager(memory_task_repo, bus, cache_enabled=True)
        try:
            task = await manager.create_task(data={})
            await manager.start_task(task.task_id, "node-1")

            # 非 waiting 状态不可直接 signal
            assert await manager.signal_task(task.task_id, "payload") is False

            await manager.suspend_for_signal(task.task_id, "approval")
            assert await manager.signal_task(task.task_id, "payload") is True
            assert await manager.signal_task(task.task_id, "payload") is False
        finally:
            await bus.stop()


# ========== SQLite 重启恢复 ==========


class TestSignalSQLite:
    async def test_waiting_survives_restart(self, tmp_path):
        db_path = str(tmp_path / "signal.db")
        config = TaskPoolConfig.sqlite(db_path)

        pool = TaskPool(executor=_approval_handler, config=config)
        try:
            pool.start()
            task_id = pool.submit({"job": "persist"})
            assert _wait_until(lambda: _status_of(pool, task_id) == "waiting")
        finally:
            pool.shutdown()

        restarted = TaskPool(executor=_approval_handler, config=config)
        try:
            restarted.start()
            assert _status_of(restarted, task_id) == "waiting"
            assert restarted.get_stats()["waiting"] == 1

            assert restarted.signal(task_id, "resumed") is True
            assert restarted.wait_for_result(task_id, timeout=10) == {
                "approved": "resumed"
            }
        finally:
            restarted.shutdown()


# ========== Redis 重启恢复 ==========


@pytest.mark.skipif(
    not os.environ.get("REDIS_TEST_URL") and not os.environ.get("NEOTASK_RUN_REDIS"),
    reason="set REDIS_TEST_URL or NEOTASK_RUN_REDIS to run Redis tests",
)
class TestSignalRedis:
    async def test_waiting_survives_restart(self):
        if not await _redis_available():
            pytest.skip("Redis not available")

        node_id = "signal-test-node"
        config = TaskPoolConfig.redis(_redis_url(), node_id=node_id)
        task_id = None
        try:
            pool = TaskPool(executor=_approval_handler, config=config)
            try:
                pool.start()
                task_id = pool.submit({"job": "redis"})
                assert _wait_until(lambda: _status_of(pool, task_id) == "waiting")
            finally:
                pool.shutdown()

            restarted = TaskPool(executor=_approval_handler, config=config)
            try:
                restarted.start()
                assert _status_of(restarted, task_id) == "waiting"
                assert restarted.signal(task_id, "resumed") is True
                assert restarted.wait_for_result(task_id, timeout=10) == {
                    "approved": "resumed"
                }
            finally:
                restarted.shutdown()
        finally:
            needles = [node_id]
            if task_id:
                needles.append(task_id)
            await _clean_redis_keys(*needles)


# ========== TaskEngine 入口 ==========


class TestSignalEngine:
    async def test_engine_signal_and_waiting_stats(self):
        engine = TaskEngine(
            EngineConfig(executor_func=_approval_handler, waiting_scan_interval=0)
        )
        await engine.start()
        try:
            task_id = await engine.submit({"job": "engine"})

            async def _waiting() -> bool:
                return await engine.get_status(task_id) == "waiting"

            assert await _await_until(_waiting)

            stats = await engine.get_stats()
            assert stats["waiting"] == 1

            assert await engine.signal(task_id, {"by": "engine"}) is True
            assert await engine.wait(task_id, timeout=10) == {
                "approved": {"by": "engine"}
            }
            assert await engine.signal(task_id, "late") is False
        finally:
            await engine.stop()
