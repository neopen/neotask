"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_checkpoint.py
@Description: Checkpoint 断点续跑测试 - 三后端仓储、双形态 API、恢复路径、
             事件与工厂装配
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio
import os
from uuid import uuid4

import pytest

from neotask import (
    CHECKPOINT_DATA_KEY,
    EventType,
    TaskPool,
    TaskPoolConfig,
    UnsupportedFeatureError,
    load_checkpoint,
    load_checkpoint_sync,
    save_checkpoint,
    save_checkpoint_sync,
)
from neotask.common.exceptions import NeoTaskError
from neotask.executor import ExecutorType
from neotask.models.config import StorageConfig
from neotask.models.task import Task, TaskPriority
from neotask.storage.factory import RepositoryFactory, StorageFactory
from neotask.storage.memory import (
    MemoryCheckpointRepository,
    MemoryTaskRepository,
)
from neotask.storage.redis import RedisCheckpointRepository
from neotask.storage.sqlite import SQLiteCheckpointRepository, SQLiteTaskRepository


def _redis_url() -> str:
    return os.environ.get("REDIS_TEST_URL", "redis://127.0.0.1:6379/2")


async def _redis_available() -> bool:
    try:
        import redis.asyncio as aioredis

        client = aioredis.from_url(_redis_url(), decode_responses=True, socket_connect_timeout=2)
        try:
            await client.ping()
        finally:
            await client.close()
        return True
    except Exception:
        return False


async def _clean_redis_prefix(prefix: str) -> None:
    import redis.asyncio as aioredis

    client = aioredis.from_url(_redis_url(), decode_responses=True)
    try:
        keys = [key async for key in client.scan_iter(f"neotask:*{prefix}*")]
        if keys:
            await client.delete(*keys)
    finally:
        await client.close()


def _make_task(task_id: str) -> Task:
    return Task(task_id=task_id, data={}, priority=TaskPriority.NORMAL)


# ========== 仓储层 ==========


class TestMemoryCheckpointRepository:
    @pytest.fixture
    def repo(self):
        return MemoryCheckpointRepository()

    async def test_versions_and_latest(self, repo):
        await repo.save("t1", 1, {"step": 1})
        await repo.save("t1", 2, {"step": 2})

        latest = await repo.load_latest("t1")
        assert latest.version == 2
        assert latest.state == {"step": 2}
        assert (await repo.load("t1", 1)).state == {"step": 1}
        assert await repo.load("t1", 99) is None

    async def test_upsert_same_version(self, repo):
        await repo.save("t1", 1, {"step": 1})
        await repo.save("t1", 1, {"step": 100})

        assert (await repo.load("t1", 1)).state == {"step": 100}

    async def test_prune_keeps_latest(self, repo):
        for version in range(1, 7):
            await repo.save("t1", version, {"step": version})

        removed = await repo.prune("t1", 2)

        assert removed == 4
        assert (await repo.load("t1", 1)) is None
        assert (await repo.load("t1", 6)).state == {"step": 6}

    async def test_prune_zero_deletes_all(self, repo):
        await repo.save("t1", 1, {"step": 1})
        assert await repo.prune("t1", 0) == 1
        assert await repo.load_latest("t1") is None

    async def test_delete(self, repo):
        await repo.save("t1", 1, {"step": 1})
        await repo.delete("t1")
        assert await repo.load_latest("t1") is None

    async def test_state_is_isolated_copy(self, repo):
        state = {"nested": {"value": 1}}
        await repo.save("t1", 1, state)
        state["nested"]["value"] = 99

        loaded = await repo.load_latest("t1")
        assert loaded.state == {"nested": {"value": 1}}


class TestSQLiteCheckpointRepository:
    @pytest.fixture
    async def db_path(self, tmp_path):
        return str(tmp_path / "checkpoint.db")

    @pytest.fixture
    async def repos(self, db_path):
        task_repo = SQLiteTaskRepository(db_path)
        checkpoint_repo = SQLiteCheckpointRepository(db_path)
        await task_repo.save(_make_task("t1"))
        try:
            yield task_repo, checkpoint_repo
        finally:
            await task_repo.close()
            await checkpoint_repo.close()

    async def test_save_updates_task_marker_atomically(self, repos):
        task_repo, checkpoint_repo = repos

        await checkpoint_repo.save("t1", 1, {"step": 1})
        await checkpoint_repo.save("t1", 2, {"step": 2})

        task = await task_repo.get("t1")
        assert task.checkpoint_version == 2
        assert (await checkpoint_repo.load_latest("t1")).state == {"step": 2}

    async def test_prune_keeps_latest(self, repos):
        _, checkpoint_repo = repos
        for version in range(1, 6):
            await checkpoint_repo.save("t1", version, {"step": version})

        assert await checkpoint_repo.prune("t1", 2) == 3
        assert await checkpoint_repo.load("t1", 3) is None
        assert (await checkpoint_repo.load("t1", 5)).state == {"step": 5}

    async def test_delete(self, repos):
        _, checkpoint_repo = repos
        await checkpoint_repo.save("t1", 1, {"step": 1})
        await checkpoint_repo.delete("t1")
        assert await checkpoint_repo.load_latest("t1") is None

    async def test_shared_connection_survives_task_repo_close(self, db_path):
        """默认路径各自持有共享连接引用：任务仓储先关闭不影响 checkpoint 读写"""
        task_repo = SQLiteTaskRepository(db_path)
        checkpoint_repo = SQLiteCheckpointRepository(db_path)
        try:
            await task_repo.save(_make_task("t1"))
            await checkpoint_repo.save("t1", 1, {"step": 1})

            await task_repo.close()

            assert (await checkpoint_repo.load_latest("t1")).state == {"step": 1}
            await checkpoint_repo.save("t1", 2, {"step": 2})
            assert (await checkpoint_repo.load_latest("t1")).version == 2
        finally:
            await checkpoint_repo.close()


class TestRedisCheckpointRepository:
    @pytest.fixture
    async def repo(self):
        if not await _redis_available():
            pytest.skip("Redis not available")

        prefix = f"ckpt-test-{uuid4().hex[:8]}"
        repo = RedisCheckpointRepository(_redis_url())
        try:
            yield repo, prefix
        finally:
            await repo.close()
            await _clean_redis_prefix(prefix)

    async def test_versions_prune_and_delete(self, repo):
        checkpoint_repo, prefix = repo
        task_id = f"{prefix}-t1"

        for version in range(1, 6):
            await checkpoint_repo.save(task_id, version, {"step": version})

        latest = await checkpoint_repo.load_latest(task_id)
        assert latest.version == 5
        assert latest.state == {"step": 5}
        assert (await checkpoint_repo.load(task_id, 2)).state == {"step": 2}

        assert await checkpoint_repo.prune(task_id, 2) == 3
        assert await checkpoint_repo.load(task_id, 2) is None
        assert await checkpoint_repo.load_latest(task_id) is not None

        await checkpoint_repo.delete(task_id)
        assert await checkpoint_repo.load_latest(task_id) is None

    async def test_save_updates_task_marker(self, repo):
        checkpoint_repo, prefix = repo
        task_id = f"{prefix}-t2"
        from neotask.storage.redis import RedisTaskRepository

        task_repo = RedisTaskRepository(_redis_url())
        try:
            await task_repo.save(_make_task(task_id))
            await checkpoint_repo.save(task_id, 3, {"step": 3})

            task = await task_repo.get(task_id)
            assert task.checkpoint_version == 3
            await task_repo.delete(task_id)
        finally:
            await task_repo.close()


class TestCheckpointFactory:
    def test_storage_factory_creates_all_backends(self, tmp_path):
        assert isinstance(
            StorageFactory.create_checkpoint_repo(StorageConfig.memory()),
            MemoryCheckpointRepository,
        )
        assert isinstance(
            StorageFactory.create_checkpoint_repo(StorageConfig.sqlite(str(tmp_path / "f.db"))),
            SQLiteCheckpointRepository,
        )
        assert isinstance(
            StorageFactory.create_checkpoint_repo(StorageConfig.redis(_redis_url())),
            RedisCheckpointRepository,
        )

    def test_repository_factory_unregistered_backend_raises(self):
        RepositoryFactory.register("custom-no-ckpt", MemoryTaskRepository, None)

        with pytest.raises(UnsupportedFeatureError) as excinfo:
            RepositoryFactory.create_checkpoint_repo("custom-no-ckpt")

        assert excinfo.value.feature == "checkpoint"
        assert excinfo.value.backend == "custom-no-ckpt"
        assert isinstance(excinfo.value, NeoTaskError)


# ========== contextvar 与显式 API ==========


class TestCheckpointContextvar:
    async def test_outside_worker_returns_false_and_none(self):
        assert await save_checkpoint({"x": 1}) is False
        assert await load_checkpoint() is None
        assert save_checkpoint_sync({"x": 1}) is False
        assert load_checkpoint_sync() is None

    async def test_async_executor_save_and_load(self):
        seen = {}

        async def work(data):
            seen["before"] = await load_checkpoint()
            seen["saved"] = await save_checkpoint({"step": 1})
            seen["after"] = await load_checkpoint()
            return {"ok": True}

        events = []
        pool = TaskPool(executor=work, config=TaskPoolConfig.memory())
        pool.event_bus.subscribe(EventType.CHECKPOINTED, lambda e: events.append(e.data))
        try:
            pool.start()
            task_id = pool.submit({})
            assert pool.wait_for_result(task_id, timeout=10) == {"ok": True}

            assert seen == {
                "before": None,
                "saved": True,
                "after": {"step": 1},
            }
            info = pool.get_task(task_id)
            assert info["checkpoint_version"] == 1
            assert {"version": 1} in events
        finally:
            pool.shutdown()

    async def test_thread_executor_sync_api(self):
        seen = {}

        def work(data):
            seen["before"] = load_checkpoint_sync()
            seen["saved"] = save_checkpoint_sync({"step": 5})
            return {"ok": True}

        config = TaskPoolConfig.memory()
        config.executor_type = ExecutorType.THREAD
        pool = TaskPool(executor=work, config=config)
        try:
            pool.start()
            task_id = pool.submit({})
            assert pool.wait_for_result(task_id, timeout=10) == {"ok": True}
            assert seen["before"] is None
            assert seen["saved"] is True
            assert pool.load_checkpoint(task_id) == {"step": 5}
        finally:
            pool.shutdown()

    async def test_explicit_task_id_api(self):
        async def work(data):
            return "done"

        pool = TaskPool(executor=work, config=TaskPoolConfig.memory())
        try:
            pool.start()
            task_id = pool.submit({})
            pool.wait_for_result(task_id, timeout=10)

            assert pool.save_checkpoint(task_id, {"step": 1}) == 1
            assert await pool.save_checkpoint_async(task_id, {"step": 2}) == 2
            assert pool.load_checkpoint(task_id) == {"step": 2}
            assert await pool.load_checkpoint_async(task_id) == {"step": 2}
            assert pool.save_checkpoint("missing-task", {"x": 1}) == 0
        finally:
            pool.shutdown()

    async def test_disabled_checkpoint_returns_zero(self):
        async def work(data):
            assert await save_checkpoint({"x": 1}) is False
            return "done"

        config = TaskPoolConfig.memory()
        config.enable_checkpoint = False
        pool = TaskPool(executor=work, config=config)
        try:
            pool.start()
            task_id = pool.submit({})
            assert pool.wait_for_result(task_id, timeout=10) == "done"
            assert pool.save_checkpoint(task_id, {"x": 1}) == 0
            assert pool.get_task(task_id)["checkpoint_version"] == 0
        finally:
            pool.shutdown()

    async def test_prune_keep_versions_config(self):
        async def work(data):
            for step in range(1, 6):
                await save_checkpoint({"step": step})
            return "done"

        config = TaskPoolConfig.memory()
        config.checkpoint_keep_versions = 2
        pool = TaskPool(executor=work, config=config)
        try:
            pool.start()
            task_id = pool.submit({})
            pool.wait_for_result(task_id, timeout=10)

            assert pool.load_checkpoint(task_id) == {"step": 5}
            repo = pool._checkpoint_repo
            assert await repo.load(task_id, 3) is None
            assert (await repo.load(task_id, 4)).state == {"step": 4}
        finally:
            pool.shutdown()


# ========== 恢复路径 ==========


def _flaky_executor(calls):
    async def work(data):
        previous = data.get(CHECKPOINT_DATA_KEY) or {}
        step = previous.get("step", 0)
        calls.append(step)
        await save_checkpoint({"step": step + 1})
        if step == 0:
            raise RuntimeError("boom")
        return {"ok": step}

    return work


class TestCheckpointResume:
    async def test_auto_retry_resumes(self):
        calls = []
        config = TaskPoolConfig.memory()
        config.max_retries = 1
        config.retry_delay = 0.05
        pool = TaskPool(executor=_flaky_executor(calls), config=config)
        try:
            pool.start()
            task_id = pool.submit({})
            assert pool.wait_for_result(task_id, timeout=10) == {"ok": 1}
            assert calls == [0, 1]
            assert pool.get_task(task_id)["checkpoint_version"] == 2
        finally:
            pool.shutdown()

    async def test_manual_retry_resumes(self):
        calls = []
        config = TaskPoolConfig.memory()
        config.max_retries = 0
        pool = TaskPool(executor=_flaky_executor(calls), config=config)
        try:
            pool.start()
            task_id = pool.submit({})
            with pytest.raises(Exception, match="boom"):
                pool.wait_for_result(task_id, timeout=10)

            assert pool.retry(task_id) is True
            assert pool.wait_for_result(task_id, timeout=10) == {"ok": 1}
            assert calls == [0, 1]
        finally:
            pool.shutdown()

    async def test_submit_resume_from_checkpoint(self):
        seen = []

        async def work(data):
            seen.append(data.get(CHECKPOINT_DATA_KEY))
            return "done"

        pool = TaskPool(executor=work, config=TaskPoolConfig.memory())
        try:
            pool.start()
            task_id = "external-resume-job"
            pool.wait_for_result(pool.submit({}, task_id=task_id), timeout=10)
            assert pool.save_checkpoint(task_id, {"pos": 7}) == 1

            # 仅移除任务记录，保留 checkpoint（模拟旧记录已清理、自带状态重入）
            await pool._task_repo.delete(task_id)
            assert pool.task_exists(task_id) is False
            assert pool.load_checkpoint(task_id) == {"pos": 7}

            pool.wait_for_result(
                pool.submit(
                    {"fresh": True},
                    task_id=task_id,
                    resume_from_checkpoint=True,
                ),
                timeout=10,
            )

            # 复用的 task_id 可能命中上一次执行留下的结果等待器，这里以任务
            # 状态与执行器实际入参为准，避免依赖 wait_for_result 的缓存行为。
            for _ in range(200):
                if len(seen) == 2 and pool.get_status(task_id) == "success":
                    break
                await asyncio.sleep(0.02)

            assert seen == [None, {"pos": 7}]
            assert pool.get_task(task_id)["checkpoint_version"] == 1
        finally:
            pool.shutdown()

    async def test_create_pending_task_resume(self):
        seen = []

        async def work(data):
            seen.append(data.get(CHECKPOINT_DATA_KEY))
            return "done"

        pool = TaskPool(executor=work, config=TaskPoolConfig.memory())
        try:
            pool.start()
            task_id = "pending-resume-job"
            first = pool.create_pending_task({"a": 1}, task_id=task_id)
            pool.save_checkpoint(first, {"pos": 3})
            await pool._task_repo.delete(first)

            resumed = pool.create_pending_task(
                {"a": 2}, task_id=task_id, resume_from_checkpoint=True
            )
            assert resumed == task_id
            assert pool.enqueue_task(resumed) is True
            assert pool.wait_for_result(resumed, timeout=10) == "done"
            assert seen == [{"pos": 3}]
        finally:
            pool.shutdown()

    async def test_resume_across_restart_sqlite(self, tmp_path):
        """SQLite 持久化：首个进程失败退出后，新进程重试能读到上次 checkpoint"""
        db_path = str(tmp_path / "resume.db")
        calls = []

        config = TaskPoolConfig.sqlite(db_path)
        config.max_retries = 0
        first_pool = TaskPool(executor=_flaky_executor(calls), config=config)
        try:
            first_pool.start()
            task_id = first_pool.submit({}, task_id="restart-job")
            with pytest.raises(Exception, match="boom"):
                first_pool.wait_for_result(task_id, timeout=10)
            assert first_pool.load_checkpoint(task_id) == {"step": 1}
        finally:
            first_pool.shutdown()

        second_pool = TaskPool(executor=_flaky_executor(calls), config=config)
        try:
            second_pool.start()
            assert second_pool.retry("restart-job") is True
            assert second_pool.wait_for_result("restart-job", timeout=10) == {"ok": 1}
            assert calls == [0, 1]
        finally:
            second_pool.shutdown()


# ========== TaskScheduler 透传 ==========


class TestSchedulerDelegation:
    async def test_scheduler_checkpoint_delegation(self):
        from neotask.api.task_scheduler import SchedulerConfig, TaskScheduler

        async def work(data):
            await save_checkpoint({"step": 1})
            return "done"

        scheduler = TaskScheduler(executor=work, config=SchedulerConfig.memory())
        try:
            scheduler.start()
            task_id = scheduler.submit({})
            assert scheduler.wait_for_result(task_id, timeout=10) == "done"
            assert scheduler.load_checkpoint(task_id) == {"step": 1}
            assert scheduler.save_checkpoint(task_id, {"step": 2}) == 2
        finally:
            scheduler.shutdown()
