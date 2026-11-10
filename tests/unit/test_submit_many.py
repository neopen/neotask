"""
@FileName: test_submit_many.py
@Description: 批量提交（submit_many / save_batch / push_batch）测试
"""

import os

import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.common.exceptions import BatchSubmitError, QueueFullError
from neotask.models.task import BatchSubmitResult, Task, TaskPriority, TaskStatus
from neotask.queue.queue_scheduler import QueueScheduler
from neotask.storage import memory as memory_storage
from neotask.storage.memory import MemoryQueueRepository, MemoryTaskRepository
from neotask.storage.sqlite import SQLiteQueueRepository, SQLiteTaskRepository


async def echo_executor(data):
    """回显执行器"""
    return data


class TestRepositoryBatch:
    """仓储层批量接口"""

    @pytest.mark.asyncio
    async def test_memory_save_batch_skips_duplicates(self):
        repo = MemoryTaskRepository()
        tasks = [Task(task_id=f"m-{i}", data={"i": i}) for i in range(3)]
        assert await repo.save_batch(tasks) == ["m-0", "m-1", "m-2"]

        # 重复提交同一批任务：应全部跳过，不覆盖已有记录
        assert await repo.save_batch(tasks) == []
        assert await repo.get("m-0") is not None

    @pytest.mark.asyncio
    async def test_memory_push_batch(self):
        repo = MemoryQueueRepository()
        items = [(f"q-{i}", TaskPriority.NORMAL.value) for i in range(3)]
        assert await repo.push_batch(items) == 3
        assert await repo.size() == 3

    @pytest.mark.asyncio
    async def test_sqlite_save_batch_skips_duplicates(self, tmp_path):
        repo = SQLiteTaskRepository(str(tmp_path / "batch.db"))
        tasks = [Task(task_id=f"s-{i}", data={"i": i}) for i in range(3)]
        assert await repo.save_batch(tasks) == ["s-0", "s-1", "s-2"]

        # 已存在任务不覆盖
        assert await repo.save_batch(tasks) == []
        stored = await repo.get("s-0")
        assert stored is not None and stored.data == {"i": 0}
        await repo.close()

    @pytest.mark.asyncio
    async def test_sqlite_save_batch_rolls_back_on_write_failure(self, tmp_path):
        """单事务：中途写入失败时整批回滚，不留下已写入的半批任务"""
        repo = SQLiteTaskRepository(str(tmp_path / "batch_rollback.db"))
        await repo._ensure_init()
        await repo._conn.execute("""
            CREATE TRIGGER fail_on_bad BEFORE INSERT ON tasks
            WHEN NEW.task_id = 'bad'
            BEGIN
                SELECT RAISE(ABORT, 'forced failure');
            END
        """)
        await repo._conn.commit()

        tasks = [
            Task(task_id="ok-1", data={"i": 1}),
            Task(task_id="ok-2", data={"i": 2}),
            Task(task_id="bad", data={"i": 3}),
        ]
        with pytest.raises(Exception):
            await repo.save_batch(tasks)

        assert await repo.get("ok-1") is None
        assert await repo.get("ok-2") is None
        assert await repo.get("bad") is None
        await repo.close()

    @pytest.mark.asyncio
    async def test_memory_save_if_absent(self):
        repo = MemoryTaskRepository()
        assert await repo.save_if_absent(Task(task_id="m-absent", data={"i": 1})) is True
        # 重复创建：返回 False 且不覆盖已有数据
        assert await repo.save_if_absent(Task(task_id="m-absent", data={"i": 2})) is False
        stored = await repo.get("m-absent")
        assert stored is not None and stored.data == {"i": 1}

    @pytest.mark.asyncio
    async def test_sqlite_save_if_absent(self, tmp_path):
        repo = SQLiteTaskRepository(str(tmp_path / "absent.db"))
        assert await repo.save_if_absent(Task(task_id="s-absent", data={"i": 1})) is True
        assert await repo.save_if_absent(Task(task_id="s-absent", data={"i": 2})) is False
        stored = await repo.get("s-absent")
        assert stored is not None and stored.data == {"i": 1}
        await repo.close()

    @pytest.mark.asyncio
    async def test_redis_save_if_absent(self):
        pytest.importorskip("redis")
        import redis as redis_lib

        from neotask.storage.redis import RedisTaskRepository

        redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/14")
        try:
            redis_lib.Redis.from_url(redis_url, decode_responses=True).ping()
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"redis unavailable: {exc}")

        repo = RedisTaskRepository(redis_url)
        try:
            assert await repo.save_if_absent(Task(task_id="r-absent", data={"i": 1})) is True
            assert await repo.save_if_absent(Task(task_id="r-absent", data={"i": 2})) is False
            stored = await repo.get("r-absent")
            assert stored is not None and stored.data == {"i": 1}
        finally:
            await repo.delete("r-absent")

    @pytest.mark.asyncio
    async def test_redis_submit_fast_path(self):
        """Redis 单条 Lua：查重 + 写任务 + 入队一次完成"""
        pytest.importorskip("redis")
        import redis as redis_lib

        from neotask.common.exceptions import TaskAlreadyExistsError
        from neotask.storage.redis import RedisQueueRepository, RedisTaskRepository

        redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/15")
        try:
            client = redis_lib.Redis.from_url(redis_url, decode_responses=True)
            client.ping()
            client.flushdb()
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"redis unavailable: {exc}")

        task_repo = RedisTaskRepository(redis_url)
        queue_repo = RedisQueueRepository(redis_url)

        task = Task(task_id="r-fast", data={"i": 1}, priority=TaskPriority.HIGH)
        assert await task_repo.save_if_absent_and_enqueue(task, 100) is True
        assert await queue_repo.pop(10) == ["r-fast"]

        # 重复 task_id：不覆盖、不入队
        clone = Task(task_id="r-fast", data={"i": 2})
        with pytest.raises(TaskAlreadyExistsError):
            await task_repo.save_if_absent_and_enqueue(clone, 100)
        stored = await task_repo.get("r-fast")
        assert stored is not None and stored.data == {"i": 1}

        # 队列已满：任务仍写入存储，但返回 False（由调用方标记失败）
        full = Task(task_id="r-full", data={"i": 3})
        assert await task_repo.save_if_absent_and_enqueue(full, 0) is False
        assert await queue_repo.pop(10) == []
        await task_repo.delete("r-fast")
        await task_repo.delete("r-full")

    @pytest.mark.asyncio
    async def test_sqlite_push_batch(self, tmp_path):
        repo = SQLiteQueueRepository(str(tmp_path / "batch.db"))
        items = [(f"sq-{i}", TaskPriority.HIGH.value) for i in range(3)]
        assert await repo.push_batch(items) == 3
        assert await repo.size() == 3
        await repo.close()

    @pytest.mark.asyncio
    async def test_queue_scheduler_push_batch_respects_capacity(self):
        scheduler = QueueScheduler(queue_repo=None, max_size=2)
        results = await scheduler.push_batch([
            ("c-0", TaskPriority.NORMAL.value, 0),
            ("c-1", TaskPriority.NORMAL.value, 0),
            ("c-2", TaskPriority.NORMAL.value, 0),
        ])
        assert results == [True, True, False]
        assert await scheduler.size() == 2


class TestSubmitMany:
    """TaskPool.submit_many"""

    def test_submit_many_memory(self):
        pool = TaskPool(executor=echo_executor, config=TaskPoolConfig.memory())
        pool.start()
        try:
            tasks = [{"i": i} for i in range(50)]
            task_ids = pool.submit_many(tasks)
            assert len(task_ids) == 50
            for index, task_id in enumerate(task_ids):
                assert pool.wait_for_result(task_id, timeout=30) == {"i": index}
        finally:
            pool.shutdown()

    def test_submit_many_sqlite(self, tmp_path):
        config = TaskPoolConfig.sqlite(str(tmp_path / "submit_many.db"))
        pool = TaskPool(executor=echo_executor, config=config)
        pool.start()
        try:
            tasks = [{"i": i} for i in range(50)]
            task_ids = pool.submit_many(tasks, priority=TaskPriority.HIGH)
            assert len(task_ids) == 50
            for index, task_id in enumerate(task_ids):
                assert pool.wait_for_result(task_id, timeout=30) == {"i": index}
        finally:
            pool.shutdown()

    def test_submit_many_empty(self):
        pool = TaskPool(executor=echo_executor, config=TaskPoolConfig.memory())
        pool.start()
        try:
            assert pool.submit_many([]) == []
        finally:
            pool.shutdown()

    def test_submit_many_queue_full(self):
        config = TaskPoolConfig.memory()
        config.queue_max_size = 2
        config.worker_concurrency = 1
        config.enable_prefetch = False
        pool = TaskPool(executor=echo_executor, config=config)
        pool.start()
        pool.pause()
        try:
            with pytest.raises(QueueFullError):
                pool.submit_many([{"i": i} for i in range(3)])
        finally:
            pool.shutdown()

    def test_submit_many_redis(self):
        pytest.importorskip("redis")
        redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/14")
        config = TaskPoolConfig.redis(redis_url)
        try:
            pool = TaskPool(executor=echo_executor, config=config)
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"redis unavailable: {exc}")
        pool.start()
        try:
            tasks = [{"i": i} for i in range(50)]
            task_ids = pool.submit_many(tasks)
            assert len(task_ids) == 50
            for index, task_id in enumerate(task_ids):
                assert pool.wait_for_result(task_id, timeout=30) == {"i": index}
        finally:
            pool.shutdown()


class TestSubmitBatchAtomic:
    """submit_batch 默认全有或全无语义"""

    def test_empty_batch(self):
        pool = TaskPool(executor=echo_executor, config=TaskPoolConfig.memory())
        pool.start()
        pool.pause()
        try:
            assert pool.submit_batch([]) == []
            partial = pool.submit_batch([], partial_on_error=True)
            assert isinstance(partial, BatchSubmitResult)
            assert len(partial) == 0 and partial.failed == []
        finally:
            pool.shutdown(graceful=False)

    def test_sqlite_serialization_failure_leaves_nothing(self, tmp_path):
        config = TaskPoolConfig.sqlite(str(tmp_path / "batch_atomic.db"))
        pool = TaskPool(executor=echo_executor, config=config)
        pool.start()
        pool.pause()
        try:
            with pytest.raises(BatchSubmitError):
                pool.submit_batch([{"i": 1}, {"bad": object()}])

            pending = pool._run_coroutine(
                pool._task_repo.list_by_status(TaskStatus.PENDING, limit=10)
            )
            assert pending == []
            assert pool._run_coroutine(pool._queue_scheduler.size()) == 0
        finally:
            pool.shutdown(graceful=False)

    def test_storage_failure_raises_without_partial_batch(self, monkeypatch):
        pool = TaskPool(executor=echo_executor, config=TaskPoolConfig.memory())
        pool.start()
        pool.pause()

        async def fail_persist(tasks):
            raise RuntimeError("disk unavailable")

        monkeypatch.setattr(pool._lifecycle, "persist_tasks", fail_persist)
        try:
            with pytest.raises(BatchSubmitError):
                pool.submit_batch([{"i": 1}, {"i": 2}])
            assert pool._run_coroutine(pool._queue_scheduler.size()) == 0
        finally:
            pool.shutdown(graceful=False)

    @pytest.mark.parametrize("storage", ["memory", "sqlite"])
    def test_large_batch(self, storage, tmp_path):
        if storage == "sqlite":
            config = TaskPoolConfig.sqlite(str(tmp_path / "batch_large.db"))
        else:
            config = TaskPoolConfig.memory()
        pool = TaskPool(executor=echo_executor, config=config)
        pool.start()
        pool.pause()
        try:
            task_ids = pool.submit_batch([{"i": i} for i in range(1200)])
            assert len(task_ids) == 1200
            assert pool._run_coroutine(pool._queue_scheduler.size()) == 1200
        finally:
            pool.shutdown(graceful=False)

    def test_partial_on_error_returns_succeeded_and_failed(self):
        config = TaskPoolConfig.memory()
        config.queue_max_size = 2
        pool = TaskPool(executor=echo_executor, config=config)
        pool.start()
        pool.pause()
        try:
            result = pool.submit_batch(
                [{"i": 0}, {"i": 1}, {"i": 2}], partial_on_error=True
            )
            assert isinstance(result, BatchSubmitResult)
            assert len(result.succeeded) == 2
            assert len(result) == 2
            assert [item["index"] for item in result.failed] == [2]
            assert pool._run_coroutine(pool._queue_scheduler.size()) == 2
        finally:
            pool.shutdown(graceful=False)

    def test_atomic_batch_rolls_back_on_capacity_rejection(self):
        config = TaskPoolConfig.memory()
        config.queue_max_size = 1
        pool = TaskPool(executor=echo_executor, config=config)
        pool.start()
        pool.pause()
        try:
            with pytest.raises(BatchSubmitError):
                pool.submit_batch([{"i": 0}, {"i": 1}])
            assert pool._run_coroutine(pool._queue_scheduler.size()) == 0
            assert pool._run_coroutine(
                pool._task_repo.list_by_status(TaskStatus.PENDING, limit=10)
            ) == []
        finally:
            pool.shutdown(graceful=False)


class TestMemoryBatchAtomic:
    """Memory 仓储批量写入失败不留下半批数据"""

    @pytest.mark.asyncio
    async def test_save_batch_deepcopy_failure_leaves_nothing(self, monkeypatch):
        repo = MemoryTaskRepository()
        tasks = [Task(task_id=f"m-a-{i}", data={"i": i}) for i in range(3)]

        original = memory_storage.copy.deepcopy
        calls = {"count": 0}

        def flaky_deepcopy(obj):
            calls["count"] += 1
            if calls["count"] == 2:
                raise RuntimeError("deepcopy failed")
            return original(obj)

        class CopyShim:
            deepcopy = staticmethod(flaky_deepcopy)

        monkeypatch.setattr(memory_storage, "copy", CopyShim)
        with pytest.raises(RuntimeError):
            await repo.save_batch(tasks)

        for task in tasks:
            assert await repo.get(task.task_id) is None


class TestTaskPoolCounters:
    """TaskPool 内部计数器与未入队记录跟踪"""

    def test_counters_track_success_and_failure(self):
        async def flaky_executor(data):
            if data.get("fail"):
                raise ValueError("boom")
            return data

        config = TaskPoolConfig.memory()
        config.max_retries = 0
        pool = TaskPool(executor=flaky_executor, config=config)
        pool.start()
        try:
            ok_id = pool.submit({"i": 1})
            bad_id = pool.submit({"fail": True})
            pool.wait_all([ok_id, bad_id], timeout=30)

            stats = pool.get_stats()
            assert stats["pending_count"] == 0
            assert stats["running_count"] == 0
            assert stats["failed_count"] == 1
        finally:
            pool.shutdown()

    def test_unenqueued_pending_is_cleared_after_enqueue(self):
        pool = TaskPool(executor=echo_executor, config=TaskPoolConfig.memory())
        pool.start()
        pool.pause()
        try:
            task_id = pool.create_pending_task({"i": 1})
            assert pool._unenqueued_count() == 1
            assert pool.enqueue_task(task_id) is True
            assert pool._unenqueued_count() == 0
        finally:
            pool.shutdown(graceful=False)
