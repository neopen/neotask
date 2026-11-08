"""
@FileName: test_submit_many.py
@Description: 批量提交（submit_many / save_batch / push_batch）测试
"""

import os

import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.common.exceptions import QueueFullError
from neotask.models.task import Task, TaskPriority
from neotask.queue.queue_scheduler import QueueScheduler
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
