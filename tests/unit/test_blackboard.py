"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_blackboard.py
@Description: Blackboard 本地上下文测试 - Memory 存储 TTL/深拷贝、Pool 与
             Scheduler API 透传、非 Memory 后端的显式报错
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio

import pytest

from neotask import TaskPool, TaskPoolConfig, UnsupportedFeatureError
from neotask.models.config import StorageType
from neotask.storage.memory import MemoryContextStore


class TestBlackboard:
    async def test_memory_set_get_delete(self):
        store = MemoryContextStore()

        assert store.set("k", {"a": 1}) is True
        assert store.get("k") == {"a": 1}
        assert store.delete("k") is True
        assert store.get("k") is None
        assert store.delete("k") is False

    async def test_memory_ttl_and_deepcopy(self):
        store = MemoryContextStore()
        value = {"nested": [1]}
        store.set("short", value, ttl=0.05)
        value["nested"].append(2)

        assert store.get("short") == {"nested": [1]}
        await asyncio.sleep(0.1)
        assert store.get("short") is None

    async def test_pool_context_api(self):
        async def work(data):
            return "done"

        pool = TaskPool(executor=work, config=TaskPoolConfig.memory())
        try:
            pool.start()
            pool.context_set("job", {"id": 1}, ttl=10)
            assert pool.context_get("job") == {"id": 1}
            assert await pool.context_get_async("job") == {"id": 1}
            assert pool.context_delete("job") is True
            assert pool.context_get("job") is None
        finally:
            pool.shutdown()

    async def test_non_memory_backend_raises(self, tmp_path):
        async def work(data):
            return "done"

        config = TaskPoolConfig.sqlite(str(tmp_path / "blackboard.db"))
        pool = TaskPool(executor=work, config=config)
        try:
            with pytest.raises(UnsupportedFeatureError) as excinfo:
                pool.context_set("k", 1)
            assert excinfo.value.feature == "blackboard"
            assert excinfo.value.backend == str(StorageType.SQLITE)

            with pytest.raises(UnsupportedFeatureError):
                pool.context_get("k")
            with pytest.raises(UnsupportedFeatureError):
                await pool.context_delete_async("k")
        finally:
            pool.shutdown()


# ========== TaskScheduler 透传 ==========


class TestSchedulerDelegation:
    async def test_scheduler_context_delegation(self):
        from neotask.api.task_scheduler import SchedulerConfig, TaskScheduler

        async def work(data):
            return "done"

        scheduler = TaskScheduler(executor=work, config=SchedulerConfig.memory())
        try:
            scheduler.start()
            scheduler.context_set("k", "v")
            assert scheduler.context_get("k") == "v"
            assert scheduler.context_delete("k") is True
        finally:
            scheduler.shutdown()
