"""
@FileName: test_periodic_persistence.py
@Description: 周期任务持久化测试（存储层往返 + 序列化 + 跨实例恢复）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/3
"""

import pytest
from unittest.mock import Mock, AsyncMock

from neotask.scheduler.periodic import PeriodicTaskManager
from neotask.models.schedule import MissedExecutionPolicy
from neotask.storage.periodic import (
    MemoryPeriodicStore,
    SQLitePeriodicStore,
    create_periodic_store,
)


def _mock_pool():
    pool = Mock()
    pool.submit_async = AsyncMock(return_value="task_instance_123")
    pool.wait_for_result_async = AsyncMock(return_value={"result": "ok"})
    return pool


class TestPeriodicStore:
    """周期任务存储层测试"""

    async def test_memory_store_task_roundtrip(self):
        store = MemoryPeriodicStore()
        await store.save_task("T1", {"task_id": "T1", "run_count": 3})
        loaded = await store.load_task("T1")
        assert loaded["run_count"] == 3

        all_tasks = await store.load_all_tasks()
        assert len(all_tasks) == 1

        await store.delete_task("T1")
        assert await store.load_task("T1") is None
        assert await store.load_all_tasks() == []

    async def test_memory_store_isolation(self):
        """内存存储应存副本，修改返回值不影响内部状态"""
        store = MemoryPeriodicStore()
        payload = {"task_id": "T1", "task_data": {"a": 1}}
        await store.save_task("T1", payload)
        got = await store.load_task("T1")
        got["task_data"]["a"] = 999
        again = await store.load_task("T1")
        assert again["task_data"]["a"] == 1

    async def test_sqlite_store_roundtrip(self, tmp_path):
        db = str(tmp_path / "periodic.db")
        store = SQLitePeriodicStore(db)
        await store.save_task("S1", {"task_id": "S1", "cron_expr": "* * * * *", "next_run": None})
        await store.save_task("S2", {"task_id": "S2", "interval_seconds": 30})

        # 新实例读取同一文件，验证真正落库
        store2 = SQLitePeriodicStore(db)
        all_tasks = await store2.load_all_tasks()
        ids = {t["task_id"] for t in all_tasks}
        assert ids == {"S1", "S2"}

        await store2.delete_task("S1")
        assert await store2.load_task("S1") is None
        await store.close()
        await store2.close()

    async def test_execution_history(self, tmp_path):
        db = str(tmp_path / "exec.db")
        store = SQLitePeriodicStore(db)
        for i in range(3):
            await store.save_execution(
                f"E{i}", {"execution_id": f"E{i}", "task_id": "T1", "scheduled_time": f"2026-01-0{i + 1}T00:00:00"}
            )
        history = await store.load_executions("T1", limit=2)
        assert len(history) == 2
        await store.close()

    async def test_factory_memory(self):
        store = create_periodic_store("memory")
        assert isinstance(store, MemoryPeriodicStore)

    async def test_factory_unknown(self):
        with pytest.raises(ValueError):
            create_periodic_store("unknown")


class TestSerializeDeserialize:
    """序列化 / 反序列化保真度测试"""

    async def test_interval_roundtrip(self):
        manager = PeriodicTaskManager(_mock_pool(), storage=None)
        await manager.start()
        task_id = await manager.create_interval(
            interval_seconds=120,
            task_data={"action": "sync", "payload": [1, 2, 3]},
            name="interval_task",
            priority=1,
            max_runs=5,
            missed_policy=MissedExecutionPolicy.CATCH_UP,
        )
        instance = manager._tasks[task_id]
        payload = manager._serialize_instance(instance)
        restored = manager._deserialize_instance(payload)

        assert restored is not None
        assert restored.task_id == task_id
        assert restored.definition.task_data == {"action": "sync", "payload": [1, 2, 3]}
        assert restored.definition.interval_seconds == 120
        assert restored.definition.max_runs == 5
        assert restored.definition.missed_policy == MissedExecutionPolicy.CATCH_UP
        assert restored.next_run == instance.next_run
        await manager.stop(graceful=False)

    async def test_cron_rebuilds_cron_obj(self):
        manager = PeriodicTaskManager(_mock_pool(), storage=None)
        await manager.start()
        task_id = await manager.create_cron(
            cron_expr="0 9 * * *",
            task_data={"action": "report"},
            name="daily",
        )
        instance = manager._tasks[task_id]
        restored = manager._deserialize_instance(manager._serialize_instance(instance))

        assert restored.definition.cron_expr == "0 9 * * *"
        assert restored.definition.cron_obj is not None
        # cron_obj 可正常计算下一次执行时间
        assert restored.definition.cron_obj.next() is not None
        await manager.stop(graceful=False)


class TestManagerRestoreAcrossInstances:
    """端到端：一个实例持久化，另一个实例启动时恢复"""

    async def test_restore_from_shared_store(self):
        store = MemoryPeriodicStore()

        manager1 = PeriodicTaskManager(_mock_pool(), storage=store)
        manager1._scan_interval = 1000  # 避免测试期间真正触发执行
        await manager1.start()
        task_id = await manager1.create_interval(
            interval_seconds=60,
            task_data={"action": "persist_me"},
            name="to_restore",
        )
        # create_interval 内部已调用 _save_task 落库
        assert await store.load_task(task_id) is not None
        await manager1.stop(graceful=True)

        manager2 = PeriodicTaskManager(_mock_pool(), storage=store)
        manager2._scan_interval = 1000
        await manager2.start()  # start() 内部 _load_tasks 恢复
        assert task_id in manager2._tasks
        restored = manager2._tasks[task_id]
        assert restored.definition.task_data == {"action": "persist_me"}
        assert await manager2.get_task(task_id) is not None
        await manager2.stop(graceful=False)

    async def test_deleted_task_not_restored(self):
        store = MemoryPeriodicStore()
        manager1 = PeriodicTaskManager(_mock_pool(), storage=store)
        manager1._scan_interval = 1000
        await manager1.start()
        keep = await manager1.create_interval(interval_seconds=60, task_data={"a": 1}, task_id="keep")
        drop = await manager1.create_interval(interval_seconds=60, task_data={"b": 2}, task_id="drop")
        await manager1.delete_task(drop)
        await manager1.stop(graceful=True)

        manager2 = PeriodicTaskManager(_mock_pool(), storage=store)
        manager2._scan_interval = 1000
        await manager2.start()
        assert keep in manager2._tasks
        assert drop not in manager2._tasks
        await manager2.stop(graceful=False)
