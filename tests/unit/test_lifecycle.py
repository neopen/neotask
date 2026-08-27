"""
@FileName: test_lifecycle.py
@Description: 任务生命周期管理器单元测试
@Author: HiPeng
@Time: 2026/4/15
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from neotask.core.lifecycle import TaskLifecycleManager
from neotask.event.bus import EventBus
from neotask.models.task import TaskStatus, TaskPriority
from neotask.storage.memory import MemoryTaskRepository


# ========== 测试辅助函数 ==========

def get_utc_now():
    """获取当前 UTC 时间"""
    return datetime.now(timezone.utc)


# ========== 基础功能测试 ==========

class TestLifecycleBasic:
    """测试基础功能"""

    @pytest.fixture
    async def lifecycle(self):
        """创建生命周期管理器"""
        repo = MemoryTaskRepository()
        event_bus = EventBus()
        await event_bus.start()
        manager = TaskLifecycleManager(repo, event_bus)
        yield manager, event_bus
        # 清理
        await event_bus.stop()
        await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_create_task(self, lifecycle):
        """测试创建任务"""
        manager, _ = lifecycle

        task = await manager.create_task(
            data={"test": "data"},
            priority=TaskPriority.HIGH
        )

        assert task is not None
        assert task.task_id is not None
        assert task.data == {"test": "data"}
        assert task.status == TaskStatus.PENDING
        assert task.priority == TaskPriority.HIGH

    @pytest.mark.asyncio
    async def test_get_task(self, lifecycle):
        """测试获取任务"""
        manager, _ = lifecycle

        created = await manager.create_task(data={"test": "get"})
        retrieved = await manager.get_task(created.task_id)

        assert retrieved is not None
        assert retrieved.task_id == created.task_id
        assert retrieved.data == {"test": "get"}

    @pytest.mark.asyncio
    async def test_get_nonexistent_task(self, lifecycle):
        """测试获取不存在的任务"""
        manager, _ = lifecycle

        task = await manager.get_task("nonexistent")
        assert task is None

    @pytest.mark.asyncio
    async def test_start_task(self, lifecycle):
        """测试开始任务"""
        manager, _ = lifecycle

        task = await manager.create_task(data={"test": "start"})
        assert task.status == TaskStatus.PENDING

        success = await manager.start_task(task.task_id, "node-1")
        assert success is True

        updated = await manager.get_task(task.task_id)
        assert updated.status == TaskStatus.RUNNING
        assert updated.node_id == "node-1"
        assert updated.started_at is not None

    @pytest.mark.asyncio
    async def test_start_already_running_task(self, lifecycle):
        """测试开始已运行的任务"""
        manager, _ = lifecycle

        task = await manager.create_task(data={"test": "start"})
        await manager.start_task(task.task_id, "node-1")

        # 第二次启动应该失败
        success = await manager.start_task(task.task_id, "node-2")
        assert success is False

    @pytest.mark.asyncio
    async def test_complete_task(self, lifecycle):
        """测试完成任务"""
        manager, _ = lifecycle

        task = await manager.create_task(data={"test": "complete"})
        await manager.start_task(task.task_id, "node-1")

        success = await manager.complete_task(task.task_id, {"result": "ok"})
        assert success is True

        updated = await manager.get_task(task.task_id)
        assert updated.status == TaskStatus.SUCCESS
        assert updated.result == {"result": "ok"}
        assert updated.completed_at is not None

    @pytest.mark.asyncio
    async def test_fail_task(self, lifecycle):
        """测试任务失败"""
        manager, _ = lifecycle

        task = await manager.create_task(data={"test": "fail"})
        await manager.start_task(task.task_id, "node-1")

        success = await manager.fail_task(task.task_id, "Something went wrong")
        assert success is True

        updated = await manager.get_task(task.task_id)
        assert updated.status == TaskStatus.FAILED
        assert updated.error == "Something went wrong"

    @pytest.mark.asyncio
    async def test_cancel_task(self, lifecycle):
        """测试取消任务"""
        manager, _ = lifecycle

        task = await manager.create_task(data={"test": "cancel"})

        success = await manager.cancel_task(task.task_id)
        assert success is True

        updated = await manager.get_task(task.task_id)
        assert updated.status == TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_running_task(self, lifecycle):
        """测试取消运行中的任务"""
        manager, _ = lifecycle

        task = await manager.create_task(data={"test": "cancel"})
        await manager.start_task(task.task_id, "node-1")

        success = await manager.cancel_task(task.task_id)
        assert success is True

        updated = await manager.get_task(task.task_id)
        assert updated.status == TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_delete_task(self, lifecycle):
        """测试删除任务"""
        manager, _ = lifecycle

        task = await manager.create_task(data={"test": "delete"})

        success = await manager.delete_task(task.task_id)
        assert success is True

        deleted = await manager.get_task(task.task_id)
        assert deleted is None


# ========== 统计功能测试 ==========

class TestLifecycleStats:
    """测试统计功能"""

    @pytest.fixture
    async def lifecycle(self):
        """创建生命周期管理器"""
        repo = MemoryTaskRepository()
        event_bus = EventBus()
        await event_bus.start()
        manager = TaskLifecycleManager(repo, event_bus)
        yield manager
        await event_bus.stop()
        await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_get_stats_empty(self, lifecycle):
        """测试空统计"""
        manager = lifecycle

        stats = await manager.get_task_stats()

        assert stats.total == 0
        assert stats.pending == 0
        assert stats.running == 0
        assert stats.completed == 0
        assert stats.failed == 0
        assert stats.cancelled == 0

    @pytest.mark.asyncio
    async def test_get_stats_with_tasks(self, lifecycle):
        """测试有任务时的统计"""
        manager = lifecycle

        # 创建各种状态的任务
        pending = await manager.create_task(data={"id": 1})
        running = await manager.create_task(data={"id": 2})
        await manager.start_task(running.task_id, "node-1")
        success = await manager.create_task(data={"id": 3})
        await manager.start_task(success.task_id, "node-1")
        await manager.complete_task(success.task_id, {"ok": True})
        failed = await manager.create_task(data={"id": 4})
        await manager.start_task(failed.task_id, "node-1")
        await manager.fail_task(failed.task_id, "error")
        cancelled = await manager.create_task(data={"id": 5})
        await manager.cancel_task(cancelled.task_id)

        stats = await manager.get_task_stats()

        assert stats.total == 5
        assert stats.pending == 1
        assert stats.running == 1
        assert stats.completed == 1
        assert stats.failed == 1
        assert stats.cancelled == 1

    @pytest.mark.asyncio
    async def test_success_rate(self, lifecycle):
        """测试成功率计算"""
        manager = lifecycle

        # 创建成功和失败的任务
        for i in range(8):
            task = await manager.create_task(data={"id": i})
            await manager.start_task(task.task_id, "node-1")
            await manager.complete_task(task.task_id, {"ok": True})

        for i in range(2):
            task = await manager.create_task(data={"id": i + 8})
            await manager.start_task(task.task_id, "node-1")
            await manager.fail_task(task.task_id, "error")

        stats = await manager.get_task_stats()

        # 成功率 = 8 / (8 + 2) = 0.8
        assert stats.success_rate == 0.8


# ========== 过期清理测试 ==========

class TestLifecycleCleanup:
    """测试过期清理功能"""

    @pytest.fixture
    async def lifecycle(self):
        """创建生命周期管理器"""
        repo = MemoryTaskRepository()
        event_bus = EventBus()
        await event_bus.start()
        manager = TaskLifecycleManager(repo, event_bus)
        yield manager
        await event_bus.stop()
        await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_cleanup_expired_tasks(self, lifecycle):
        """测试清理过期任务"""
        manager = lifecycle

        # 创建新任务（不应被清理）
        new_task = await manager.create_task(
            data={"test": "new"},
            ttl=3600
        )

        # 创建已完成的任务
        completed_task = await manager.create_task(
            data={"test": "completed"},
            ttl=1  # TTL 1秒
        )
        await manager.start_task(completed_task.task_id, "node-1")
        await manager.complete_task(completed_task.task_id, {"ok": True})

        # 等待任务过期
        await asyncio.sleep(1.5)

        # 执行清理
        cleaned = await manager.cleanup_expired()

        # 验证
        assert cleaned >= 1

        # 新任务应该还在
        assert await manager.get_task(new_task.task_id) is not None

        # 过期任务应该被删除
        assert await manager.get_task(completed_task.task_id) is None

    @pytest.mark.asyncio
    async def test_cleanup_by_time(self, lifecycle):
        """测试按时间清理"""
        manager = lifecycle

        # 创建旧任务 - 直接通过存储层创建并手动修改时间
        old_task = await manager.create_task(data={"test": "old"})

        # 完成旧任务（这样才能被清理）
        await manager.start_task(old_task.task_id, "node-1")
        await manager.complete_task(old_task.task_id, {"ok": True})

        # 手动修改 completed_at 为过去时间（2天前）
        old_task.completed_at = datetime.now(timezone.utc) - timedelta(days=2)
        await manager._task_repo.save(old_task)

        # 更新缓存
        if manager._cache_enabled:
            async with manager._lock:
                manager._cache[old_task.task_id] = old_task

        # 创建新任务
        new_task = await manager.create_task(data={"test": "new"})
        await manager.start_task(new_task.task_id, "node-1")
        await manager.complete_task(new_task.task_id, {"ok": True})

        # 验证旧任务的 completed_at 已修改
        verify_old = await manager.get_task(old_task.task_id)
        assert verify_old is not None
        assert verify_old.completed_at is not None
        age = (datetime.now(timezone.utc) - verify_old.completed_at).total_seconds()
        print(f"Old task age: {age} seconds (should be ~172800)")

        # 按1天时间清理
        cleaned = await manager.cleanup_expired_by_time(max_age_seconds=86400)

        print(f"Cleaned count: {cleaned}")
        assert cleaned >= 1, f"Expected at least 1 cleaned, got {cleaned}"

        # 旧任务应该被删除
        assert await manager.get_task(old_task.task_id) is None

        # 新任务应该还在
        assert await manager.get_task(new_task.task_id) is not None

    @pytest.mark.asyncio
    async def test_no_cleanup_active_tasks(self, lifecycle):
        """测试不会清理活跃任务"""
        manager = lifecycle

        # 创建运行中的任务（不应被清理）
        running_task = await manager.create_task(
            data={"test": "running"},
            ttl=1
        )
        await manager.start_task(running_task.task_id, "node-1")

        await asyncio.sleep(1.5)

        cleaned = await manager.cleanup_expired()

        # 运行中的任务不应被清理
        assert await manager.get_task(running_task.task_id) is not None


# ========== 等待功能测试 ==========

class TestLifecycleWait:
    """测试等待功能"""

    @pytest.fixture
    async def lifecycle(self):
        """创建生命周期管理器"""
        repo = MemoryTaskRepository()
        event_bus = EventBus()
        await event_bus.start()
        manager = TaskLifecycleManager(repo, event_bus)
        yield manager
        await event_bus.stop()
        await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_wait_for_completion(self, lifecycle):
        """测试等待任务完成"""
        manager = lifecycle

        task = await manager.create_task(data={"test": "wait"})

        # 异步完成任务
        async def complete_after_delay():
            await asyncio.sleep(0.2)
            await manager.start_task(task.task_id, "node-1")
            await manager.complete_task(task.task_id, {"result": "done"})

        # 创建任务但不等待
        asyncio.create_task(complete_after_delay())

        # 给任务一点时间启动
        await asyncio.sleep(0.05)

        # 等待完成
        result = await manager.wait_for_task(task.task_id, timeout=5)

        assert result == {"result": "done"}

    @pytest.mark.asyncio
    async def test_wait_for_failure(self, lifecycle):
        """测试等待失败任务"""
        manager = lifecycle

        task = await manager.create_task(data={"test": "wait_fail"})

        async def fail_after_delay():
            await asyncio.sleep(0.2)
            await manager.start_task(task.task_id, "node-1")
            await manager.fail_task(task.task_id, "Task failed")

        asyncio.create_task(fail_after_delay())
        await asyncio.sleep(0.05)

        with pytest.raises(Exception) as exc_info:
            await manager.wait_for_task(task.task_id, timeout=5)

        assert "failed" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_wait_for_already_completed(self, lifecycle):
        """测试等待已完成的任务"""
        manager = lifecycle

        task = await manager.create_task(data={"test": "already_done"})
        await manager.start_task(task.task_id, "node-1")
        await manager.complete_task(task.task_id, {"result": "done"})

        # 直接返回结果，不需要等待
        result = await manager.wait_for_task(task.task_id, timeout=1)

        assert result == {"result": "done"}

    @pytest.mark.asyncio
    async def test_wait_for_nonexistent_task(self, lifecycle):
        """测试等待不存在的任务"""
        manager = lifecycle

        from neotask.common.exceptions import TaskNotFoundError

        with pytest.raises(TaskNotFoundError):
            await manager.wait_for_task("nonexistent", timeout=1)


# ========== 缓存测试 ==========

class TestLifecycleCache:
    """测试缓存功能"""

    @pytest.fixture
    async def lifecycle_with_cache(self):
        """创建带缓存的生命周期管理器"""
        repo = MemoryTaskRepository()
        event_bus = EventBus()
        await event_bus.start()
        manager = TaskLifecycleManager(repo, event_bus, cache_enabled=True)
        yield manager
        await event_bus.stop()
        await asyncio.sleep(0.1)

    @pytest.fixture
    async def lifecycle_without_cache(self):
        """创建禁用缓存的生命周期管理器"""
        repo = MemoryTaskRepository()
        event_bus = EventBus()
        await event_bus.start()
        # 传入 cache_enabled=False
        return TaskLifecycleManager(repo, event_bus, cache_enabled=False)

    @pytest.mark.asyncio
    async def test_cache_enabled(self, lifecycle_with_cache):
        """测试缓存启用"""
        manager = lifecycle_with_cache

        task = await manager.create_task(data={"test": "cache"})

        # 第一次获取（从缓存）
        retrieved1 = await manager.get_task(task.task_id)
        # 第二次获取（从缓存）
        retrieved2 = await manager.get_task(task.task_id)

        # 应该是同一个对象
        assert retrieved1 is retrieved2

    @pytest.mark.asyncio
    async def test_cache_disabled(self, lifecycle_without_cache):
        """测试缓存禁用"""
        manager = lifecycle_without_cache

        task = await manager.create_task(data={"test": "no_cache"})

        # 每次获取都从存储读取
        retrieved1 = await manager.get_task(task.task_id)
        retrieved2 = await manager.get_task(task.task_id)

        # 应该是不同的对象（因为存储层返回深拷贝）
        assert retrieved1 is not retrieved2
        assert retrieved1.task_id == retrieved2.task_id
        assert retrieved1.data == retrieved2.data


# ========== 独立运行测试 ==========

if __name__ == "__main__":
    # 使用 pytest 运行，避免事件循环问题
    pytest.main([__file__, "-v", "--asyncio-mode=auto", "-x"])
