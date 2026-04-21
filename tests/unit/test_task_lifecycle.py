"""
@FileName: test_task_lifecycle.py
@Description: 任务生命周期管理单元测试
@Author: HiPeng
@Time: 2026/4/21
"""

import asyncio

import pytest

from neotask.common.exceptions import TaskNotFoundError, TaskAlreadyExistsError
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.event.bus import EventBus, TaskEvent
from neotask.models.task import Task, TaskStatus, TaskPriority


class TestTaskLifecycleManager:
    """任务生命周期管理器测试"""

    @pytest.fixture
    def task_repo(self, memory_task_repo):
        """任务存储"""
        return memory_task_repo

    @pytest.fixture
    def event_bus(self):
        """事件总线"""
        bus = EventBus()
        return bus

    @pytest.fixture
    async def lifecycle_manager(self, task_repo, event_bus):
        """生命周期管理器"""
        manager = TaskLifecycleManager(task_repo, event_bus, cache_enabled=True)
        await event_bus.start()
        yield manager
        await event_bus.stop()

    @pytest.mark.asyncio
    async def test_create_task(self, lifecycle_manager):
        """测试创建任务"""
        task = await lifecycle_manager.create_task(
            data={"test": "data"},
            priority=TaskPriority.HIGH
        )

        assert task is not None
        assert task.task_id is not None
        assert task.data == {"test": "data"}
        assert task.status == TaskStatus.PENDING
        assert task.priority == TaskPriority.HIGH
        assert task.retry_count == 0
        assert task.ttl == 3600

    @pytest.mark.asyncio
    async def test_create_task_with_custom_id(self, lifecycle_manager):
        """测试使用自定义ID创建任务"""
        custom_id = "my_custom_task_123"

        task = await lifecycle_manager.create_task(
            data={"test": "data"},
            task_id=custom_id
        )

        assert task.task_id == custom_id

    @pytest.mark.asyncio
    async def test_create_duplicate_task(self, lifecycle_manager):
        """测试创建重复任务"""
        task_id = "duplicate_task"

        await lifecycle_manager.create_task(
            data={"test": "data"},
            task_id=task_id
        )

        with pytest.raises(TaskAlreadyExistsError) as exc_info:
            await lifecycle_manager.create_task(
                data={"test": "data2"},
                task_id=task_id
            )

        assert exc_info.value.task_id == task_id

    @pytest.mark.asyncio
    async def test_get_task(self, lifecycle_manager):
        """测试获取任务"""
        created = await lifecycle_manager.create_task(
            data={"test": "get_task"}
        )

        retrieved = await lifecycle_manager.get_task(created.task_id)

        assert retrieved is not None
        assert retrieved.task_id == created.task_id
        assert retrieved.data == created.data

    @pytest.mark.asyncio
    async def test_get_nonexistent_task(self, lifecycle_manager):
        """测试获取不存在的任务"""
        task = await lifecycle_manager.get_task("nonexistent_task_123")
        assert task is None

    @pytest.mark.asyncio
    async def test_update_status(self, lifecycle_manager):
        """测试更新任务状态"""
        task = await lifecycle_manager.create_task(data={"test": "update"})

        # 更新状态
        result = await lifecycle_manager.update_status(
            task.task_id,
            TaskStatus.RUNNING,
            node_id="worker-001"
        )

        assert result is True

        updated = await lifecycle_manager.get_task(task.task_id)
        assert updated.status == TaskStatus.RUNNING
        assert updated.node_id == "worker-001"

    @pytest.mark.asyncio
    async def test_update_status_nonexistent(self, lifecycle_manager):
        """测试更新不存在的任务状态"""
        result = await lifecycle_manager.update_status(
            "nonexistent",
            TaskStatus.RUNNING
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_start_task(self, lifecycle_manager):
        """测试开始任务"""
        task = await lifecycle_manager.create_task(data={"test": "start"})

        result = await lifecycle_manager.start_task(
            task.task_id,
            node_id="worker-001"
        )

        assert result is True

        started = await lifecycle_manager.get_task(task.task_id)
        assert started.status == TaskStatus.RUNNING
        assert started.node_id == "worker-001"
        assert started.started_at is not None

    @pytest.mark.asyncio
    async def test_start_non_pending_task(self, lifecycle_manager):
        """测试开始非待处理状态的任务"""
        task = await lifecycle_manager.create_task(data={"test": "start"})

        # 先开始
        await lifecycle_manager.start_task(task.task_id, "worker-001")

        # 再次开始应该失败
        result = await lifecycle_manager.start_task(task.task_id, "worker-002")
        assert result is False

    @pytest.mark.asyncio
    async def test_complete_task(self, lifecycle_manager):
        """测试完成任务"""
        task = await lifecycle_manager.create_task(data={"test": "complete"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")

        result = await lifecycle_manager.complete_task(
            task.task_id,
            {"result": "success", "data": "processed"}
        )

        assert result is True

        completed = await lifecycle_manager.get_task(task.task_id)
        assert completed.status == TaskStatus.SUCCESS
        assert completed.result == {"result": "success", "data": "processed"}
        assert completed.completed_at is not None

    @pytest.mark.asyncio
    async def test_fail_task(self, lifecycle_manager):
        """测试任务失败"""
        task = await lifecycle_manager.create_task(data={"test": "fail"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")

        error_msg = "Task execution failed due to timeout"
        result = await lifecycle_manager.fail_task(task.task_id, error_msg)

        assert result is True

        failed = await lifecycle_manager.get_task(task.task_id)
        assert failed.status == TaskStatus.FAILED
        assert failed.error == error_msg
        assert failed.completed_at is not None

    @pytest.mark.asyncio
    async def test_cancel_task(self, lifecycle_manager):
        """测试取消任务"""
        task = await lifecycle_manager.create_task(data={"test": "cancel"})

        result = await lifecycle_manager.cancel_task(task.task_id)

        assert result is True

        cancelled = await lifecycle_manager.get_task(task.task_id)
        assert cancelled.status == TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_running_task(self, lifecycle_manager):
        """测试取消运行中的任务"""
        task = await lifecycle_manager.create_task(data={"test": "cancel"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")

        result = await lifecycle_manager.cancel_task(task.task_id)

        assert result is True

        cancelled = await lifecycle_manager.get_task(task.task_id)
        assert cancelled.status == TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_completed_task(self, lifecycle_manager):
        """测试取消已完成的任务"""
        task = await lifecycle_manager.create_task(data={"test": "cancel"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")
        await lifecycle_manager.complete_task(task.task_id, {"result": "done"})

        result = await lifecycle_manager.cancel_task(task.task_id)

        assert result is False  # 已完成的任务不能取消

    @pytest.mark.asyncio
    async def test_delete_task(self, lifecycle_manager):
        """测试删除任务"""
        task = await lifecycle_manager.create_task(data={"test": "delete"})

        result = await lifecycle_manager.delete_task(task.task_id)

        assert result is True

        deleted = await lifecycle_manager.get_task(task.task_id)
        assert deleted is None

    @pytest.mark.asyncio
    async def test_wait_for_task_completion(self, lifecycle_manager):
        """测试等待任务完成"""
        task = await lifecycle_manager.create_task(data={"test": "wait"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")  # 【修复】先开始任务

        # 异步完成
        async def complete_later():
            await asyncio.sleep(0.1)
            await lifecycle_manager.complete_task(task.task_id, {"result": "done"})

        asyncio.create_task(complete_later())

        # 等待完成
        result = await lifecycle_manager.wait_for_task(task.task_id, timeout=1)

        assert result == {"result": "done"}

    @pytest.mark.asyncio
    async def test_wait_for_already_completed_task(self, lifecycle_manager):
        """测试等待已完成的任务"""
        task = await lifecycle_manager.create_task(data={"test": "wait"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")  # 【修复】先开始任务
        await lifecycle_manager.complete_task(task.task_id, {"result": "done"})

        result = await lifecycle_manager.wait_for_task(task.task_id, timeout=1)

        assert result == {"result": "done"}

    @pytest.mark.asyncio
    async def test_wait_for_failed_task(self, lifecycle_manager):
        """测试等待失败的任务"""
        task = await lifecycle_manager.create_task(data={"test": "wait"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")  # 【修复】先开始任务

        async def fail_later():
            await asyncio.sleep(0.1)
            await lifecycle_manager.fail_task(task.task_id, "Task failed")

        asyncio.create_task(fail_later())

        with pytest.raises(Exception) as exc_info:
            await lifecycle_manager.wait_for_task(task.task_id, timeout=1)

        assert "Task failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_wait_for_nonexistent_task(self, lifecycle_manager):
        """测试等待不存在的任务"""
        with pytest.raises(TaskNotFoundError):
            await lifecycle_manager.wait_for_task("nonexistent", timeout=1)

    @pytest.mark.asyncio
    async def test_wait_timeout(self, lifecycle_manager):
        """测试等待超时"""
        from neotask.common.exceptions import TimeoutError

        task = await lifecycle_manager.create_task(data={"test": "timeout"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")  # 【修复】先开始任务

        with pytest.raises(TimeoutError):
            await lifecycle_manager.wait_for_task(task.task_id, timeout=0.1)

    @pytest.mark.asyncio
    async def test_list_tasks(self, lifecycle_manager):
        """测试列出任务"""
        # 创建多个任务
        for i in range(10):
            await lifecycle_manager.create_task(
                data={"index": i},
                task_id=f"list_task_{i}"
            )

        tasks = await lifecycle_manager.list_tasks(limit=5)

        assert len(tasks) <= 5
        assert all(isinstance(t, Task) for t in tasks)

    @pytest.mark.asyncio
    async def test_list_tasks_by_status(self, lifecycle_manager):
        """测试按状态列出任务"""
        # 创建不同状态的任务
        task_ids = []
        for i in range(5):
            task = await lifecycle_manager.create_task(
                data={"index": i},
                task_id=f"pending_task_{i}"
            )
            task_ids.append(task.task_id)

        # 开始一些任务
        for i in range(2):
            await lifecycle_manager.start_task(f"pending_task_{i}", "worker")

        # 完成一些任务 - 【修复】先 start 再 complete
        for i in range(2, 4):
            await lifecycle_manager.start_task(f"pending_task_{i}", "worker")
            await lifecycle_manager.complete_task(f"pending_task_{i}", {"result": "done"})

        pending_tasks = await lifecycle_manager.list_tasks(TaskStatus.PENDING)
        running_tasks = await lifecycle_manager.list_tasks(TaskStatus.RUNNING)
        completed_tasks = await lifecycle_manager.list_tasks(TaskStatus.SUCCESS)

        # pending_task_4 仍然是 PENDING
        assert len(pending_tasks) >= 1
        assert len(running_tasks) == 2
        assert len(completed_tasks) == 2

    @pytest.mark.asyncio
    async def test_cache_functionality(self, lifecycle_manager):
        """测试缓存功能"""
        task = await lifecycle_manager.create_task(data={"test": "cache"})

        # 第一次获取（从存储）
        retrieved1 = await lifecycle_manager.get_task(task.task_id)

        # 第二次获取（应从缓存）
        retrieved2 = await lifecycle_manager.get_task(task.task_id)

        # 应该是同一个对象（缓存命中）
        assert retrieved1 is retrieved2

        # 清空缓存 - 【修复】使用同步清空
        lifecycle_manager.clear_cache()

        # 等待一小段时间让异步清空完成
        await asyncio.sleep(0.05)

        # 验证缓存已被清空
        async with lifecycle_manager._lock:
            assert task.task_id not in lifecycle_manager._cache

        # 再次获取（应从存储重新加载）
        retrieved3 = await lifecycle_manager.get_task(task.task_id)

        # 新获取的对象应该被重新缓存
        assert retrieved3 is not None


class TestTaskLifecycleWithEvents:
    """带事件的生命周期测试"""

    @pytest.fixture
    def task_repo(self, memory_task_repo):
        return memory_task_repo

    @pytest.fixture
    async def event_bus(self):
        bus = EventBus()
        await bus.start()
        yield bus
        await bus.stop()

    @pytest.fixture
    async def lifecycle_manager(self, task_repo, event_bus):
        return TaskLifecycleManager(task_repo, event_bus, cache_enabled=True)

    @pytest.mark.asyncio
    async def test_create_task_emits_event(self, lifecycle_manager, event_bus):
        """测试创建任务时发送事件"""
        events_received = []

        async def event_handler(event: TaskEvent):
            events_received.append(event)

        event_bus.subscribe("task.created", event_handler)

        await lifecycle_manager.create_task(data={"test": "event"})

        await asyncio.sleep(0.1)

        assert len(events_received) == 1
        assert events_received[0].event_type == "task.created"

    @pytest.mark.asyncio
    async def test_start_task_emits_event(self, lifecycle_manager, event_bus):
        """测试开始任务时发送事件"""
        events_received = []

        async def event_handler(event: TaskEvent):
            events_received.append(event)

        event_bus.subscribe("task.started", event_handler)

        task = await lifecycle_manager.create_task(data={"test": "event"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")

        await asyncio.sleep(0.1)

        assert len(events_received) == 1
        assert events_received[0].event_type == "task.started"

    @pytest.mark.asyncio
    async def test_complete_task_emits_event(self, lifecycle_manager, event_bus):
        """测试完成任务时发送事件"""
        events_received = []

        async def event_handler(event: TaskEvent):
            events_received.append(event)

        event_bus.subscribe("task.completed", event_handler)

        task = await lifecycle_manager.create_task(data={"test": "event"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")
        await lifecycle_manager.complete_task(task.task_id, {"result": "done"})

        await asyncio.sleep(0.1)

        assert len(events_received) == 1
        assert events_received[0].event_type == "task.completed"
        # 修复：事件数据直接是 result，不包装
        assert events_received[0].data == {"result": "done"}

    @pytest.mark.asyncio
    async def test_fail_task_emits_event(self, lifecycle_manager, event_bus):
        """测试任务失败时发送事件"""
        events_received = []

        async def event_handler(event: TaskEvent):
            events_received.append(event)

        event_bus.subscribe("task.failed", event_handler)

        task = await lifecycle_manager.create_task(data={"test": "event"})
        await lifecycle_manager.start_task(task.task_id, "worker-001")
        await lifecycle_manager.fail_task(task.task_id, "Something went wrong")

        await asyncio.sleep(0.1)

        assert len(events_received) == 1
        assert events_received[0].event_type == "task.failed"
        assert events_received[0].data == {"error": "Something went wrong"}

    @pytest.mark.asyncio
    async def test_cancel_task_emits_event(self, lifecycle_manager, event_bus):
        """测试取消任务时发送事件"""
        events_received = []

        async def event_handler(event: TaskEvent):
            events_received.append(event)

        event_bus.subscribe("task.cancelled", event_handler)

        task = await lifecycle_manager.create_task(data={"test": "event"})
        await lifecycle_manager.cancel_task(task.task_id)

        await asyncio.sleep(0.1)

        assert len(events_received) == 1
        assert events_received[0].event_type == "task.cancelled"


class TestTaskLifecycleEdgeCases:
    """任务生命周期边界测试"""

    @pytest.fixture
    def task_repo(self, memory_task_repo):
        return memory_task_repo

    @pytest.fixture
    async def lifecycle_manager(self, task_repo):
        return TaskLifecycleManager(task_repo, cache_enabled=True)

    @pytest.mark.asyncio
    async def test_complete_already_completed_task(self, lifecycle_manager):
        """测试完成已完成的任务"""
        task = await lifecycle_manager.create_task(data={"test": "edge"})
        await lifecycle_manager.start_task(task.task_id, "worker")

        # 第一次完成
        result1 = await lifecycle_manager.complete_task(task.task_id, {"result": "done"})
        assert result1 is True

        # 再次完成应该失败（任务已经是终态）
        result2 = await lifecycle_manager.complete_task(task.task_id, {"result": "again"})
        assert result2 is False

    @pytest.mark.asyncio
    async def test_fail_already_failed_task(self, lifecycle_manager):
        """测试失败已失败的任务"""
        task = await lifecycle_manager.create_task(data={"test": "edge"})
        await lifecycle_manager.start_task(task.task_id, "worker")

        # 第一次失败
        result1 = await lifecycle_manager.fail_task(task.task_id, "First error")
        assert result1 is True

        # 再次失败应该失败（任务已经是终态）
        result2 = await lifecycle_manager.fail_task(task.task_id, "Second error")
        assert result2 is False

    @pytest.mark.asyncio
    async def test_multiple_start_attempts(self, lifecycle_manager):
        """测试多次开始尝试"""
        task = await lifecycle_manager.create_task(data={"test": "edge"})

        # 第一次开始
        result1 = await lifecycle_manager.start_task(task.task_id, "worker-1")
        assert result1 is True

        # 第二次开始
        result2 = await lifecycle_manager.start_task(task.task_id, "worker-2")
        assert result2 is False

    @pytest.mark.asyncio
    async def test_complete_without_start(self, lifecycle_manager):
        """测试未开始就完成任务"""
        task = await lifecycle_manager.create_task(data={"test": "edge"})

        # 没有 start 直接 complete 应该失败
        result = await lifecycle_manager.complete_task(task.task_id, {"result": "done"})
        assert result is False

    @pytest.mark.asyncio
    async def test_fail_without_start(self, lifecycle_manager):
        """测试未开始就失败任务"""
        task = await lifecycle_manager.create_task(data={"test": "edge"})

        # 没有 start 直接 fail 应该失败
        result = await lifecycle_manager.fail_task(task.task_id, "Error")
        assert result is False

    @pytest.mark.asyncio
    async def test_large_number_of_tasks(self, lifecycle_manager):
        """测试大量任务"""
        import time

        start = time.time()

        # 创建 100 个任务
        tasks = []
        for i in range(100):
            task = await lifecycle_manager.create_task(
                data={"index": i},
                task_id=f"bulk_task_{i}"
            )
            tasks.append(task)

        create_time = time.time() - start
        assert create_time < 5.0  # 应该在 5 秒内完成

        # 获取所有任务
        for task in tasks:
            retrieved = await lifecycle_manager.get_task(task.task_id)
            assert retrieved is not None

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, lifecycle_manager):
        """测试并发操作"""
        import time

        async def create_and_complete(i: int):
            task = await lifecycle_manager.create_task(
                data={"index": i},
                task_id=f"concurrent_{i}"
            )
            await lifecycle_manager.start_task(task.task_id, f"worker_{i}")
            await asyncio.sleep(0.001)
            await lifecycle_manager.complete_task(task.task_id, {"result": i})

        start = time.time()

        # 并发执行 50 个任务
        await asyncio.gather(*[create_and_complete(i) for i in range(50)])

        duration = time.time() - start
        assert duration < 3.0  # 应该在 3 秒内完成

        # 验证所有任务都完成
        for i in range(50):
            task = await lifecycle_manager.get_task(f"concurrent_{i}")
            assert task.status == TaskStatus.SUCCESS


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
