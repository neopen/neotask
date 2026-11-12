"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_event_bus.py
@Description: 事件总线单元测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/21
"""

import asyncio
import time

import pytest

from neotask.common.constants import EventType
from neotask.event.bus import EventBus, TaskEvent


class TestEventBus:
    """事件总线测试"""

    @pytest.mark.asyncio
    async def test_subscribe_and_emit(self, event_bus):
        """测试订阅和发送事件"""
        async with event_bus as bus:
            received_events = []

            async def handler(event: TaskEvent):
                received_events.append(event)

            bus.subscribe("test.event", handler)

            event = TaskEvent("test.event", "task_001", {"data": "value"})
            await bus.emit(event)

            # 等待事件处理
            await asyncio.sleep(0.1)

            assert len(received_events) == 1
            assert received_events[0].event_type == "test.event"
            assert received_events[0].task_id == "task_001"

    @pytest.mark.asyncio
    async def test_multiple_subscribers(self, event_bus):
        """测试多个订阅者"""
        async with event_bus as bus:
            received_count = 0

            async def handler1(event: TaskEvent):
                nonlocal received_count
                received_count += 1

            async def handler2(event: TaskEvent):
                nonlocal received_count
                received_count += 1

            bus.subscribe("test.event", handler1)
            bus.subscribe("test.event", handler2)

            await bus.emit(TaskEvent("test.event", "task_001"))

            await asyncio.sleep(0.1)

            assert received_count == 2

    @pytest.mark.asyncio
    async def test_global_subscriber(self, event_bus):
        """测试全局订阅者"""
        async with event_bus as bus:
            received_events = []

            async def global_handler(event: TaskEvent):
                received_events.append(event)

            bus.subscribe_global(global_handler)

            await bus.emit(TaskEvent("event.1", "task_001"))
            await bus.emit(TaskEvent("event.2", "task_002"))

            await asyncio.sleep(0.1)

            assert len(received_events) == 2
            assert received_events[0].event_type == "event.1"
            assert received_events[1].event_type == "event.2"

    @pytest.mark.asyncio
    async def test_unsubscribe(self, event_bus):
        """测试取消订阅"""
        async with event_bus as bus:
            received_count = 0

            async def handler(event: TaskEvent):
                nonlocal received_count
                received_count += 1

            bus.subscribe("test.event", handler)

            # 发送一次
            await bus.emit(TaskEvent("test.event", "task_001"))
            await asyncio.sleep(0.1)
            assert received_count == 1

            # 取消订阅
            bus.unsubscribe("test.event", handler)

            # 再次发送
            await bus.emit(TaskEvent("test.event", "task_002"))
            await asyncio.sleep(0.1)
            assert received_count == 1  # 没有增加

    @pytest.mark.asyncio
    async def test_multiple_event_types(self, event_bus):
        """测试多种事件类型"""
        async with event_bus as bus:
            events_by_type = {}

            async def handler(event: TaskEvent):
                if event.event_type not in events_by_type:
                    events_by_type[event.event_type] = []
                events_by_type[event.event_type].append(event)

            bus.subscribe_global(handler)

            await bus.emit(TaskEvent("type.a", "task_001"))
            await bus.emit(TaskEvent("type.b", "task_002"))
            await bus.emit(TaskEvent("type.a", "task_003"))

            await asyncio.sleep(0.1)

            assert len(events_by_type.get("type.a", [])) == 2
            assert len(events_by_type.get("type.b", [])) == 1

    @pytest.mark.asyncio
    async def test_handler_error_isolation(self, event_bus):
        """测试处理器错误隔离（一个失败不影响其他）"""
        async with event_bus as bus:
            results = []

            async def failing_handler(event: TaskEvent):
                results.append("failing_called")
                raise Exception("Handler error")

            async def success_handler(event: TaskEvent):
                results.append("success_called")

            bus.subscribe("test.event", failing_handler)
            bus.subscribe("test.event", success_handler)

            # 不应该抛出异常
            await bus.emit(TaskEvent("test.event", "task_001"))
            await asyncio.sleep(0.1)

            assert "failing_called" in results
            assert "success_called" in results

    @pytest.mark.asyncio
    async def test_event_with_data(self, event_bus):
        """测试带数据的事件"""
        async with event_bus as bus:
            received_data = None

            async def handler(event: TaskEvent):
                nonlocal received_data
                received_data = event.data

            bus.subscribe("test.event", handler)

            test_data = {"key": "value", "number": 42}
            await bus.emit(TaskEvent("test.event", "task_001", test_data))

            await asyncio.sleep(0.1)

            assert received_data == test_data

    @pytest.mark.asyncio
    async def test_concurrent_emits(self, event_bus):
        """测试并发发送事件"""
        async with event_bus as bus:
            received_count = 0

            async def handler(event: TaskEvent):
                nonlocal received_count
                received_count += 1
                await asyncio.sleep(0.01)  # 模拟处理时间

            bus.subscribe("test.event", handler)

            # 并发发送多个事件
            events = [TaskEvent("test.event", f"task_{i}") for i in range(10)]
            await asyncio.gather(*[bus.emit(e) for e in events])

            await asyncio.sleep(0.2)

            assert received_count == 10

    @pytest.mark.asyncio
    async def test_bounded_queue_drops_overflow(self):
        """有界队列满时丢弃新事件，并在统计中体现"""
        bus = EventBus(max_queue_size=2)
        await bus.start()

        release = asyncio.Event()
        started = asyncio.Event()

        async def blocking_handler(event: TaskEvent):
            started.set()
            await release.wait()

        bus.subscribe("test.event", blocking_handler)

        try:
            # 第一条事件被 worker 取走后阻塞在回调里，后续事件只能排队
            await bus.emit(TaskEvent("test.event", "task_0"))
            await asyncio.wait_for(started.wait(), timeout=1.0)

            for i in range(1, 5):
                await bus.emit(TaskEvent("test.event", f"task_{i}"))
            await asyncio.sleep(0.05)

            stats = bus.get_stats()
            assert stats["queue_size"] == 2
            assert stats["dropped_events"] == 2
            assert stats["processed_events"] == 0
        finally:
            release.set()
            await bus.stop()

    @pytest.mark.asyncio
    async def test_slow_handler_counted(self):
        """回调耗时超阈值时计入 slow_handler_calls"""
        bus = EventBus(slow_handler_threshold=0.01)

        async def slow_handler(event: TaskEvent):
            await asyncio.sleep(0.05)

        bus.subscribe("test.event", slow_handler)

        # 未启动时 emit 走同步处理路径，便于直接断言耗时统计
        await bus.emit(TaskEvent("test.event", "task_001"))

        stats = bus.get_stats()
        assert stats["slow_handler_calls"] == 1
        assert stats["slow_handler_threshold"] == 0.01

    @pytest.mark.asyncio
    async def test_slow_handler_does_not_block_fast_handler(self):
        """同一事件的订阅者并发执行，慢回调不阻塞快回调"""
        bus = EventBus()
        await bus.start()

        fast_called = asyncio.Event()

        async def slow_handler(event: TaskEvent):
            await asyncio.sleep(0.5)

        async def fast_handler(event: TaskEvent):
            fast_called.set()

        bus.subscribe("test.event", slow_handler)
        bus.subscribe("test.event", fast_handler)

        try:
            await bus.emit(TaskEvent("test.event", "task_001"))
            # 若串行执行，快回调要等慢回调 0.5s 才会被调用
            await asyncio.wait_for(fast_called.wait(), timeout=0.2)
        finally:
            await bus.stop()


class TestEventBusConfigPassthrough:
    """背压配置透传测试：三个入口 Config 的背压字段须到达池内 EventBus"""

    def test_task_pool_config_passthrough(self):
        from neotask.api.task_pool import TaskPool
        from neotask.models.config import TaskPoolConfig

        config = TaskPoolConfig(
            event_queue_max_size=5,
            event_slow_handler_threshold=0.5,
        )
        pool = TaskPool(config=config)
        stats = pool.event_bus.get_stats()
        assert stats["max_queue_size"] == 5
        assert stats["slow_handler_threshold"] == 0.5

    def test_scheduler_config_passthrough(self):
        from neotask.api.task_scheduler import TaskScheduler
        from neotask.models.config import SchedulerConfig

        config = SchedulerConfig(
            event_queue_max_size=7,
            event_slow_handler_threshold=0.25,
        )
        scheduler = TaskScheduler(config=config)
        assert scheduler.event_bus is scheduler._pool.event_bus
        stats = scheduler.event_bus.get_stats()
        assert stats["max_queue_size"] == 7
        assert stats["slow_handler_threshold"] == 0.25

    @pytest.mark.asyncio
    async def test_engine_config_passthrough(self):
        from neotask.core.engine import TaskEngine, EngineConfig

        config = EngineConfig(
            event_queue_max_size=9,
            event_slow_handler_threshold=0.75,
            enable_metrics=False,
        )
        engine = TaskEngine(config)
        await engine.initialize()
        stats = engine.event_bus.get_stats()
        assert stats["max_queue_size"] == 9
        assert stats["slow_handler_threshold"] == 0.75

    def test_default_config_keeps_unbounded(self):
        """默认配置保持无界队列与默认阈值（向后兼容）"""
        from neotask.api.task_pool import TaskPool

        pool = TaskPool()
        stats = pool.event_bus.get_stats()
        assert stats["max_queue_size"] is None
        assert stats["slow_handler_threshold"] == 1.0

    def test_invalid_overflow_policy_raises(self):
        """非法溢出策略在构造期即失败"""
        from neotask.api.task_pool import TaskPool
        from neotask.models.config import TaskPoolConfig

        config = TaskPoolConfig(event_overflow_policy="block")
        with pytest.raises(ValueError):
            TaskPool(config=config)


class TestEventTypeRegistry:
    """事件名常量注册表测试（v1.1.0 批次 1）"""

    def test_registry_values_match_1_0_strings(self):
        """常量值与 1.0.x 既有裸字符串逐一对齐，防止改名破坏订阅兼容"""
        expected = {
            "CREATED": "task.created",
            "STARTED": "task.started",
            "COMPLETED": "task.completed",
            "FAILED": "task.failed",
            "CANCELLED": "task.cancelled",
            "RETRY": "task.retry",
            "PROGRESS": "task.progress",
            "STATUS_CHANGED": "task.status_changed",
            "DELETED": "task.deleted",
            "RECLAIMED": "task.reclaimed",
            "CHECKPOINTED": "task.checkpointed",
            "COST_REPORTED": "task.cost_reported",
        }
        for name, value in expected.items():
            assert getattr(EventType, name) == value

    def test_constants_format_as_plain_event_names(self):
        """常量参与格式化后仍是事件名字符串

        事件名会写进日志与 Web UI；若改用 ``str``-Enum，3.11+ 的
        ``__format__`` 变更会让 ``f"{EventType.COMPLETED}"`` 变成成员名。
        """
        assert f"{EventType.COMPLETED}" == "task.completed"
        assert format(EventType.COMPLETED) == "task.completed"
        assert str(EventType.COMPLETED) == "task.completed"

    @pytest.mark.asyncio
    async def test_plain_string_subscription_receives_constant_event(self, event_bus):
        """调用方用裸字符串订阅，仍能收到以 EventType 发出的事件（双向兼容）"""
        received = []

        async def handler(event: TaskEvent):
            received.append(event)

        async with event_bus as bus:
            bus.subscribe("task.completed", handler)
            await bus.emit(TaskEvent(EventType.COMPLETED, "task_001"))
            await asyncio.sleep(0.1)

        assert len(received) == 1
        assert received[0].event_type == "task.completed"

    @pytest.mark.asyncio
    async def test_constant_subscription_receives_string_event(self, event_bus):
        """反向兼容：以 EventType 订阅，接收调用方用裸字符串发出的事件"""
        received = []

        async def handler(event: TaskEvent):
            received.append(event)

        async with event_bus as bus:
            bus.subscribe(EventType.COMPLETED, handler)
            await bus.emit(TaskEvent("task.completed", "task_002"))
            await asyncio.sleep(0.1)

        assert len(received) == 1


class TestEventEnvelope:
    """事件信封稳定化测试（v1.1.0 批次 1）"""

    def test_defaults_are_populated(self):
        """timestamp 构造时自动填充，schema_version 固定为 1"""
        before = time.time()
        event = TaskEvent(EventType.CREATED, "task_001")
        assert event.schema_version == 1
        assert event.node_id == ""
        assert before <= event.timestamp <= time.time() + 1

    def test_positional_construction_stays_compatible(self):
        """既有三位置参数构造方式不受新增字段影响"""
        event = TaskEvent("task.completed", "task_001", {"ok": True})
        assert event.data == {"ok": True}
        assert event.timestamp > 0

    @pytest.mark.asyncio
    async def test_bus_stamps_configured_node_id(self):
        """总线按自身配置补齐缺失的 node_id"""
        received = []

        async def handler(event: TaskEvent):
            received.append(event)

        bus = EventBus(node_id="host-a-1001")
        async with bus:
            bus.subscribe_global(handler)
            await bus.emit(TaskEvent(EventType.STARTED, "task_001"))
            await asyncio.sleep(0.1)

        assert received[0].node_id == "host-a-1001"

    @pytest.mark.asyncio
    async def test_explicit_node_id_not_overwritten(self):
        """事件已带 node_id 时不被总线覆盖"""
        received = []

        async def handler(event: TaskEvent):
            received.append(event)

        bus = EventBus(node_id="host-a-1001")
        async with bus:
            bus.subscribe_global(handler)
            await bus.emit(TaskEvent(EventType.STARTED, "task_001", node_id="worker-7"))
            await asyncio.sleep(0.1)

        assert received[0].node_id == "worker-7"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
