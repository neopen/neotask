"""
@FileName: test_event_bus.py
@Description: 事件总线单元测试
@Author: HiPeng
@Time: 2026/4/21
"""

import asyncio

import pytest

from neotask.event.bus import TaskEvent


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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
