"""
@FileName: test_delayed_queue.py
@Description: 延迟队列单元测试
@Author: HiPeng
@Time: 2026/4/21
"""

import pytest
import asyncio
import time
from neotask.queue.delayed_queue import DelayedQueue


class TestDelayedQueue:
    """延迟队列测试"""

    @pytest.fixture
    async def delayed_queue(self):
        """创建延迟队列 fixture"""
        queue = DelayedQueue(check_interval=0.01)
        return queue

    async def test_schedule_and_execute(self):
        """测试调度和执行"""
        executed_tasks = []

        async def callback(task_id, priority, data=None):
            executed_tasks.append((task_id, priority))

        queue = DelayedQueue(check_interval=0.01)
        await queue.start(callback)

        # 调度任务
        await queue.schedule("task_1", 1, 0.1)
        await queue.schedule("task_2", 2, 0.2)

        # 等待执行
        await asyncio.sleep(0.3)

        await queue.stop()

        assert len(executed_tasks) >= 2
        task_ids = [t[0] for t in executed_tasks]
        assert "task_1" in task_ids
        assert "task_2" in task_ids

    async def test_schedule_at(self):
        """测试指定时间点调度"""
        from datetime import datetime, timedelta

        executed = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        queue = DelayedQueue()
        await queue.start(callback)

        execute_at = datetime.now() + timedelta(seconds=0.1)
        await queue.schedule_at("task_at", 1, execute_at)

        await asyncio.sleep(0.2)
        await queue.stop()

        assert "task_at" in executed

    async def test_cancel_task(self):
        """测试取消任务"""
        executed = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        queue = DelayedQueue()
        await queue.start(callback)

        await queue.schedule("task_1", 1, 0.1)
        await queue.schedule("task_2", 2, 0.2)

        # 取消 task_1
        cancelled = await queue.cancel("task_1")
        assert cancelled is True

        await asyncio.sleep(0.3)
        await queue.stop()

        assert "task_1" not in executed
        assert "task_2" in executed

    async def test_cancel_nonexistent_task(self):
        """测试取消不存在的任务"""
        queue = DelayedQueue()
        await queue.start(lambda x, y, z: None)

        cancelled = await queue.cancel("nonexistent")
        assert cancelled is False

        await queue.stop()

    async def test_size(self):
        """测试队列大小"""
        queue = DelayedQueue()
        await queue.start(lambda x, y, z: None)

        assert await queue.size() == 0

        await queue.schedule("task_1", 1, 10)
        await queue.schedule("task_2", 2, 20)

        assert await queue.size() == 2

        await queue.stop()

    async def test_is_empty(self):
        """测试队列是否为空"""
        queue = DelayedQueue()
        await queue.start(lambda x, y, z: None)

        assert await queue.is_empty() is True

        await queue.schedule("task_1", 1, 10)
        assert await queue.is_empty() is False

        await queue.stop()

    async def test_clear(self):
        """测试清空队列"""
        queue = DelayedQueue()
        await queue.start(lambda x, y, z: None)

        await queue.schedule("task_1", 1, 10)
        await queue.schedule("task_2", 2, 20)

        assert await queue.size() == 2

        await queue.clear()
        assert await queue.size() == 0

        await queue.stop()

    async def test_contains(self):
        """测试包含检查"""
        queue = DelayedQueue()
        await queue.start(lambda x, y, z: None)

        await queue.schedule("task_1", 1, 10)
        assert await queue.contains("task_1") is True
        assert await queue.contains("task_2") is False

        await queue.stop()

    async def test_get_next_execution_time(self):
        """测试获取下次执行时间"""
        queue = DelayedQueue()
        await queue.start(lambda x, y, z: None)

        assert await queue.get_next_execution_time() is None

        await queue.schedule("task_1", 1, 10)
        next_time = await queue.get_next_execution_time()
        assert next_time is not None
        assert next_time > time.time()

        await queue.stop()

    async def test_get_stats(self):
        """测试获取统计信息"""
        queue = DelayedQueue()
        await queue.start(lambda x, y, z: None)

        await queue.schedule("task_1", 1, 10)
        await queue.schedule("task_2", 2, 20)

        stats = await queue.get_stats()
        assert stats["type"] == "delayed_queue"
        assert stats["size"] == 2
        assert stats["check_interval"] == 0.1
        assert stats["stats"]["scheduled"] == 2

        await queue.stop()

    async def test_multiple_callbacks(self):
        """测试多个回调"""
        executed = []

        async def callback1(task_id, priority, data=None):
            executed.append(f"cb1_{task_id}")

        async def callback2(task_id, priority, data=None):
            executed.append(f"cb2_{task_id}")

        queue = DelayedQueue()
        await queue.start(callback1)
        await queue.start(callback2)  # 第二次调用会覆盖

        await queue.schedule("task_1", 1, 0.05)

        await asyncio.sleep(0.1)
        await queue.stop()

        # 只有一个回调被执行
        assert len(executed) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
