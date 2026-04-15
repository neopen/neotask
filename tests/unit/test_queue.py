"""
@FileName: test_queue.py
@Description: 队列模块单元测试
@Author: HiPeng
@Time: 2026/4/15
"""

import asyncio

import pytest

from neotask.queue.delayed_queue import DelayedQueue
from neotask.queue.priority_queue import PriorityQueue
from neotask.queue.scheduler import QueueScheduler
from neotask.storage.memory import MemoryQueueRepository


# ========== 优先级队列测试 ==========

class TestPriorityQueue:
    """测试优先级队列"""

    @pytest.mark.asyncio
    async def test_push_and_pop(self):
        """测试入队和出队"""
        queue = PriorityQueue()

        await queue.push("task-1", 5)
        await queue.push("task-2", 1)  # 更高优先级
        await queue.push("task-3", 3)

        assert await queue.size() == 3

        # 应该按优先级弹出（数字越小优先级越高）
        popped = await queue.pop(3)
        assert popped == ["task-2", "task-3", "task-1"]

    @pytest.mark.asyncio
    async def test_pop_with_priority(self):
        """测试带优先级的弹出"""
        queue = PriorityQueue()

        await queue.push("task-1", 5)
        await queue.push("task-2", 1)
        await queue.push("task-3", 3)

        popped = await queue.pop_with_priority(3)
        assert popped == [("task-2", 1), ("task-3", 3), ("task-1", 5)]

    @pytest.mark.asyncio
    async def test_peek(self):
        """测试查看队首"""
        queue = PriorityQueue()

        await queue.push("task-1", 5)
        await queue.push("task-2", 1)

        peeked = await queue.peek(2)
        assert peeked == ["task-2", "task-1"]

        # peek 不应改变队列大小
        assert await queue.size() == 2

    @pytest.mark.asyncio
    async def test_remove(self):
        """测试移除任务"""
        queue = PriorityQueue()

        await queue.push("task-1", 5)
        await queue.push("task-2", 1)
        await queue.push("task-3", 3)

        removed = await queue.remove("task-2")
        assert removed is True

        assert await queue.size() == 2
        assert await queue.contains("task-2") is False

        # 验证剩余任务
        popped = await queue.pop(2)
        assert "task-2" not in popped

    @pytest.mark.asyncio
    async def test_contains(self):
        """测试包含检查"""
        queue = PriorityQueue()

        await queue.push("task-1", 5)
        assert await queue.contains("task-1") is True
        assert await queue.contains("task-2") is False

    @pytest.mark.asyncio
    async def test_clear(self):
        """测试清空队列"""
        queue = PriorityQueue()

        await queue.push("task-1", 5)
        await queue.push("task-2", 1)

        await queue.clear()
        assert await queue.size() == 0
        assert await queue.is_empty()

    @pytest.mark.asyncio
    async def test_max_size_limit(self):
        """测试最大大小限制"""
        queue = PriorityQueue(max_size=2)

        assert await queue.push("task-1", 5) is True
        assert await queue.push("task-2", 1) is True
        assert await queue.push("task-3", 3) is False  # 队列已满

        assert await queue.size() == 2

    @pytest.mark.asyncio
    async def test_same_priority_fifo(self):
        """测试相同优先级时的 FIFO 顺序"""
        queue = PriorityQueue()

        await queue.push("task-1", 5)
        await queue.push("task-2", 5)
        await queue.push("task-3", 5)

        popped = await queue.pop(3)
        # 相同优先级应按入队顺序弹出
        assert popped == ["task-1", "task-2", "task-3"]


# ========== 延迟队列测试 ==========

class TestDelayedQueue:
    """测试延迟队列"""

    @pytest.mark.asyncio
    async def test_schedule_and_callback(self):
        """测试调度延迟任务"""
        called_tasks = []

        async def callback(task_id, priority):
            called_tasks.append((task_id, priority))

        delayed_queue = DelayedQueue(check_interval=0.05)
        await delayed_queue.start(callback)

        # 调度延迟任务
        await delayed_queue.schedule("task-1", 5, 0.1)
        await delayed_queue.schedule("task-2", 1, 0.2)

        assert await delayed_queue.size() == 2

        # 等待回调执行
        await asyncio.sleep(0.3)

        assert len(called_tasks) == 2
        assert ("task-1", 5) in called_tasks
        assert ("task-2", 1) in called_tasks

        await delayed_queue.stop()

    @pytest.mark.asyncio
    async def test_cancel_delayed_task(self):
        """测试取消延迟任务"""
        called_tasks = []

        async def callback(task_id, priority):
            called_tasks.append((task_id, priority))

        delayed_queue = DelayedQueue(check_interval=0.05)
        await delayed_queue.start(callback)

        await delayed_queue.schedule("task-1", 5, 0.1)
        await delayed_queue.schedule("task-2", 1, 0.2)

        # 取消第一个任务
        cancelled = await delayed_queue.cancel("task-1")
        assert cancelled is True

        assert await delayed_queue.size() == 1
        assert await delayed_queue.contains("task-1") is False

        await asyncio.sleep(0.25)

        # 只有 task-2 应该被执行
        assert len(called_tasks) == 1
        assert called_tasks[0] == ("task-2", 1)

        await delayed_queue.stop()

    @pytest.mark.asyncio
    async def test_get_next_execution_time(self):
        """测试获取下次执行时间"""
        delayed_queue = DelayedQueue()

        assert await delayed_queue.get_next_execution_time() is None

        await delayed_queue.schedule("task-1", 5, 0.1)

        next_time = await delayed_queue.get_next_execution_time()
        assert next_time is not None
        assert next_time > 0

    @pytest.mark.asyncio
    async def test_clear(self):
        """测试清空延迟队列"""
        delayed_queue = DelayedQueue()

        await delayed_queue.schedule("task-1", 5, 0.1)
        await delayed_queue.schedule("task-2", 1, 0.2)

        await delayed_queue.clear()
        assert await delayed_queue.size() == 0


# ========== 队列调度器测试 ==========

class TestQueueScheduler:
    """测试队列调度器"""

    @pytest.mark.asyncio
    async def test_push_immediate(self):
        """测试即时任务入队"""
        scheduler = QueueScheduler()

        await scheduler.start()

        success = await scheduler.push("task-1", 5, delay=0)
        assert success is True

        assert await scheduler.size() == 1

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_push_delayed(self):
        """测试延迟任务入队"""
        scheduler = QueueScheduler()

        await scheduler.start()

        success = await scheduler.push("task-1", 5, delay=0.1)
        assert success is True

        # 延迟任务应该在延迟队列中
        assert await scheduler.priority_size() == 0
        assert await scheduler.delayed_size() == 1

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_pop_immediate(self):
        """测试弹出即时任务"""
        scheduler = QueueScheduler()

        await scheduler.start()

        await scheduler.push("task-1", 5)
        await scheduler.push("task-2", 1)

        popped = await scheduler.pop(2)
        assert popped == ["task-2", "task-1"]

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_pop_with_priority(self):
        """测试带优先级弹出"""
        scheduler = QueueScheduler()

        await scheduler.start()

        await scheduler.push("task-1", 5)
        await scheduler.push("task-2", 1)

        popped = await scheduler.pop_with_priority(2)
        assert popped == [("task-2", 1), ("task-1", 5)]

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_delayed_task_moves_to_priority_queue(self):
        """测试延迟任务到期后移动到优先级队列"""
        scheduler = QueueScheduler()

        await scheduler.start()

        await scheduler.push("task-1", 5, delay=0.1)

        assert await scheduler.priority_size() == 0
        assert await scheduler.delayed_size() == 1

        # 等待延迟任务到期
        await asyncio.sleep(0.15)

        # 任务应该已移动到优先级队列
        assert await scheduler.priority_size() == 1
        assert await scheduler.delayed_size() == 0

        popped = await scheduler.pop(1)
        assert popped == ["task-1"]

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_remove_task(self):
        """测试移除任务"""
        scheduler = QueueScheduler()

        await scheduler.start()

        await scheduler.push("task-1", 5)
        await scheduler.push("task-2", 1, delay=0.1)

        # 移除即时任务
        removed = await scheduler.remove("task-1")
        assert removed is True

        assert await scheduler.size() == 1

        # 移除延迟任务
        removed = await scheduler.remove("task-2")
        assert removed is True

        assert await scheduler.size() == 0

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_pause_resume(self):
        """测试暂停/恢复"""
        scheduler = QueueScheduler()

        await scheduler.start()

        await scheduler.push("task-1", 5)
        await scheduler.push("task-2", 1)

        await scheduler.pause()

        # 暂停时 pop 应该返回空列表
        popped = await scheduler.pop(2)
        assert popped == []

        await scheduler.resume()

        # 恢复后应该能正常弹出
        popped = await scheduler.pop(2)
        assert len(popped) == 2

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_disable_enable(self):
        """测试禁用/启用入队"""
        scheduler = QueueScheduler()

        await scheduler.start()

        await scheduler.disable()

        # 禁用时 push 应该返回 False
        success = await scheduler.push("task-1", 5)
        assert success is False

        await scheduler.enable()

        success = await scheduler.push("task-2", 1)
        assert success is True

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_wait_until_empty(self):
        """测试等待队列清空"""
        scheduler = QueueScheduler()

        await scheduler.start()

        await scheduler.push("task-1", 5)
        await scheduler.push("task-2", 1)

        # 异步清空队列
        async def clear_queue():
            await asyncio.sleep(0.1)
            await scheduler.clear()

        asyncio.create_task(clear_queue())

        # 等待清空
        result = await scheduler.wait_until_empty(timeout=1.0)
        assert result is True

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_get_stats(self):
        """测试获取统计信息"""
        scheduler = QueueScheduler(max_size=100)

        await scheduler.start()

        await scheduler.push("task-1", 5)
        await scheduler.push("task-2", 1, delay=0.1)

        stats = await scheduler.get_stats()

        assert stats.priority_queue_size == 1
        assert stats.delayed_queue_size == 1
        assert stats.total_size == 2
        assert stats.is_paused is False
        assert stats.is_disabled is False
        assert stats.max_size == 100

        await scheduler.stop()


# ========== 持久化队列测试 ==========

class TestPersistentQueue:
    """测试持久化队列"""

    @pytest.mark.asyncio
    async def test_memory_queue_repository_integration(self):
        """测试内存队列存储集成"""
        repo = MemoryQueueRepository()
        scheduler = QueueScheduler(queue_repo=repo)

        await scheduler.start()

        await scheduler.push("task-1", 5)
        await scheduler.push("task-2", 1)

        assert await scheduler.size() == 2

        popped = await scheduler.pop(2)
        assert popped == ["task-2", "task-1"]

        await scheduler.stop()


# ========== 并发测试 ==========

class TestQueueConcurrency:
    """测试队列并发"""

    @pytest.mark.asyncio
    async def test_concurrent_push_pop(self):
        """测试并发入队出队"""
        scheduler = QueueScheduler()
        await scheduler.start()

        async def push_tasks(start_id, count):
            for i in range(count):
                await scheduler.push(f"task-{start_id}-{i}", i % 4)

        async def pop_tasks(count):
            results = []
            for _ in range(count):
                popped = await scheduler.pop(1)
                if popped:
                    results.extend(popped)
            return results

        # 并发执行
        push_tasks_coro = push_tasks(1, 100)
        pop_tasks_coro = pop_tasks(100)

        results = await asyncio.gather(push_tasks_coro, pop_tasks_coro)

        # 最终队列应该为空
        assert await scheduler.size() == 0

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_concurrent_delayed_tasks(self):
        """测试并发延迟任务"""
        scheduler = QueueScheduler()
        await scheduler.start()

        executed = []

        async def delayed_callback(task_id, priority):
            executed.append(task_id)

        # 使用内部延迟队列
        await scheduler._delayed_queue.start(delayed_callback)

        # 并发调度延迟任务
        async def schedule_delayed(delay):
            await scheduler.push(f"task-{delay}", 5, delay=delay)

        delays = [0.05, 0.1, 0.15, 0.2]
        await asyncio.gather(*[schedule_delayed(d) for d in delays])

        await asyncio.sleep(0.3)

        assert len(executed) == 4

        await scheduler.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
