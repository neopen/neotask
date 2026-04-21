"""
@FileName: test_priority_queue.py
@Description: 优先级队列单元测试
@Author: HiPeng
@Time: 2026/4/21
"""

import asyncio

import pytest


class TestPriorityQueue:
    """优先级队列测试"""

    @pytest.mark.asyncio
    async def test_push_and_pop(self, priority_queue):
        """测试入队和出队"""
        # 入队
        await priority_queue.push("task_1", 1)
        await priority_queue.push("task_2", 2)
        await priority_queue.push("task_3", 0)  # 最高优先级

        # 出队
        tasks = await priority_queue.pop(3)

        assert tasks == ["task_3", "task_1", "task_2"]
        assert await priority_queue.size() == 0

    @pytest.mark.asyncio
    async def test_priority_order(self, priority_queue):
        """测试优先级顺序"""
        # 入队不同优先级的任务
        tasks = [
            ("low", 3),
            ("normal", 2),
            ("high", 1),
            ("critical", 0),
            ("medium", 2),
        ]

        for task_id, priority in tasks:
            await priority_queue.push(task_id, priority)

        # 按优先级出队
        popped = []
        for _ in range(5):
            result = await priority_queue.pop(1)
            popped.extend(result)

        # 优先级 0 最先，然后是 1，最后是 2 和 3（按入队顺序）
        assert popped[0] == "critical"  # priority 0
        assert popped[1] == "high"  # priority 1
        # priority 2 的任务按 FIFO 顺序
        assert popped[2] == "normal" or popped[2] == "medium"
        assert popped[3] == "normal" or popped[3] == "medium"
        assert popped[4] == "low"  # priority 3

    @pytest.mark.asyncio
    async def test_pop_with_count(self, priority_queue):
        """测试批量出队"""
        for i in range(10):
            await priority_queue.push(f"task_{i}", i % 3)

        # 批量出队
        tasks = await priority_queue.pop(5)
        assert len(tasks) == 5
        assert await priority_queue.size() == 5

        # 再次出队
        tasks = await priority_queue.pop(10)
        assert len(tasks) == 5
        assert await priority_queue.size() == 0

    @pytest.mark.asyncio
    async def test_remove_task(self, priority_queue):
        """测试移除任务"""
        await priority_queue.push("task_1", 1)
        await priority_queue.push("task_2", 2)
        await priority_queue.push("task_3", 1)

        # 移除中间任务
        removed = await priority_queue.remove("task_2")
        assert removed is True
        assert await priority_queue.size() == 2

        # 移除不存在的任务
        removed = await priority_queue.remove("task_not_exist")
        assert removed is False

        # 验证剩余任务
        tasks = await priority_queue.pop(2)
        assert "task_2" not in tasks
        assert len(tasks) == 2

    @pytest.mark.asyncio
    async def test_peek(self, priority_queue):
        """测试查看队首"""
        await priority_queue.push("task_1", 2)
        await priority_queue.push("task_2", 1)
        await priority_queue.push("task_3", 0)

        # 查看队首（不删除）
        peeked = await priority_queue.peek(1)
        assert peeked == ["task_3"]

        # 查看多个
        peeked = await priority_queue.peek(2)
        assert peeked == ["task_3", "task_2"]

        # 队列大小不变
        assert await priority_queue.size() == 3

    @pytest.mark.asyncio
    async def test_peek_with_priority(self, priority_queue):
        """测试查看队首及优先级"""
        await priority_queue.push("task_1", 2)
        await priority_queue.push("task_2", 1)
        await priority_queue.push("task_3", 0)

        # 查看带优先级
        tasks = await priority_queue.peek_with_priority(2)
        assert tasks == [("task_3", 0), ("task_2", 1)]

    @pytest.mark.asyncio
    async def test_clear(self, priority_queue):
        """测试清空队列"""
        for i in range(10):
            await priority_queue.push(f"task_{i}", i)

        assert await priority_queue.size() == 10

        await priority_queue.clear()
        assert await priority_queue.size() == 0

        # 清空后 pop 返回空列表
        tasks = await priority_queue.pop(5)
        assert tasks == []

    @pytest.mark.asyncio
    async def test_contains(self, priority_queue):
        """测试包含检查"""
        await priority_queue.push("task_1", 1)
        await priority_queue.push("task_2", 2)

        assert await priority_queue.contains("task_1") is True
        assert await priority_queue.contains("task_2") is True
        assert await priority_queue.contains("task_3") is False

    @pytest.mark.asyncio
    async def test_pop_with_priority(self, priority_queue):
        """测试出队并返回优先级"""
        await priority_queue.push("task_1", 1)
        await priority_queue.push("task_2", 0)
        await priority_queue.push("task_3", 2)

        tasks = await priority_queue.pop_with_priority(2)
        assert tasks == [("task_2", 0), ("task_1", 1)]

        # 验证剩余
        remaining = await priority_queue.pop_with_priority(1)
        assert remaining == [("task_3", 2)]

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, priority_queue):
        """测试并发操作"""

        async def producer(start_id: int, count: int):
            for i in range(count):
                await priority_queue.push(f"task_{start_id + i}", i % 3)

        async def consumer(count: int):
            results = []
            for _ in range(count):
                tasks = await priority_queue.pop(1)
                if tasks:
                    results.extend(tasks)
            return results

        # 并发生产
        await asyncio.gather(
            producer(0, 50),
            producer(50, 50),
            producer(100, 50)
        )

        assert await priority_queue.size() == 150

        # 并发消费
        consumers = [consumer(30) for _ in range(5)]
        results_list = await asyncio.gather(*consumers)
        total_consumed = sum(len(r) for r in results_list)

        assert total_consumed == 150
        assert await priority_queue.size() == 0

    @pytest.mark.asyncio
    async def test_pop_empty_queue(self, priority_queue):
        """测试空队列出队"""
        tasks = await priority_queue.pop(5)
        assert tasks == []

        tasks_with_priority = await priority_queue.pop_with_priority(5)
        assert tasks_with_priority == []

    @pytest.mark.asyncio
    async def test_fifo_for_same_priority(self, priority_queue):
        """测试相同优先级下的 FIFO 顺序"""
        await priority_queue.push("first", 1)
        await priority_queue.push("second", 1)
        await priority_queue.push("third", 1)

        tasks = await priority_queue.pop(3)
        assert tasks == ["first", "second", "third"]


class TestPriorityQueueWithRepository:
    """带存储的优先级队列测试"""

    @pytest.mark.asyncio
    async def test_persistent_push_pop(self, priority_queue_with_repo):
        """测试持久化入队出队"""
        await priority_queue_with_repo.push("task_1", 1)
        await priority_queue_with_repo.push("task_2", 0)

        tasks = await priority_queue_with_repo.pop(2)
        assert tasks == ["task_2", "task_1"]
        assert await priority_queue_with_repo.size() == 0

    @pytest.mark.asyncio
    async def test_persistent_remove(self, priority_queue_with_repo):
        """测试持久化移除"""
        await priority_queue_with_repo.push("task_1", 1)
        await priority_queue_with_repo.push("task_2", 2)
        await priority_queue_with_repo.push("task_3", 1)

        removed = await priority_queue_with_repo.remove("task_2")
        assert removed is True
        assert await priority_queue_with_repo.size() == 2

        tasks = await priority_queue_with_repo.pop(2)
        assert "task_2" not in tasks

    @pytest.mark.asyncio
    async def test_persistent_clear(self, priority_queue_with_repo):
        """测试持久化清空"""
        for i in range(10):
            await priority_queue_with_repo.push(f"task_{i}", i % 3)

        assert await priority_queue_with_repo.size() == 10

        await priority_queue_with_repo.clear()
        assert await priority_queue_with_repo.size() == 0


class TestPriorityQueueEdgeCases:
    """优先级队列边界测试"""

    @pytest.mark.asyncio
    async def test_single_task(self, priority_queue):
        """测试单个任务"""
        await priority_queue.push("task_1", 5)
        assert await priority_queue.size() == 1

        tasks = await priority_queue.pop(1)
        assert tasks == ["task_1"]
        assert await priority_queue.size() == 0

    @pytest.mark.asyncio
    async def test_large_priority_range(self, priority_queue):
        """测试大范围优先级"""
        for i in range(100):
            await priority_queue.push(f"task_{i}", i)

        # 最高优先级应该是 0
        tasks = await priority_queue.pop(1)
        assert tasks[0] == "task_0"

        # 最低优先级应该是 99
        # 清空队列找到最后一个
        all_tasks = await priority_queue.pop(100)
        assert all_tasks[-1] == "task_99"

    @pytest.mark.asyncio
    async def test_negative_priority(self, priority_queue):
        """测试负优先级（更高优先级）"""
        await priority_queue.push("task_1", -1)
        await priority_queue.push("task_2", 0)
        await priority_queue.push("task_3", 1)

        tasks = await priority_queue.pop(3)
        assert tasks == ["task_1", "task_2", "task_3"]

    @pytest.mark.asyncio
    async def test_remove_nonexistent(self, priority_queue):
        """测试移除不存在的任务"""
        removed = await priority_queue.remove("nonexistent")
        assert removed is False

        # 队列状态不变
        assert await priority_queue.size() == 0

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
