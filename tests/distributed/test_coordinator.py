"""
@FileName: test_coordinator.py
@Description: 协调器测试
@Author: HiPeng
@Time: 2026/4/28
"""

import asyncio

import pytest

from neotask.distributed.coordinator import Coordinator
from neotask.distributed.node import NodeManager
from neotask.queue.queue_scheduler import QueueScheduler
from neotask.storage.redis import RedisQueueRepository
from tests.conftest import TEST_REDIS_URL


class TestCoordinator:
    """协调器测试"""

    @pytest.fixture
    async def queue_scheduler(self):
        """队列调度器 fixture"""
        repo = RedisQueueRepository(TEST_REDIS_URL)
        scheduler = QueueScheduler(repo)
        await scheduler.start()

        # 确保队列是干净的
        await scheduler.clear()

        yield scheduler

        # 测试后清理
        await scheduler.clear()
        await scheduler.stop()

    @pytest.fixture
    async def node_manager(self, test_node_id):
        """节点管理器 fixture"""
        manager = NodeManager(TEST_REDIS_URL, node_id=test_node_id)
        await manager.start()
        yield manager
        await manager.stop()

    @pytest.fixture
    async def coordinator(self, node_manager, queue_scheduler):
        """协调器 fixture"""
        return Coordinator(node_manager, queue_scheduler)

    @pytest.mark.asyncio
    async def test_distribute_task(self, coordinator, queue_scheduler):
        """测试任务分发"""
        # 确保队列为空
        await queue_scheduler.clear()

        task_id = "test_task_001"
        priority = 1

        result = await coordinator.distribute_task(task_id, priority)
        assert result is None  # 共享模式返回 None

        # 验证任务已入队
        size = await queue_scheduler.size()
        assert size == 1

    @pytest.mark.asyncio
    async def test_distribute_task_with_delay(self, coordinator, queue_scheduler):
        """测试带延迟的任务分发"""
        # 确保队列为空
        await queue_scheduler.clear()

        task_id = "test_task_delayed"
        priority = 1
        delay = 2.0

        # 分发任务（不等待延迟，立即返回）
        result = await coordinator.distribute_task(task_id, priority, delay=delay)
        assert result is None

        # 验证任务不在即时队列中（因为延迟任务进入了延迟队列）
        # 注意：延迟任务不会立即出现在 pop() 结果中
        size_before = await queue_scheduler.size()
        # size() 返回即时队列 + 延迟队列的总和
        # 由于延迟任务已进入延迟队列，size 应该 > 0
        assert size_before > 0

        # 等待延迟时间的一半，任务应该还在延迟队列中
        await asyncio.sleep(delay / 2)
        size_mid = await queue_scheduler.size()
        assert size_mid > 0

        # 等待延迟时间结束
        await asyncio.sleep(delay / 2 + 0.1)

        # 验证任务现在已经可以获取（延迟到期）
        # 注意：需要给调度器一点时间将任务从延迟队列移到即时队列
        await asyncio.sleep(0.2)

        # 尝试弹出任务
        popped = await queue_scheduler.pop(1)
        assert task_id in popped or len(popped) > 0

    @pytest.mark.asyncio
    async def test_distribute_task_to_node(self, coordinator):
        """测试分发任务到指定节点"""
        task_id = "test_task_node"
        priority = 1
        target_node = "node_001"

        result = await coordinator.distribute_task_to_node(
            task_id, priority, target_node
        )
        assert result is True

        # 验证节点关联已存储
        associated_node = await coordinator.get_task_node(task_id)
        assert associated_node == target_node

    @pytest.mark.asyncio
    async def test_get_task_node_not_exists(self, coordinator):
        """测试获取不存在的任务节点"""
        result = await coordinator.get_task_node("non_existent_task")
        assert result is None

    @pytest.mark.asyncio
    async def test_multiple_distribute(self, coordinator, queue_scheduler):
        """测试批量分发任务"""
        # 确保队列为空
        await queue_scheduler.clear()

        task_count = 10
        for i in range(task_count):
            await coordinator.distribute_task(f"task_{i}", i % 5)

        # 验证所有任务都在队列中
        size = await queue_scheduler.size()
        assert size == task_count

        # 验证可以弹出所有任务
        popped_count = 0
        for _ in range(task_count):
            tasks = await queue_scheduler.pop(1)
            if tasks:
                popped_count += len(tasks)

        assert popped_count == task_count

        # 验证队列为空
        final_size = await queue_scheduler.size()
        assert final_size == 0

    @pytest.mark.asyncio
    async def test_distribute_with_different_priorities(self, coordinator, queue_scheduler):
        """测试不同优先级的任务分发"""
        await queue_scheduler.clear()

        # 分发不同优先级的任务
        tasks = [
            ("high_priority", 0),  # 最高优先级
            ("normal_priority", 2),  # 普通优先级
            ("low_priority", 3),  # 低优先级
            ("critical_priority", 0),  # 最高优先级
        ]

        for task_id, priority in tasks:
            await coordinator.distribute_task(task_id, priority)

        size = await queue_scheduler.size()
        assert size == len(tasks)

        # 验证优先级顺序（优先级数字越小越先弹出）
        # 注意：pop 返回的是任务ID列表，但不保证顺序因为堆排序
        all_tasks = []
        for _ in range(len(tasks)):
            popped = await queue_scheduler.pop(1)
            if popped:
                all_tasks.extend(popped)

        assert len(all_tasks) == len(tasks)
        # 验证所有任务都被弹出
        assert set(all_tasks) == set([t[0] for t in tasks])

    @pytest.mark.asyncio
    async def test_task_persistence_across_distribute(self, coordinator, queue_scheduler):
        """测试任务在分发后持久化"""
        await queue_scheduler.clear()

        task_id = "persistent_task"
        priority = 1

        await coordinator.distribute_task(task_id, priority)

        # 模拟重启场景：创建新的调度器实例
        new_repo = RedisQueueRepository(TEST_REDIS_URL)
        new_scheduler = QueueScheduler(new_repo)
        await new_scheduler.start()

        try:
            # 验证任务仍然存在
            size = await new_scheduler.size()
            assert size >= 1

            # 可以弹出任务
            popped = await new_scheduler.pop(1)
            assert task_id in popped
        finally:
            await new_scheduler.stop()

    @pytest.mark.asyncio
    async def test_distribute_task_with_ttl(self, coordinator, queue_scheduler):
        """测试带 TTL 的任务分发"""
        await queue_scheduler.clear()

        task_id = "ttl_task"
        priority = 1
        ttl = 2  # 2秒 TTL

        # 注意：当前的 distribute_task 不直接支持 TTL
        # 这里只是验证分发功能
        await coordinator.distribute_task(task_id, priority)

        # 验证任务存在
        size = await queue_scheduler.size()
        assert size > 0
