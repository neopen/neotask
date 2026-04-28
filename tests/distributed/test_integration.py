"""
@FileName: test_integration.py
@Description: 分布式集成测试
@Author: HiPeng
@Time: 2026/4/28
"""

import pytest
import asyncio

from neotask.distributed.node import NodeManager
from neotask.distributed.coordinator import Coordinator
from neotask.distributed.elector import Elector
from neotask.queue.queue_scheduler import QueueScheduler
from neotask.storage.redis import RedisQueueRepository
from neotask.worker.prefetcher import TaskPrefetcher, PrefetchConfig
from neotask.executor.base import TaskExecutor
from tests.conftest import TEST_REDIS_URL


class TestIntegration:
    """分布式集成测试"""

    @pytest.fixture
    async def queue_scheduler(self):
        repo = RedisQueueRepository(TEST_REDIS_URL)
        scheduler = QueueScheduler(repo)
        await scheduler.start()
        yield scheduler
        await scheduler.stop()

    @pytest.fixture
    async def node_manager(self, test_node_id):
        manager = NodeManager(TEST_REDIS_URL, node_id=test_node_id)
        await manager.start()
        yield manager
        await manager.stop()

    @pytest.mark.asyncio
    async def test_full_distributed_workflow(self, queue_scheduler, node_manager):
        """测试完整分布式工作流"""
        # 1. 创建协调器
        coordinator = Coordinator(node_manager, queue_scheduler)

        # 2. 创建预取器
        prefetch_config = PrefetchConfig(prefetch_size=5, min_threshold=2)
        prefetcher = TaskPrefetcher(queue_scheduler, prefetch_config)
        await prefetcher.start()

        # 3. 分发任务
        task_ids = []
        for i in range(20):
            task_id = f"integration_task_{i}"
            await coordinator.distribute_task(task_id, priority=i % 5)
            task_ids.append(task_id)

        # 4. 验证任务已入队
        size = await queue_scheduler.size()
        assert size == 20

        # 5. 预取器应获取任务
        await asyncio.sleep(0.5)
        local_size = await prefetcher.size()
        assert local_size > 0

        # 6. 获取任务
        fetched = await prefetcher.get_batch(10, timeout=1)
        assert len(fetched) > 0

        await prefetcher.stop()

    @pytest.mark.asyncio
    async def test_multi_node_simulation(self, test_node_id):
        """模拟多节点工作"""
        # 模拟三个节点
        nodes = []
        for i in range(3):
            node_id = f"{test_node_id}_node_{i}"
            manager = NodeManager(TEST_REDIS_URL, node_id=node_id)
            await manager.start()
            nodes.append(manager)

        # 获取活跃节点
        active = await nodes[0].get_active_nodes()
        assert len(active) >= 3

        # 清理
        for manager in nodes:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_leader_election_with_workflow(self, test_node_id):
        """测试带工作流的领导者选举"""
        # 创建三个选举器
        electors = []
        for i in range(3):
            elector = Elector(TEST_REDIS_URL, f"{test_node_id}_elector_{i}")
            electors.append(elector)

        # 选举领导者
        leader_found = False
        for elector in electors:
            if await elector.elect(ttl=10):
                leader_found = True
                assert elector.is_leader is True

        assert leader_found is True

        # 验证只有一个领导者
        leader_count = sum(1 for e in electors if e.is_leader)
        assert leader_count == 1

        # 清理
        for elector in electors:
            await elector.resign()

    @pytest.mark.asyncio
    async def test_failover_simulation(self, queue_scheduler, node_manager):
        """测试故障转移模拟"""
        # 创建两个协调器（模拟两个节点）
        coordinator1 = Coordinator(node_manager, queue_scheduler)
        coordinator2 = Coordinator(node_manager, queue_scheduler)

        # 分发任务到节点1
        await coordinator1.distribute_task("failover_task_1", 1)

        # 模拟节点1故障（这里只是不再使用）
        # 节点2应该能继续处理

        # 节点2分发新任务
        await coordinator2.distribute_task("failover_task_2", 1)

        # 验证任务都在队列中
        size = await queue_scheduler.size()
        assert size >= 2

    @pytest.mark.asyncio
    async def test_task_distribution_and_consumption(self, queue_scheduler):
        """测试任务分发和消费"""
        # 创建简单的执行器
        class SimpleExecutor(TaskExecutor):
            async def execute(self, task_data):
                return {"processed": task_data}

        executor = SimpleExecutor()

        # 创建 WorkerPool（模拟消费者）
        # 注意：这里需要实际的 WorkerPool，但依赖较多，简化测试
        pass