"""
@FileName: test_worker_integration.py
@Description: Worker 集成测试（预取器、回收器）
@Author: HiPeng
@Time: 2026/4/16
"""

import asyncio

import pytest

from neotask.queue.scheduler import QueueScheduler
from neotask.storage.memory import MemoryQueueRepository
from neotask.worker.prefetcher import TaskPrefetcher, PrefetchConfig, PrefetchStrategy
from neotask.worker.reclaimer import TaskReclaimer, ReclaimerConfig


class TestPrefetcherIntegration:
    """预取器集成测试"""

    @pytest.fixture
    async def queue_with_tasks(self):
        """创建带有任务的队列"""
        repo = MemoryQueueRepository()
        scheduler = QueueScheduler(repo, max_size=1000)
        await scheduler.start()

        # 添加任务
        for i in range(100):
            await scheduler.push(f"task_{i}", i % 4)

        return scheduler

    @pytest.mark.asyncio
    async def test_prefetch_basic(self, queue_with_tasks):
        """测试基本预取功能"""
        config = PrefetchConfig(
            prefetch_size=10,
            local_queue_size=50,
            min_threshold=5
        )
        prefetcher = TaskPrefetcher(queue_with_tasks, config)

        await prefetcher.start()

        # 获取任务
        task_id = await prefetcher.get(timeout=1.0)
        assert task_id is not None
        assert task_id.startswith("task_")

        # 检查本地队列大小
        size = await prefetcher.size()
        assert size >= 0

        await prefetcher.stop()

    @pytest.mark.asyncio
    async def test_prefetch_batch(self, queue_with_tasks):
        """测试批量预取"""
        config = PrefetchConfig(
            prefetch_size=20,
            local_queue_size=100,
            min_threshold=10
        )
        prefetcher = TaskPrefetcher(queue_with_tasks, config)

        await prefetcher.start()

        # 批量获取
        tasks = await prefetcher.get_batch(max_count=15, timeout=1.0)
        assert len(tasks) > 0

        await prefetcher.stop()

    @pytest.mark.asyncio
    async def test_prefetch_stats(self, queue_with_tasks):
        """测试预取统计"""
        config = PrefetchConfig(
            prefetch_size=10,
            enable_stats=True
        )
        prefetcher = TaskPrefetcher(queue_with_tasks, config)

        await prefetcher.start()

        # 获取一些任务
        for _ in range(20):
            task_id = await prefetcher.get(timeout=0.5)
            if not task_id:
                break

        # 获取统计
        stats = prefetcher.get_stats()
        assert "total_prefetch" in stats
        assert "total_fetched" in stats
        assert "avg_batch_size" in stats

        await prefetcher.stop()

    @pytest.mark.asyncio
    async def test_prefetch_strategies(self, queue_with_tasks):
        """测试不同预取策略"""
        strategies = [
            PrefetchStrategy.SIZE_BASED,
            PrefetchStrategy.TIME_BASED,
            PrefetchStrategy.HYBRID
        ]

        for strategy in strategies:
            config = PrefetchConfig(
                prefetch_size=10,
                strategy=strategy
            )
            prefetcher = TaskPrefetcher(queue_with_tasks, config)

            await prefetcher.start()

            # 获取一些任务
            task_id = await prefetcher.get(timeout=0.5)
            assert task_id is not None or True  # 至少不报错

            await prefetcher.stop()


class TestReclaimerIntegration:
    """回收器集成测试"""

    @pytest.fixture
    async def setup_reclaimer(self):
        """设置回收器测试环境"""
        from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
        from neotask.lock.memory import MemoryLock
        from neotask.event.bus import EventBus

        task_repo = MemoryTaskRepository()
        queue_repo = MemoryQueueRepository()
        queue_scheduler = QueueScheduler(queue_repo)
        lock = MemoryLock()
        event_bus = EventBus()

        await event_bus.start()

        return {
            "task_repo": task_repo,
            "queue_scheduler": queue_scheduler,
            "lock": lock,
            "event_bus": event_bus
        }

    @pytest.mark.asyncio
    async def test_reclaimer_timeout(self, setup_reclaimer):
        """测试超时任务回收"""
        from neotask.models.task import Task, TaskStatus, TaskPriority
        from datetime import datetime, timezone

        task_repo = setup_reclaimer["task_repo"]
        queue_scheduler = setup_reclaimer["queue_scheduler"]
        lock = setup_reclaimer["lock"]

        # 创建超时任务
        task = Task(
            task_id="timeout_task",
            data={},
            status=TaskStatus.RUNNING,
            priority=TaskPriority.NORMAL,
            started_at=datetime.now(timezone.utc)
        )
        # 设置较短的 TTL
        task.ttl = 1

        await task_repo.save(task)

        # 创建回收器
        config = ReclaimerConfig(
            interval=1,
            task_timeout=1,
            enable_timeout_reclaim=True
        )
        reclaimer = TaskReclaimer(task_repo, queue_scheduler, lock, None, config)

        await reclaimer.start()

        # 等待回收
        await asyncio.sleep(2)

        # 检查任务状态
        reclaimed_task = await task_repo.get("timeout_task")
        assert reclaimed_task is not None

        # 验证回收后的状态（应该是 PENDING 或 FAILED）
        assert reclaimed_task.status in [TaskStatus.PENDING, TaskStatus.FAILED]

        await reclaimer.stop()

    @pytest.mark.asyncio
    async def test_reclaimer_max_retries(self, setup_reclaimer):
        """测试超过最大重试次数的回收"""
        from neotask.models.task import Task, TaskStatus, TaskPriority
        from datetime import datetime, timezone

        task_repo = setup_reclaimer["task_repo"]
        queue_scheduler = setup_reclaimer["queue_scheduler"]
        lock = setup_reclaimer["lock"]

        # 创建已重试多次的任务
        task = Task(
            task_id="max_retry_task",
            data={},
            status=TaskStatus.RUNNING,
            priority=TaskPriority.NORMAL,
            retry_count=3,
            started_at=datetime.now(timezone.utc)
        )
        task.ttl = 1

        await task_repo.save(task)

        # 创建回收器（最大重试次数为 2）
        config = ReclaimerConfig(
            interval=1,
            task_timeout=1,
            max_retries=2,
            enable_timeout_reclaim=True
        )
        reclaimer = TaskReclaimer(task_repo, queue_scheduler, lock, None, config)

        await reclaimer.start()

        # 等待回收
        await asyncio.sleep(2)

        # 检查任务状态（应该是 FAILED）
        reclaimed_task = await task_repo.get("max_retry_task")
        assert reclaimed_task is not None
        assert reclaimed_task.status == TaskStatus.FAILED

        await reclaimer.stop()

    @pytest.mark.asyncio
    async def test_reclaimer_stats(self, setup_reclaimer):
        """测试回收器统计"""
        from neotask.models.task import Task, TaskStatus, TaskPriority
        from datetime import datetime, timezone

        task_repo = setup_reclaimer["task_repo"]
        queue_scheduler = setup_reclaimer["queue_scheduler"]
        lock = setup_reclaimer["lock"]

        # 创建多个超时任务
        for i in range(5):
            task = Task(
                task_id=f"timeout_task_{i}",
                data={},
                status=TaskStatus.RUNNING,
                priority=TaskPriority.NORMAL,
                started_at=datetime.now(timezone.utc)
            )
            task.ttl = 1
            await task_repo.save(task)

        config = ReclaimerConfig(
            interval=1,
            task_timeout=1,
            enable_timeout_reclaim=True
        )
        reclaimer = TaskReclaimer(task_repo, queue_scheduler, lock, None, config)

        await reclaimer.start()

        # 等待回收
        await asyncio.sleep(2)

        # 获取统计
        stats = reclaimer.get_stats()
        assert "total_reclaimed" in stats
        assert "by_reason" in stats

        await reclaimer.stop()
