"""
@FileName: test_prefetcher.py
@Description: 预取器测试
@Author: HiPeng
@Time: 2026/4/28
"""

import pytest
import asyncio

from neotask.worker.prefetcher import TaskPrefetcher, PrefetchConfig, PrefetchStrategy
from neotask.queue.queue_scheduler import QueueScheduler
from neotask.storage.redis import RedisQueueRepository
from tests.conftest import TEST_REDIS_URL


class TestTaskPrefetcher:
    """预取器测试"""

    @pytest.fixture
    async def queue_scheduler(self):
        """队列调度器 fixture"""
        repo = RedisQueueRepository(TEST_REDIS_URL)
        scheduler = QueueScheduler(repo)
        await scheduler.start()
        yield scheduler
        await scheduler.stop()

    @pytest.fixture
    async def prefetcher(self, queue_scheduler):
        """预取器 fixture"""
        config = PrefetchConfig(
            prefetch_size=10,
            local_queue_size=50,
            min_threshold=3,
            max_threshold=40,
            prefetch_interval=0.1,
            enable_stats=True
        )
        prefetcher = TaskPrefetcher(queue_scheduler, config)
        await prefetcher.start()
        yield prefetcher
        await prefetcher.stop()

    @pytest.mark.asyncio
    async def test_start_stop(self, prefetcher):
        """测试启动和停止"""
        stats = prefetcher.get_stats()
        assert stats["is_running"] is True

        await prefetcher.stop()
        stats = prefetcher.get_stats()
        assert stats["is_running"] is False

    @pytest.mark.asyncio
    async def test_get_from_empty_queue(self, prefetcher):
        """测试从空队列获取任务"""
        result = await prefetcher.get(timeout=0.5)
        assert result is None

    @pytest.mark.asyncio
    async def test_prefetch_on_demand(self, queue_scheduler, prefetcher):
        """测试按需预取"""
        # 添加任务到共享队列
        for i in range(20):
            await queue_scheduler.push(f"task_{i}", i % 5)

        # 等待预取
        await asyncio.sleep(0.5)

        # 本地队列应该有任务
        local_size = await prefetcher.size()
        assert local_size > 0

    @pytest.mark.asyncio
    async def test_get_batch(self, queue_scheduler, prefetcher):
        """测试批量获取"""
        # 添加任务
        for i in range(30):
            await queue_scheduler.push(f"task_{i}", i % 5)

        await asyncio.sleep(0.5)

        # 批量获取
        batch = await prefetcher.get_batch(max_count=10, timeout=0.5)
        assert len(batch) > 0
        assert len(batch) <= 10

    @pytest.mark.asyncio
    async def test_clear_local_queue(self, queue_scheduler, prefetcher):
        """测试清空本地队列"""
        # 添加任务并预取
        for i in range(10):
            await queue_scheduler.push(f"task_{i}", 1)

        await asyncio.sleep(0.5)
        assert await prefetcher.size() > 0

        # 清空
        await prefetcher.clear()
        assert await prefetcher.size() == 0

    @pytest.mark.asyncio
    async def test_stats_collection(self, queue_scheduler, prefetcher):
        """测试统计信息收集"""
        # 添加任务
        for i in range(50):
            await queue_scheduler.push(f"task_{i}", 1)

        await asyncio.sleep(0.5)

        stats = prefetcher.get_stats()
        assert stats["total_prefetch"] > 0
        assert stats["total_fetched"] > 0
        assert stats["avg_batch_size"] >= 0
        assert stats["is_running"] is True

    @pytest.mark.asyncio
    async def test_reset_stats(self, queue_scheduler, prefetcher):
        """测试重置统计"""
        # 添加任务触发预取
        for i in range(20):
            await queue_scheduler.push(f"task_{i}", 1)

        await asyncio.sleep(0.5)

        stats_before = prefetcher.get_stats()
        assert stats_before["total_prefetch"] > 0

        prefetcher.reset_stats()

        stats_after = prefetcher.get_stats()
        assert stats_after["total_prefetch"] == 0
        assert stats_after["total_fetched"] == 0

    @pytest.mark.asyncio
    async def test_size_based_strategy(self, queue_scheduler):
        """测试基于大小的预取策略"""
        config = PrefetchConfig(
            prefetch_size=5,
            local_queue_size=20,
            min_threshold=2,
            max_threshold=15,
            strategy=PrefetchStrategy.SIZE_BASED
        )

        prefetcher = TaskPrefetcher(queue_scheduler, config)
        await prefetcher.start()

        # 添加任务
        for i in range(30):
            await queue_scheduler.push(f"task_{i}", 1)

        await asyncio.sleep(0.5)

        # 本地队列大小应该小于 max_threshold
        size = await prefetcher.size()
        assert size < config.max_threshold

        await prefetcher.stop()

    @pytest.mark.asyncio
    async def test_graceful_stop(self, queue_scheduler):
        """测试优雅停止"""
        config = PrefetchConfig(prefetch_size=10, local_queue_size=50, min_threshold=1)
        prefetcher = TaskPrefetcher(queue_scheduler, config)

        await prefetcher.start()

        # 添加任务
        for i in range(50):
            await queue_scheduler.push(f"task_{i}", 1)

        await asyncio.sleep(0.5)

        # 优雅停止（等待本地队列清空）
        await prefetcher.stop(graceful=True, timeout=5.0)

        stats = prefetcher.get_stats()
        assert stats["is_running"] is False