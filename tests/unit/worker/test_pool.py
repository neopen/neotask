"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: test_pool.py
@Description: Worker 池与预取器停机协作测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/28 22:22
"""

import asyncio

import pytest

from neotask.queue.queue_scheduler import QueueScheduler
from neotask.worker.prefetcher import PrefetchConfig, PrefetchStrategy, TaskPrefetcher


class TestPrefetcherDrain:
    """预取器停机回灌测试

    覆盖两个回归点：
    1. 停机时共享队列已 ``disable()``，``drain_back`` 仍必须能把已出队的
       任务放回队列（否则任务变成孤儿，只能等 ttl 兜底）。
    2. ``wait_until_empty`` 提供带超时的等待接口，供 WorkerPool 停机时使用。
    """

    @pytest.mark.asyncio
    async def test_drain_back_when_queue_disabled(self):
        """共享队列被禁用后，本地残留任务仍能回灌"""
        queue = QueueScheduler()
        await queue.start()

        try:
            for i in range(5):
                await queue.push(f"task_{i}", 1)

            prefetcher = TaskPrefetcher(
                queue,
                PrefetchConfig(
                    prefetch_size=5,
                    local_queue_size=10,
                    min_threshold=1,
                    max_threshold=8,
                    prefetch_interval=0.05,
                    strategy=PrefetchStrategy.SIZE_BASED,
                ),
            )
            await prefetcher.start()

            # 等待预取循环把任务搬进本地队列
            for _ in range(20):
                if await prefetcher.size() > 0:
                    break
                await asyncio.sleep(0.05)
            assert await prefetcher.size() > 0

            # 只停预取循环，模拟 WorkerPool.stop 的第一步
            await prefetcher.stop(graceful=False)
            # 模拟 TaskPool.shutdown 先 disable() 停止接收新任务
            await queue.disable()

            drained = await prefetcher.drain_back()
            assert drained > 0
            # 5 个任务一个都不能丢：回灌到共享队列 + 尚未预取的仍在队列中
            assert await queue.size() == 5
        finally:
            await queue.stop()

    @pytest.mark.asyncio
    async def test_wait_until_empty_timeout(self):
        """本地队列不会被消费时，等待接口应按超时返回 False"""
        queue = QueueScheduler()
        await queue.start()

        try:
            prefetcher = TaskPrefetcher(queue, PrefetchConfig(prefetch_size=5))
            await prefetcher.start()

            await queue.push("task_1", 1)
            for _ in range(20):
                if await prefetcher.size() > 0:
                    break
                await asyncio.sleep(0.05)

            await prefetcher.stop(graceful=False)

            # 没有消费者，本地队列不会清空
            assert await prefetcher.wait_until_empty(timeout=0.3) is False

            # 消费掉之后应立即返回 True
            await prefetcher.get(timeout=0.1)
            assert await prefetcher.wait_until_empty(timeout=0.3) is True
        finally:
            await queue.stop()


class TestPrefetcherGetFastPath:
    """本地队列非空时不走 ``wait_for``，避免每任务一次 Task 创建 + 定时器"""

    @pytest.mark.asyncio
    async def test_get_uses_nowait_when_local_queue_has_task(self, monkeypatch):
        prefetcher = TaskPrefetcher(QueueScheduler(), PrefetchConfig())
        prefetcher._local_queue = asyncio.Queue()
        prefetcher._demand_event = asyncio.Event()
        prefetcher._local_queue.put_nowait("task_1")

        calls = []
        real_wait_for = asyncio.wait_for

        async def spy(coro, timeout):
            calls.append(timeout)
            return await real_wait_for(coro, timeout)

        monkeypatch.setattr(asyncio, "wait_for", spy)

        assert await prefetcher.get(timeout=0.05) == "task_1"
        assert calls == []

    @pytest.mark.asyncio
    async def test_get_waits_when_local_queue_empty(self, monkeypatch):
        prefetcher = TaskPrefetcher(QueueScheduler(), PrefetchConfig())
        prefetcher._local_queue = asyncio.Queue()
        prefetcher._demand_event = asyncio.Event()

        calls = []
        real_wait_for = asyncio.wait_for

        async def spy(coro, timeout):
            calls.append(timeout)
            return await real_wait_for(coro, timeout)

        monkeypatch.setattr(asyncio, "wait_for", spy)

        assert await prefetcher.get(timeout=0.01) is None
        assert len(calls) == 1
