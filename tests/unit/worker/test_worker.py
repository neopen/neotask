"""
@FileName: test_worker.py
@Description: Worker 池单元测试
@Author: HiPeng
@Time: 2026/4/28
"""

import asyncio
from unittest.mock import Mock, AsyncMock

import pytest

from neotask.core.lifecycle import TaskLifecycleManager
from neotask.event.bus import EventBus
from neotask.executor.base import TaskExecutor
from neotask.models.task import Task, TaskStatus, TaskPriority
from neotask.queue.queue_scheduler import QueueScheduler
from neotask.worker.pool import WorkerPool, WorkerStats


@pytest.fixture
def mock_task_repo():
    """模拟任务仓库"""
    repo = Mock()
    repo.save = AsyncMock()
    repo.get = AsyncMock()
    repo.delete = AsyncMock()
    repo.exists = AsyncMock(return_value=True)
    repo.update_status = AsyncMock()
    repo.list_by_status = AsyncMock(return_value=[])
    return repo


@pytest.fixture
def mock_queue_scheduler():
    """模拟队列调度器"""
    queue = Mock(spec=QueueScheduler)
    queue.push = AsyncMock(return_value=True)
    queue.pop = AsyncMock(return_value=["task_1", "task_2"])
    queue.remove = AsyncMock(return_value=True)
    queue.size = AsyncMock(return_value=0)
    queue.clear = AsyncMock()
    queue.pause = AsyncMock()
    queue.resume = AsyncMock()
    queue.disable = AsyncMock()
    queue.wait_until_empty = AsyncMock(return_value=True)
    return queue


@pytest.fixture
def mock_event_bus():
    """模拟事件总线"""
    bus = Mock(spec=EventBus)
    bus.emit = AsyncMock()
    bus.start = AsyncMock()
    bus.stop = AsyncMock()
    bus.subscribe = Mock()
    bus.subscribe_global = Mock()
    return bus


@pytest.fixture
def mock_executor():
    """模拟任务执行器"""
    executor = Mock(spec=TaskExecutor)
    executor.execute = AsyncMock(return_value={"result": "success"})
    return executor


@pytest.fixture
def mock_lifecycle(mock_task_repo, mock_event_bus):
    """模拟生命周期管理器"""
    lifecycle = Mock(spec=TaskLifecycleManager)

    # 创建一个模拟任务
    mock_task = Mock(spec=Task)
    mock_task.task_id = "test_task"
    mock_task.status = TaskStatus.PENDING
    mock_task.priority = TaskPriority.NORMAL
    mock_task.data = {"test": "data"}
    mock_task.retry_count = 0
    mock_task.ttl = 3600

    lifecycle.get_task = AsyncMock(return_value=mock_task)
    lifecycle.create_task = AsyncMock(return_value=mock_task)
    lifecycle.start_task = AsyncMock(return_value=True)
    lifecycle.complete_task = AsyncMock(return_value=True)
    lifecycle.fail_task = AsyncMock(return_value=True)
    lifecycle.cancel_task = AsyncMock(return_value=True)
    lifecycle.wait_for_task = AsyncMock(return_value={"result": "success"})
    lifecycle.get_task_stats = AsyncMock(return_value=Mock(total=0, pending=0, running=0, completed=0, failed=0, cancelled=0))

    return lifecycle


class TestWorkerPool:
    """Worker 池测试"""

    @pytest.mark.asyncio
    async def test_start_stop(self, mock_executor, mock_task_repo, mock_queue_scheduler,
                              mock_event_bus, mock_lifecycle):
        """测试启动和停止"""
        # concurrency=2，期望2个worker
        pool = WorkerPool(
            executor=mock_executor,
            task_repo=mock_task_repo,
            queue_scheduler=mock_queue_scheduler,
            event_bus=mock_event_bus,
            lifecycle_manager=mock_lifecycle,
            concurrency=2,  # 明确设置为2
            prefetch_size=10
        )

        await pool.start()
        assert pool._running is True
        assert len(pool._workers) == 2

        await pool.stop()
        assert pool._running is False
        assert len(pool._workers) == 0
        assert len(pool._running_tasks) == 0

    @pytest.mark.asyncio
    async def test_active_count(self, mock_executor, mock_task_repo, mock_queue_scheduler,
                                mock_event_bus, mock_lifecycle):
        """测试活跃 worker 数量"""
        pool = WorkerPool(
            executor=mock_executor,
            task_repo=mock_task_repo,
            queue_scheduler=mock_queue_scheduler,
            event_bus=mock_event_bus,
            lifecycle_manager=mock_lifecycle,
            concurrency=2,  # 设置为2
            prefetch_size=10
        )

        await pool.start()

        # 初始活跃任务数为0
        assert pool.active_count() == 0

        # 模拟正在执行的任务
        async with pool._lock:
            pool._running_tasks["task_1"] = asyncio.create_task(asyncio.sleep(0.1))
            pool._running_tasks["task_2"] = asyncio.create_task(asyncio.sleep(0.1))

        # 等待任务开始
        await asyncio.sleep(0.05)
        assert pool.active_count() == 2

        await pool.stop()

    @pytest.mark.asyncio
    async def test_cancel_task(self, mock_executor, mock_task_repo, mock_queue_scheduler,
                               mock_event_bus, mock_lifecycle):
        """测试取消任务"""
        pool = WorkerPool(
            executor=mock_executor,
            task_repo=mock_task_repo,
            queue_scheduler=mock_queue_scheduler,
            event_bus=mock_event_bus,
            lifecycle_manager=mock_lifecycle,
            concurrency=2,
            prefetch_size=10
        )

        await pool.start()

        # 模拟一个正在执行的任务
        async def long_task():
            await asyncio.sleep(1)

        async with pool._lock:
            pool._running_tasks["task_to_cancel"] = asyncio.create_task(long_task())

        # 取消任务
        result = await pool.cancel_task("task_to_cancel")
        assert result is True

        # 等待取消完成
        await asyncio.sleep(0.1)

        # 验证任务已从运行中移除
        assert "task_to_cancel" not in pool._running_tasks

        await pool.stop()

    @pytest.mark.asyncio
    async def test_get_stats(self, mock_executor, mock_task_repo, mock_queue_scheduler,
                             mock_event_bus, mock_lifecycle):
        """测试获取 worker 统计"""
        pool = WorkerPool(
            executor=mock_executor,
            task_repo=mock_task_repo,
            queue_scheduler=mock_queue_scheduler,
            event_bus=mock_event_bus,
            lifecycle_manager=mock_lifecycle,
            concurrency=2,
            prefetch_size=10
        )

        await pool.start()

        # 更新一些统计
        for i in range(2):
            pool._worker_stats[i] = WorkerStats(
                worker_id=i,
                active_tasks=i,
                completed_tasks=i * 10,
                failed_tasks=i,
                is_busy=i > 0
            )

        stats = pool.get_stats()
        assert len(stats) == 2
        assert stats[0].worker_id == 0
        assert stats[0].completed_tasks == 0
        assert stats[1].worker_id == 1
        assert stats[1].completed_tasks == 10

        await pool.stop()

    @pytest.mark.asyncio
    async def test_set_retry_config(self, mock_executor, mock_task_repo, mock_queue_scheduler,
                                    mock_event_bus, mock_lifecycle):
        """测试设置重试配置"""
        pool = WorkerPool(
            executor=mock_executor,
            task_repo=mock_task_repo,
            queue_scheduler=mock_queue_scheduler,
            event_bus=mock_event_bus,
            lifecycle_manager=mock_lifecycle,
            concurrency=2,
            prefetch_size=10
        )

        assert pool._max_retries == 3
        assert pool._retry_delay == 1.0

        pool.set_retry_config(max_retries=5, retry_delay=2.0)

        assert pool._max_retries == 5
        assert pool._retry_delay == 2.0

    @pytest.mark.asyncio
    async def test_concurrent_execution(self, mock_executor, mock_task_repo, mock_queue_scheduler,
                                        mock_event_bus, mock_lifecycle):
        """测试并发执行限制"""
        # 设置并发数为1
        pool = WorkerPool(
            executor=mock_executor,
            task_repo=mock_task_repo,
            queue_scheduler=mock_queue_scheduler,
            event_bus=mock_event_bus,
            lifecycle_manager=mock_lifecycle,
            concurrency=1,  # 单并发
            prefetch_size=10
        )

        await pool.start()

        # 模拟 semaphore 限制
        assert pool._semaphore._value == 1

        # 模拟多个任务，但 semaphore 会限制并发
        tasks = []
        async with pool._semaphore:
            # 只能同时执行一个
            pass

        await pool.stop()

    @pytest.mark.asyncio
    async def test_graceful_stop(self, mock_executor, mock_task_repo, mock_queue_scheduler,
                                 mock_event_bus, mock_lifecycle):
        """测试优雅停止"""
        pool = WorkerPool(
            executor=mock_executor,
            task_repo=mock_task_repo,
            queue_scheduler=mock_queue_scheduler,
            event_bus=mock_event_bus,
            lifecycle_manager=mock_lifecycle,
            concurrency=2,
            prefetch_size=10
        )

        await pool.start()

        # 模拟正在执行的任务
        async def slow_task():
            await asyncio.sleep(0.2)

        async with pool._lock:
            pool._running_tasks["slow_task"] = asyncio.create_task(slow_task())

        # 优雅停止，等待任务完成
        await pool.stop(graceful=True, timeout=1.0)

        assert pool._running is False
        assert len(pool._running_tasks) == 0

    @pytest.mark.asyncio
    async def test_force_stop(self, mock_executor, mock_task_repo, mock_queue_scheduler,
                              mock_event_bus, mock_lifecycle):
        """测试强制停止"""
        pool = WorkerPool(
            executor=mock_executor,
            task_repo=mock_task_repo,
            queue_scheduler=mock_queue_scheduler,
            event_bus=mock_event_bus,
            lifecycle_manager=mock_lifecycle,
            concurrency=2,
            prefetch_size=10
        )

        await pool.start()

        # 模拟长时间运行的任务
        async def long_task():
            await asyncio.sleep(10)

        async with pool._lock:
            pool._running_tasks["long_task"] = asyncio.create_task(long_task())

        # 强制停止，不等待任务完成
        await pool.stop(graceful=False)

        assert pool._running is False

    @pytest.mark.asyncio
    async def test_worker_stats_update(self, mock_executor, mock_task_repo, mock_queue_scheduler,
                                       mock_event_bus, mock_lifecycle):
        """测试 worker 统计更新"""
        pool = WorkerPool(
            executor=mock_executor,
            task_repo=mock_task_repo,
            queue_scheduler=mock_queue_scheduler,
            event_bus=mock_event_bus,
            lifecycle_manager=mock_lifecycle,
            concurrency=2,
            prefetch_size=10
        )

        await pool.start()

        # 手动更新统计
        for i in range(2):
            pool._worker_stats[i].completed_tasks = 100
            pool._worker_stats[i].failed_tasks = 5
            pool._worker_stats[i].active_tasks = 2
            pool._worker_stats[i].is_busy = True

        stats = pool.get_stats()
        assert stats[0].completed_tasks == 100
        assert stats[0].failed_tasks == 5
        assert stats[1].completed_tasks == 100
        assert stats[1].failed_tasks == 5

        await pool.stop()
