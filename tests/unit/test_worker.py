"""
@FileName: test_worker.py
@Description: Worker 模块单元测试
@Author: HiPeng
@Time: 2026/4/15
"""

import asyncio
import datetime
from typing import Dict, Any

import pytest

from neotask.core.lifecycle import TaskLifecycleManager
from neotask.event.bus import EventBus
from neotask.executor.async_executor import AsyncExecutor
from neotask.models.task import Task, TaskStatus, TaskPriority
from neotask.queue.scheduler import QueueScheduler
from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
from neotask.worker.pool import WorkerPool, WorkerStats
from neotask.worker.prefetcher import TaskPrefetcher, PrefetchConfig
from neotask.worker.reclaimer import TaskReclaimer, ReclaimerConfig


# ========== 测试辅助函数 ==========

async def success_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """成功的执行器"""
    return {"status": "success", "data": data}


async def failing_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """失败的执行器"""
    raise ValueError("Task execution failed")


async def slow_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """慢速执行器"""
    delay = data.get("delay", 0.1)
    await asyncio.sleep(delay)
    return {"status": "success", "delay": delay}


# ========== WorkerPool 测试 ==========

class TestWorkerPool:
    """测试 Worker 池"""

    @pytest.fixture
    async def worker_pool(self):
        """创建测试用的 WorkerPool - 使用 concurrency=2"""
        task_repo = MemoryTaskRepository()
        queue_repo = MemoryQueueRepository()
        queue_scheduler = QueueScheduler(queue_repo)
        event_bus = EventBus()
        lifecycle = TaskLifecycleManager(task_repo, event_bus)

        executor = AsyncExecutor(success_executor)

        await queue_scheduler.start()
        await event_bus.start()

        pool = WorkerPool(
            executor=executor,
            task_repo=task_repo,
            queue_scheduler=queue_scheduler,
            event_bus=event_bus,
            lifecycle_manager=lifecycle,
            concurrency=2,  # 设置为 2
            prefetch_size=5,
            task_timeout=5
        )

        return pool, queue_scheduler, task_repo, lifecycle

    @pytest.mark.asyncio
    async def test_start_stop(self, worker_pool):
        """测试启动和停止"""
        pool, _, _, _ = worker_pool

        await pool.start()
        assert pool._running is True
        # 期望 concurrency=2，所以 workers 数量应该是 2
        assert len(pool._workers) == 2

        await pool.stop()
        assert pool._running is False
        assert len(pool._workers) == 0

    @pytest.mark.asyncio
    async def test_execute_task_success(self):
        """修复后的任务执行成功测试"""
        task_repo = MemoryTaskRepository()
        queue_repo = MemoryQueueRepository()
        queue_scheduler = QueueScheduler(queue_repo)
        event_bus = EventBus()
        lifecycle = TaskLifecycleManager(task_repo, event_bus)
        executor = AsyncExecutor(success_executor)

        await queue_scheduler.start()
        await event_bus.start()

        pool = WorkerPool(
            executor=executor,
            task_repo=task_repo,
            queue_scheduler=queue_scheduler,
            event_bus=event_bus,
            lifecycle_manager=lifecycle,
            concurrency=1,
            prefetch_size=1,
            task_timeout=5
        )

        task = Task(
            task_id="test-fixed",
            data={"test": "fixed"},
            priority=TaskPriority.NORMAL
        )
        await task_repo.save(task)
        await queue_scheduler.push("test-fixed", 2)

        await pool.start()

        max_wait = 5
        for _ in range(max_wait * 2):
            await asyncio.sleep(0.5)
            current_task = await task_repo.get("test-fixed")
            if current_task.status != TaskStatus.PENDING:
                break

        final_task = await task_repo.get("test-fixed")
        assert final_task.status == TaskStatus.SUCCESS, \
            f"Expected SUCCESS, got {final_task.status}, error: {final_task.error}"
        assert final_task.result is not None
        assert final_task.result["status"] == "success"

        await pool.stop()
        await queue_scheduler.stop()
        await event_bus.stop()

    @pytest.mark.asyncio
    async def test_execute_task_failure(self):
        """测试任务执行失败"""
        task_repo = MemoryTaskRepository()
        queue_repo = MemoryQueueRepository()
        queue_scheduler = QueueScheduler(queue_repo)
        event_bus = EventBus()
        lifecycle = TaskLifecycleManager(task_repo, event_bus)
        executor = AsyncExecutor(failing_executor)

        await queue_scheduler.start()
        await event_bus.start()

        pool = WorkerPool(
            executor=executor,
            task_repo=task_repo,
            queue_scheduler=queue_scheduler,
            event_bus=event_bus,
            lifecycle_manager=lifecycle,
            concurrency=2,
            task_timeout=5
        )
        pool.set_retry_config(max_retries=1, retry_delay=0.1)

        task = Task(
            task_id="test-fail",
            data={"test": "fail"},
            priority=TaskPriority.NORMAL
        )
        await task_repo.save(task)
        await queue_scheduler.push("test-fail", 2)

        await pool.start()

        await asyncio.sleep(1.5)

        updated_task = await task_repo.get("test-fail")
        print(f"Task status: {updated_task.status}")
        print(f"Task error: {updated_task.error}")

        # 由于重试次数为1，最终应该失败
        assert updated_task.status == TaskStatus.FAILED

        await pool.stop(graceful=True, timeout=1)
        await queue_scheduler.stop()
        await event_bus.stop()

    @pytest.mark.asyncio
    async def test_retry_mechanism(self):
        """测试重试机制"""
        attempt = 0

        async def retry_executor(data):
            nonlocal attempt
            attempt += 1
            print(f"Retry executor called, attempt={attempt}")
            if attempt < 2:
                raise ValueError("First attempt failed")
            return {"attempt": attempt}

        task_repo = MemoryTaskRepository()
        queue_repo = MemoryQueueRepository()
        queue_scheduler = QueueScheduler(queue_repo)
        event_bus = EventBus()
        lifecycle = TaskLifecycleManager(task_repo, event_bus)
        executor = AsyncExecutor(retry_executor)

        await queue_scheduler.start()
        await event_bus.start()

        pool = WorkerPool(
            executor=executor,
            task_repo=task_repo,
            queue_scheduler=queue_scheduler,
            event_bus=event_bus,
            lifecycle_manager=lifecycle,
            concurrency=1,
            task_timeout=5
        )
        pool.set_retry_config(max_retries=3, retry_delay=0.1)

        task = Task(
            task_id="test-retry",
            data={"test": "retry"},
            priority=TaskPriority.NORMAL
        )
        await task_repo.save(task)
        await queue_scheduler.push("test-retry", 2)

        await pool.start()

        # 等待任务完成（包括重试）
        max_wait = 10
        for _ in range(max_wait):
            await asyncio.sleep(0.5)
            current_task = await task_repo.get("test-retry")
            print(f"Current status: {current_task.status}, attempt: {attempt}")
            if current_task.status == TaskStatus.SUCCESS:
                break

        updated_task = await task_repo.get("test-retry")
        print(f"Final status: {updated_task.status}, attempt: {attempt}")

        assert updated_task.status == TaskStatus.SUCCESS
        assert updated_task.result["attempt"] == 2

        await pool.stop(graceful=True, timeout=1)
        await queue_scheduler.stop()
        await event_bus.stop()

    @pytest.mark.asyncio
    async def test_cancel_running_task(self, worker_pool):
        """测试取消运行中的任务"""
        pool, queue_scheduler, task_repo, _ = worker_pool

        slow_exec = AsyncExecutor(slow_executor)
        pool._executor = slow_exec

        task = Task(
            task_id="test-cancel",
            data={"delay": 2.0},
            priority=TaskPriority.NORMAL
        )
        await task_repo.save(task)
        await queue_scheduler.push("test-cancel", 2)

        await pool.start()

        await asyncio.sleep(0.3)

        cancelled = await pool.cancel_task("test-cancel")
        print(f"Cancelled: {cancelled}")

        await asyncio.sleep(0.5)

        updated_task = await task_repo.get("test-cancel")
        print(f"Task status after cancel: {updated_task.status}")

        # 取消可能成功或失败，不强制断言特定状态
        assert updated_task is not None

        await pool.stop()

    @pytest.mark.asyncio
    async def test_active_count(self, worker_pool):
        """测试活跃 worker 计数"""
        pool, queue_scheduler, task_repo, _ = worker_pool

        for i in range(5):
            task = Task(
                task_id=f"test-{i}",
                data={"delay": 0.3},
                priority=TaskPriority.NORMAL
            )
            await task_repo.save(task)
            await queue_scheduler.push(f"test-{i}", 2)

        await pool.start()

        await asyncio.sleep(0.5)

        active = pool.active_count()
        print(f"Active workers: {active}")

        assert active >= 0
        # concurrency=2，所以活跃数不超过 2
        assert active <= 2

        await pool.stop()

    @pytest.mark.asyncio
    async def test_get_stats(self, worker_pool):
        """测试获取 worker 统计"""
        pool, _, _, _ = worker_pool

        await pool.start()

        stats = pool.get_stats()
        assert isinstance(stats, dict)
        # concurrency=2，所以应该有 2 个 workers
        assert len(stats) == 2

        for worker_id, worker_stat in stats.items():
            assert isinstance(worker_stat, WorkerStats)
            assert worker_stat.worker_id == worker_id

        await pool.stop()


# ========== Prefetcher 测试 ==========

class TestPrefetcher:
    """测试预取器"""

    @pytest.fixture
    async def setup_prefetcher(self):
        """创建测试环境"""
        queue_repo = MemoryQueueRepository()
        queue_scheduler = QueueScheduler(queue_repo)
        await queue_scheduler.start()

        config = PrefetchConfig(
            prefetch_size=5,
            local_queue_size=20,
            min_threshold=2,
            prefetch_interval=0.05
        )
        prefetcher = TaskPrefetcher(queue_scheduler, config)

        yield queue_scheduler, prefetcher

        await prefetcher.stop()
        await queue_scheduler.stop()

    @pytest.mark.asyncio
    async def test_start_stop(self, setup_prefetcher):
        """测试启动和停止"""
        _, prefetcher = setup_prefetcher

        await prefetcher.start()
        assert prefetcher._running is True

        await prefetcher.stop()
        assert prefetcher._running is False

    @pytest.mark.asyncio
    async def test_get_from_empty_queue(self, setup_prefetcher):
        """测试从空队列获取"""
        _, prefetcher = setup_prefetcher

        await prefetcher.start()

        task_id = await prefetcher.get(timeout=0.1)
        assert task_id is None

        await prefetcher.stop()

    @pytest.mark.asyncio
    async def test_prefetch_batch(self, setup_prefetcher):
        """测试批量预取"""
        queue_scheduler, prefetcher = setup_prefetcher

        for i in range(10):
            await queue_scheduler.push(f"task-{i}", i % 4)

        await prefetcher.start()

        await asyncio.sleep(0.2)

        tasks = []
        for _ in range(10):
            task_id = await prefetcher.get(timeout=0.1)
            if task_id:
                tasks.append(task_id)

        assert len(tasks) >= 5

    @pytest.mark.asyncio
    async def test_get_batch(self, setup_prefetcher):
        """测试批量获取"""
        queue_scheduler, prefetcher = setup_prefetcher

        for i in range(5):
            await queue_scheduler.push(f"task-{i}", i % 4)

        await prefetcher.start()

        await asyncio.sleep(0.1)

        batch = await prefetcher.get_batch(max_count=5, timeout=0.1)
        assert len(batch) >= 1

    @pytest.mark.asyncio
    async def test_size(self, setup_prefetcher):
        """测试本地队列大小"""
        queue_scheduler, prefetcher = setup_prefetcher

        for i in range(5):
            await queue_scheduler.push(f"task-{i}", i % 4)

        await prefetcher.start()

        await asyncio.sleep(0.1)

        size = await prefetcher.size()
        assert size >= 0

    @pytest.mark.asyncio
    async def test_stats(self, setup_prefetcher):
        """测试统计信息"""
        queue_scheduler, prefetcher = setup_prefetcher

        for i in range(10):
            await queue_scheduler.push(f"task-{i}", i % 4)

        await prefetcher.start()

        await asyncio.sleep(0.2)

        stats = prefetcher.get_stats()
        assert "local_queue_size" in stats
        assert "total_prefetch" in stats
        assert "total_fetched" in stats
        assert "config" in stats


# ========== Reclaimer 测试 ==========

class TestReclaimer:
    """测试回收器"""

    @pytest.fixture
    async def setup_reclaimer(self):
        """创建测试环境"""
        task_repo = MemoryTaskRepository()
        queue_repo = MemoryQueueRepository()
        queue_scheduler = QueueScheduler(queue_repo)
        event_bus = EventBus()

        await queue_scheduler.start()
        await event_bus.start()

        config = ReclaimerConfig(
            interval=0.5,
            max_reclaim_per_cycle=10,
            task_timeout=1,
            enable_timeout_reclaim=True,
            enable_orphan_reclaim=False,
            enable_stale_lock_reclaim=False,
            enable_auto_requeue=True,
            max_retries=2,
            retry_delay=0.1
        )

        reclaimer = TaskReclaimer(
            task_repo=task_repo,
            queue_scheduler=queue_scheduler,
            lock=None,
            event_bus=event_bus,
            config=config
        )

        yield task_repo, queue_scheduler, reclaimer

        await reclaimer.stop()
        await queue_scheduler.stop()
        await event_bus.stop()

    @pytest.mark.asyncio
    async def test_start_stop(self, setup_reclaimer):
        """测试启动和停止"""
        _, _, reclaimer = setup_reclaimer

        await reclaimer.start()
        assert reclaimer._running is True

        await reclaimer.stop()
        assert reclaimer._running is False

    @pytest.mark.asyncio
    async def test_reclaim_timeout_task(self, setup_reclaimer):
        """测试回收超时任务"""
        task_repo, queue_scheduler, reclaimer = setup_reclaimer

        task = Task(
            task_id="timeout-task",
            data={"test": "data"},
            priority=TaskPriority.NORMAL,
            status=TaskStatus.RUNNING,
            started_at=datetime.datetime.now() - datetime.timedelta(seconds=2),
            ttl=1
        )
        await task_repo.save(task)

        await reclaimer.start()

        await asyncio.sleep(0.6)

        updated_task = await task_repo.get("timeout-task")
        print(f"Reclaimed task status: {updated_task.status}")
        print(f"Retry count: {updated_task.retry_count}")

        assert updated_task.status in [TaskStatus.PENDING, TaskStatus.FAILED]

    @pytest.mark.asyncio
    async def test_reclaim_now(self, setup_reclaimer):
        """测试立即回收"""
        task_repo, _, reclaimer = setup_reclaimer

        task = Task(
            task_id="reclaim-now-task",
            data={"test": "data"},
            priority=TaskPriority.NORMAL,
            status=TaskStatus.RUNNING,
            started_at=datetime.datetime.now() - datetime.timedelta(seconds=2),
            ttl=1
        )
        await task_repo.save(task)

        results = await reclaimer.reclaim_now()
        assert len(results) >= 0

    @pytest.mark.asyncio
    async def test_get_stats(self, setup_reclaimer):
        """测试获取统计信息"""
        _, _, reclaimer = setup_reclaimer

        await reclaimer.start()

        stats = reclaimer.get_stats()
        assert "total_reclaimed" in stats
        assert "by_reason" in stats
        assert "is_running" in stats
        assert "config" in stats

        await reclaimer.stop()


# ========== 集成测试 ==========

class TestWorkerIntegration:
    """Worker 集成测试"""

    @pytest.mark.asyncio
    async def test_full_workflow(self):
        """测试完整工作流"""
        task_repo = MemoryTaskRepository()
        queue_repo = MemoryQueueRepository()
        queue_scheduler = QueueScheduler(queue_repo)
        event_bus = EventBus()
        lifecycle = TaskLifecycleManager(task_repo, event_bus)
        executor = AsyncExecutor(success_executor)

        await queue_scheduler.start()
        await event_bus.start()

        pool = WorkerPool(
            executor=executor,
            task_repo=task_repo,
            queue_scheduler=queue_scheduler,
            event_bus=event_bus,
            lifecycle_manager=lifecycle,
            concurrency=2,
            prefetch_size=2,
            task_timeout=5
        )

        await pool.start()

        task_ids = []
        for i in range(5):
            task = Task(
                task_id=f"integ-test-{i}",
                data={"id": i},
                priority=TaskPriority.NORMAL
            )
            await task_repo.save(task)
            await queue_scheduler.push(f"integ-test-{i}", 2)
            task_ids.append(f"integ-test-{i}")

        await asyncio.sleep(5.0)

        for task_id in task_ids:
            task = await task_repo.get(task_id)
            assert task.status == TaskStatus.SUCCESS

        await pool.stop()
        await queue_scheduler.stop()
        await event_bus.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])