"""
@FileName: pool.py
@Description: Worker池 - 管理任务执行器
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

from neotask.core.lifecycle import TaskLifecycleManager
from neotask.event.bus import EventBus, TaskEvent
from neotask.executor.base import TaskExecutor
from neotask.models.task import TaskStatus
from neotask.queue.scheduler import QueueScheduler
from neotask.storage.base import TaskRepository


@dataclass
class WorkerStats:
    """Worker统计信息"""
    worker_id: int
    active_tasks: int
    completed_tasks: int = 0
    failed_tasks: int = 0
    last_active: Optional[datetime] = None
    is_busy: bool = False


class WorkerPool:
    """Worker池

    管理多个worker协程，控制并发执行。
    """

    def __init__(
            self,
            executor: TaskExecutor,
            task_repo: TaskRepository,
            queue_scheduler: QueueScheduler,
            event_bus: EventBus,
            lifecycle_manager: TaskLifecycleManager,
            concurrency: int = 10,
            prefetch_size: int = 20,
            task_timeout: Optional[float] = None
    ):
        self._executor = executor
        self._task_repo = task_repo
        self._queue = queue_scheduler
        self._event_bus = event_bus
        self._lifecycle = lifecycle_manager
        self._concurrency = concurrency
        self._prefetch_size = prefetch_size
        self._task_timeout = task_timeout

        self._workers: Dict[int, asyncio.Task] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._worker_stats: Dict[int, WorkerStats] = {}
        self._semaphore = asyncio.Semaphore(concurrency)
        self._running = False
        self._lock = asyncio.Lock()

        # 重试配置
        self._max_retries = 3
        self._retry_delay = 1.0

    async def start(self) -> None:
        """启动worker池"""
        if self._running:
            return

        self._running = True

        for i in range(self._concurrency):
            worker_task = asyncio.create_task(self._worker_loop(i))
            self._workers[i] = worker_task
            self._worker_stats[i] = WorkerStats(worker_id=i, active_tasks=0)

    async def stop(self, graceful: bool = True, timeout: float = 30) -> None:
        """停止worker池

        Args:
            graceful: 是否优雅停止（等待当前任务完成）
            timeout: 优雅停止超时时间
        """
        self._running = False

        if graceful and self._running_tasks:
            # 等待当前任务完成
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._running_tasks.values(), return_exceptions=True),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                # 超时后强制取消
                for task in self._running_tasks.values():
                    if not task.done():
                        task.cancel()

        # 取消所有worker
        for worker_id, worker_task in self._workers.items():
            if not worker_task.done():
                worker_task.cancel()

        # 等待worker完成
        if self._workers:
            await asyncio.gather(*self._workers.values(), return_exceptions=True)

        self._workers.clear()
        self._running_tasks.clear()

    async def cancel_task(self, task_id: str) -> bool:
        """取消运行中的任务"""
        if task_id in self._running_tasks:
            self._running_tasks[task_id].cancel()
            return True
        return False

    def active_count(self) -> int:
        """获取活跃worker数量"""
        return len(self._running_tasks)

    def get_stats(self) -> Dict[int, WorkerStats]:
        """获取worker统计信息"""
        return self._worker_stats.copy()

    def set_retry_config(self, max_retries: int, retry_delay: float) -> None:
        """设置重试配置"""
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    async def _worker_loop(self, worker_id: int) -> None:
        """Worker主循环"""
        while self._running:
            try:
                # 获取任务
                task_ids = await self._queue.pop(self._prefetch_size)

                for task_id in task_ids:
                    # 启动任务执行
                    exec_task = asyncio.create_task(self._execute_task(worker_id, task_id))
                    async with self._lock:
                        self._running_tasks[task_id] = exec_task
                        self._worker_stats[worker_id].active_tasks += 1
                        self._worker_stats[worker_id].is_busy = True

                # 清理已完成的任务
                await self._cleanup_completed_tasks()

                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                await asyncio.sleep(0.5)

    async def _execute_task(self, worker_id: int, task_id: str) -> None:
        """执行单个任务"""
        async with self._semaphore:
            # 获取任务
            task = await self._lifecycle.get_task(task_id)
            if not task or task.status != TaskStatus.PENDING:
                return

            # 开始执行
            if not await self._lifecycle.start_task(task_id, f"worker-{worker_id}"):
                return

            retry_count = 0

            while retry_count <= self._max_retries:
                try:
                    # 执行任务
                    if self._task_timeout:
                        result = await asyncio.wait_for(
                            self._executor.execute(task.data),
                            timeout=self._task_timeout
                        )
                    else:
                        result = await self._executor.execute(task.data)

                    # 标记完成
                    await self._lifecycle.complete_task(task_id, result)

                    # 更新统计
                    self._worker_stats[worker_id].completed_tasks += 1

                    return

                except asyncio.CancelledError:
                    await self._lifecycle.cancel_task(task_id)
                    return

                except asyncio.TimeoutError:
                    error = f"Task execution timeout after {self._task_timeout}s"
                    if retry_count < self._max_retries:
                        retry_count += 1
                        await self._handle_retry(task_id, error, retry_count)
                    else:
                        await self._lifecycle.fail_task(task_id, error)
                        self._worker_stats[worker_id].failed_tasks += 1
                    return

                except Exception as e:
                    error = str(e)
                    if retry_count < self._max_retries:
                        retry_count += 1
                        await self._handle_retry(task_id, error, retry_count)
                    else:
                        await self._lifecycle.fail_task(task_id, error)
                        self._worker_stats[worker_id].failed_tasks += 1
                    return

            await self._cleanup_completed_tasks()

    async def _handle_retry(self, task_id: str, error: str, retry_count: int) -> None:
        """处理任务重试"""
        # 更新任务状态
        task = await self._lifecycle.get_task(task_id)
        if task:
            task.retry_count = retry_count
            await self._task_repo.save(task)

        # 重新入队
        await self._queue.push(task_id, task.priority.value, delay=self._retry_delay)

        # 发送重试事件
        await self._event_bus.emit(TaskEvent(
            "task.retry",
            task_id,
            {"error": error, "retry_count": retry_count}
        ))

    async def _cleanup_completed_tasks(self) -> None:
        """清理已完成的任务"""
        async with self._lock:
            completed = [
                (tid, task) for tid, task in self._running_tasks.items()
                if task.done()
            ]

            for task_id, task in completed:
                self._running_tasks.pop(task_id, None)

                # 更新worker统计
                for stats in self._worker_stats.values():
                    if stats.active_tasks > 0:
                        stats.active_tasks -= 1
                    stats.is_busy = stats.active_tasks > 0
                    stats.last_active = datetime.now()
