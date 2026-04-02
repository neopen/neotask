"""
@FileName: scheduler.py
@Description: 主调度器。对外 API，生命周期管理
@Author: HiPeng
@Time: 2026/4/1 18:16
"""

import asyncio
import threading
import uuid
from datetime import timezone, datetime
from typing import Optional, Any, Dict, Callable, Union

from neotask.core.future import FutureManager
from neotask.core.queue import PriorityQueue
from neotask.core.worker import WorkerPool
from neotask.executors.base import TaskExecutor
from neotask.models.config import SchedulerConfig
from neotask.models.task import Task, TaskPriority, TaskStatus
from neotask.monitor.event_bus import EventBus, TaskEvent
from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
from neotask.storage.redis import RedisTaskRepository, RedisQueueRepository
from neotask.web.server import WebUIServer


class TaskScheduler:
    """Main scheduler facade - single entry point for all operations."""

    def __init__(self, executor: TaskExecutor, config: Optional[SchedulerConfig] = None):
        self._config = config or SchedulerConfig()
        self._executor = executor

        # Initialize storage based on config
        self._task_repo, self._queue_repo = self._create_storage()

        # Initialize components
        self._queue = PriorityQueue(self._queue_repo, self._config.queue.max_size)
        self._future_manager = FutureManager()
        self._event_bus = EventBus()

        # Initialize worker pool
        self._worker_pool = WorkerPool(
            executor=self._executor,
            task_repo=self._task_repo,
            queue=self._queue,
            future_manager=self._future_manager,
            event_bus=self._event_bus,
            node_id=self._config.node_id,
            max_concurrent=self._config.worker.max_concurrent,
            prefetch_size=self._config.worker.prefetch_size,
        )

        # Start background loop
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        # Start worker pool
        self._run_async(self._worker_pool.start())

        # Start web UI if enabled
        self._webui = None
        if self._config.webui.enabled:
            self._webui = WebUIServer(
                scheduler=self,
                host=self._config.webui.host,
                port=self._config.webui.port,
                auto_open=self._config.webui.auto_open,
            )
            self._webui.start()

    def _create_storage(self):
        """Create storage repositories based on config."""
        storage_type = self._config.storage.type

        if storage_type == "memory":
            return MemoryTaskRepository(), MemoryQueueRepository()

        elif storage_type == "redis":
            redis_url = self._config.storage.redis_url
            if not redis_url:
                raise ValueError("Redis URL is required for Redis storage")
            return (
                RedisTaskRepository(redis_url),
                RedisQueueRepository(redis_url)
            )

        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")

    def _run_loop(self) -> None:
        """Run asyncio event loop in background thread."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run_async(self, coro) -> Any:
        """Run coroutine and wait for result."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def _run_async_future(self, coro) -> asyncio.Future:
        """Run coroutine and return future."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    # ========== Task Submission ==========

    def submit(self, data: Dict[str, Any], priority: Union[int, TaskPriority] = TaskPriority.NORMAL) -> str:
        """Submit task for immediate execution."""
        priority_value = priority.value if isinstance(priority, TaskPriority) else priority
        return self._run_async(self._submit_async(data, priority_value))

    async def submit_async(self, data: Dict[str, Any], priority: Union[int, TaskPriority] = TaskPriority.NORMAL) -> str:
        """Submit task asynchronously."""
        priority_value = priority.value if isinstance(priority, TaskPriority) else priority
        return await self._submit_async(data, priority_value)

    async def _submit_async(self, data: Dict[str, Any], priority: int) -> str:
        """Async implementation of submit."""
        task_id = self._generate_task_id()
        task = Task(
            task_id=task_id,
            data=data,
            priority=TaskPriority(priority),
        )

        # Save task
        await self._task_repo.save(task)

        # Add to queue
        success = await self._queue.push(task_id, TaskPriority(priority))
        if not success:
            raise Exception(f"Queue is full (max: {self._config.queue.max_size})")

        # Create future for waiting
        await self._future_manager.create(task_id)

        # Emit event
        await self._event_bus.emit(TaskEvent("task.submitted", task_id))

        return task_id

    # ========== Task Waiting ==========

    # ========== Task Waiting ==========

    def wait_for_result(self, task_id: str, timeout: float = 300) -> Dict[str, Any]:
        """Wait for task completion synchronously.

        Args:
            task_id: Task identifier
            timeout: Maximum wait time in seconds

        Returns:
            Task result dictionary

        Raises:
            TimeoutError: If task doesn't complete within timeout
            Exception: If task failed with error
        """
        return self._run_async(self._wait_for_result_async(task_id, timeout))

    async def wait_for_result_async(self, task_id: str, timeout: float = 300) -> Dict[str, Any]:
        """Wait for task completion asynchronously.

        Args:
            task_id: Task identifier
            timeout: Maximum wait time in seconds

        Returns:
            Task result dictionary

        Raises:
            TimeoutError: If task doesn't complete within timeout
            Exception: If task failed with error
        """
        return await self._wait_for_result_async(task_id, timeout)

    async def _wait_for_result_async(self, task_id: str, timeout: float) -> Dict[str, Any]:
        """Internal async implementation of wait_for_result."""
        task_future = await self._future_manager.get(task_id)
        return await task_future.wait(timeout)

    # ========== Task Query ==========

    def get_status(self, task_id: str) -> Optional[str]:
        """Get task status."""
        task = self._run_async(self._task_repo.get(task_id))
        return task.status.value if task else None

    def get_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task result."""
        task = self._run_async(self._task_repo.get(task_id))
        if not task:
            return None
        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "result": task.result,
            "error": task.error,
            "created_at": task.created_at.isoformat(),
            "started_at": task.started_at.isoformat() if task.started_at else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        }

    def list_tasks(self, status: Optional[str] = None, limit: int = 100) -> list:
        """List tasks, optionally filtered by status."""
        if status:
            task_status = TaskStatus(status)
            tasks = self._run_async(self._task_repo.list_by_status(task_status, limit))
        else:
            # Get from queue and processing
            tasks = []
        return [t.to_dict() for t in tasks]

    # ========== Task Control ==========

    def cancel(self, task_id: str) -> bool:
        """Cancel a task."""
        return self._run_async(self._cancel_async(task_id))

    async def _cancel_async(self, task_id: str) -> bool:
        """Async implementation of cancel."""
        return await self._worker_pool.cancel_task(task_id)

    # ========== Statistics ==========

    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        return self._run_async(self._get_stats_async())

    async def _get_stats_async(self) -> Dict[str, Any]:
        """Async implementation of get_stats."""
        return {
            "queue_size": await self._queue.size(),
            "node_id": self._config.node_id,
            "max_concurrent": self._config.worker.max_concurrent,
            "storage_type": self._config.storage.type,
            "webui_enabled": self._config.webui.enabled,
        }

    # ========== Lifecycle ==========

    def shutdown(self) -> None:
        """Shutdown scheduler gracefully."""
        # Stop web UI
        if self._webui:
            self._webui.stop()

        # Stop worker pool
        self._run_async(self._worker_pool.stop())

        # Stop event loop
        self._loop.call_soon_threadsafe(self._loop.stop)

    # ========== Event Callbacks ==========

    def on_task_submitted(self, handler: Callable, async_handler: bool = False) -> None:
        """Register handler for task submitted event."""
        self._event_bus.subscribe("task.submitted", handler, async_handler)

    def on_task_started(self, handler: Callable, async_handler: bool = False) -> None:
        """Register handler for task started event."""
        self._event_bus.subscribe("task.started", handler, async_handler)

    def on_task_completed(self, handler: Callable, async_handler: bool = False) -> None:
        """Register handler for task completed event."""
        self._event_bus.subscribe("task.completed", handler, async_handler)

    def on_task_failed(self, handler: Callable, async_handler: bool = False) -> None:
        """Register handler for task failed event."""
        self._event_bus.subscribe("task.failed", handler, async_handler)

    # ========== Utilities ==========

    def _generate_task_id(self) -> str:
        """Generate unique task ID."""
        return "TSK" + datetime.now(timezone.utc).strftime("%Y%m%d") + uuid.uuid4().hex[:10]
