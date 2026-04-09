"""
@FileName: future.py
@Description: 任务 Task Future for async wait operations.
@Author: HiPeng
@Time: 2026/4/1 17:41
"""

import asyncio
from typing import Optional, Any
from neotask.common.exceptions import TimeoutError
from neotask.common.logger import debug


class TaskFuture:
    """Future object for waiting task completion."""

    def __init__(self, task_id: str):
        self.task_id = task_id
        self._event = asyncio.Event()
        self._result: Optional[Any] = None
        self._error: Optional[str] = None
        self._completed = False

    def set_result(self, result: Any) -> None:
        """Set task result and notify waiters."""
        debug(f"[Future] Setting result for task {self.task_id}")
        self._result = result
        self._completed = True
        self._event.set()

    def set_error(self, error: str) -> None:
        """Set task error and notify waiters."""
        debug(f"[Future] Setting error for task {self.task_id}: {error}")
        self._error = error
        self._completed = True
        self._event.set()

    async def wait(self, timeout: float = 300) -> Any:
        """Wait for task completion."""
        try:
            await asyncio.wait_for(self._event.wait(), timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(self.task_id, timeout)

        if self._error:
            raise Exception(self._error)
        return self._result

    @property
    def is_completed(self) -> bool:
        """Check if task is completed."""
        return self._completed


class FutureManager:
    """Manager for task futures."""

    def __init__(self):
        self._futures: dict[str, TaskFuture] = {}
        self._lock = asyncio.Lock()

    async def create(self, task_id: str) -> TaskFuture:
        """Create a new future."""
        async with self._lock:
            future = TaskFuture(task_id)
            self._futures[task_id] = future
            return future

    async def get(self, task_id: str) -> TaskFuture:
        """Get existing future or create new."""
        async with self._lock:
            if task_id not in self._futures:
                debug(f"[FutureManager] Creating new future for task {task_id}")
                self._futures[task_id] = TaskFuture(task_id)
            return self._futures[task_id]

    async def complete(self, task_id: str, result: Any = None, error: str = None) -> None:
        """Complete a future."""
        async with self._lock:
            future = self._futures.get(task_id)
            if future:
                debug(f"[FutureManager] Completing future for task {task_id}, error={error}")
                if error:
                    future.set_error(error)
                else:
                    future.set_result(result)
            else:
                debug(f"[FutureManager] No future found for task {task_id}")

    async def remove(self, task_id: str) -> None:
        """Remove a future."""
        async with self._lock:
            self._futures.pop(task_id, None)

    async def get_and_remove(self, task_id: str) -> Optional[TaskFuture]:
        """Get future and remove it from manager."""
        async with self._lock:
            future = self._futures.pop(task_id, None)
            return future

    async def cancel_all(self) -> None:
        """Cancel all pending futures."""
        async with self._lock:
            for task_id, future in self._futures.items():
                if not future.is_completed:
                    future.set_error("Scheduler shutting down")
            self._futures.clear()