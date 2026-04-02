"""
@FileName: future.py
@Description: 任务 Task Future for async wait operations.
@Author: HiPeng
@Time: 2026/4/1 17:41
"""

import asyncio
from typing import Optional, Any
from neotask.common.exceptions import TimeoutError


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
        self._result = result
        self._completed = True
        self._event.set()

    def set_error(self, error: str) -> None:
        """Set task error and notify waiters."""
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
                self._futures[task_id] = TaskFuture(task_id)
            return self._futures[task_id]

    async def complete(self, task_id: str, result: Any = None, error: str = None) -> None:
        """Complete a future."""
        async with self._lock:
            future = self._futures.get(task_id)
            if future:
                if error:
                    future.set_error(error)
                else:
                    future.set_result(result)
                del self._futures[task_id]

    async def remove(self, task_id: str) -> None:
        """Remove a future."""
        async with self._lock:
            self._futures.pop(task_id, None)
