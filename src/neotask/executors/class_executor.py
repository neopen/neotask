"""
@FileName: class_executor.py
@Description: Class executor for instances with execute method.
@Author: HiPeng
@Time: 2026/4/2 22:10
"""

import asyncio
import inspect
from typing import Any, Dict, Optional
from neotask.executors.base import TaskExecutor
from neotask.executors.exceptions import ExecutionTimeoutError, ExecutionCancelledError


class ClassExecutor(TaskExecutor):
    """Executor for class instances with execute method.

    This executor wraps any object that has an execute method (sync or async).

    Examples:
        >>> class MyWorker:
        ...     async def execute(self, data):
        ...         return {"result": data["value"] * 2}
        >>>
        >>> executor = ClassExecutor(MyWorker())

        >>> class SyncWorker:
        ...     def execute(self, data):
        ...         return {"result": data["value"] * 2}
        >>>
        >>> executor = ClassExecutor(SyncWorker())  # Auto-wraps in thread pool
    """

    def __init__(self, instance: Any, timeout: Optional[float] = None):
        """Initialize with class instance.

        Args:
            instance: Object with execute method
            timeout: Optional timeout in seconds for execution

        Raises:
            TypeError: If instance doesn't have an execute method
        """
        self._instance = instance
        self._timeout = timeout
        self._current_task: Optional[asyncio.Task] = None

        # Validate that instance has an execute method
        if not hasattr(instance, 'execute'):
            raise TypeError(f"{type(instance).__name__} must have an 'execute' method")

        if not callable(instance.execute):
            raise TypeError(f"{type(instance).__name__}.execute must be callable")

        # Check if execute is async or sync
        self._is_async = inspect.iscoroutinefunction(instance.execute)

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute instance's execute method with optional timeout."""
        try:
            if self._is_async:
                # Async execute
                if self._timeout is not None:
                    self._current_task = asyncio.current_task()
                    return await asyncio.wait_for(
                        self._instance.execute(task_data),
                        timeout=self._timeout
                    )
                else:
                    return await self._instance.execute(task_data)
            else:
                # Sync execute - run in thread pool
                import concurrent.futures
                loop = asyncio.get_event_loop()

                if self._timeout is not None:
                    future = loop.run_in_executor(
                        None, self._instance.execute, task_data
                    )
                    return await asyncio.wait_for(future, timeout=self._timeout)
                else:
                    return await loop.run_in_executor(
                        None, self._instance.execute, task_data
                    )
        except asyncio.TimeoutError:
            raise ExecutionTimeoutError(
                f"Class task execution timed out after {self._timeout} seconds"
            )
        except asyncio.CancelledError:
            raise ExecutionCancelledError("Class task execution was cancelled")

    async def cancel(self) -> bool:
        """Cancel the currently running async task.

        Returns:
            True if task was cancelled, False if no task is running or not async
        """
        if self._is_async and self._current_task and not self._current_task.done():
            return self._current_task.cancel()
        return False