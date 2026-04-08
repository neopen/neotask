"""
@FileName: async_executor.py
@Description: 异步执行器
@Author: HiPeng
@Time: 2026/3/27 23:52
"""

import asyncio
from typing import Any, Dict, Callable, Awaitable, Optional
import inspect
from neotask.executors.base import TaskExecutor
from neotask.executors.exceptions import ExecutionTimeoutError, ExecutionCancelledError


class AsyncExecutor(TaskExecutor):
    """Executor for async functions.

    This executor wraps an async callable and executes it directly.

    Examples:
        >>> async def my_task(data):
        ...     return {"result": data["value"] * 2}
        >>>
        >>> executor = AsyncExecutor(my_task)
        >>> result = executor.execute({"value": 10})
    """

    def __init__(self, func: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
                 timeout: Optional[float] = None):
        """Initialize with async function.

        Args:
            func: Async function that takes task_data and returns result
            timeout: Optional timeout in seconds for execution

        Raises:
            TypeError: If func is not an async function
        """
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"Expected async function, got {type(func).__name__}")
        self._func = func
        self._timeout = timeout
        self._current_task: Optional[asyncio.Task] = None

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the async function with optional timeout and cancellation support."""
        try:
            if self._timeout is not None:
                # Execute with timeout
                self._current_task = asyncio.current_task()
                return await asyncio.wait_for(
                    self._func(task_data),
                    timeout=self._timeout
                )
            else:
                # Execute without timeout
                return await self._func(task_data)
        except asyncio.TimeoutError:
            raise ExecutionTimeoutError(
                f"Task execution timed out after {self._timeout} seconds"
            )
        except asyncio.CancelledError:
            raise ExecutionCancelledError("Task execution was cancelled")

    async def cancel(self) -> bool:
        """Cancel the currently running task.

        Returns:
            True if task was cancelled, False if no task is running
        """
        if self._current_task and not self._current_task.done():
            return self._current_task.cancel()
        return False

    async def shutdown(self) -> None:
        """Cleanup resources."""
        await self.cancel()