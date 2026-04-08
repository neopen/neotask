"""
@FileName: thread_executor.py
@Description: 线程执行器
@Author: HiPeng
@Time: 2026/3/27 23:53
"""

import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Callable, Optional

from neotask.executors.base import TaskExecutor
from neotask.executors.exceptions import ExecutionTimeoutError


class ThreadExecutor(TaskExecutor):
    """Executor for synchronous functions using thread pool.

    This executor runs synchronous functions in a thread pool to avoid
    blocking the asyncio event loop.

    Examples:
        >>> def my_task(data):
        ...     return {"result": data["value"] * 2}
        >>>
        >>> executor = ThreadExecutor(my_task, max_workers=4)
    """

    def __init__(self, func: Callable[[Dict[str, Any]], Dict[str, Any]],
                 max_workers: int = 10,
                 timeout: Optional[float] = None):
        """Initialize with sync function and thread pool.

        Args:
            func: Synchronous function that takes task_data and returns result
            max_workers: Maximum number of worker threads
            timeout: Optional timeout in seconds for execution
        """
        self._func = func
        self._max_workers = max_workers
        self._timeout = timeout
        self._executor: Optional[ThreadPoolExecutor] = None
        self._shutdown = False

    def _get_executor(self) -> ThreadPoolExecutor:
        """Lazy initialization of thread pool."""
        if self._executor is None and not self._shutdown:
            self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        return self._executor

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute sync function in thread pool with optional timeout."""
        executor = self._get_executor()
        loop = asyncio.get_event_loop()

        # Create a partial function to pass task_data
        func = functools.partial(self._func, task_data)

        try:
            if self._timeout is not None:
                # Execute with timeout
                future = loop.run_in_executor(executor, func)
                return await asyncio.wait_for(future, timeout=self._timeout)
            else:
                # Execute without timeout
                return await loop.run_in_executor(executor, func)
        except asyncio.TimeoutError:
            raise ExecutionTimeoutError(
                f"Thread task execution timed out after {self._timeout} seconds"
            )

    async def shutdown(self, wait: bool = True) -> None:
        """Shutdown thread pool.

        Args:
            wait: Whether to wait for pending tasks to complete
        """
        self._shutdown = True
        if self._executor:
            self._executor.shutdown(wait=wait)
            self._executor = None
