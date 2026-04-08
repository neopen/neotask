"""
@FileName: process_executor.py
@Description: 进程执行器
@Author: HiPeng
@Time: 2026/3/27 23:56
"""

import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, Callable, Optional
import functools
import multiprocessing

from neotask.executors.base import TaskExecutor
from neotask.executors.exceptions import ExecutionTimeoutError


class ProcessExecutor(TaskExecutor):
    """Executor for CPU-bound tasks using process pool.

    This executor runs CPU-intensive synchronous functions in a process pool
    to bypass the GIL and utilize multiple CPU cores.

    Examples:
        >>> def cpu_intensive(data):
        ...     # Heavy computation here
        ...     return {"result": "123456"}
        >>>
        >>> executor = ProcessExecutor(cpu_intensive, max_workers=4)
    """

    def __init__(self, func: Callable[[Dict[str, Any]], Dict[str, Any]],
                 max_workers: Optional[int] = None,
                 timeout: Optional[float] = None):
        """Initialize with sync function and process pool.

        Args:
            func: Synchronous function for CPU-bound work
            max_workers: Maximum number of worker processes (default: CPU count)
            timeout: Optional timeout in seconds for execution
        """
        self._func = func
        self._max_workers = max_workers or multiprocessing.cpu_count()
        self._timeout = timeout
        self._executor: Optional[ProcessPoolExecutor] = None
        self._shutdown = False

    def _get_executor(self) -> ProcessPoolExecutor:
        """Lazy initialization of process pool."""
        if self._executor is None and not self._shutdown:
            self._executor = ProcessPoolExecutor(max_workers=self._max_workers)
        return self._executor

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute CPU-bound function in process pool with optional timeout."""
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
                f"Process task execution timed out after {self._timeout} seconds"
            )

    async def shutdown(self, wait: bool = True) -> None:
        """Shutdown process pool.

        Args:
            wait: Whether to wait for pending tasks to complete
        """
        self._shutdown = True
        if self._executor:
            self._executor.shutdown(wait=wait)
            self._executor = None