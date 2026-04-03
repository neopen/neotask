"""
@FileName: process_executor.py
@Description: 进程执行器
@Author: HiPeng
@Time: 2026/3/27 23:56
"""

import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, Callable

from neotask.executors.base import TaskExecutor


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
                 max_workers: int = None):
        """Initialize with sync function and process pool.

        Args:
            func: Synchronous function for CPU-bound work
            max_workers: Maximum number of worker processes (default: CPU count)
        """
        self._func = func
        self._executor = ProcessPoolExecutor(max_workers=max_workers)

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute CPU-bound function in process pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._func, task_data)

    async def shutdown(self) -> None:
        """Shutdown process pool."""
        self._executor.shutdown(wait=True)
