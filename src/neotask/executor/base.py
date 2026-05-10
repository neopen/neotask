"""
@FileName: base.py
@Description: 执行器接口
@Author: HiPeng
@Time: 2026/3/27 23:52
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Callable, Optional
import inspect


class TaskExecutor(ABC):
    """Abstract task executor interface."""

    @abstractmethod
    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute task and return result."""
        pass

    async def execute_with_progress(
        self,
        task_data: Dict[str, Any],
        progress_callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """Execute task with progress reporting.

        Args:
            task_data: Task data
            progress_callback: Async callback for progress updates
                               signature: async def callback(progress: float, message: str)

        Returns:
            Task result
        """
        # Default implementation falls back to execute()
        return await self.execute(task_data)

    async def shutdown(self) -> None:
        """Optional shutdown hook for cleanup."""
        pass


class CallbackExecutor(TaskExecutor):
    """Executor wrapper for callback functions."""

    def __init__(self, func: Callable):
        self._func = func

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute callback function."""
        if inspect.iscoroutinefunction(self._func):
            return await self._func(task_data)
        else:
            import asyncio
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._func, task_data)