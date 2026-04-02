"""
@FileName: async_executor.py
@Description: 异步执行器
@Author: HiPeng
@Time: 2026/3/27 23:52
"""
from typing import Any, Dict, Callable, Awaitable

from neotask.executors.base import TaskExecutor


class AsyncExecutor(TaskExecutor):
    """Executor for async functions."""

    def __init__(self, func: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        self._func = func

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self._func(task_data)
