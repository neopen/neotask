"""
@FileName: thread_executor.py
@Description: 线程执行器 - 在线程池中执行同步函数
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Callable, Optional

from neotask.executor.base import TaskExecutor


class ThreadExecutor(TaskExecutor):
    """线程执行器

    在线程池中执行同步函数，避免阻塞事件循环。

    使用示例：
        >>> def sync_func(data):
        ...     return {"result": data}
        >>>
        >>> executor = ThreadExecutor(sync_func, max_workers=4)
        >>> result = await executor.execute({"key": "value"})
    """

    def __init__(self, func: Callable, max_workers: int = 10):
        """初始化线程执行器

        Args:
            func: 要执行的函数（同步或异步）
            max_workers: 最大工作线程数
        """
        self._func = func
        self._max_workers = max_workers
        self._executor: Optional[ThreadPoolExecutor] = None

    def _get_executor(self) -> ThreadPoolExecutor:
        """获取线程池"""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="neotask-worker"
            )
        return self._executor

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """执行任务

        Args:
            task_data: 任务数据

        Returns:
            执行结果
        """
        import inspect

        # 如果是协程函数，直接await
        if inspect.iscoroutinefunction(self._func):
            return await self._func(task_data)

        # 同步函数在线程池中执行
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._get_executor(),
            self._func,
            task_data
        )

    async def shutdown(self) -> None:
        """关闭线程池"""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

    def __repr__(self) -> str:
        return f"ThreadExecutor(func={self._func.__name__}, max_workers={self._max_workers})"