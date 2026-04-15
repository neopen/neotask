"""
@FileName: process_executor.py
@Description: 进程执行器 - 在进程池中执行CPU密集型任务
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, Callable, Optional

from neotask.executor.base import TaskExecutor


class ProcessExecutor(TaskExecutor):
    """进程执行器

    在进程池中执行CPU密集型任务，绕过GIL限制。

    使用示例：
        >>> def cpu_intensive(data):
        ...     # CPU密集型计算
        ...     return {"result": result}
        >>>
        >>> executor = ProcessExecutor(cpu_intensive, max_workers=4)
        >>> result = await executor.execute({"key": "value"})
    """

    def __init__(self, func: Callable, max_workers: Optional[int] = None):
        """初始化进程执行器

        Args:
            func: 要执行的函数（必须是可pickle的）
            max_workers: 最大工作进程数，默认使用CPU核心数
        """
        self._func = func
        self._max_workers = max_workers
        self._executor: Optional[ProcessPoolExecutor] = None

    def _get_executor(self) -> ProcessPoolExecutor:
        """获取进程池"""
        if self._executor is None:
            self._executor = ProcessPoolExecutor(
                max_workers=self._max_workers
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

        # 注意：ProcessPoolExecutor 要求函数可 pickle
        # 协程函数不能直接序列化，需要特殊处理
        if inspect.iscoroutinefunction(self._func):
            # 对于异步函数，回退到线程执行器
            from neotask.executor.thread_executor import ThreadExecutor
            fallback = ThreadExecutor(self._func, max_workers=1)
            return await fallback.execute(task_data)

        # 同步函数在进程池中执行
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._get_executor(),
            self._func,
            task_data
        )

    async def shutdown(self) -> None:
        """关闭进程池"""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

    def __repr__(self) -> str:
        return f"ProcessExecutor(func={self._func.__name__}, max_workers={self._max_workers})"