"""
@FileName: process_executor.py
@Description: 进程执行器 - 在进程池中执行CPU密集型任务（支持 cloudpickle 处理嵌套函数）
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, Callable, Optional

try:
    import cloudpickle
    HAS_CLOUDPICKLE = True
except ImportError:
    HAS_CLOUDPICKLE = False

from neotask.executor.base import TaskExecutor


class ProcessExecutor(TaskExecutor):
    """进程执行器

    在进程池中执行CPU密集型任务，绕过GIL限制。

    支持：
    - 模块级函数
    - 静态方法
    - 使用 cloudpickle 时支持嵌套函数和 lambda

    使用示例：
        >>> # 模块级函数
        >>> def cpu_task(data):
        ...     return {"result": sum(range(data["value"]))}
        >>> executor = ProcessExecutor(cpu_task)
        >>>
        >>> # 使用 cloudpickle 支持嵌套函数
        >>> executor = ProcessExecutor(cpu_task, use_cloudpickle=True)
    """

    def __init__(
            self,
            func: Callable,
            max_workers: Optional[int] = None,
            use_cloudpickle: bool = False
    ):
        """初始化进程执行器

        Args:
            func: 要执行的函数（必须是可pickle的）
            max_workers: 最大工作进程数，默认使用CPU核心数
            use_cloudpickle: 是否使用 cloudpickle 序列化（支持嵌套函数）
        """
        self._func = func
        self._max_workers = max_workers
        self._executor: Optional[ProcessPoolExecutor] = None
        self._use_cloudpickle = use_cloudpickle and HAS_CLOUDPICKLE

        if self._use_cloudpickle:
            self._pickled_func = cloudpickle.dumps(func)
        else:
            self._pickled_func = None

    def _get_func(self) -> Callable:
        """获取可调用的函数"""
        if self._use_cloudpickle and self._pickled_func:
            return cloudpickle.loads(self._pickled_func)
        return self._func

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

        func = self._get_func()

        # 如果是协程函数，直接 await
        if inspect.iscoroutinefunction(func):
            return await func(task_data)

        # 检查是否需要回退到线程执行器
        if not self._use_cloudpickle and not self._is_picklable(func):
            from neotask.executor.thread_executor import ThreadExecutor
            fallback = ThreadExecutor(func, max_workers=1)
            return await fallback.execute(task_data)

        # 同步函数在进程池中执行
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._get_executor(),
            func,
            task_data
        )

    def _is_picklable(self, func: Callable) -> bool:
        """检查函数是否可 pickle"""
        import pickle
        try:
            pickle.dumps(func)
            return True
        except (pickle.PicklingError, TypeError, AttributeError):
            return False

    async def shutdown(self) -> None:
        """关闭进程池"""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

    def __repr__(self) -> str:
        return f"ProcessExecutor(func={self._func.__name__}, max_workers={self._max_workers})"
