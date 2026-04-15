"""
@FileName: async_executor.py
@Description: 异步执行器 - 直接调用异步函数
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

from typing import Any, Dict, Callable
import inspect

from neotask.executor.base import TaskExecutor


class AsyncExecutor(TaskExecutor):
    """异步执行器

    直接执行异步函数，无额外线程开销。

    使用示例：
        >>> async def my_func(data):
        ...     return {"result": data}
        >>>
        >>> executor = AsyncExecutor(my_func)
        >>> result = await executor.execute({"key": "value"})
    """

    def __init__(self, func: Callable):
        """初始化异步执行器

        Args:
            func: 要执行的异步函数
        """
        self._func = func

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """执行异步任务

        Args:
            task_data: 任务数据

        Returns:
            执行结果
        """
        # 如果是同步函数，包装为异步
        if not inspect.iscoroutinefunction(self._func):
            return self._func(task_data)
        return await self._func(task_data)

    def __repr__(self) -> str:
        return f"AsyncExecutor(func={self._func.__name__})"