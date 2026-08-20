"""
@FileName: factory.py
@Description: 执行器工厂 - Factory Pattern
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import inspect
from enum import Enum
from typing import Callable, Optional

from neotask.executor.async_executor import AsyncExecutor
from neotask.executor.base import TaskExecutor
from neotask.executor.class_executor import ClassExecutor
from neotask.executor.process_executor import ProcessExecutor
from neotask.executor.thread_executor import ThreadExecutor


class ExecutorType(Enum):
    """执行器类型枚举"""
    ASYNC = "async"
    THREAD = "thread"
    PROCESS = "process"
    CLASS = "class"
    AUTO = "auto"


class ExecutorFactory:
    """执行器工厂

    设计模式：Factory Pattern - 根据类型创建执行器
    """

    @staticmethod
    def create(
            func: Callable,
            executor_type: ExecutorType = ExecutorType.AUTO,
            max_workers: Optional[int] = None
    ) -> TaskExecutor:
        """创建执行器

        Args:
            func: 要执行的函数
            executor_type: 执行器类型 (async, thread, process, auto, class)
            max_workers: 最大工作线程数

        Returns:
            TaskExecutor 实例
        """
        # 处理 Enum 类型输入
        # if isinstance(executor_type, Enum):
        #     executor_type = executor_type.value

        if executor_type == ExecutorType.AUTO:
            # 自动选择执行器类型
            if inspect.iscoroutinefunction(func):
                return ExecutorFactory._create_async(func)
            elif hasattr(func, 'execute') and callable(getattr(func, 'execute')):
                # 如果对象有 execute 方法，使用 ClassExecutor
                return ExecutorFactory._create_class(func)
            else:
                return ExecutorFactory._create_thread(func, max_workers or 10)
        elif executor_type == ExecutorType.ASYNC:
            return ExecutorFactory._create_async(func)
        elif executor_type == ExecutorType.THREAD:
            return ExecutorFactory._create_thread(func, max_workers or 10)
        elif executor_type == ExecutorType.PROCESS:
            return ExecutorFactory._create_process(func, max_workers)
        elif executor_type == ExecutorType.CLASS:
            return ExecutorFactory._create_class(func)
        else:
            raise ValueError(f"Unknown executor type: {executor_type}")

    @staticmethod
    def _create_async(func: Callable) -> AsyncExecutor:
        """创建异步执行器"""
        # 如果是同步函数，包装为异步函数
        if not inspect.iscoroutinefunction(func):
            async def wrapper(data):
                return func(data)

            func = wrapper
        return AsyncExecutor(func)

    @staticmethod
    def _create_thread(func: Callable, max_workers: int) -> ThreadExecutor:
        """创建线程执行器"""
        return ThreadExecutor(func, max_workers=max_workers)

    @staticmethod
    def _create_process(func: Callable, max_workers: Optional[int]) -> ProcessExecutor:
        """创建进程执行器"""
        return ProcessExecutor(func, max_workers=max_workers)

    @staticmethod
    def _create_class(obj: object) -> ClassExecutor:
        """创建类执行器"""
        return ClassExecutor(obj)


# 便捷函数
def create_executor(
        func: Callable,
        executor_type: ExecutorType = ExecutorType.AUTO,
        max_workers: Optional[int] = None
) -> TaskExecutor:
    """创建执行器的便捷函数"""
    return ExecutorFactory.create(func, executor_type, max_workers)

