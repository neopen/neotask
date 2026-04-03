"""
@FileName: __init__.py.py
@Description: NeoTask - 轻量级 Python 异步任务队列管理器。
@Author: HiPeng
@Time: 2026/4/1 19:02
"""

from neotask.api import TaskPool, TaskScheduler, TaskExecutor, SchedulerConfig, TaskPriority
from neotask.api.task_scheduler import TaskScheduler
from neotask.executors.async_executor import AsyncExecutor
from neotask.executors.base import TaskExecutor
from neotask.models.config import SchedulerConfig
from neotask.models.task import TaskPriority

__version__ = "0.1.0"

__all__ = [
    "TaskPool",
    "TaskScheduler",
    "TaskExecutor",
    "SchedulerConfig",
    "TaskPriority",
]
