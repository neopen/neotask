"""
@FileName: exceptions.py
@Description: 异常定义
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/1 19:02
"""
from typing import Optional


class TaskSchedulerError(Exception):
    """Base exception for task scheduler."""
    pass


class TaskNotFoundError(TaskSchedulerError):
    """Raised when task is not found."""

    def __init__(self, task_id: str):
        super().__init__(f"Task not found: {task_id}")
        self.task_id = task_id

class TaskError(TaskSchedulerError):
    def __init__(self, task_id: str):
        super().__init__(f"Task Error: {task_id}")
        self.task_id = task_id

class TaskAlreadyExistsError(TaskSchedulerError):
    """Raised when task already exists."""

    def __init__(self, task_id: str):
        super().__init__(f"Task already exists: {task_id}")
        self.task_id = task_id


class QueueFullError(TaskSchedulerError):
    """Raised when queue is full."""

    def __init__(self, queue_size: int):
        super().__init__(f"Queue is full (max: {queue_size})")
        self.queue_size = queue_size


class BatchSubmitError(TaskSchedulerError):
    """批量提交失败（默认全有或全无语义）

    只要任意一条任务创建或入队失败，整批任务都不会保留，已写入的任务
    会在抛出前回滚。``index`` 为失败任务在下发批次中的位置，``cause``
    为底层异常（序列化失败、存储不可写等）。
    """

    def __init__(self, message: str, index: int = -1, cause: Optional[BaseException] = None):
        super().__init__(message)
        self.index = index
        self.cause = cause


class TimeoutError(TaskSchedulerError):
    """Raised when wait operation times out."""

    def __init__(self, task_id: str, timeout: float):
        super().__init__(f"Timeout waiting for task {task_id} after {timeout}s")
        self.task_id = task_id
        self.timeout = timeout