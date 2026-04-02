"""
@FileName: exceptions.py
@Description: 异常定义
@Author: HiPeng
@Time: 2026/4/1 19:02
"""
class TaskSchedulerError(Exception):
    """Base exception for task scheduler."""
    pass


class TaskNotFoundError(TaskSchedulerError):
    """Raised when task is not found."""

    def __init__(self, task_id: str):
        super().__init__(f"Task not found: {task_id}")
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


class TimeoutError(TaskSchedulerError):
    """Raised when wait operation times out."""

    def __init__(self, task_id: str, timeout: float):
        super().__init__(f"Timeout waiting for task {task_id} after {timeout}s")
        self.task_id = task_id
        self.timeout = timeout