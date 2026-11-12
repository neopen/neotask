"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

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


class NeoTaskError(TaskSchedulerError):
    """能力级错误基类。

    用于「当前后端/配置不支持某特性」这类可预期失败；调用方据此决定降级
    或换后端，而不是把它当成任务执行失败。
    """
    pass


class UnsupportedFeatureError(NeoTaskError):
    """后端或运行配置不支持请求的能力

    Args:
        feature: 能力名，如 ``checkpoint`` / ``blackboard``
        backend: 后端名，如 ``redis`` / ``sqlite``
    """

    def __init__(self, feature: str, backend: str):
        super().__init__(
            f"{feature} is not supported by the '{backend}' backend"
        )
        self.feature = feature
        self.backend = backend


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