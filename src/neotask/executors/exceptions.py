"""
@FileName: exceptions.py
@Description: Executor module exceptions.
@Author: HiPeng
@Time: 2026/4/8 00:00
"""


class ExecutorError(Exception):
    """Base exception for executor errors."""
    pass


class ExecutionTimeoutError(ExecutorError):
    """Raised when task execution times out."""
    pass


class ExecutionCancelledError(ExecutorError):
    """Raised when task execution is cancelled."""
    pass


class InvalidExecutorError(ExecutorError):
    """Raised when executor is invalid."""
    pass