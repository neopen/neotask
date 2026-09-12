"""
@FileName: exceptions.py
@Description: Storage module exceptions.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

class StorageError(Exception):
    """Base exception for storage errors."""
    pass


class TaskNotFoundError(StorageError):
    """Raised when a task is not found."""
    pass


class QueueEmptyError(StorageError):
    """Raised when trying to pop from an empty queue."""
    pass


class ConnectionError(StorageError):
    """Raised when connection to storage fails."""
    pass


class TransactionError(StorageError):
    """Raised when a transaction fails."""
    pass