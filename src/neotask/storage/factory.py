"""
@FileName: factory.py
@Description: Storage factory for creating repository instances.
@Author: HiPeng
@Time: 2026/4/2 21:32
"""

from typing import Tuple, Optional
from neotask.models.config import StorageConfig
from neotask.storage.base import TaskRepository, QueueRepository
from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
from neotask.storage.redis import RedisTaskRepository, RedisQueueRepository
from neotask.storage.sqlite import SQLiteTaskRepository, SQLiteQueueRepository


class StorageFactory:
    """Factory for creating storage repositories.

    This factory implements the Factory pattern to create appropriate
    repository instances based on configuration.

    Examples:
        >>> config = StorageConfig.memory()
        >>> task_repo, queue_repo = StorageFactory.create(config)

        >>> config = StorageConfig.redis("redis://localhost:6379")
        >>> task_repo, queue_repo = StorageFactory.create(config)
    """

    @staticmethod
    def create_storage(storage_type: str, sqlite_path: str, redis_url: Optional[str]):
        """初始化存储。"""
        from neotask.models.config import StorageConfig

        if storage_type == "memory":
            config = StorageConfig.memory()
        elif storage_type == "sqlite":
            config = StorageConfig.sqlite(sqlite_path)
        elif storage_type == "redis":
            if not redis_url:
                raise ValueError("Redis URL is required for redis storage")
            config = StorageConfig.redis(redis_url)
        else:
            raise ValueError(f"Unknown storage type: {storage_type}")

        return StorageFactory.create(config)


    @staticmethod
    def create(config: StorageConfig) -> Tuple[TaskRepository, QueueRepository]:
        """Create task and queue repositories based on configuration.

        Args:
            config: Storage configuration

        Returns:
            Tuple of (TaskRepository, QueueRepository)

        Raises:
            ValueError: If storage type is not supported
        """
        storage_type = config.type

        if storage_type == "memory":
            return MemoryTaskRepository(), MemoryQueueRepository()

        elif storage_type == "redis":
            if not config.redis_url:
                raise ValueError("Redis URL is required for Redis storage")
            return (
                RedisTaskRepository(config.redis_url),
                RedisQueueRepository(config.redis_url)
            )

        elif storage_type == "sqlite":
            return (
                SQLiteTaskRepository(config.sqlite_path),
                SQLiteQueueRepository(config.sqlite_path)
            )

        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")

    @staticmethod
    def create_task_repository(config: StorageConfig) -> TaskRepository:
        """Create only task repository.

        Args:
            config: Storage configuration

        Returns:
            TaskRepository instance
        """
        task_repo, _ = StorageFactory.create(config)
        return task_repo

    @staticmethod
    def create_queue_repository(config: StorageConfig) -> QueueRepository:
        """Create only queue repository.

        Args:
            config: Storage configuration

        Returns:
            QueueRepository instance
        """
        _, queue_repo = StorageFactory.create(config)
        return queue_repo


class RepositoryFactory:
    """Alternative factory with registration pattern.

    This allows dynamic registration of new storage backends.

    Examples:
        >>> class CustomRepository(TaskRepository): pass
        >>> factory = RepositoryFactory()
        >>> factory.register("custom", CustomRepository)
        >>> repo = factory.create("custom", **kwargs)
    """

    _repositories: dict = {}

    @classmethod
    def register(cls, name: str, task_repo_class, queue_repo_class):
        """Register a new repository type.

        Args:
            name: Storage type name
            task_repo_class: Task repository class
            queue_repo_class: Queue repository class
        """
        cls._repositories[name] = {
            "task": task_repo_class,
            "queue": queue_repo_class
        }

    @classmethod
    def create(cls, config: StorageConfig) -> Tuple[TaskRepository, QueueRepository]:
        """Create repositories from registered types.

        Args:
            config: Storage configuration

        Returns:
            Tuple of (TaskRepository, QueueRepository)
        """
        storage_type = config.type

        if storage_type not in cls._repositories:
            raise ValueError(f"Unknown storage type: {storage_type}")

        repo_classes = cls._repositories[storage_type]

        if storage_type == "memory":
            return repo_classes["task"](), repo_classes["queue"]()

        elif storage_type == "redis":
            if not config.redis_url:
                raise ValueError("Redis URL is required for Redis storage")
            return (
                repo_classes["task"](config.redis_url),
                repo_classes["queue"](config.redis_url)
            )

        elif storage_type == "sqlite":
            return (
                repo_classes["task"](config.sqlite_path),
                repo_classes["queue"](config.sqlite_path)
            )

        else:
            return (
                repo_classes["task"](),
                repo_classes["queue"]()
            )


# Register default repositories
RepositoryFactory.register("memory", MemoryTaskRepository, MemoryQueueRepository)
RepositoryFactory.register("redis", RedisTaskRepository, RedisQueueRepository)
RepositoryFactory.register("sqlite", SQLiteTaskRepository, SQLiteQueueRepository)