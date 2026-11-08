"""
@FileName: factory.py
@Description: Storage factory for creating repository instances.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 21:32
"""

from typing import Tuple, Optional
from neotask.models.config import StorageConfig
from neotask.storage.base import TaskRepository, QueueRepository
from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
from neotask.storage.redis import RedisTaskRepository, RedisQueueRepository
from neotask.storage.sqlite import SQLiteTaskRepository, SQLiteQueueRepository

#: synchronous 允许的取值，避免配置值直接拼进 PRAGMA
_VALID_SYNCHRONOUS = {"OFF", "NORMAL", "FULL", "EXTRA"}


def _sqlite_pragmas(config: StorageConfig) -> Tuple[str, ...]:
    """根据存储配置生成 SQLite 连接 PRAGMA"""
    synchronous = str(config.sqlite_synchronous).upper()
    if synchronous not in _VALID_SYNCHRONOUS:
        synchronous = "NORMAL"
    busy_timeout_ms = max(0, int(config.sqlite_busy_timeout * 1000))
    return (
        "PRAGMA journal_mode=WAL" if config.sqlite_wal else "PRAGMA journal_mode=DELETE",
        f"PRAGMA synchronous={synchronous}",
        f"PRAGMA busy_timeout={busy_timeout_ms}",
    )


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
    def create_storage(
            storage_type: str,
            sqlite_path: str,
            redis_url: Optional[str],
            *,
            persist_queue: bool = True,
            redis_max_connections: int = 50,
    ) -> Tuple[TaskRepository, QueueRepository]:
        """初始化存储。

        Args:
            storage_type: 存储类型 ("memory", "sqlite", "redis")
            sqlite_path: SQLite 数据库路径
            redis_url: Redis 连接 URL
            persist_queue: SQLite 下是否持久化队列
            redis_max_connections: Redis 客户端连接池上限

        Returns:
            Tuple of (TaskRepository, QueueRepository)
        """
        from neotask.models.config import StorageConfig

        if storage_type == "memory":
            config = StorageConfig.memory()
        elif storage_type == "sqlite":
            config = StorageConfig.sqlite(sqlite_path, persist_queue=persist_queue)
        elif storage_type == "redis":
            if not redis_url:
                raise ValueError("Redis URL is required for redis storage")
            config = StorageConfig.redis(redis_url, redis_max_connections)
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
                RedisTaskRepository(config.redis_url, config.redis_max_connections),
                RedisQueueRepository(config.redis_url, config.redis_max_connections)
            )

        elif storage_type == "sqlite":
            pragmas = _sqlite_pragmas(config)
            task_repo = SQLiteTaskRepository(config.sqlite_path, pragmas=pragmas)
            queue_repo: QueueRepository
            if config.persist_queue:
                queue_repo = SQLiteQueueRepository(config.sqlite_path, pragmas=pragmas)
            else:
                # 队列不落盘：重启后由任务表的 PENDING 任务重建队列
                queue_repo = MemoryQueueRepository()
            return task_repo, queue_repo

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

    @staticmethod
    def create(config: StorageConfig) -> Tuple[TaskRepository, QueueRepository]:
        """创建存储实例

        Args:
            config: 存储配置

        Returns:
            (TaskRepository, QueueRepository)
        """
        if config.type == "memory":
            return MemoryTaskRepository(), MemoryQueueRepository()
        elif config.type == "sqlite":
            pragmas = _sqlite_pragmas(config)
            queue_repo = (
                SQLiteQueueRepository(config.sqlite_path, pragmas=pragmas)
                if config.persist_queue
                else MemoryQueueRepository()
            )
            return (
                SQLiteTaskRepository(config.sqlite_path, pragmas=pragmas),
                queue_repo
            )
        elif config.type == "redis":
            if not config.redis_url:
                raise ValueError("Redis URL is required for redis storage")
            return (
                RedisTaskRepository(config.redis_url, config.redis_max_connections),
                RedisQueueRepository(config.redis_url, config.redis_max_connections)
            )
        else:
            raise ValueError(f"Unknown storage type: {config.type}")

    @staticmethod
    async def close(repositories: Tuple[TaskRepository, QueueRepository]) -> None:
        """关闭存储连接"""
        task_repo, queue_repo = repositories
        if hasattr(task_repo, 'close'):
            await task_repo.close()
        if hasattr(queue_repo, 'close'):
            await queue_repo.close()


# Register default repositories
RepositoryFactory.register("memory", MemoryTaskRepository, MemoryQueueRepository)
RepositoryFactory.register("redis", RedisTaskRepository, RedisQueueRepository)
RepositoryFactory.register("sqlite", SQLiteTaskRepository, SQLiteQueueRepository)