"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: factory.py
@Description: Storage factory for creating repository instances.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 21:32
"""

from typing import Any, Tuple, Optional, cast
from neotask.common.exceptions import UnsupportedFeatureError
from neotask.models.config import StorageConfig, StorageType
from neotask.storage.base import (
    CheckpointRepository,
    TaskRepository,
    QueueRepository,
)
from neotask.storage.memory import (
    MemoryCheckpointRepository,
    MemoryTaskRepository,
    MemoryQueueRepository,
)
from neotask.storage.redis import (
    RedisCheckpointRepository,
    RedisTaskRepository,
    RedisQueueRepository,
)
from neotask.storage.sqlite import (
    SQLiteCheckpointRepository,
    SQLiteTaskRepository,
    SQLiteQueueRepository,
)

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
            storage_type: StorageType,
            sqlite_path: str,
            redis_url: Optional[str],
            *,
            persist_queue: bool = True,
            redis_max_connections: int = 50,
    ) -> Tuple[TaskRepository, QueueRepository]:
        """初始化存储。

        Args:
            storage_type: 存储类型枚举（``StorageType.MEMORY`` 等）
            sqlite_path: SQLite 数据库路径
            redis_url: Redis 连接 URL
            persist_queue: SQLite 下是否持久化队列
            redis_max_connections: Redis 客户端连接池上限

        Returns:
            Tuple of (TaskRepository, QueueRepository)
        """
        from neotask.models.config import StorageConfig

        storage_type = StorageType.coerce(storage_type)
        if storage_type == StorageType.MEMORY:
            config = StorageConfig.memory()
        elif storage_type == StorageType.SQLITE:
            config = StorageConfig.sqlite(sqlite_path, persist_queue=persist_queue)
        elif storage_type == StorageType.REDIS:
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

        if storage_type == StorageType.MEMORY:
            return MemoryTaskRepository(), MemoryQueueRepository()

        elif storage_type == StorageType.REDIS:
            if not config.redis_url:
                raise ValueError("Redis URL is required for Redis storage")
            return (
                RedisTaskRepository(config.redis_url, config.redis_max_connections),
                RedisQueueRepository(config.redis_url, config.redis_max_connections)
            )

        elif storage_type == StorageType.SQLITE:
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

    @staticmethod
    def create_checkpoint_repo(
            config: StorageConfig,
            *,
            connection: Optional[Any] = None,
    ) -> CheckpointRepository:
        """Create checkpoint repository（与任务/队列仓储同构的独立工厂）。

        单独成函数是为了不改 ``create()`` 的返回签名：既有装配代码零改动，
        新增能力只需在构造函数里多一个可选参数。

        Args:
            config: 存储配置
            connection: SQLite 专用，测试注入共享连接。调用方须保证本仓储
                先于该连接关闭；默认路径自行取共享连接并按引用计数释放。

        Returns:
            CheckpointRepository 实例

        Raises:
            UnsupportedFeatureError: 后端未注册 checkpoint 仓储
        """
        storage_type = config.type

        if storage_type == StorageType.MEMORY:
            return MemoryCheckpointRepository()

        if storage_type == StorageType.SQLITE:
            pragmas = _sqlite_pragmas(config)
            return SQLiteCheckpointRepository(
                config.sqlite_path,
                pragmas=pragmas,
                connection=connection,
            )

        if storage_type == StorageType.REDIS:
            if not config.redis_url:
                raise ValueError("Redis URL is required for Redis storage")
            return RedisCheckpointRepository(
                config.redis_url, config.redis_max_connections
            )

        raise UnsupportedFeatureError("checkpoint", str(storage_type))


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
    def register(
            cls,
            name: str,
            task_repo_class,
            queue_repo_class,
            ckpt_repo_class=None,
    ):
        """Register a new repository type.

        Args:
            name: Storage type name
            task_repo_class: Task repository class
            queue_repo_class: Queue repository class
            ckpt_repo_class: 可选的 checkpoint 仓储类；未注册的后端调用
                :meth:`create_checkpoint_repo` 会显式报「后端不支持」
        """
        cls._repositories[name] = {
            "task": task_repo_class,
            "queue": queue_repo_class,
            "checkpoint": ckpt_repo_class,
        }

    @classmethod
    def create_checkpoint_repo(cls, name: str, **kwargs: Any) -> CheckpointRepository:
        """按注册名创建 checkpoint 仓储。

        Raises:
            UnsupportedFeatureError: 后端未注册 ``ckpt_repo_class``
        """
        entry = cls._repositories.get(name)
        factory = entry.get("checkpoint") if entry else None
        if factory is None:
            raise UnsupportedFeatureError("checkpoint", name)
        return cast(CheckpointRepository, factory(**kwargs))

    @staticmethod
    def create(config: StorageConfig) -> Tuple[TaskRepository, QueueRepository]:
        """创建存储实例

        Args:
            config: 存储配置

        Returns:
            (TaskRepository, QueueRepository)
        """
        if config.type == StorageType.MEMORY:
            return MemoryTaskRepository(), MemoryQueueRepository()
        elif config.type == StorageType.SQLITE:
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
        elif config.type == StorageType.REDIS:
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
        await task_repo.close()
        await queue_repo.close()


# Register default repositories
RepositoryFactory.register(
    "memory", MemoryTaskRepository, MemoryQueueRepository,
    MemoryCheckpointRepository,
)
RepositoryFactory.register(
    "redis", RedisTaskRepository, RedisQueueRepository,
    RedisCheckpointRepository,
)
RepositoryFactory.register(
    "sqlite", SQLiteTaskRepository, SQLiteQueueRepository,
    SQLiteCheckpointRepository,
)