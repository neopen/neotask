"""
@FileName: config.py
@Description: 配置
@Author: HiPeng
@Time: 2026/4/1 18:23
"""

from dataclasses import dataclass, field
from typing import Optional, Literal


@dataclass
class StorageConfig:
    """Storage configuration."""

    type: Literal["memory", "redis", "sqlite"] = "memory"
    redis_url: Optional[str] = None
    sqlite_path: Optional[str] = None

    @classmethod
    def memory(cls) -> "StorageConfig":
        """Create memory storage config."""
        return cls(type="memory")

    @classmethod
    def redis(cls, url: str) -> "StorageConfig":
        """Create Redis storage config."""
        return cls(type="redis", redis_url=url)

    @classmethod
    def sqlite(cls, path: str = "tasks.db") -> "StorageConfig":
        """Create SQLite storage config."""
        return cls(type="sqlite", sqlite_path=path)


@dataclass
class LockConfig:
    """Distributed lock configuration."""

    type: Literal["memory", "redis"] = "memory"
    redis_url: Optional[str] = None
    lock_timeout: int = 30
    heartbeat_interval: int = 10
    watch_dog_enabled: bool = True

    @classmethod
    def memory(cls) -> "LockConfig":
        """Create memory lock config."""
        return cls(type="memory")

    @classmethod
    def redis(cls, url: str, timeout: int = 30) -> "LockConfig":
        """Create Redis lock config."""
        return cls(type="redis", redis_url=url, lock_timeout=timeout)


@dataclass
class WorkerConfig:
    """Worker pool configuration."""

    max_concurrent: int = 10
    prefetch_size: int = 20
    min_threshold: int = 5
    task_timeout: int = 300
    retry_max: int = 3

    @classmethod
    def default(cls) -> "WorkerConfig":
        """Create default worker config."""
        return cls()

    @classmethod
    def high_performance(cls) -> "WorkerConfig":
        """Create high performance worker config."""
        return cls(max_concurrent=50, prefetch_size=100)


@dataclass
class QueueConfig:
    """Queue configuration."""

    max_size: int = 1000
    priority_levels: int = 4

    @classmethod
    def default(cls) -> "QueueConfig":
        """Create default queue config."""
        return cls()


@dataclass
class WebUIConfig:
    """Web UI configuration."""

    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8080
    auto_open: bool = False
    enable_websocket: bool = True

    @classmethod
    def disabled(cls) -> "WebUIConfig":
        """Create disabled web UI config."""
        return cls(enabled=False)

    @classmethod
    def enabled(cls, port: int = 8080, auto_open: bool = False) -> "WebUIConfig":
        """Create enabled web UI config."""
        return cls(enabled=True, port=port, auto_open=auto_open)


@dataclass
class SchedulerConfig:
    """Main scheduler configuration."""

    node_id: str = ""
    storage: StorageConfig = field(default_factory=StorageConfig.memory)
    lock: LockConfig = field(default_factory=LockConfig.memory)
    worker: WorkerConfig = field(default_factory=WorkerConfig.default)
    queue: QueueConfig = field(default_factory=QueueConfig.default)
    webui: WebUIConfig = field(default_factory=WebUIConfig.disabled)

    def __post_init__(self):
        """Generate node ID if not provided."""
        if not self.node_id:
            import socket
            self.node_id = socket.gethostname()

    @classmethod
    def memory(cls, node_id: Optional[str] = None) -> "SchedulerConfig":
        """Create memory-only config (single node)."""
        config = cls()
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def redis(cls, redis_url: str, node_id: Optional[str] = None) -> "SchedulerConfig":
        """Create Redis-based config (distributed)."""
        config = cls(
            storage=StorageConfig.redis(redis_url),
            lock=LockConfig.redis(redis_url),
        )
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def with_webui(cls, port: int = 8080, auto_open: bool = False, **kwargs) -> "SchedulerConfig":
        """Create config with web UI enabled."""
        config = cls(**kwargs)
        config.webui = WebUIConfig.enabled(port, auto_open)
        return config
