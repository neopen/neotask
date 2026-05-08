"""
@FileName: config.py
@Description: 配置 - 包含所有模块的配置类
@Author: HiPeng
@Time: 2026/4/1 18:23
"""

from dataclasses import dataclass, field
from typing import Optional, Literal

from neotask.executor import ExecutorType


@dataclass
class StorageConfig:
    """存储配置"""

    type: Literal["memory", "redis", "sqlite"] = "memory"
    redis_url: Optional[str] = None
    sqlite_path: Optional[str] = None

    @classmethod
    def memory(cls) -> "StorageConfig":
        """创建内存存储配置"""
        return cls(type="memory")

    @classmethod
    def redis(cls, url: str) -> "StorageConfig":
        """创建Redis存储配置"""
        return cls(type="redis", redis_url=url)

    @classmethod
    def sqlite(cls, path: str = "tasks.db") -> "StorageConfig":
        """创建SQLite存储配置"""
        return cls(type="sqlite", sqlite_path=path)


@dataclass
class LockConfig:
    """分布式锁配置"""

    type: Literal["memory", "redis"] = "memory"
    redis_url: Optional[str] = None
    lock_timeout: int = 30
    heartbeat_interval: int = 10
    watch_dog_enabled: bool = True

    @classmethod
    def memory(cls) -> "LockConfig":
        """创建内存锁配置"""
        return cls(type="memory")

    @classmethod
    def redis(cls, url: str, timeout: int = 30) -> "LockConfig":
        """创建Redis锁配置"""
        return cls(type="redis", redis_url=url, lock_timeout=timeout)


@dataclass
class WorkerConfig:
    """Worker池配置"""

    max_concurrent: int = 10
    prefetch_size: int = 20
    min_threshold: int = 5
    task_timeout: int = 300
    retry_max: int = 3
    retry_delay: float = 1.0

    @classmethod
    def default(cls) -> "WorkerConfig":
        """创建默认Worker配置"""
        return cls()

    @classmethod
    def high_performance(cls) -> "WorkerConfig":
        """创建高性能Worker配置"""
        return cls(max_concurrent=50, prefetch_size=100)


@dataclass
class QueueConfig:
    """队列配置"""

    max_size: int = 1000
    priority_levels: int = 4
    delayed_queue_check_interval: float = 0.1

    @classmethod
    def default(cls) -> "QueueConfig":
        """创建默认队列配置"""
        return cls()


@dataclass
class WebUIConfig:
    """Web UI配置"""

    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8080
    auto_open: bool = False
    enable_websocket: bool = True

    @classmethod
    def disable(cls) -> "WebUIConfig":
        """创建禁用Web UI配置"""
        return cls(enabled=False)

    @classmethod
    def enable(cls, port: int = 8080, auto_open: bool = False) -> "WebUIConfig":
        """创建启用Web UI配置"""
        return cls(enabled=True, port=port, auto_open=auto_open)


@dataclass
class ExecutorConfig:
    """执行器配置"""

    type: str = "async"  # async, thread, process, class, auto
    max_workers: Optional[int] = None

    @classmethod
    def async_executor(cls) -> "ExecutorConfig":
        """创建异步执行器配置"""
        return cls(type="async")

    @classmethod
    def thread_executor(cls, max_workers: int = 10) -> "ExecutorConfig":
        """创建线程执行器配置"""
        return cls(type="thread", max_workers=max_workers)

    @classmethod
    def process_executor(cls, max_workers: int = None) -> "ExecutorConfig":
        """创建进程执行器配置"""
        return cls(type="process", max_workers=max_workers)


@dataclass
class DistributedConfig:
    """分布式配置"""
    node_id: str = ""
    redis_url: Optional[str] = None
    enable_heartbeat: bool = True
    heartbeat_interval: int = 5
    enable_election: bool = False
    shard_count: int = 1


@dataclass
class TaskPoolConfig:
    """TaskPool配置 - 专注于即时任务"""

    # 存储配置
    storage_type: str = "memory"
    sqlite_path: str = "neotask.db"
    redis_url: Optional[str] = None

    # 执行器配置
    executor_type: ExecutorType = ExecutorType.ASYNC
    max_workers: int = 10

    # Worker配置
    worker_concurrency: int = 10
    prefetch_size: int = 20
    task_timeout: Optional[float] = None

    # 队列配置
    queue_max_size: int = 10000
    priority_levels: int = 4

    # 锁配置
    lock_type: str = "memory"
    lock_timeout: int = 30

    # 监控配置
    enable_metrics: bool = True
    enable_health_check: bool = True
    enable_reporter: bool = False

    # 重试配置
    max_retries: int = 3
    retry_delay: float = 1.0

    # 预取配置
    enable_prefetch: bool = True
    prefetch_min_threshold: int = 5

    # 回收配置
    enable_reclaimer: bool = True
    reclaimer_interval: float = 30.0

    # 节点标识
    node_id: str = ""

    def __post_init__(self):
        if not self.node_id:
            import socket
            self.node_id = socket.gethostname()

    @classmethod
    def memory(cls, node_id: Optional[str] = None) -> "TaskPoolConfig":
        """创建内存存储配置"""
        config = cls(storage_type="memory")
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def sqlite(cls, path: str = "neotask.db", node_id: Optional[str] = None) -> "TaskPoolConfig":
        """创建SQLite存储配置"""
        config = cls(storage_type="sqlite", sqlite_path=path)
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def redis(cls, url: str, node_id: Optional[str] = None, enable_prefetch: bool = None, prefetch_size: int = None) -> "TaskPoolConfig":
        """创建Redis存储配置"""
        config = cls(storage_type="redis", redis_url=url)
        if node_id:
            config.node_id = node_id
        if enable_prefetch:
            config.enable_prefetch = enable_prefetch
        if prefetch_size:
            config.prefetch_size = prefetch_size

        return config


@dataclass
class SchedulerConfig:
    """调度器配置 - 专注于定时任务"""

    # 存储配置
    storage_type: str = "memory"
    sqlite_path: str = "neotask.db"
    redis_url: Optional[str] = None

    # Worker配置
    worker_concurrency: int = 10

    # 重试配置
    max_retries: int = 3
    retry_delay: float = 1.0

    # 持久化配置
    enable_persistence: bool = False

    # 调度配置
    scan_interval: float = 1.0  # 调度循环扫描间隔（秒）
    enable_periodic_manager: bool = True  # 是否启用周期任务管理器
    enable_time_wheel: bool = False  # 是否启用时间轮（高性能）
    time_wheel_slots: int = 60  # 时间轮槽位数
    time_wheel_tick: float = 1.0  # 时间轮滴答间隔（秒）

    # 周期任务默认配置
    default_max_runs: Optional[int] = None  # 默认最大执行次数
    default_missed_policy: str = "skip"  # 默认错过执行策略: ignore, run_once, catch_up, skip

    @classmethod
    def memory(cls) -> "SchedulerConfig":
        """创建内存存储配置"""
        return cls(storage_type="memory")

    @classmethod
    def sqlite(cls, path: str = "neotask.db") -> "SchedulerConfig":
        """创建SQLite存储配置"""
        return cls(storage_type="sqlite", sqlite_path=path)

    @classmethod
    def redis(cls, url: str) -> "SchedulerConfig":
        """创建Redis存储配置"""
        return cls(storage_type="redis", redis_url=url)

    @classmethod
    def high_performance(cls) -> "SchedulerConfig":
        """创建高性能配置（启用时间轮）"""
        return cls(
            enable_time_wheel=True,
            time_wheel_slots=360,
            time_wheel_tick=1.0,
            scan_interval=0.5
        )

    @classmethod
    def lightweight(cls) -> "SchedulerConfig":
        """创建轻量级配置（不启用周期任务管理器）"""
        return cls(
            enable_periodic_manager=False,
            enable_time_wheel=False,
            scan_interval=2.0
        )


@dataclass
class TaskConfig:
    """统一任务配置（兼容旧版 API）"""

    node_id: str = ""
    storage: StorageConfig = field(default_factory=StorageConfig.memory)
    lock: LockConfig = field(default_factory=LockConfig.memory)
    worker: WorkerConfig = field(default_factory=WorkerConfig.default)
    queue: QueueConfig = field(default_factory=QueueConfig.default)
    executor: ExecutorConfig = field(default_factory=ExecutorConfig.async_executor)
    webui: WebUIConfig = field(default_factory=WebUIConfig.disable)

    def __post_init__(self):
        """Generate node ID if not provided."""
        if not self.node_id:
            import socket
            self.node_id = socket.gethostname()

    @classmethod
    def memory(cls, node_id: Optional[str] = None) -> "TaskConfig":
        """创建内存存储配置（单节点）"""
        config = cls()
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def redis(cls, redis_url: str, node_id: Optional[str] = None) -> "TaskConfig":
        """创建Redis存储配置（分布式）"""
        config = cls(
            storage=StorageConfig.redis(redis_url),
            lock=LockConfig.redis(redis_url),
        )
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def sqlite(cls, path: str = "tasks.db", node_id: Optional[str] = None) -> "TaskConfig":
        """创建SQLite存储配置"""
        config = cls(
            storage=StorageConfig.sqlite(path),
            lock=LockConfig.memory(),
        )
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def with_webui(cls, port: int = 8080, auto_open: bool = False, **kwargs) -> "TaskConfig":
        """创建启用Web UI的配置"""
        config = cls(**kwargs)
        config.webui = WebUIConfig.enable(port, auto_open)
        return config


# 预定义配置常量
DEFAULT_TASK_POOL_CONFIG = TaskPoolConfig()
DEFAULT_SCHEDULER_CONFIG = SchedulerConfig()
DEFAULT_TASK_CONFIG = TaskConfig()
