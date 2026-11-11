"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: config.py
@Description: 配置 - 包含所有模块的配置类
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/1 18:23
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Literal, Union

from neotask.common.constants import (
    DEFAULT_LIFECYCLE_CACHE_MAX_SIZE,
    DEFAULT_PROGRESS_FLUSH_BATCH_INTERVAL,
    DEFAULT_PROGRESS_FLUSH_DELTA,
    DEFAULT_PROGRESS_FLUSH_INTERVAL,
    DEFAULT_RUNNING_PERSIST_DELAY,
)
from neotask.executor import ExecutorType


class StorageType(str, Enum):
    """存储后端类型枚举

    继承 ``str`` 以兼容历史字符串配置（``"memory"`` / ``"sqlite"`` / ``"redis"``），
    各配置类的 ``__post_init__`` 会把字符串归一化为枚举成员。
    """

    MEMORY = "memory"
    SQLITE = "sqlite"
    REDIS = "redis"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def coerce(cls, value: Union["StorageType", str]) -> "StorageType":
        """把字符串或枚举统一转换为 :class:`StorageType`"""
        try:
            return cls(value)
        except ValueError as exc:
            raise ValueError(f"Unknown storage type: {value}") from exc


@dataclass
class StorageConfig:
    """存储配置"""

    type: StorageType = StorageType.MEMORY
    redis_url: Optional[str] = None
    redis_max_connections: int = 50          # 每个仓储客户端连接池上限
    sqlite_path: Optional[str] = None

    # SQLite 高并发选项
    sqlite_wal: bool = True                 # 启用 WAL，读写并发
    sqlite_synchronous: str = "NORMAL"      # WAL 下避免每次提交 fsync
    sqlite_busy_timeout: float = 30.0       # 写锁冲突等待时间（秒）
    persist_queue: bool = True              # 队列是否落盘；关闭后由任务表重建

    def __post_init__(self) -> None:
        self.type = StorageType.coerce(self.type)

    @classmethod
    def memory(cls) -> "StorageConfig":
        """创建内存存储配置"""
        return cls(type=StorageType.MEMORY)

    @classmethod
    def redis(cls, url: str, max_connections: int = 50) -> "StorageConfig":
        """创建Redis存储配置

        Args:
            url: Redis 连接地址
            max_connections: 单个仓储客户端连接池上限，需 >= worker 并发数
        """
        return cls(
            type=StorageType.REDIS,
            redis_url=url,
            redis_max_connections=max_connections,
        )

    @classmethod
    def sqlite(
            cls,
            path: str = "/data/neotask.db",
            *,
            wal: bool = True,
            synchronous: str = "NORMAL",
            busy_timeout: float = 30.0,
            persist_queue: bool = True,
    ) -> "StorageConfig":

        """创建SQLite存储配置"""
        return cls(
            type=StorageType.SQLITE,
            sqlite_path=path,
            sqlite_wal=wal,
            sqlite_synchronous=synchronous,
            sqlite_busy_timeout=busy_timeout,
            persist_queue=persist_queue,
        )


@dataclass
class LockConfig:
    """分布式锁配置"""

    type: Literal["memory", "redis"] = "memory"
    redis_url: Optional[str] = None
    redis_max_connections: int = 50          # 锁客户端连接池上限
    lock_timeout: int = 30
    heartbeat_interval: int = 10
    watch_dog_enabled: bool = True

    @classmethod
    def memory(cls) -> "LockConfig":
        """创建内存锁配置"""
        return cls(type="memory")

    @classmethod
    def redis(cls, url: str, timeout: int = 30, max_connections: int = 50) -> "LockConfig":
        """创建Redis锁配置"""
        return cls(
            type="redis",
            redis_url=url,
            lock_timeout=timeout,
            redis_max_connections=max_connections,
        )


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
class HeartbeatConfig:
    """心跳配置"""
    interval: float = 5.0  # 心跳间隔（秒）
    timeout: float = 20.0  # 心跳超时（秒）
    cleanup_interval: float = 30.0  # 清理检查间隔
    max_reclaim_per_cycle: int = 100  # 每周期最大回收数量


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
    storage_type: StorageType = StorageType.MEMORY
    sqlite_path: str = "/data/neotask.db"
    redis_url: Optional[str] = None
    redis_max_connections: int = 50  # Redis 仓储/锁客户端连接池上限
    persist_queue: bool = True  # SQLite 下是否持久化队列；关闭后重启由任务表重建

    # 执行器配置
    executor_type: ExecutorType = ExecutorType.ASYNC
    max_workers: int = 10

    # Worker配置
    worker_concurrency: int = 10
    prefetch_size: int = 20
    task_timeout: Optional[float] = None  # 任务默认超时时间（秒）

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

    # 进度落库节流（时间间隔 或 增量阈值任一满足即写入）
    progress_flush_interval: float = DEFAULT_PROGRESS_FLUSH_INTERVAL
    progress_flush_delta: float = DEFAULT_PROGRESS_FLUSH_DELTA
    # 写回缓存：后台批量落库间隔（<=0 关闭写回，进度改为即时写入）
    progress_flush_batch_interval: float = DEFAULT_PROGRESS_FLUSH_BATCH_INTERVAL
    # RUNNING 状态延迟落库窗口（秒）：0 立即落库；>0 时在窗口内完成的任务
    # 跳过 RUNNING 写入，只写一次终态（含 started_at）。窗口内任务在存储中仍
    # 显示为 PENDING，回收器/心跳看不到它，请保持窗口远小于 reclaimer_interval。
    running_persist_delay: float = DEFAULT_RUNNING_PERSIST_DELAY
    # 生命周期缓存上限，超出后优先淘汰已结束任务
    cache_max_size: int = DEFAULT_LIFECYCLE_CACHE_MAX_SIZE

    # 预取配置
    enable_prefetch: bool = True
    prefetch_min_threshold: int = 5

    # 终态任务保留：周期清理 SUCCESS/FAILED/CANCELLED，避免内存与存储
    # （尤其 Redis 键）随任务量无限增长。默认按任务自身 ttl 清理。
    enable_terminal_cleanup: bool = True
    terminal_cleanup_interval: float = 60.0  # 扫描间隔（秒）
    terminal_retention_seconds: int = 0      # 0 = 使用任务自身 ttl

    # 停机行为：为 True 时把未到期的延迟任务立即转入优先队列并等待执行；
    # 默认 False 保持原有“未到期任务留待下次启动”的兼容行为
    drain_delayed_on_shutdown: bool = False

    # 回收配置
    enable_reclaimer: bool = True  # 是否启用任务回收器
    reclaimer_interval: float = 30.0  # 回收器扫描间隔（秒）
    enable_heartbeat: bool = True  # 是否启用心跳
    heartbeat_interval: int = 5  # 心跳间隔（秒）
    heartbeat_timeout: int = 20  # 心跳超时（秒）
    enable_dead_letter: bool = True  # 是否启用死信队列
    dead_letter_max_size: int = 10000  # 死信队列最大容量
    dead_letter_ttl: int = 604800  # 死信保留时间（秒，默认7天）

    # 节点标识
    node_id: str = ""

    def __post_init__(self):
        self.storage_type = StorageType.coerce(self.storage_type)
        if not self.node_id:
            import os
            import socket
            # 多进程部署时同一台机器的 hostname 相同，追加 pid 保证每个进程
            # 在 Redis 节点注册表中是独立节点，避免心跳/回收器把它们当成同一个节点
            self.node_id = f"{socket.gethostname()}-{os.getpid()}"

    @classmethod
    def memory(cls, node_id: Optional[str] = None) -> "TaskPoolConfig":
        """创建内存存储配置"""
        config = cls(storage_type=StorageType.MEMORY)
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def sqlite(cls, path: str = "/data/neotask.db", node_id: Optional[str] = None) -> "TaskPoolConfig":
        """创建SQLite存储配置"""
        config = cls(storage_type=StorageType.SQLITE, sqlite_path=path)
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def redis(
            cls,
            url: str,
            node_id: Optional[str] = None,
            enable_prefetch: Optional[bool] = None,
            prefetch_size: Optional[int] = None,
    ) -> "TaskPoolConfig":
        """创建Redis存储配置

        多进程共享同一 Redis 队列时，任务锁也必须使用 Redis 分布式锁，
        否则锁只在进程内生效，回收器可能把其他进程正在执行的任务判为孤儿。
        """
        config = cls(storage_type=StorageType.REDIS, redis_url=url, lock_type="redis")
        if node_id:
            config.node_id = node_id
        if enable_prefetch is not None:
            config.enable_prefetch = enable_prefetch
        if prefetch_size is not None:
            config.prefetch_size = prefetch_size

        return config


@dataclass
class SchedulerConfig:
    """调度器配置 - 专注于定时任务"""

    # 存储配置
    storage_type: StorageType = StorageType.MEMORY
    sqlite_path: str = "/data/neotask.db"
    redis_url: Optional[str] = None
    redis_max_connections: int = 50  # Redis 仓储/锁客户端连接池上限
    persist_queue: bool = True  # SQLite 下是否持久化队列；关闭后重启由任务表重建

    # Worker配置
    worker_concurrency: int = 10

    # 重试配置
    max_retries: int = 3
    retry_delay: float = 1.0

    # 进度落库节流（时间间隔 或 增量阈值任一满足即写入）
    progress_flush_interval: float = DEFAULT_PROGRESS_FLUSH_INTERVAL
    progress_flush_delta: float = DEFAULT_PROGRESS_FLUSH_DELTA
    # 写回缓存：后台批量落库间隔（<=0 关闭写回，进度改为即时写入）
    progress_flush_batch_interval: float = DEFAULT_PROGRESS_FLUSH_BATCH_INTERVAL
    # RUNNING 状态延迟落库窗口（秒）：0 立即落库；>0 时在窗口内完成的任务
    # 跳过 RUNNING 写入，只写一次终态（含 started_at）。窗口内任务在存储中仍
    # 显示为 PENDING，回收器/心跳看不到它，请保持窗口远小于 reclaimer_interval。
    running_persist_delay: float = DEFAULT_RUNNING_PERSIST_DELAY
    # 生命周期缓存上限，超出后优先淘汰已结束任务
    cache_max_size: int = DEFAULT_LIFECYCLE_CACHE_MAX_SIZE

    # 持久化配置
    # 默认开启：sqlite/redis 下周期任务会跨重启恢复；memory 模式下仍只存在内存中，
    # 注册周期任务时会输出一次明确的降级警告。
    enable_persistence: bool = True

    # 调度配置
    scan_interval: float = 1.0  # 调度循环扫描间隔（秒）
    enable_periodic_manager: bool = True  # 是否启用周期任务管理器
    enable_time_wheel: bool = False  # 是否启用时间轮（高性能）
    time_wheel_slots: int = 60  # 时间轮槽位数
    time_wheel_tick: float = 1.0  # 时间轮滴答间隔（秒）

    # 周期任务默认配置
    default_max_runs: Optional[int] = None  # 默认最大执行次数
    default_missed_policy: str = "skip"  # 默认错过执行策略: ignore, run_once, catch_up, skip

    # 停机行为：为 True 时把未到期的延迟任务立即转入优先队列并等待执行；
    # 默认 False 保持原有“未到期任务留待下次启动”的兼容行为
    drain_delayed_on_shutdown: bool = False

    def __post_init__(self) -> None:
        self.storage_type = StorageType.coerce(self.storage_type)

    @classmethod
    def memory(cls) -> "SchedulerConfig":
        """创建内存存储配置"""
        return cls(storage_type=StorageType.MEMORY)

    @classmethod
    def sqlite(cls, path: str = "/data/neotask.db") -> "SchedulerConfig":
        """创建SQLite存储配置"""
        return cls(storage_type=StorageType.SQLITE, sqlite_path=path)

    @classmethod
    def redis(cls, url: str) -> "SchedulerConfig":
        """创建Redis存储配置"""
        return cls(storage_type=StorageType.REDIS, redis_url=url)

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
    def sqlite(cls, path: str = "/data/neotask.db", node_id: Optional[str] = None) -> "TaskConfig":
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
