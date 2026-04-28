"""
@FileName: __init__.py
@Description: NeoTask - 通用分布式任务池
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

__version__ = "0.3.0"
__author__ = "HiPeng"

# API 入口
from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.api.task_scheduler import TaskScheduler, SchedulerConfig

# 核心组件
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.core.dispatcher import TaskDispatcher
from neotask.core.future import TaskFuture, FutureManager

# 事件总线
from neotask.event.bus import EventBus, TaskEvent

# 执行器
from neotask.executor.base import TaskExecutor
from neotask.executor.async_executor import AsyncExecutor
from neotask.executor.thread_executor import ThreadExecutor
from neotask.executor.process_executor import ProcessExecutor
from neotask.executor.factory import ExecutorFactory

# 锁
from neotask.lock.base import TaskLock
from neotask.lock.memory import MemoryLock
from neotask.lock.redis import RedisLock
from neotask.lock.watchdog import WatchDog
from neotask.lock.factory import LockFactory, LockManager

# 存储
from neotask.storage.base import TaskRepository, QueueRepository
from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
from neotask.storage.sqlite import SQLiteTaskRepository, SQLiteQueueRepository
from neotask.storage.redis import RedisTaskRepository, RedisQueueRepository
from neotask.storage.factory import StorageFactory

# 队列
from neotask.queue.base import BaseQueue, QueueItem
from neotask.queue.priority_queue import PriorityQueue
from neotask.queue.delayed_queue import DelayedQueue
from neotask.queue.queue_scheduler import QueueScheduler

# Worker
from neotask.worker.pool import WorkerPool, WorkerStats
from neotask.worker.supervisor import WorkerSupervisor, SupervisorConfig

# 监控
from neotask.monitor.metrics import MetricsCollector, TaskMetrics
from neotask.monitor.health import HealthChecker, SystemHealthChecker, HealthStatus
from neotask.monitor.reporter import (
    MetricsReporter, ConsoleReporter, FileReporter,
    PrometheusReporter, ReporterManager
)

# 模型
from neotask.models.task import Task, TaskStatus, TaskPriority
from neotask.models.config import (
    StorageConfig, LockConfig, WorkerConfig,
    QueueConfig, ExecutorConfig, SchedulerConfig
)

# 异常
from neotask.common.exceptions import (
    TaskSchedulerError, TaskNotFoundError, TaskError,
    TaskAlreadyExistsError, QueueFullError, TimeoutError
)

#
from neotask.distributed import (
    NodeManager, NodeInfo, NodeStatus,
    Coordinator, CoordinatorConfig,
    Elector, LeaderInfo,
    Sharder, ConsistentHashSharder
)

__all__ = [
    # 版本
    "__version__",
    "__author__",

    # API
    "TaskPool",
    "TaskPoolConfig",
    "TaskScheduler",
    "SchedulerConfig",

    # 核心
    "TaskLifecycleManager",
    "TaskDispatcher",
    "TaskFuture",
    "FutureManager",

    # 事件
    "EventBus",
    "TaskEvent",

    # 执行器
    "TaskExecutor",
    "AsyncExecutor",
    "ThreadExecutor",
    "ProcessExecutor",
    "ExecutorFactory",

    # 锁
    "TaskLock",
    "MemoryLock",
    "RedisLock",
    "WatchDog",
    "LockFactory",
    "LockManager",

    # 存储
    "TaskRepository",
    "QueueRepository",
    "MemoryTaskRepository",
    "MemoryQueueRepository",
    "SQLiteTaskRepository",
    "SQLiteQueueRepository",
    "RedisTaskRepository",
    "RedisQueueRepository",
    "StorageFactory",

    # 队列
    "BaseQueue",
    "QueueItem",
    "PriorityQueue",
    "DelayedQueue",
    "QueueScheduler",

    # Worker
    "WorkerPool",
    "WorkerStats",
    "WorkerSupervisor",
    "SupervisorConfig",

    # 监控
    "MetricsCollector",
    "TaskMetrics",
    "HealthChecker",
    "SystemHealthChecker",
    "HealthStatus",
    "MetricsReporter",
    "ConsoleReporter",
    "FileReporter",
    "PrometheusReporter",
    "ReporterManager",

    # 模型
    "Task",
    "TaskStatus",
    "TaskPriority",
    "StorageConfig",
    "LockConfig",
    "WorkerConfig",
    "QueueConfig",
    "ExecutorConfig",

    # 异常
    "TaskSchedulerError",
    "TaskNotFoundError",
    "TaskError",
    "TaskAlreadyExistsError",
    "QueueFullError",
    "TimeoutError",
]