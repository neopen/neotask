"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: __init__.py
@Description: NeoTask - 通用分布式任务池
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

__version__ = "1.0.8"
__author__ = "neopen"

# API 入口
from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.api.task_scheduler import TaskScheduler, SchedulerConfig

# 核心组件
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.core.dispatcher import TaskDispatcher
from neotask.core.future import TaskFuture, FutureManager
from neotask.core.progress import (
    ProgressReporter,
    get_progress_reporter,
    report_progress,
    report_progress_sync,
)

# 业务级失败登记
from neotask.core.failure import (
    BusinessFailure,
    get_failure_recorder,
    report_failure,
)

# 成本上报
from neotask.core.cost import (
    CostRecorder,
    get_cost_recorder,
    report_cost,
)

# Checkpoint 断点续跑
from neotask.core.checkpoint import (
    CHECKPOINT_DATA_KEY,
    CheckpointContext,
    load_checkpoint,
    load_checkpoint_sync,
    save_checkpoint,
    save_checkpoint_sync,
)

# 事件总线
from neotask.common.constants import EventType
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
from neotask.storage.base import (
    CheckpointRepository,
    QueueRepository,
    TaskRepository,
)
from neotask.storage.memory import (
    MemoryCheckpointRepository,
    MemoryContextStore,
    MemoryQueueRepository,
    MemoryTaskRepository,
)
from neotask.storage.sqlite import (
    SQLiteCheckpointRepository,
    SQLiteQueueRepository,
    SQLiteTaskRepository,
)
from neotask.storage.redis import (
    RedisCheckpointRepository,
    RedisQueueRepository,
    RedisTaskRepository,
)
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
from neotask.models.task import Task, TaskStatus, TaskPriority, BatchSubmitResult
from neotask.models.config import (
    StorageConfig, StorageType, LockConfig, WorkerConfig,
    QueueConfig, ExecutorConfig, SchedulerConfig
)

# 异常
from neotask.common.exceptions import (
    TaskSchedulerError, TaskNotFoundError, TaskError,
    TaskAlreadyExistsError, QueueFullError, BatchSubmitError, TimeoutError,
    NeoTaskError, UnsupportedFeatureError,
)

#
from neotask.distributed import (
    NodeManager, NodeInfo, NodeStatus,
    Coordinator, CoordinatorConfig,
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
    "ProgressReporter",
    "report_progress",
    "report_progress_sync",
    "get_progress_reporter",
    "BusinessFailure",
    "get_failure_recorder",
    "report_failure",
    "CostRecorder",
    "get_cost_recorder",
    "report_cost",
    "CHECKPOINT_DATA_KEY",
    "CheckpointContext",
    "save_checkpoint",
    "save_checkpoint_sync",
    "load_checkpoint",
    "load_checkpoint_sync",

    # 事件
    "EventBus",
    "TaskEvent",
    "EventType",

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
    "CheckpointRepository",
    "MemoryTaskRepository",
    "MemoryQueueRepository",
    "MemoryCheckpointRepository",
    "MemoryContextStore",
    "SQLiteTaskRepository",
    "SQLiteQueueRepository",
    "SQLiteCheckpointRepository",
    "RedisTaskRepository",
    "RedisQueueRepository",
    "RedisCheckpointRepository",
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
    "BatchSubmitResult",
    "BatchSubmitError",
    "StorageConfig",
    "StorageType",
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
    "NeoTaskError",
    "UnsupportedFeatureError",
]