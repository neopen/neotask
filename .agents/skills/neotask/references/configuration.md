# NeoTask Configuration Reference

## TaskPoolConfig

```python
from neotask import TaskPoolConfig

TaskPoolConfig(
    # Storage
    storage_type="memory",    # memory | sqlite | redis
    sqlite_path="neotask.db",
    redis_url=None,

    # Executor
    executor_type="auto",     # auto | async | thread | process
    max_workers=10,

    # Worker Pool
    worker_concurrency=10,
    prefetch_size=20,
    task_timeout=300,

    # Queue
    queue_max_size=10000,

    # Lock
    lock_type="memory",       # memory | redis
    lock_timeout=30,

    # Retry
    max_retries=0,
    retry_delay=5,

    # Monitoring
    enable_metrics=True,
    enable_health_check=True,
    enable_reporter=False,

    # Identity
    node_id="default",
)
```

## SchedulerConfig

```python
from neotask import SchedulerConfig

SchedulerConfig(
    # Inherits TaskPoolConfig fields
    storage_type="memory",
    sqlite_path="neotask.db",
    redis_url=None,
    worker_concurrency=10,
    max_retries=3,
    retry_delay=5,
    task_timeout=300,

    # Scheduler-specific
    scan_interval=1.0,              # Seconds between schedule scans
    enable_periodic_manager=True,   # Use PeriodicTaskManager
    enable_time_wheel=True,         # Use TimeWheel for delays
    time_wheel_slots=60,            # TimeWheel slot count
    time_wheel_tick=1.0,            # Tick interval in seconds
    default_max_runs=None,          # Default max runs for periodic tasks
    default_missed_policy="ignore", # ignore | run_once | catch_up | skip
)

# Presets
SchedulerConfig.high_performance()   # time_wheel enabled, higher throughput
SchedulerConfig.lightweight()        # no periodic manager, minimal overhead
```

## Storage Configs

```python
from neotask import StorageConfig

# Memory (zero config)
StorageConfig.memory()

# SQLite (single-machine persistence)
StorageConfig.sqlite(path="neotask.db")

# Redis (distributed)
StorageConfig.redis(url="redis://localhost:6379")
```

## Lock Configs

```python
from neotask import LockConfig

# Memory lock
LockConfig.memory()

# Redis distributed lock
LockConfig.redis(url="redis://localhost:6379", ttl=30)
```
