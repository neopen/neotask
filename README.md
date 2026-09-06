<p align="center">
  <h1 align="center">⚡ NeoTask</h1>
  <p align="center">
    <strong>Distributed Task Scheduling System / 轻量级 Python 异步任务队列管理器</strong>
  </p>
  <p align="center">
    <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3.8+-3776AB.svg?style=flat-square&logo=python&logoColor=white" alt="Python"></a>
    <a href="./LICENSE"><img src="https://img.shields.io/badge/License-MIT-green.svg?style=flat-square" alt="License"></a>
  </p>
  <p align="center">
    <a href="https://pypi.org/project/neotask/"><img src="https://img.shields.io/pypi/v/neotask.svg?style=flat-square&logo=pypi&logoColor=white" alt="PyPI"></a>
    <a href="https://pepy.tech/project/neotask"><img src="https://img.shields.io/pepy/dt/neotask?style=flat-square&color=blue" alt="Downloads"></a>
    <a href="https://github.com/neopen/neotask"><img src="https://img.shields.io/github/stars/neopen/neotask?style=flat-square&logo=github" alt="GitHub Stars"></a>
  </p>
  <p align="center">
    <a href="./README_zh.md">中文</a> •
    <a href="https://pengline.cn/2026/04/243d5a536d064df59c2ec8668362b8b5">Documentation</a> •
    <a href="https://pypi.org/project/neotask/">PyPI</a> •
    <a href="https://task.helpenx.com">Website Demo</a>
  </p>
</p>

> 🚀 **Lightweight & Async**: Pure Python task queue manager designed for time-consuming jobs (AI generation, video processing, data scraping, etc.)  
> 📦 **Zero External Dependencies**: No extra deployment required (no Redis/PostgreSQL needed). Ready to run out of the box  
> ⏰ **Flexible Scheduling**: Native support for scheduled, periodic (cron-like), and delayed tasks  
> ⚡ **Quick Integration**: `pip install neotask` + few lines of configuration

---

## 💡 Why NeoTask?

**NeoTask** is designed to solve complex task orchestration, async queueing, and priority management for modern distributed systems and AI multi-agent pipelines.

| Pain Point | NeoTask Solution |
| :--- | :--- |
| **Task blockage under heavy concurrency** | Priority queueing & task worker pool isolation |
| **Unreliable long-running tasks** | Automatic state persistence, health check & retry mechanism |
| **Complex SDK setup** | Lightweight Pythonic interface with zero friction |


---

## Features

- **Zero-Dependency Deployment** - Pure Python implementation, no Redis/PostgreSQL required
- **Immediate Tasks** - Supports priority scheduling, high-priority tasks execute first
- **Scheduled Tasks** - Supports delayed execution, fixed intervals, and Cron expressions
- **Asynchronous Concurrency** - Based on asyncio, multi-worker concurrent processing
- **Automatic Retry** - Failed tasks automatically retry with configurable attempts
- **Persistence** - Multiple storage backends: Memory/SQLite/Redis
- **DAG Workflow** - Support for task orchestration, conditional branches, and parallel execution
- **Distributed Support** - Distributed task scheduling, high availability, and fault tolerance
- **Event Callbacks** - Supports task lifecycle event listeners

------

## Use Cases

| Scenario | Description | Recommended Configuration | Entry Point |
| :--------------------- | :--------------------------- | :---------------------- | :------------ |
| **AI Text-to-Image/Video Generation** | Queue time-consuming tasks to avoid blocking main flow | `worker_concurrency=3` | TaskPool |
| **Batch File Processing** | Batch operations like transcoding, compression, uploading | `worker_concurrency=10` | TaskPool |
| **Web Scraping Scheduling** | Distributed scraping to prevent being blocked | `storage_type="redis"` | TaskPool |
| **Scheduled Report Sending** | Send daily reports at 9 AM | `cron="0 9 * * *"` | TaskScheduler |
| **Delayed Notifications** | Send reminders 5 minutes after user action | `delay_seconds=300` | TaskScheduler |
| **Heartbeat Detection** | Check service health status every 30 seconds | `interval_seconds=30` | TaskScheduler |
| **Background Data Analysis** | Execute data aggregation tasks at night | `cron="0 2 * * *"` | TaskScheduler |

---

## Architecture & Evolution

```mermaid
graph TB
    subgraph User["User Application Layer"]
        APP[User Code]
    end
    
    subgraph NeoTask["NeoTask Core"]
        TP[TaskPool<br/>Immediate Task Entry]
        TS[TaskScheduler<br/>Scheduled Task Entry]
        
        subgraph Core["Shared Core Components"]
            LM[LifecycleManager<br/>Task Lifecycle Management]
            QS[QueueScheduler<br/>Priority + Delayed Queue]
            WP[WorkerPool<br/>Worker Pool/Concurrency Control]
            FM[FutureManager<br/>Async Wait/Result Callback]
        end
        
        subgraph Internal["Internal Components"]
            EB[EventBus<br/>Event Bus]
            MC[MetricsCollector<br/>Metrics Collection]
            LF[LockFactory<br/>Distributed Lock]
        end
        
        EX[TaskExecutor<br/>User Business Logic]
    end
    
    subgraph Storage["Storage Layer"]
        MEM[MemoryStorage]
        SQLITE[(SQLiteStorage)]
        REDIS[(RedisStorage)]
    end
    
    APP -->|Immediate Task| TP
    APP -->|Scheduled Task| TS
    TS -->|Delegates| TP
    TP --> LM
    TP --> QS
    TP --> WP
    TP --> FM
    
    LM --> MEM
    LM --> SQLITE
    LM --> REDIS
    
    WP --> EX
    WP --> EB
    WP --> MC
    WP --> LF
```

Development Roadmap

```mermaid
timeline
    title NeoTask Architecture Evolution Roadmap
    
    section v0.1
        Basic Task Pool : Local Memory Queue
                        : Async Execution Engine
                        : Memory/SQLite Storage
                        
    section v0.2
        Observability : Event Bus
                      : Metrics Collection
                      : Health Check
                      
    section v0.3
        Scheduled Tasks : Delayed Queue/Time Wheel
                        : Periodic Tasks
                        : Cron Expression
                        
    section v0.4
        Distributed Base : Redis Shared Queue
                         : Distributed Lock
                         
    section v0.5
        Performance Optimization : Prefetch Mechanism
                                : Batch Operations
                                : Connection Pool

    section v1.0
        High Availability : Watchdog Renewal
                          : Timeout Detection
                          : Automatic Fault Recovery

    section v1.5
        Task Orchestration : DAG Workflow
                           : Conditional Branch
                           : Parallel Execution

    section v2.0
        Enterprise Features : Independent Web UI
                            : Multi-Tenancy Isolation
                            : Prometheus Integration
```

------

## Quick Start

### Installation

```sh
# Basic installation
pip install neotask

# With Redis distributed support
pip install neotask[redis]

# Full installation
pip install neotask[full]
```



### Immediate Tasks (TaskPool)

```python
from neotask import TaskPool

async def process(data):
    return {"result": "done", "data": data}

# Create task pool
pool = TaskPool(executor=process)

# Submit task
task_id = pool.submit({"id": 123})

# Wait for result
result = pool.wait_for_result(task_id)

pool.shutdown()
```

### Scheduled Tasks (TaskScheduler)

```python
from neotask import TaskScheduler

scheduler = TaskScheduler(executor=process)

# Execute after 60 seconds delay
scheduler.submit_delayed({"id": 123}, delay_seconds=60)

# Execute every 5 minutes
scheduler.submit_interval({"id": 123}, interval_seconds=300)

# Execute daily at 9 AM
scheduler.submit_cron({"id": 123}, "0 9 * * *")

scheduler.shutdown()
```

### Using Context Manager

```python
with TaskPool(executor=process) as pool:
    task_id = pool.submit({"id": 123})
    result = pool.wait_for_result(task_id)
```

### Using Event Callbacks

```python
from neotask import TaskPool

async def on_task_created(event):
    print(f"Task created: {event.task_id}")

async def on_task_completed(event):
    print(f"Task completed: {event.task_id}, Result: {event.data}")

async def on_task_failed(event):
    print(f"Task failed: {event.task_id}, Error: {event.data}")

pool = TaskPool(executor=my_executor)
pool.start()

# Register event callbacks
pool.on_created(on_task_created)
pool.on_completed(on_task_completed)
pool.on_failed(on_task_failed)

task_id = pool.submit({"test": "event"})
result = pool.wait_for_result(task_id)
```



## API Reference

| Method | Description |
| :------------------------------------------- | :-------- |
| `pool.submit(data, priority=2, delay=0)` | Submit task |
| `pool.wait_for_result(task_id, timeout=300)` | Wait for result |
| `pool.get_status(task_id)` | Get status |
| `pool.cancel(task_id)` | Cancel task |
| `scheduler.submit_delayed(data, delay)` | Delayed task |
| `scheduler.submit_interval(data, interval)` | Periodic task |
| `scheduler.submit_cron(data, cron)` | Cron task |
| `engine.submit_workflow(definition)` | Submit workflow (v1.5) |
| `engine.wait_workflow(execution_id)` | Wait for workflow (v1.5) |

Detailed API documentation can be found [here](https://pengline.cn/2026/04/650ac5bb41c74e26bc4effcec88bf26c/)



## Configuration Example

```python
from neotask import TaskPool, TaskPoolConfig

config = TaskPoolConfig(
    worker_concurrency=10,      # Number of concurrent workers
    max_retries=3,              # Number of retries
    storage_type="sqlite",      # Storage type
)

pool = TaskPool(executor=process, config=config)
```

Detailed usage examples can be found [here](https://pengline.cn/2026/04/fa51edd849b24f48b4d7fa8e27efef77/)



## Contribution Guide

### Setting Up Development Environment

```sh
# Clone repository
git clone https://github.com/neopen/neotask.git
cd neotask

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/

# View test coverage
pytest --cov=neotask tests/

# Run specific module tests
pytest tests/test_task_pool.py -v
pytest tests/test_task_scheduler.py -v
```

### Project Structure

```
neotask/
├── api/           # TaskPool, TaskScheduler
├── core/          # Lifecycle, Queue, Worker
├── workflow/      # Workflow  Engine
├── engine/        # Task Orchestration
├── executor/      # Async Execution Engine
├── storage/       # Memory/SQLite/Redis
├── event/         # Event Bus
└── models/        # Data Models
```



### Contribution Workflow

Welcome to submit Issues and Pull Requests

1. Fork the project
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push branch (`git push origin feature/amazing`)
5. Submit Pull Request

### Code Style

- Follow [PEP 8](https://peps.python.org/pep-0008/) code style
- Add appropriate [type annotations](https://peps.python.org/pep-0484/)
- Write unit tests for new features (coverage ≥ 80%)
- Update relevant documentation and example code
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)

### Testing Requirements

```
# Run all tests
pytest tests/

# Run specific module tests
pytest tests/unit/test_task.py

# Run manual tests
python examples/01_simple.py
python examples/05_webui.py
```

## Issue Reporting

- **Submit Issue**: https://github.com/neopen/neotask/issues
- **Feature Suggestions**: Use Enhancement label
- **Bug Reports**: Use Bug label and provide reproduction steps
- **Security Vulnerabilities**: Please send email directly to the author's email

------

## License

MIT License © 2026 NeoPen

------

## Acknowledgments

Thanks to all contributors and the open source community for their support.
