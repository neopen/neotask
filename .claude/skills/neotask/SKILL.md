---
name: neotask
description: 轻量级 Python 异步任务队列管理器 — 帮助用户编写使用 neotask 库的代码，支持即时/延时/周期/Cron 任务调度
---

# NeoTask 技能

## 概述

NeoTask 是一个纯 Python 实现的轻量级异步任务队列调度系统。零依赖部署，无需 Redis/PostgreSQL 即可使用。适用于 AI 生成任务、文件处理、定时报表、延迟通知等场景。

**版本**: 0.4.0 | **作者**: HiPeng | **许可证**: MIT

## 核心入口

两个主要 API 类：

| 类 | 用途 | 何时使用 |
|---|---|---|
| `TaskPool` | 即时任务池，提交后立即执行 | 后台数据处理、AI 任务异步执行 |
| `TaskScheduler` | 定时任务调度器，延时/周期/Cron | 定时报表、延迟通知、心跳检测 |

### 安装

```python
pip install neotask          # 基础安装
pip install neotask[redis]   # Redis 分布式支持
pip install neotask[ui]      # Web UI 监控面板
pip install neotask[full]    # 完整功能
```

## TaskPool — 即时任务

### 基本用法

```python
from neotask import TaskPool

async def process(data):
    return {"result": "done", "data": data}

pool = TaskPool(executor=process)
task_id = pool.submit({"id": 123})
result = pool.wait_for_result(task_id)
pool.shutdown()
```

### 上下文管理器

```python
with TaskPool(executor=process) as pool:
    task_id = pool.submit({"data": "value"})
    result = pool.wait_for_result(task_id)
```

### 带配置

```python
from neotask import TaskPool, TaskPoolConfig

config = TaskPoolConfig(
    storage_type="sqlite",
    worker_concurrency=10,
    max_retries=3,
    retry_delay=5,
    task_timeout=300,
    queue_max_size=10000,
)

pool = TaskPool(executor=process, config=config)
```

### TaskPoolConfig 参数

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `storage_type` | str | `"memory"` | 存储后端：`memory` / `sqlite` / `redis` |
| `sqlite_path` | str | `"neotask.db"` | SQLite 数据库路径 |
| `redis_url` | str | `None` | Redis 连接 URL |
| `worker_concurrency` | int | `10` | 并发 Worker 数量 |
| `max_retries` | int | `0` | 失败重试次数 |
| `retry_delay` | float | `5` | 重试延迟（秒） |
| `task_timeout` | int | `300` | 任务超时（秒） |
| `prefetch_size` | int | `20` | 预取任务数 |
| `queue_max_size` | int | `10000` | 队列最大容量 |
| `executor_type` | str | `"auto"` | 执行器类型 |
| `lock_type` | str | `"memory"` | 锁类型 |
| `enable_metrics` | bool | `True` | 启用指标收集 |
| `enable_health_check` | bool | `True` | 启用健康检查 |
| `enable_reporter` | bool | `False` | 启用指标报告 |

### 提交任务

```python
# 提交任务
task_id = pool.submit(data, priority=2, delay=0, ttl=3600)

# 批量提交
task_ids = pool.submit_batch([{"id": 1}, {"id": 2}, {"id": 3}])

# 异步提交
task_id = await pool.submit_async(data, priority=2)

# 优先级：1 (最高) → 5 (最低)
```

### 获取结果

```python
# 阻塞等待
result = pool.wait_for_result(task_id, timeout=300)

# 查询状态
status = pool.get_status(task_id)  # 'pending' / 'running' / 'completed' / 'failed'

# 查询结果
info = pool.get_result(task_id)    # {task_id, status, result, error, ...}
```

### 事件回调

```python
async def on_created(event):
    print(f"Task created: {event.task_id}")

async def on_completed(event):
    print(f"Task completed: {event.task_id}")

pool.on_created(on_created)
pool.on_completed(on_completed)
```

支持的回调：`on_created`, `on_started`, `on_completed`, `on_failed`, `on_cancelled`

### 任务管理

```python
pool.cancel(task_id)   # 取消任务
pool.retry(task_id)    # 重试失败的任务
pool.delete(task_id)   # 删除任务
pool.pause()           # 暂停处理新任务
pool.resume()          # 恢复处理
pool.clear_queue()     # 清空队列
```

### 统计与监控

```python
stats = pool.get_stats()
# {queue_size, total, pending, running, completed, failed, cancelled, success_rate}

pool.get_queue_size()
pool.get_worker_stats()
pool.get_health_status()
```

## TaskScheduler — 定时任务

### 基本用法

```python
from neotask import TaskScheduler

scheduler = TaskScheduler(executor=process)

# 延时任务
scheduler.submit_delayed(data, delay_seconds=60)

# 周期任务
scheduler.submit_interval(data, interval_seconds=300)

# Cron 任务
scheduler.submit_cron(data, "0 9 * * *")  # 每天 9 点

scheduler.shutdown()
```

### SchedulerConfig 参数

```python
from neotask import SchedulerConfig

config = SchedulerConfig(
    storage_type="sqlite",
    worker_concurrency=5,
    max_retries=3,
    scan_interval=1.0,            # 调度扫描间隔
    enable_periodic_manager=True, # 周期任务管理器
    enable_time_wheel=True,       # 时间轮
    time_wheel_slots=60,          # 时间轮槽位数
    time_wheel_tick=1.0,          # 时间轮滴答间隔
)

scheduler = TaskScheduler(executor=process, config=config)
```

### 延时任务

```python
# 60 秒后执行
task_id = scheduler.submit_delayed(data, delay_seconds=60)

# 指定时间点执行
from datetime import datetime, timedelta
execute_at = datetime.now() + timedelta(hours=2)
task_id = scheduler.submit_at(data, execute_at)
```

### 周期任务

```python
# 每 5 分钟执行
task_id = scheduler.submit_interval(data, interval_seconds=300)

# 参数选项
task_id = scheduler.submit_interval(
    data,
    interval_seconds=300,
    run_immediately=True,   # 是否立即执行第一次
    max_runs=100,           # 最大执行次数
    name="daily_report"     # 任务名称
)
```

### Cron 任务

```python
# 标准 5 字段 Cron 表达式（分 时 日 月 周）
scheduler.submit_cron(data, "0 9 * * *")     # 每天 9:00
scheduler.submit_cron(data, "*/5 * * * *")   # 每 5 分钟
scheduler.submit_cron(data, "0 2 * * 1")     # 每周一凌晨 2:00
scheduler.submit_cron(data, "0 0 1 * *")     # 每月 1 号

# 预定义表达式也支持
scheduler.submit_cron(data, "@hourly")
scheduler.submit_cron(data, "@daily")
scheduler.submit_cron(data, "@weekly")
```

### 周期任务管理

```python
scheduler.pause_periodic(task_id)       # 暂停
scheduler.resume_periodic(task_id)      # 恢复
scheduler.cancel_periodic(task_id)      # 取消
tasks = scheduler.get_periodic_tasks()  # 查询所有
task = scheduler.get_periodic_task(id)  # 查询单个
```

### 委托方法（与 TaskPool 相同）

```python
scheduler.submit(data, priority=2)
scheduler.wait_for_result(task_id, timeout=300)
scheduler.get_status(task_id)
scheduler.cancel(task_id)
scheduler.retry(task_id)
scheduler.get_stats()
```

## 存储后端选择

| 后端 | 适用场景 | 持久化 | 分布式 |
|---|---|---|---|
| `memory` | 开发、测试 | ❌ | ❌ |
| `sqlite` | 单机部署 | ✅ | ❌ |
| `redis` | 生产、高可用 | ✅ | ✅ |

## 常见模式

### 模式 1：后台任务处理

```python
async def handle_request(image_data):
    pool = TaskPool(executor=generate_image)
    task_id = pool.submit({"prompt": "a sunset"})
    return {"task_id": task_id, "status_url": f"/status/{task_id}"}
```

### 模式 2：定时报表

```python
scheduler = TaskScheduler(executor=send_report)
scheduler.submit_cron(
    {"report_type": "daily"},
    "0 9 * * *",
    name="morning_report"
)
```

### 模式 3：重试与容错

```python
config = TaskPoolConfig(
    max_retries=3,
    retry_delay=10,
    task_timeout=60,
)
pool = TaskPool(executor=unreliable_api, config=config)
```

### 模式 4：事件驱动

```python
async def on_failed(event):
    await send_alert(f"Task {event.task_id} failed: {event.data}")

pool.on_failed(on_failed)
```

### 模式 5：分布式锁保护临界区

```python
task_id = pool.submit({"resource": "shared_file"})
if pool.acquire_lock(task_id, ttl=60):
    try:
        result = pool.wait_for_result(task_id)
    finally:
        pool.release_lock(task_id)
```

### 模式 6：FastAPI 集成（使用 TaskEngine）

```python
from fastapi import FastAPI
from neotask import TaskEngine

app = FastAPI()
engine = TaskEngine()

@app.on_event("startup")
async def startup():
    await engine.initialize()
    await engine.start()

@app.post("/tasks")
async def create_task(data: dict):
    task_id = await engine.submit(data)
    return {"task_id": task_id}

@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    return await engine.get_result(task_id)
```

### 模式 7：自定义执行器类

```python
class ImageProcessor:
    def __init__(self, model_path):
        self.model = load_model(model_path)

    def execute(self, data):
        return self.model.process(data["image"])

from neotask import ExecutorType
config = TaskPoolConfig(executor_type="class")
pool = TaskPool(executor=ImageProcessor("model.onnx"), config=config)
```

## TaskEngine — 纯异步入口

`TaskEngine` 是 `TaskPool` 的低级替代品，适合已在 async 上下文中的场景。不创建隐式事件循环线程，所有 API 均为 async。

```python
from neotask import TaskEngine, EngineConfig

config = EngineConfig(
    storage_type="sqlite",
    worker_concurrency=10,
    max_retries=3,
)

engine = TaskEngine(config=config)
await engine.initialize()
await engine.start()

task_id = await engine.submit({"data": "value"})
result = await engine.wait(task_id, timeout=300)

await engine.stop()
```

**TaskEngine vs TaskPool 选择：**
| 场景 | 推荐 |
|---|---|
| 同步代码调用、简单脚本 | `TaskPool` |
| 已有 async 运行时（FastAPI、asyncio 应用） | `TaskEngine` |
| 需要细粒度控制启停顺序 | `TaskEngine` |
| 需要同步+异步双 API | `TaskPool` |

## 执行器类型

```python
from neotask import ExecutorFactory, ExecutorType

# 自动检测（根据函数签名选择）
executor = ExecutorFactory.create(func=my_func, executor_type=ExecutorType.AUTO)

# 显式指定
ExecutorFactory.create(func=my_func, executor_type=ExecutorType.ASYNC)    # 纯 async
ExecutorFactory.create(func=my_func, executor_type=ExecutorType.THREAD)   # 阻塞 I/O
ExecutorFactory.create(func=my_func, executor_type=ExecutorType.PROCESS)  # CPU 密集
ExecutorFactory.create(func=my_obj, executor_type=ExecutorType.CLASS)     # 有 execute() 方法的对象
```

| 类型 | 适用场景 | 说明 |
|---|---|---|
| `AUTO` | 默认推荐 | 根据函数签名自动选择 |
| `ASYNC` | 异步 I/O | 零线程开销，直接 await |
| `THREAD` | 同步阻塞 I/O | ThreadPoolExecutor |
| `PROCESS` | CPU 密集计算 | 绕过 GIL，需 cloudpickle |
| `CLASS` | 有状态 worker | 对象需有 `execute()` 方法 |

**TaskPool 中指定执行器类型：**
```python
config = TaskPoolConfig(executor_type="thread", max_workers=10)
pool = TaskPool(executor=my_sync_func, config=config)
```

## 分布式锁管理

```python
# 获取锁
if pool.acquire_lock(task_id, ttl=30):
    # 执行需要互斥的操作
    ...
    pool.release_lock(task_id)

# 异步版本
locked = await pool.acquire_lock_async(task_id, ttl=30)
```

**锁后端选择：**
```python
from neotask import LockConfig

# 内存锁（单进程）
config = TaskPoolConfig(lock_type="memory")

# Redis 分布式锁（多节点）
config = TaskPoolConfig(
    lock_type="redis",
    redis_url="redis://localhost:6379",
    lock_timeout=30,
)
```

**独立使用 LockManager：**
```python
from neotask import LockManager, LockFactory

lock = LockFactory.create_redis("redis://localhost:6379")
manager = LockManager(lock)

# 上下文管理器（自动续期）
async with manager.lock("resource_key", ttl=30, auto_extend=True):
    await do_critical_work()
```

## EventBus 直接使用

当需要超出 `pool.on_*()` 便利方法之外的灵活事件处理时：

```python
from neotask import EventBus, TaskEvent

bus = EventBus()

@bus.subscribe("task.completed")
async def on_complete(event: TaskEvent):
    print(f"Completed: {event.task_id}")

@bus.subscribe_global
async def log_all(event: TaskEvent):
    print(f"[{event.event_type}] {event.task_id}")

# 生命周期
await bus.start()
await bus.emit(TaskEvent(event_type="task.created", task_id="123", data={}))
await bus.stop()

# 或使用上下文管理器
async with bus:
    await bus.emit(...)
```

**内置处理器：**
```python
from neotask.event.handlers import setup_default_handlers

# 一键注册日志、指标、持久化、通知处理器
setup_default_handlers(event_bus, metrics_collector=metrics)
```

## 分布式组件

```python
from neotask import NodeManager, Coordinator, Elector

# 节点管理（自动心跳）
node = NodeManager(redis_url="redis://localhost:6379")
await node.register(metadata={"role": "worker"})
active_nodes = await node.get_active_nodes()

# Leader 选举
elector = Elector(redis_url="redis://localhost:6379")
if await elector.elect(ttl=30):
    print("I am the leader")
leader = await elector.get_leader()

# 负载均衡
coordinator = Coordinator(strategy="round_robin")  # round_robin | random | least_loaded
await coordinator.distribute_task(task_data)
```

**分片策略：**
```python
from neotask import ConsistentHashSharder, ModuloSharder

sharder = ConsistentHashSharder(nodes=["node-1", "node-2", "node-3"])
shard = sharder.get_shard("task_key_123")  # 一致性哈希路由
```

## Web UI 监控面板

```python
from neotask.web.server import WebUIServer

server = WebUIServer(scheduler, host="127.0.0.1", port=8080)
server.start()   # 在后台守护线程中启动 uvicorn
# 访问 http://127.0.0.1:8080
server.stop()
```

安装依赖：`pip install neotask[ui]`

## 限制与约束

- 单任务最大数据量：10MB
- 内存模式最大并发：1000
- 超时范围：1-3600 秒
- 优先级范围：1-5（1 最高）
- 最大延迟：30 天

## 向用户询问

当用户请求不明确时，依次澄清：

1. **任务类型**：即时 / 延时 / 周期 / Cron？
2. **执行内容**：同步函数还是异步函数？CPU 密集还是 IO 密集？
3. **部署规模**：单机 (memory/sqlite) 还是分布式 (redis)？
4. **持久化需求**：需要任务持久化吗？
5. **重试策略**：失败后需要重试吗？几次？间隔多久？

## 代码生成规范

- 始终先 `from neotask import TaskPool, TaskScheduler` 导入
- executor 可以是同步函数（自动包装为异步）或异步函数
- 推荐使用上下文管理器 `with TaskPool(...) as pool:` 模式
- 按时调用 `shutdown()` 或使用 `with` 语句自动清理
- 使用异步 executor 以获得最佳性能
- 生产环境推荐 `storage_type="sqlite"` 持久化
