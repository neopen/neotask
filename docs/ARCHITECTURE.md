# 任务管理器（NeoTask）

轻量级 Python 异步任务队列管理器，无需额外服务，开箱即用。

NeoTask 是一个纯 Python 实现的异步任务调度系统，专为耗时任务（AI 生成、视频处理、数据爬取等）设计。无需部署 Redis、PostgreSQL 等外部服务，安装后即可在任意 Python 项目中直接使用。

[中文](../README_zh.md) | [English](../README.md) | [文档](https://pengline.cn/2026/04/243d5a536d064df59c2ec8668362b8b5/) | [PyPI](https://pypi.org/project/neotask/) | [官网](https://task.helpenx.com)

---

## 核心功能

| 功能 | 说明 |
|------|------|
| **无外部服务依赖** | 纯 Python 实现，内置 SQLite 支持，无需启动独立服务（基础包仅需 aiosqlite、croniter 两个依赖） |
| **即时任务** | 提交后立即进入队列，支持优先级调度 |
| **延时任务** | 指定延迟时间或具体时间点执行 |
| **周期任务** | 支持固定间隔和 Cron 表达式周期执行 |
| **异步并发调度** | 基于 asyncio，支持多 Worker 并发执行 |
| **优先级队列** | 高优先级任务优先执行 |
| **自动重试** | 失败任务自动重试，支持配置重试次数 |
| **持久化存储** | 内存/SQLite/Redis 多种存储后端 |
| **任务等待** | 支持同步/异步等待任务完成并获取结果 |
| **事件回调** | 支持任务创建、开始、完成、失败等事件回调 |
| **进度上报** | executor 内可发布进度（`task.progress` 事件），并持久化到任务记录 |

---

## 架构设计

```mermaid
graph TB
    subgraph User["用户应用层"]
        APP[用户代码]
    end
    
    subgraph NeoTask["NeoTask 核心"]
        TP[TaskPool<br/>即时任务入口]
        TS[TaskScheduler<br/>定时任务入口]
        
        subgraph Core["共享核心组件"]
            LM[LifecycleManager<br/>任务生命周期管理]
            QS[QueueScheduler<br/>优先级+延迟队列]
            WP[WorkerPool<br/>Worker池/并发控制]
            FM[FutureManager<br/>异步等待/结果回调]
        end
        
        subgraph Internal["内部组件"]
            EB[EventBus<br/>事件总线]
            MC[MetricsCollector<br/>指标收集]
            LF[LockFactory<br/>分布式锁]
        end
        
        EX[TaskExecutor<br/>用户业务逻辑]
    end
    
    subgraph Storage["存储层"]
        MEM[MemoryStorage]
        SQLITE[(SQLiteStorage)]
        REDIS[(RedisStorage)]
    end
    
    APP -->|即时任务| TP
    APP -->|定时任务| TS
    TS -->|委托| TP
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

### 即时任务数据流图

```mermaid
sequenceDiagram
    participant User
    participant Pool as TaskPool
    participant LM as LifecycleManager
    participant QS as QueueScheduler
    participant WP as WorkerPool
    participant FM as FutureManager
    participant Storage as Storage

    User->>Pool: submit(data, priority, delay)
    Pool->>LM: create_task()
    LM->>Storage: 保存任务(PENDING)
    LM-->>Pool: 返回 Task 对象
    Pool->>QS: push(task_id, priority, delay)
    Pool->>FM: get(task_id)
    Pool-->>User: 返回 task_id
    
    loop Worker 调度循环
        WP->>QS: pop() 获取任务
        WP->>LM: start_task(task_id)
        WP->>Storage: 更新状态(RUNNING)
        
        alt 任务执行成功
            WP->>WP: executor.execute(data)
            WP->>LM: complete_task(result)
            WP->>FM: complete(task_id, result)
            WP->>Storage: 更新状态(SUCCESS)
        else 任务执行失败
            WP->>LM: fail_task(error)
            WP->>FM: complete(task_id, error)
            WP->>Storage: 更新状态(FAILED)
            WP->>QS: push(重试入队)
        else 任务取消
            WP->>LM: cancel_task()
            WP->>FM: complete(task_id, error)
        end
    end
    
    User->>Pool: wait_for_result(task_id)
    Pool->>FM: wait()
    FM-->>Pool: 返回结果
    Pool-->>User: 任务结果
```

### 定时任务数据流图

```mermaid
sequenceDiagram
    participant User
    participant TS as TaskScheduler
    participant Pool as TaskPool
    participant Scheduler as 调度循环
    participant Storage as Storage

    User->>TS: submit_delayed(data, delay)
    TS->>Pool: submit(data, delay)
    Pool-->>TS: task_id
    TS-->>User: task_id
    
    User->>TS: submit_interval(data, interval, run_immediately)
    TS->>TS: 创建 PeriodicTask
    TS->>Storage: 保存周期任务配置
    TS-->>User: task_id
    
    alt run_immediately = True
        TS->>Pool: submit(data, task_id)
    end
    
    loop 调度循环扫描
        Scheduler->>Scheduler: 检查到期周期任务
        alt 任务到期
            Scheduler->>Pool: submit(data, task_id)
            Scheduler->>Scheduler: 更新 next_run
        end
    end
```



### 组件关系图

```mermaid
graph LR
    subgraph External["外部"]
        USER[用户代码]
        EXEC[业务执行函数]
    end
    
    subgraph Entry["入口层"]
        POOL[TaskPool<br/>即时任务]
        SCHED[TaskScheduler<br/>定时任务]
    end
    
    subgraph Core["共享核心"]
        LM[LifecycleManager]
        QS[QueueScheduler]
        WP[WorkerPool]
        FM[FutureManager]
    end
    
    subgraph Internal["内部组件"]
        EB[EventBus]
        MC[MetricsCollector]
        LF[LockFactory]
        DQ[DelayedQueue]
        PQ[PriorityQueue]
    end
    
    subgraph Storage["存储层"]
        MEM[Memory]
        SQL[SQLite]
        RDS[Redis]
    end
    
    USER -->|即时| POOL
    USER -->|定时| SCHED
    SCHED -->|委托| POOL
    
    POOL --> LM
    POOL --> QS
    POOL --> WP
    POOL --> FM
    
    LM --> MEM
    LM --> SQL
    LM --> RDS
    
    QS --> DQ
    QS --> PQ
    
    WP --> EB
    WP --> MC
    WP --> LF
```

### 事件循环隔离与存储客户端

`TaskPool` 在守护线程中运行自己的事件循环，同步方法通过 `run_coroutine_threadsafe` 投递协程；而 `ConnectionPool` 等异步原语一旦在某个 loop 中首次使用，就无法再从其它 loop 访问。为避免跨事件循环错误，Redis 与 SQLite 访问统一经过 `src/neotask/utils` 下的代理层：

| 组件 | 职责 |
| :--- | :--- |
| `DedicatedLoopRunner`（`utils/dedicated_loop.py`） | 进程内共享的专用事件循环线程（daemon，惰性启动，异常退出后自动重建）；Redis 与 SQLite 共用同一 loop |
| `LoopBoundRedisClient`（`utils/redis_compat.py`） | 首次使用时创建 `ConnectionPool` 并缓存客户端，所有命令投递到专用 loop 执行；检测到共享 loop 变化时丢弃旧客户端并重新建池 |
| `LoopBoundSQLiteConnection`（`utils/sqlite_compat.py`） | 同上，SQLite 连接与游标只在专用 loop 内使用 |

要点：

- 连接池仍然复用，不会每条命令新建连接；代价是每条命令一次跨线程投递（微秒级）。
- 每个仓储 / 锁 / 节点管理器实例持有独立的客户端与连接池，`max_connections` 默认 10。
- 专用 loop 线程异常退出并被重建时，代理会自动丢弃绑定在已关闭 loop 上的旧客户端，在新 loop 中重新建池。

### 分布式与高可用

| 组件 | 说明 |
| :--- | :--- |
| `NodeManager`（`distributed/node.py`） | 节点注册/注销、心跳上报、活跃节点查询 |
| `Coordinator`（`distributed/coordinator.py`） | 去中心化对等调度，支持 `round_robin` / `random` / `least_loaded`；无 Leader 选举（`Elector` 已移除） |
| 分片器（`distributed/sharding.py`） | `ConsistentHashSharder`（支持 md5 / sha1 / sha256 / sha3_256）、`ModuloSharder`（稳定 md5 取模）、`RangeSharder`（闭区间命中；高于末尾上界归末分片；其余返回 -1） |
| `HeartbeatManager`（`core/heartbeat.py`） | 节点心跳与超时判定 |
| `Reclaimer`（`worker/reclaimer.py`） | 回收离线节点的 RUNNING 任务并重新入队 |
| `DeadLetterQueue`（`queue/dead_letter.py`） | Redis 模式下默认启用，保留 7 天，支持 `replay` |

跨节点等待：`LifecycleManager.wait_for_task` 采用“Future 通知 + 0.2s 时间片仓储回查”，远端节点写入的终态会被及时拾取，不会等满 `timeout`。



## 快速上手

### 安装

**环境要求**：Python ≥ 3.8。基础包依赖 `aiosqlite`（SQLite 存储）与 `croniter`（Cron 表达式）两个库；Redis、psutil 等为可选扩展，按需安装。

```sh
# 基础安装
pip install neotask

# 带 Redis 分布式支持
pip install neotask[redis]

# 完整安装
pip install neotask[full]
```



### 即时任务（TaskPool）

```python
from neotask import TaskPool, TaskPoolConfig

# 1. 定义任务处理函数（异步）
async def my_executor(data: dict) -> dict:
    """业务逻辑处理函数"""
    print(f"处理任务: {data}")
    # 模拟耗时操作
    import asyncio
    await asyncio.sleep(0.1)
    return {"result": "success", "input": data}

# 2. 创建任务池
pool = TaskPool(
    executor=my_executor,
    config=TaskPoolConfig(
        worker_concurrency=5,      # 并发 Worker 数
        max_retries=3,             # 最大重试次数
        storage_type="sqlite",     # 存储类型: memory/sqlite/redis
    )
)

# 3. 提交即时任务
task_id = pool.submit({"action": "process", "id": 123})

# 4. 等待结果（同步）
result = pool.wait_for_result(task_id, timeout=30)
print(f"任务结果: {result}")

# 5. 异步方式
import asyncio
async def main():
    task_id = await pool.submit_async({"action": "async_process"})
    result = await pool.wait_for_result_async(task_id)
    return result

# 6. 优雅关闭
pool.shutdown()
```

### 定时任务（TaskScheduler）

```python
from neotask import TaskScheduler, SchedulerConfig
from datetime import datetime, timedelta

async def my_executor(data: dict) -> dict:
    """业务逻辑处理函数"""
    print(f"定时任务执行: {data}")
    return {"status": "done", "timestamp": datetime.now().isoformat()}

# 创建调度器
scheduler = TaskScheduler(
    executor=my_executor,
    config=SchedulerConfig(
        storage_type="sqlite",
        worker_concurrency=5,
        max_retries=3
    )
)

# 延时执行（60秒后执行）
task_id = scheduler.submit_delayed(
    {"action": "delayed_task"},
    delay_seconds=60
)

# 指定时间点执行
tomorrow = datetime.now() + timedelta(days=1)
task_id = scheduler.submit_at(
    {"action": "scheduled_task"},
    execute_at=tomorrow
)

# 周期执行（每5分钟执行一次）
task_id = scheduler.submit_interval(
    {"action": "periodic_task"},
    interval_seconds=300,
    run_immediately=True   # 是否立即执行第一次
)

# Cron 表达式（每天9点执行）
task_id = scheduler.submit_cron(
    {"action": "daily_report"},
    cron_expr="0 9 * * *"
)

# 管理周期任务
scheduler.pause_periodic(task_id)   # 暂停
scheduler.resume_periodic(task_id)  # 恢复
scheduler.cancel_periodic(task_id)  # 取消

# 等待任务完成
result = scheduler.wait_for_result(task_id, timeout=300)

# 关闭调度器
scheduler.shutdown()
```



### 使用上下文管理器

```python
from neotask import TaskPool

# 自动启动和关闭
with TaskPool(executor=my_executor) as pool:
    task_id = pool.submit({"data": "value"})
    result = pool.wait_for_result(task_id)
    print(result)
# 退出上下文时自动关闭
```

### 使用事件回调

```python
from neotask import TaskPool

async def on_task_created(event):
    print(f"任务创建: {event.task_id}")

async def on_task_completed(event):
    print(f"任务完成: {event.task_id}, 结果: {event.data}")

async def on_task_failed(event):
    print(f"任务失败: {event.task_id}, 错误: {event.data}")

pool = TaskPool(executor=my_executor)
pool.start()

# 注册事件回调
pool.on_created(on_task_created)
pool.on_completed(on_task_completed)
pool.on_failed(on_task_failed)

task_id = pool.submit({"test": "event"})
result = pool.wait_for_result(task_id)
```



## API 参考

### TaskPool（即时任务）

| 方法                                                        | 说明                       |
| :---------------------------------------------------------- | :------------------------- |
| `submit(data, task_id=None, priority=2, delay=0, ttl=3600)` | 提交即时任务，返回 task_id |
| `submit_async(...)`                                         | 异步提交任务               |
| `submit_batch(tasks, priority=2)`                           | 批量提交任务               |
| `wait_for_result(task_id, timeout=300)`                     | 同步等待任务完成           |
| `wait_for_result_async(task_id, timeout)`                   | 异步等待任务完成           |
| `wait_all(task_ids, timeout)`                               | 等待所有任务完成           |
| `get_status(task_id)`                                       | 获取任务状态               |
| `get_result(task_id)`                                       | 获取任务结果               |
| `get_task(task_id)`                                         | 获取完整任务信息           |
| `task_exists(task_id)`                                      | 检查任务是否存在           |
| `cancel(task_id)`                                           | 取消任务                   |
| `delete(task_id)`                                           | 删除任务                   |
| `retry(task_id, delay=0)`                                   | 重试失败的任务             |
| `get_stats()`                                               | 获取实时统计               |
| `pause()`                                                   | 暂停处理新任务             |
| `resume()`                                                  | 恢复处理新任务             |
| `clear_queue()`                                             | 清空队列                   |
| `shutdown(graceful=True)`                                   | 关闭任务池                 |
| `on_created(handler)`                                       | 注册任务创建事件           |
| `on_started(handler)`                                       | 注册任务开始事件           |
| `on_completed(handler)`                                     | 注册任务完成事件           |
| `on_failed(handler)`                                        | 注册任务失败事件           |
| `on_cancelled(handler)`                                     | 注册任务取消事件           |
| `on_progress(handler)`                                      | 注册任务进度事件           |

### TaskScheduler（定时任务）

| 方法                                                         | 说明                |
| :----------------------------------------------------------- | :------------------ |
| `submit_delayed(data, delay_seconds, task_id=None, priority=2)` | 延时执行任务        |
| `submit_at(data, execute_at, task_id=None, priority=2)`      | 指定时间点执行      |
| `submit_interval(data, interval_seconds, task_id=None, priority=2, run_immediately=True, max_runs=None, name="", missed_policy=None)` | 固定间隔周期执行    |
| `submit_cron(data, cron_expr, task_id=None, priority=2, max_runs=None, name="", missed_policy=None)` | Cron 表达式周期执行 |
| `cancel_periodic(task_id)`                                   | 取消周期任务        |
| `pause_periodic(task_id)`                                    | 暂停周期任务        |
| `resume_periodic(task_id)`                                   | 恢复周期任务        |
| `get_periodic_tasks()`                                       | 获取所有周期任务    |
| `wait_for_result(task_id, timeout)`                          | 等待任务完成        |
| `get_status(task_id)`                                        | 获取任务状态        |
| `get_result(task_id)`                                        | 获取任务结果        |
| `cancel(task_id)`                                            | 取消任务            |
| `get_stats()`                                                | 获取实时统计        |
| `shutdown(graceful=True)`                                    | 关闭调度器          |

### TaskStatus 状态枚举

| 状态      | 值          | 说明               |
| :-------- | :---------- | :----------------- |
| PENDING   | "pending"   | 等待执行           |
| RUNNING   | "running"   | 执行中             |
| SUCCESS   | "success"   | 执行成功           |
| FAILED    | "failed"    | 执行失败           |
| CANCELLED | "cancelled" | 已取消             |
| SCHEDULED | "scheduled" | 已调度（定时任务） |

### TaskPriority 优先级

| 优先级   | 值   | 说明               |
| :------- | :--- | :----------------- |
| CRITICAL | 0    | 最高优先级         |
| HIGH     | 1    | 高优先级           |
| NORMAL   | 2    | 普通优先级（默认） |
| LOW      | 3    | 低优先级           |

### 配置选项

#### TaskPoolConfig

```python
from neotask import TaskPoolConfig

config = TaskPoolConfig(
    # 存储配置
    storage_type="memory",      # memory / sqlite / redis
    sqlite_path="neotask.db",   # SQLite 数据库路径
    redis_url=None,             # Redis 连接 URL
    node_id="",                 # 节点标识（默认取主机名）

    # 执行器配置（也接受 "async"/"thread"/"process"/"class"/"auto" 字符串）
    executor_type="async",      # ExecutorType.ASYNC（默认）
    max_workers=10,             # 最大工作线程数

    # Worker 配置
    worker_concurrency=10,      # 并发 Worker 数
    prefetch_size=20,           # 预取任务数量
    enable_prefetch=True,       # 是否启用预取
    task_timeout=None,          # 任务超时时间（秒），None 表示不超时

    # 队列配置
    queue_max_size=10000,       # 队列最大长度
    priority_levels=4,          # 优先级级别数

    # 锁配置
    lock_type="memory",         # memory / redis
    lock_timeout=30,            # 锁超时时间

    # 重试配置
    max_retries=3,              # 最大重试次数
    retry_delay=1.0,            # 重试延迟（秒）

    # 高可用配置
    enable_reclaimer=True,      # 任务回收器
    reclaimer_interval=30.0,    # 回收扫描间隔（秒）
    enable_heartbeat=True,      # 节点心跳
    heartbeat_interval=5,       # 心跳间隔（秒）
    heartbeat_timeout=20,       # 心跳超时（秒）
    enable_dead_letter=True,    # 死信队列
    dead_letter_max_size=10000,
    dead_letter_ttl=604800,     # 死信保留时间（秒，默认 7 天）

    # 监控配置
    enable_metrics=True,        # 启用指标收集
    enable_health_check=True,   # 启用健康检查
    enable_reporter=False,      # 启用上报器
)
```

#### SchedulerConfig

```python
from neotask import SchedulerConfig

config = SchedulerConfig(
    storage_type="memory",      # memory / sqlite / redis
    sqlite_path="neotask.db",   # SQLite 数据库路径
    redis_url=None,             # Redis 连接 URL
    worker_concurrency=10,      # 并发 Worker 数
    max_retries=3,              # 最大重试次数
    retry_delay=1.0,            # 重试延迟（秒）
    enable_persistence=True,    # 启用周期任务持久化（默认开启；memory 存储会告警）
    scan_interval=1.0,          # 调度循环扫描间隔（秒）
    enable_time_wheel=False,    # 启用时间轮（高精度延时）
    time_wheel_slots=60,        # 时间轮槽位数
    time_wheel_tick=1.0,        # 时间轮滴答间隔（秒）
    default_max_runs=None,      # 周期任务默认最大执行次数
    default_missed_policy="skip",  # ignore / run_once / catch_up / skip
)
```



## 版本与展望

### 当前版本：v1.0.3

- ✅ 即时任务队列（优先级、并发控制、批量提交）
- ✅ 内存/SQLite/Redis 存储；周期任务持久化与跨重启恢复
- ✅ 延时 / 固定间隔 / Cron 调度，可选时间轮加速
- ✅ 同步/异步双 API，跨节点结果等待
- ✅ 事件总线、指标收集、健康检查、进度上报
- ✅ 分布式锁、节点心跳、故障任务回收、死信队列
- ✅ 可选 Web UI（`pip install neotask[ui]`）

### 发展路线图

```mermaid
timeline
    title NeoTask 发展路线图

    section v0.1 基础
        即时任务 : TaskPool入口
                 : 优先级队列
                 : 内存/SQLite存储

    section v0.2 可观测性
        监控能力 : EventBus事件总线
                 : MetricsCollector
                 : 健康检查

    section v0.3 定时调度
        调度能力 : TaskScheduler入口
                 : 延时执行/时间轮
                 : 周期任务与Cron表达式

    section v0.4 分布式
        分布式基础 : Redis共享队列
                   : 分布式锁
                   : 节点管理与分片

    section v0.5 性能优化
        吞吐提升 : 预取机制
                 : 批量操作
                 : 连接池复用

    section v1.0 高可用
        可靠性 : 看门狗续期
               : 超时检测
               : 故障任务回收/死信队列

    section v1.5 任务编排
        计划中 : DAG工作流
               : 条件分支
               : 并行执行

    section v2.0 企业级
        计划中 : 多租户隔离
               : Prometheus集成
               : 独立Web UI集群
```

### 能力矩阵

| 能力 | 状态 | 所在模块 |
| :--- | :--- | :--- |
| TaskPool 即时任务 / 优先级 / 批量提交 | ✅ 已交付 | `api/task_pool.py` |
| TaskScheduler 延时 / 周期 / Cron | ✅ 已交付 | `api/task_scheduler.py`、`scheduler/` |
| 内存 / SQLite / Redis 存储 | ✅ 已交付 | `storage/` |
| 周期任务持久化与跨重启恢复 | ✅ 已交付 | `scheduler/periodic.py`、`storage/periodic.py` |
| 事件总线 / 指标 / 健康检查 / 进度上报 | ✅ 已交付 | `event/`、`monitor/` |
| 预取、批量操作、连接池复用 | ✅ 已交付 | `worker/prefetcher.py`、`utils/redis_compat.py` |
| 分布式锁 / 看门狗续期 | ✅ 已交付 | `lock/` |
| 节点心跳 / 任务回收 / 死信队列 | ✅ 已交付 | `core/heartbeat.py`、`worker/reclaimer.py`、`queue/dead_letter.py` |
| 分片路由（一致性哈希 / 取模 / 范围） | ✅ 已交付 | `distributed/sharding.py` |
| Web UI 监控面板（可选 extra） | ✅ 已交付 | `web/` |
| DAG 工作流 / 多租户 / Prometheus | 🚧 计划中（v1.5 / v2.0） | - |

------

## 典型应用场景

| 场景                   | 说明                         | 推荐配置                | 使用入口      |
| :--------------------- | :--------------------------- | :---------------------- | :------------ |
| **AI 文生图/视频生成** | 耗时任务排队，避免阻塞主流程 | `worker_concurrency=3`  | TaskPool      |
| **批量文件处理**       | 转码、压缩、上传等批量操作   | `worker_concurrency=10` | TaskPool      |
| **网页爬虫调度**       | 分布式爬取，防止被封         | `storage_type="redis"`  | TaskPool      |
| **定时报表发送**       | 每天9点发送日报              | `cron="0 9 * * *"`      | TaskScheduler |
| **延迟通知**           | 用户操作后5分钟发送提醒      | `delay_seconds=300`     | TaskScheduler |
| **心跳检测**           | 每30秒检测服务健康状态       | `interval_seconds=30`   | TaskScheduler |
| **后台数据分析**       | 夜间执行数据聚合任务         | `cron="0 2 * * *"`      | TaskScheduler |

------

## 性能参考

| 配置                       | 吞吐量       | 平均延迟 | 说明       |
| :------------------------- | :----------- | :------- | :--------- |
| 单 Worker，任务耗时 100ms  | ~10 tasks/s  | ~100ms   | 基础配置   |
| 10 Workers，任务耗时 100ms | ~100 tasks/s | ~110ms   | 推荐配置   |
| 50 Workers，任务耗时 100ms | ~500 tasks/s | ~120ms   | 高性能配置 |

*注：实际性能受 CPU 核心数、任务复杂度、存储 I/O 影响；内存后端的空操作吞吐实测参见 [性能基准测试](性能基准测试.md)*

------

## 贡献指南

### 开发环境设置

```python
# 克隆仓库
git clone https://github.com/neopen/neotask.git
cd neotask

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安装开发依赖
pip install -e ".[dev]"

# 运行测试
pytest tests/

# 查看测试覆盖率
pytest --cov=src/neotask tests/

# 运行特定模块测试
pytest tests/test_task_pool.py -v
pytest tests/test_task_scheduler.py -v
```

### 项目结构

```
neotask/
├── __init__.py            # 包入口，导出公共 API
├── api/                   # TaskPool / TaskScheduler
├── cli/                   # 命令行入口
├── common/                # 日志、异常、常量
├── config/                # 配置加载
├── contrib/               # 第三方集成
├── core/                  # 生命周期、分发器、Future、心跳
├── distributed/           # 节点管理、协调器、分片
├── event/                 # 事件总线与内置处理器
├── executor/              # 异步/线程/进程/类执行器与工厂
├── lock/                  # 内存/Redis 锁、看门狗
├── models/                # 任务、配置、周期任务模型
├── monitor/               # 指标收集、健康检查、上报
├── queue/                 # 优先级/延迟队列、队列调度器、死信队列
├── scheduler/             # Cron 解析、周期任务管理器、时间轮
├── storage/               # 内存/SQLite/Redis 仓储、周期任务存储
├── utils/                 # 专用事件循环、Redis/SQLite 代理、日志工具
├── web/                   # 可选 Web UI（neotask[ui]）
└── worker/                # Worker 池、监督者、预取、任务回收
```



### 贡献流程

1. Fork 项目仓库
2. 创建特性分支：`git checkout -b feature/amazing_feature`
3. 提交更改：`git commit -m 'Add amazing feature'`
4. 推送分支：`git push origin feature/amazing_feature`
5. 提交 Pull Request

### 代码规范

- 遵循 [PEP 8](https://peps.python.org/pep-0008/) 代码风格
- 添加适当的 [类型注解](https://peps.python.org/pep-0484/)
- 编写单元测试覆盖新功能（覆盖率 ≥ 80%）
- 更新相关文档和示例代码
- 提交信息遵循 [Conventional Commits](https://www.conventionalcommits.org/)

### 测试要求

```sh
# 运行所有测试
pytest tests/

# 运行特定模块测试
pytest tests/unit/test_task.py

# 运行手动测试
python examples/01_simple.py
python examples/05_cron_tasks.py
```

## 问题反馈

- **提交 Issue**：https://github.com/neopen/neotask/issues
- **功能建议**：使用 Enhancement 标签
- **Bug 报告**：使用 Bug 标签并提供复现步骤
- **安全漏洞**：请直接发送邮件至作者邮箱

------

## 许可证

MIT License - 详见 [LICENSE](../LICENSE) 文件

Copyright (c) 2026 neopen

------

## 致谢

感谢所有贡献者和开源社区的支持。

------

## 联系方式

- 项目主页：https://github.com/neopen/neotask
- 作者：NeoPen
- 邮箱：helpenx@gmail.com
- 文档：https://pengline.cn/2026/03/0ec8d887591a40c98be76645d33a2f23