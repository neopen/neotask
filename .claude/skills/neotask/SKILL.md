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
