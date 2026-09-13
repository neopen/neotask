# Changelog

所有重要的项目更改都将记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 计划中 (v1.5 / v2.0)
- **任务编排（v1.5）**
  - DAG 工作流引擎（`WorkflowEngine`）
  - 条件分支执行
  - 并行任务编排
- **企业级功能（v2.0）**
  - 独立 Web UI 监控面板（多进程形态）
  - Prometheus 指标拉取端点
  - 多租户隔离支持

> v0.4/v0.5/v1.0 的能力（Redis 共享队列、分布式锁、预取、看门狗续期、超时检测、故障自动恢复）均已发布，记录见下。

---

## [1.0.2] - 2026-09-12

### 修复

- **分布式链路缺陷修复**：Redis 延迟队列迁移回优先级队列时丢失优先级的问题——`queue:delayed` 只存 task_id 到到期时间的映射、不带优先级字段，迁移时统一按 `TaskPriority.NORMAL` 入队
- **延时队列回调静默失败**：`DelayedQueue` 中回调异常被 `except Exception: pass` 吞掉，改为记录错误日志
- **Python 3.8 运行期阻塞点**：`core/future.py` 的 `self._futures: dict[str, TaskFuture]` 是会在运行期求值的属性注解，PEP 585 泛型在 3.8 抛 `TypeError`；改回 `typing.Dict`

### 变更

- **依赖收敛**：`pip install neotask` 的强制依赖由 4 个（aiosqlite/redis/psutil/croniter）收敛为 2 个（`aiosqlite>=0.19.0`、`croniter>=1.0.0`）；`redis`、`psutil` 移回可选 extra，源码中原有 import 守卫（`HAS_REDIS`、psutil 惰性导入）保证缺失时仍可 `import neotask`
- **最低 Python 版本统一为 ≥ 3.8**：`requires-python`、classifiers、black/mypy/ruff 的 target-version 全部对齐 3.8
- **版本号三方对齐**：`pyproject.toml` / `[tool.bumpversion]` / `neotask.__version__` 统一为 `1.0.2`（此前 1.0.0/1.0.1/1.0.2 混用）
- **CI 矩阵**：`.github/workflows/ci.yml` 改为 3.8 / 3.10 / 3.12 三版本矩阵，并新增 `import neotask` 冒烟测试作为最低版本保护线

### 文档

- 依赖与版本口径统一到 1.0.2 / Python ≥ 3.8，覆盖 `README.md`、`README_zh.md`、`docs/`、示例与系列文章
- 移除 README 中尚未实现的 `WorkflowEngine` / `engine.submit_workflow` 等 API 条目与「DAG 工作流」特性宣传，改由路线图 timeline 的 `v1.5` 段承载
- 将「零依赖部署」表述修正为「无外部服务依赖」，并如实标注 2 个基础依赖

---

## [1.0.1] - 2026-08-20

### 修复

- 状态守门、回收判据、心跳契约等行为修复

---

## [1.0.0] - 2026-05-11

### 新增

- 死信队列（`DeadLetterQueue`，Redis 模式默认启用，保留 7 天，支持 `replay`）

---

## [0.5.0] - 2026-05-10

### 变更

- 该 tag 无文件级变化（预取机制与批量操作已在此前版本落地）

---

## [0.4.1] - 2026-05-08

### 移除

- 删除 `distributed/elector`（Redis 主节点选举存活 7 天后弃用）

---

## [0.4.0] - 2026-05-01

### 新增

- `distributed` 包：`node`（节点管理与心跳）、`coordinator`、`elector`、`sharding`
- Redis 共享队列与分布式锁

---

## [0.3.0] - 2026-04-22

### 新增功能

#### 定时任务调度器 (`TaskScheduler`)
- **延时任务** (`submit_delayed`) - 支持指定延迟秒数后执行
- **定时任务** (`submit_at`) - 支持指定具体时间点执行
- **周期任务** (`submit_interval`) - 支持固定间隔重复执行
- **Cron 任务** (`submit_cron`) - 支持标准 5 字段 Cron 表达式
- 任务暂停/恢复 (`pause_periodic` / `resume_periodic`)
- 任务取消 (`cancel_periodic`)
- 周期任务列表查询 (`get_periodic_tasks`)
- 优雅关闭支持

#### 延迟队列 (`DelayedQueue`)
- 独立的时间堆实现
- 自动到期触发机制
- 任务取消支持
- 批量调度支持
- 统计信息收集

#### Cron 表达式解析器 (`CronParser`)
- 标准 5 字段 Cron 语法支持（分钟、小时、日、月、周）
- 特殊字符支持：`*`（任意）、`,`（列表）、`-`（范围）、`/`（步长）
- 预定义表达式：`@yearly`、`@monthly`、`@weekly`、`@daily`、`@hourly`
- 获取下次执行时间 (`next`)
- 获取上次执行时间 (`previous`)
- 批量获取多次执行时间 (`get_next_n`)
- 表达式验证 (`is_valid`)
- 中文描述生成 (`describe`)

#### 时间轮 (`TimeWheel`)
- 高性能定时器实现
- 多轮调度支持（大延迟任务）
- 槽位数量可配置
- 滴答间隔可配置
- 任务取消支持

#### 周期任务管理器 (`PeriodicTaskManager`)
- 周期任务完整生命周期管理
- 固定间隔和 Cron 两种调度模式
- 最大执行次数限制 (`max_runs`)
- 时间范围限制 (`start_at` / `end_at`)
- 错过执行策略 (`MissedExecutionPolicy`)
  - `IGNORE` - 忽略错过的执行
  - `RUN_ONCE` - 只执行一次（不追赶）
  - `CATCH_UP` - 追赶所有错过的执行
  - `SKIP` - 跳过错过的执行
- 任务更新 (`update_task`)
- 任务暂停/恢复 (`pause` / `resume`)
- 任务删除 (`delete_task`)
- 标签过滤和分页查询 (`list_tasks`)
- 执行历史记录 (`get_execution_history`)

#### 配置增强
- 新增 `SchedulerConfig` 配置类
  - `scan_interval` - 调度循环扫描间隔
  - `enable_periodic_manager` - 是否启用周期任务管理器
  - `enable_time_wheel` - 是否启用时间轮
  - `time_wheel_slots` - 时间轮槽位数
  - `time_wheel_tick` - 时间轮滴答间隔
  - `default_max_runs` - 默认最大执行次数
  - `default_missed_policy` - 默认错过执行策略
- 预设配置工厂方法
  - `high_performance()` - 高性能配置（启用时间轮）
  - `lightweight()` - 轻量级配置（禁用周期管理器）

### 改进
- 优化 `TaskScheduler` 与 `TaskPool` 的委托关系
- 完善事件循环处理，支持混合线程/异步场景
- 增强 Cron 表达式中月末(`L`)和星期(`#`)支持
- 改进延迟队列的调度精度
- 统一配置类命名和结构
- 完善异常处理和错误信息

### 文档
- 新增定时任务使用示例
  - `examples/05_cron_tasks.py` - Cron 任务示例
  - `examples/06_delayed_tasks.py` - 延时任务示例
  - `examples/08_periodic.py` - 周期任务示例
- 更新 README 添加 TaskScheduler API 说明
- 补充架构演进路线图
- 新增 API 参考文档

### 测试
- 新增 Cron 表达式解析器单元测试 (`test_cron_parser.py`)
- 新增延迟队列功能测试 (`test_delayed_queue.py`)
- 新增时间轮单元测试 (`test_time_wheel.py`)
- 新增周期任务管理器测试 (`test_periodic_manager.py`)
- 新增 TaskScheduler 集成测试 (`test_task_scheduler.py`)
- 测试覆盖率提升至 85%+

### 修复
- 修复事件循环嵌套导致的 `RuntimeError`
- 修复 `_periodic_tasks` 缺失问题
- 修复 `_execute_periodic_task` 参数传递错误
- 修复调度线程中事件循环处理

---

## [0.2.0] - 2026-04-15

### 新增功能
- **事件总线系统** (`EventBus`)
  - 支持任务生命周期事件监听
  - 内置事件类型：created, started, completed, failed, cancelled
  - 异步事件处理器注册
  - 全局事件订阅支持
  
- **指标收集器** (`MetricsCollector`)
  - 实时任务统计（总数、成功数、失败数、取消数）
  - 执行时间统计（平均值、P95）
  - 排队时间统计
  - Worker 池状态监控
  - 队列长度跟踪
  
- **健康检查** (`HealthChecker` / `SystemHealthChecker`)
  - 系统资源监控（CPU、内存、磁盘）
  - Worker 存活检测
  - 存储后端可用性检查
  - 队列健康状态检查
  - 健康状态聚合 (`HEALTHY` / `DEGRADED` / `UNHEALTHY`)

- **指标报告器**
  - 控制台输出 (`ConsoleReporter`)
  - 文件日志 (`FileReporter`)
  - Prometheus 导出 (`PrometheusReporter`)
  - 报告管理器 (`ReporterManager`)

- **分布式锁** (`LockFactory`)
  - 内存锁实现 (`MemoryLock`)
  - Redis 分布式锁 (`RedisLock`)
  - 看门狗自动续期机制 (`WatchDog`)
  - 锁管理器 (`LockManager`)
  - 上下文管理器支持 (`async with lock.lock()`)

### 改进
- 重构 Worker 池管理逻辑
- 优化优先级队列性能
- 增强 SQLite 存储的事务处理
- 改进日志格式和输出
- 完善类型注解覆盖

### 文档
- 新增事件回调使用示例 (`examples/04_events.py`)
- 补充监控和可观测性文档
- 更新架构图展示新组件
- 新增配置说明文档

### 修复
- 修复任务取消时的资源泄漏问题
- 修复并发场景下的竞态条件
- 修复 SQLite 连接未正确关闭的问题
- 修复指标收集器线程安全问题

---

## [0.1.0] - 2026-04-08

### 初始发布

#### 核心任务池 (`TaskPool`)
- 异步任务提交和执行
- 优先级队列支持（0: CRITICAL, 1: HIGH, 2: NORMAL, 3: LOW）
- 多 Worker 并发处理
- 任务状态追踪（PENDING, RUNNING, SUCCESS, FAILED, CANCELLED, SCHEDULED）
- 任务结果存储和查询
- 任务取消支持

#### 存储后端
- **内存存储** (`MemoryTaskRepository` / `MemoryQueueRepository`)
  - 适合开发和测试环境
  - 零配置，高性能
- **SQLite 存储** (`SQLiteTaskRepository` / `SQLiteQueueRepository`)
  - 单机持久化
  - 支持事务
  - 自动创建索引
- **Redis 存储** (`RedisTaskRepository` / `RedisQueueRepository`)
  - 分布式共享队列
  - 连接池支持
  - Lua 脚本原子操作

#### 任务执行器
- **异步执行器** (`AsyncExecutor`) - 直接执行异步函数
- **线程执行器** (`ThreadExecutor`) - 线程池执行同步函数
- **进程执行器** (`ProcessExecutor`) - 进程池执行 CPU 密集型任务
- **回调执行器** (`CallbackExecutor`) - 包装普通函数
- 执行器工厂 (`ExecutorFactory`) - 自动选择执行器类型

#### Worker 管理
- 动态 Worker 池 (`WorkerPool`)
- 任务预取机制 (`prefetch_size`)
- Worker 监督者 (`WorkerSupervisor`)
  - 健康检查
  - 自动重启故障 Worker
  - 失败率监控
- 重试机制（可配置次数和延迟）

#### 队列系统
- **优先级队列** (`PriorityQueue`) - 基于堆实现
- **延迟队列** (`DelayedQueue`) - 基于时间堆
- **队列调度器** (`QueueScheduler`) - 整合两种队列
- 暂停/恢复支持
- 队列清空等待

#### 辅助功能
- Future 异步结果等待 (`TaskFuture` / `FutureManager`)
- 任务上下文 (`TaskContext`) - 链路追踪
- 事件总线基础框架
- 异常体系 (`TaskSchedulerError` 系列)

#### Web UI（可选）
- FastAPI 后端服务
- 实时任务监控面板
- WebSocket 实时推送
- 任务控制操作（取消、重试）

#### 配置类
- `StorageConfig` - 存储配置
- `LockConfig` - 锁配置
- `WorkerConfig` - Worker 配置
- `QueueConfig` - 队列配置
- `ExecutorConfig` - 执行器配置
- `WebUIConfig` - Web UI 配置
- `TaskPoolConfig` - TaskPool 配置
- `TaskConfig` - 统一配置（兼容旧版）

### 示例代码
- `examples/00_quick_start.py` - 快速入门
- `examples/01_simple.py` - 基础用法
- `examples/02_context_manager.py` - 上下文管理器
- `examples/03_priority.py` - 优先级调度
- `examples/04_events.py` - 事件回调
- `examples/05_cron_tasks.py` - Cron 任务
- `examples/06_delayed_tasks.py` - 延时任务
- `examples/07_batch.py` - 批量任务处理
- `examples/08_periodic.py` - 周期任务
- `examples/09_retry_and_cancel.py` - 重试和取消
- `examples/10_task_query.py` - 任务查询
- `examples/99_all_features.py` - 全功能演示

---

## 版本说明

### 版本号规则
- **主版本号** (MAJOR)：不兼容的 API 修改
- **次版本号** (MINOR)：向下兼容的功能性新增
- **修订号** (PATCH)：向下兼容的问题修正

### 符号说明
- `新增` - 新功能或组件
- `改进` - 现有功能的优化
- `修复` - Bug 修复
- `变更` - 破坏性变更（会在升级指南中详细说明）
- `废弃` - 即将移除的功能
- `移除` - 已移除的功能
- `安全` - 安全相关的修复

---

## 升级指南

### 从 1.0.1 升级到 1.0.2

#### 依赖变更（唯一需要动作的地方）

`redis`、`psutil` 不再是强制依赖。如果你的项目此前依赖 `pip install neotask` 顺带装上它们，升级后需显式声明：

```bash
# 用到 Redis 存储 / 分布式锁 / 去中心化
pip install "neotask[redis]"

# 用到系统指标采集（get_stats()["metrics"]["system"]、健康检查 system 项）
pip install "neotask[monitor]"

# 或一次装全
pip install "neotask[full]"
```

#### 最低 Python 版本

`requires-python` 由 `>=3.9` 降为 `>=3.8`，对已有环境无影响；Python 3.8 用户此前会被 pip 拒绝安装，现在可用。

#### API 变更

无破坏性变更，完全向后兼容。

---

### 从 0.2.x 升级到 0.3.0

#### 新增依赖
如果使用定时任务功能，无需额外依赖。Cron 解析器为纯 Python 实现。

#### API 变更
无破坏性变更，完全向后兼容。

#### 迁移步骤

```python
# 1. 导入新的调度器
from neotask import TaskScheduler

# 2. 创建调度器实例（与 TaskPool 类似）
scheduler = TaskScheduler(executor=your_executor)

# 3. 使用新的定时任务方法
# 延时任务
scheduler.submit_delayed(data, delay_seconds=60)

# 周期任务
scheduler.submit_interval(data, interval_seconds=300)

# Cron 任务
scheduler.submit_cron(data, "0 9 * * *")

# 4. 周期任务管理
scheduler.pause_periodic(task_id)
scheduler.resume_periodic(task_id)
scheduler.cancel_periodic(task_id)

# 5. 查询周期任务
tasks = scheduler.get_periodic_tasks()
task = scheduler.get_periodic_task(task_id)
```



#### 配置迁移

```python
# 旧配置（0.2.x）- 无变化
config = TaskPoolConfig(storage_type="memory")

# 新配置（0.3.x）- 可选使用 SchedulerConfig
from neotask.models.config import SchedulerConfig

scheduler_config = SchedulerConfig(
    scan_interval=1.0,
    enable_periodic_manager=True,
    enable_time_wheel=False
)
scheduler = TaskScheduler(executor=executor, config=scheduler_config)
```



### 从 0.1.x 升级到 0.2.0

#### 新增依赖

如果使用 Redis 分布式锁，需要安装：

```bash
pip install neotask[redis]
```



#### API 变更

无破坏性变更，事件总线和监控组件为可选功能。

#### 迁移步骤

```python
# 1. 注册事件处理器（可选）
pool.on_completed(async def on_complete(event): ...)

# 2. 启用指标收集（可选）
from neotask.monitor import MetricsCollector
collector = MetricsCollector()

# 3. 使用分布式锁（可选）
from neotask.lock import LockFactory
lock = LockFactory.create_redis("redis://localhost:6379")
```



------

## 贡献者

感谢以下贡献者对项目的支持：

- **neopen** - 项目创始人和维护者
- 欢迎更多开发者参与贡献！

------

**注意**: 对于更详细的提交历史，请查看 [GitHub Releases](https://github.com/neopen/neotask/releases) 或 Git 提交记录。
