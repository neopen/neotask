---
kind: error_handling
name: NeoTask 错误处理体系：分层异常、死信队列与重试策略
category: error_handling
scope:
    - '**'
source_files:
    - src/neotask/common/exceptions.py
    - src/neotask/storage/exceptions.py
    - src/neotask/executor/exceptions.py
    - src/neotask/core/lifecycle.py
    - src/neotask/queue/dead_letter.py
    - src/neotask/core/engine.py
    - src/neotask/worker/supervisor.py
---

## 1. 整体方案概述

NeoTask 采用「分层异常 + 事件总线 + 死信队列」的组合式错误处理架构，将错误分为三类路径：
- 调用方可见的领域异常（通过 `common/exceptions` 暴露）
- 执行期异常（由执行器包装为 `ExecutorError` 子类型）
- 最终失败任务持久化到死信队列（Dead Letter Queue），供后续重放或人工干预

该设计遵循 Python 标准异常层次，未使用 `panic/recover` 类机制，所有异步流程均基于 `asyncio.CancelledError` 和显式 `raise`。

## 2. 核心文件与包

- **领域异常定义**：`src/neotask/common/exceptions.py`
  - `TaskSchedulerError` 作为调度层基类，派生出 `TaskNotFoundError`、`TaskAlreadyExistsError`、`QueueFullError`、`TimeoutError` 等
- **存储层异常**：`src/neotask/storage/exceptions.py`
  - `StorageError` 基类 → `ConnectionError`、`TransactionError`、`QueueEmptyError`、`TaskNotFoundError`
- **执行器异常**：`src/neotask/executor/exceptions.py`
  - `ExecutorError` 基类 → `ExecutionTimeoutError`、`ExecutionCancelledError`、`InvalidExecutorError`
- **生命周期管理**：`src/neotask/core/lifecycle.py`
  - 统一封装任务状态转换，在 `wait_for_task` 中把终态结果转换为异常抛出
- **死信队列**：`src/neotask/queue/dead_letter.py`
  - `DeadLetterReason` 枚举（`MAX_RETRIES`、`NODE_CRASH`、`TIMEOUT`、`CANCELLED`、`ORPHANED`、`BUSINESS_ERROR`）
  - `DeadLetterEntry` 记录原始任务、失败次数、最后错误、重试历史等
- **引擎入口**：`src/neotask/core/engine.py`
  - 顶层 Facade，内部组件错误通过事件总线传播，不直接吞掉异常
- **Worker 监督者**：`src/neotask/worker/supervisor.py`
  - 健康检查失败时自动重启 worker，捕获 `asyncio.CancelledError` 优雅退出

## 3. 架构与约定

### 3.1 异常层次
```
Exception
├── TaskSchedulerError          # 调度层
│   ├── TaskNotFoundError
│   ├── TaskAlreadyExistsError
│   ├── QueueFullError
│   └── TimeoutError
├── StorageError                # 存储层
│   ├── ConnectionError
│   ├── TransactionError
│   ├── QueueEmptyError
│   └── TaskNotFoundError
└── ExecutorError               # 执行层
    ├── ExecutionTimeoutError
    ├── ExecutionCancelledError
    └── InvalidExecutorError
```
每个模块仅抛出自域异常，跨层调用时由上层统一捕获并转换为对应语义的异常。

### 3.2 等待与超时
- `lifecycle.wait_for_task` 对终态任务直接 raise 内置 `Exception`（含错误消息），对非终态委托给 `FutureManager.wait`；若底层返回 `common.TimeoutError` 则原样向上抛出。
- 调用方应优先捕获 `TaskSchedulerError` 及其子类，而非裸 `Exception`。

### 3.3 执行期错误传播
- 执行器在执行失败时抛出 `ExecutorError` 子类型；`supervisor` 监控循环以 `except Exception: await asyncio.sleep(5)` 兜底，避免单点故障扩散。
- 引擎启动/停止阶段遇到配置错误（如未知 storage_type）直接 `raise ValueError/RuntimeError`，属于初始化期快速失败。

### 3.4 死信队列与重试
- 当任务达到最大重试次数、节点崩溃、超时或被取消时，由上层逻辑将其序列化后写入 Redis 列表 `neotask:dead_letter`，同时维护 Hash 索引 `neotask:dead_letter:index`。
- 提供 `replay(task_id)` 接口支持从死信恢复，但实际重新提交需外部调度器配合。
- 可注册 `set_alert_callback(entry)` 实现告警通知。

### 3.5 事件驱动的错误观测
- 所有状态变更（包括 `task.failed`、`task.cancelled`）通过 `EventBus` 广播，消费者可据此做审计、指标上报或触发告警。

## 4. 开发者规则

1. **自定义业务错误**：继承 `TaskSchedulerError` 或对应模块基类，不要直接使用裸 `Exception`。
2. **捕获粒度**：外层只捕获具体异常类型；仅在边界处（CLI、Web 路由）才 catch 宽泛异常并转为 HTTP 响应。
3. **超时处理**：等待任务时使用 `TimeoutError` 区分“等待超时”和“任务本身失败”。
4. **死信消费**：通过 `DeadLetterQueue.list/replay` 定期巡检，结合 `DeadLetterReason` 决定是重试还是转人工。
5. **异步取消**：只捕获 `asyncio.CancelledError` 做资源清理，不要吞掉它。
6. **日志关联**：所有异常抛出前通过 `logger.debug/warning/info` 记录上下文，确保可追踪。
