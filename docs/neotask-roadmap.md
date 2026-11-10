# NeoTask 版本演进规划

**项目定位**：轻量级、可嵌入、可被编排的 Python 异步分布式执行引擎。当前阶段聚焦本地任务能力全面性、性能与轻量监控；编排集成后移。

**修订原则**：
1. 编排能力（DAG、动态路由、插件系统、Web Dashboard）由独立编排服务承担，NeoTask 不实现。
2. 当前阶段优先本地任务功能全面性与性能，编排集成（OrchestratorClient）整体后移。
3. 轻量监控作为本地能力的一部分同步交付，不做独立 Web UI。

---

## 一、版本总览

| 版本   | 主题                                           | 状态                  |
| ------ | ---------------------------------------------- | --------------------- |
| v1.0.x | 基础任务池、定时调度、分布式基础、可观测性基础 | 已发布              |
| v1.1.0 | 本地任务能力补全                               | 规划中（当前重点）    |
| v1.2.0 | 性能优化与基准测试、轻量监控                   | 规划中                |
| v1.3.0 | 监控导出器与可靠性补强                         | 规划中                |
| v2.0.0 | 编排集成（后移）                               | 后移                  |

---

## 二、v1.1.0 — 本地任务能力补全

**目标**：在 Memory / SQLite 本地场景下，任务执行能力覆盖 Checkpoint、成本采集、父子关联、自定义失败、本地上下文共享与完整事件类型。

### P0 功能项

#### 1. Checkpoint 断点续跑
- `save_checkpoint(state)`：执行器写入中间状态，持久化到任务记录（Memory / SQLite / Redis）。
- `load_checkpoint()`：执行器读取上次状态。
- `pool.submit(..., resume_from_checkpoint=True)`：重试时从 checkpoint 恢复。
- `task.checkpointed` 事件纳入事件总线。
- `get_task(task_id)` 返回中体现 checkpoint 存在性。
- 优先完整支持 Memory / SQLite，Redis 后端同步跟进。

#### 2. 成本与耗时采集
- 任务元数据标准化 `cost_tracking` 字段：
  `{tokens_in, tokens_out, api_calls, estimated_cost, currency}`
- `report_cost(...)`：执行器上报成本（与 `report_progress` 同风格）。
- 耗时拆分：`queue_wait_ms`（排队等待）+ `duration_ms`（执行耗时）。
- `pool.get_cost_summary(task_ids)`：聚合接口。
- `task.completed` / `task.failed` 事件信封携带 `cost` 字段。

#### 3. 父子任务关联字段
- 任务元数据增加：`parent_task_id`、`child_count`、`completed_children`。
- 查询接口：`pool.get_children(task_id)`、`pool.get_parent(task_id)`。
- **不做自动汇总触发**：父任务完成判断由调用方或编排层决定，NeoTask 不越界。

#### 4. 自定义失败状态
- `report_failure(reason=...)`：执行器标记业务级失败（如 `qa_failed`）。
- `task.failed` 事件携带 `reason` 字段，区别于系统异常。

#### 5. 本地上下文读写原语（Blackboard）
- `context_set(key, value, ttl=...)`
- `context_get(key)`
- `context_delete(key)`
- Memory 后端进程内 dict + TTL 清理；SQLite / Redis 落对应存储。
- 仅在本地生效，不承担跨服务数据传递职责。

#### 6. 事件类型补全
- 已有：`task.created` / `task.started` / `task.completed` / `task.failed` / `task.cancelled` / `task.retry` / `task.progress`
- 新增：`task.checkpointed`、`task.cost_reported`
- 事件信封稳定化，为未来远程上报预留字段（当前不做上报）。

#### 7. 外部指定 task_id（提前小项）
- `pool.submit(data, task_id=..., priority=...)`：支持外部传入 ID。
- `pool.has_task(task_id)`：幂等检查。
- 成本低且本地有价值，提前交付，不构成编排依赖。

### 验收标准
- Memory / SQLite 后端下所有功能可用，每项配示例与测试（覆盖率 ≥80%）。
- CHANGELOG 记录每项功能与对应文档更新。

---

## 三、v1.2.0 — 性能优化与轻量监控

**目标**：以基准测试驱动性能优化，交付 `get_metrics()` 快照与定时推送，形成性能可观测闭环。

### P0 性能优化

#### 1. 批量提交路径深化
- `submit_batch` 改为单事务批量写入（SQLite `executemany`，Memory 批量入队）。
- 压测目标：批量提交 1000 条耗时相比逐条降低一个数量级。

#### 2. 任务状态查询优化
- SQLite 后端增加索引：`task_id`、`status`、`created_at`。
- Memory 后端用字典索引替代线性查找。

#### 3. 工作池调度开销
- 评估 `heapq` 替代当前优先级队列结构。
- 同优先级任务批量出队。
- 减少高并发（`worker_concurrency` 50+）下每任务的调度切换次数。

#### 4. 进度上报节流
- `report_progress` 可配置节流（默认每 0.5s 或进度变化 ≥5% 才落库）。
- 事件总线仍实时推送，避免高频写入拖慢执行器。

#### 5. SQLite WAL 与连接复用
- 确认 WAL 模式启用。
- 连接池复用，避免每任务新开连接。
- 批量提交与进度写入走同一连接事务边界。

#### 6. 基准测试套件
- 新增 `benchmarks/` 目录，覆盖：
  - 单任务提交延迟
  - 批量提交吞吐
  - 高并发执行（1000 任务）
  - 进度上报频率对吞吐的影响
  - Checkpoint 写入开销
- 每次版本发布跑基准，防止性能回退。

### P0 轻量监控

#### 1. `pool.get_metrics()` 快照接口
返回字段：
- 各状态任务数：`pending_count` / `running_count` / `completed_count` / `failed_count`
- `worker_concurrency` / `active_workers`
- 按优先级队列深度 `queue_depth_by_priority`
- `retry_queue_size`
- `oldest_pending_age_seconds`
- `storage_backend` / `storage_healthy`
- `checkpoint_count`
- `context_key_count`
- 同步、无副作用。

#### 2. `pool.on_metrics(handler, interval=...)`
- 定时推送快照，避免调用方轮询。

#### 3. 监控指标纳入性能观测
- 基准测试的吞吐、延迟、队列深度通过 `get_metrics()` 暴露，形成"性能可观测"闭环。

### 验收标准
- 基准测试显示批量提交吞吐提升、查询延迟下降，无性能回退（优化前后数据记入 CHANGELOG）。
- `get_metrics()` 字段完整，示例可运行。

---

## 四、v1.3.0 — 监控导出器与可靠性补强

**目标**：提供 Prometheus 导出器（可选 extra），补强崩溃恢复、优雅停机、重试语义。

### P1 监控导出器

#### 1. Prometheus 导出器（可选 extra）
- `pip install neotask[metrics]`。
- `MetricsExporter`：
  - 用户已有 HTTP 服务：`exporter.attach(app, path="/metrics")`
  - 无 HTTP 服务：`exporter.render()` 返回文本（供 Pushgateway 或自行暴露）
- 不做独立 Web UI，不做默认监听端口（除用户显式挂载）。

### P1 可靠性补强

#### 1. 崩溃恢复边界明确化
- `create_pending_task()` 未入队记录重启后不自动执行，需在文档与 `get_metrics()` 中体现"未入队 pending"计数。
- Checkpoint 恢复路径与崩溃恢复路径交互测试：重启后已入队任务重跑时能否读到上次 checkpoint。

#### 2. 优雅停机配置
- 当前 `shutdown(graceful=True)` 不触发未到期延迟任务与周期调度。
- 新增配置项：停机时是否 drain 到期延迟任务。
- 文档明确默认行为。

#### 3. 重试语义统一
- 明确自动重试、手动重试、Checkpoint 续跑三者交互：
  - 手动重试时是否保留 checkpoint
  - `resume_from_checkpoint` 与 `max_retries` 的关系

### 验收标准
- Prometheus 导出器可挂载到 FastAPI 示例。
- 崩溃恢复与 checkpoint 交互测试通过。
- 重试语义在文档中有明确矩阵。

---

## 五、v2.0.0 — 编排集成（后移）

**状态**：后移。本地能力稳定、有真实多服务需求时启动。

**内容概要**（详见通信数据契约文档）：
- `OrchestratorClient` 全链路：TaskConsumer、EventReporter、Heartbeat、LocalRetryStore、控制消息处理。
- Redis Stream 消费者组、`neotask:events` 上报。
- 实例注册与心跳、失联任务回收（`XAUTOCLAIM`）。
- 跨语言协议文档。

**移出 NeoTask、归编排层的内容**：
- DAG 工作流、条件分支、并行执行
- 动态路由
- 业务编排插件系统
- 独立 Web UI、多租户隔离
- Webhook / WebSocket 面向终端的广播

---

## 六、路线图文档同步要求

v1.1.0 开发开始时同步修订 README：
- 删除 Development Roadmap 中 v1.5 的 "Task Orchestration"（DAG / Conditional Branch / Parallel Execution）。
- 删除 v2.0 的 "Independent Web UI" 与 "Multi-Tenancy Isolation"。
- 标注："编排能力由上层编排服务负责，NeoTask 保持执行层定位，仅提供轻量级监控接口。"
- 新增 v1.1 / v1.2 / v1.3 条目，与本文档一致。

---

## 七、下一步行动

**本周**：
- 修订 README 路线图，与本文档对齐。
- 建立 `benchmarks/` 目录骨架，先写"批量提交吞吐"与"单任务提交延迟"两个基准，作为优化基线。

**下月**：
- 完成 v1.1.0 全部功能项，每项配示例与测试（覆盖率 ≥80%）。
- 启动 v1.2.0 性能优化，以基准测试驱动，优化前后数据记入 CHANGELOG。

**中期**：
- v1.3.0 监控导出器与可靠性补强。
- 本地能力稳定后，再评估编排阶段启动时机。