# NeoTask 版本演进规划

**项目定位**：轻量级、可嵌入的 Python 异步执行引擎（SDK）。NeoTask 只承担执行面，控制面归 NeoFlow。

| 平面 | 归属 | 职责 |
| :--- | :--- | :--- |
| 控制面 | NeoFlow | DAG 拆解、任务下发、事件监听、状态推进、跨服务重试决策 |
| 执行面 | NeoTask | 拉取任务、执行 Handler、本地重试与断点续跑、上报事件、实例注册与心跳 |

1.x 是**自成体系的基础执行 SDK**：Memory / SQLite / Redis 三种后端均可独立使用，不依赖任何控制面。2.0 才增加对 NeoFlow 执行面的接入能力，并以可选方式提供——不使用 DAG 编排的用户继续停留在 1.x 即可。

线协议（任务 JSON、事件信封与负载、状态机、幂等与失败语义）在 1.x 冻结，详见
[neotask-execution-contract.md](./neotask-execution-contract.md)。

**修订原则**：

1. NeoTask 不实现 DAG 拆解、状态推进与任务下发策略；这些逻辑固定在 NeoFlow 控制面。
2. 1.x 的每个功能都必须能独立服务于基础 SDK 使用者，不以“为 2.0 打基础”为立项理由。
3. 控制面接入（任务拉取、事件上报）整体放在 2.0，做成可选能力，不改变 1.x 公开结构。
4. 线协议只做增量扩展；破坏性变更提升 `schema_version` 并保留旧版本可读一个 minor。
5. 轻量监控属于本地能力的一部分，不做独立 Web UI。

---

## 一、版本总览

| 版本   | 主题                             | 状态                       |
| ------ | -------------------------------- | -------------------------- |
| v1.0.x | 基础任务池、定时调度、分布式基础、可观测性基础 | 已发布          |
| v1.1.0 | 本地任务能力补全                 | 已发布                     |
| v1.1.1 | 稳定性与升级体验修复             | 已发布                     |
| v1.1.2 | 调度精度与执行器配置修复         | 已发布                     |
| v1.2.0 | 性能优化与基准测试、轻量监控     | 进行中                     |
| v1.3.0 | 监控导出器与可靠性补强           | 规划中                     |
| v2.0.0 | NeoFlow 执行面接入               | 后移                       |

---

## 二、v1.1.0 — 本地任务能力补全

**目标**：在不引入任何控制面前提下，把本地执行能力补全到可直接支撑生产任务的程度。

> **实现依据**：本节范围已由 [neotask-v1.1.0-design.md](./neotask-v1.1.0-design.md) 定稿细化，
> 其中第 5 项 Blackboard 收缩为仅 Memory 后端，第 7 项外部 task_id 在 1.0.x 已基本落地、本版本
> 仅补 `task_exists()` 的重复提交校验。与本文档冲突处以设计文档为准。

### P0 功能项

#### 1. Checkpoint 断点续跑（已交付）
- `save_checkpoint(state)`：执行器写入中间状态，持久化到任务记录（Memory / SQLite / Redis）。
- `load_checkpoint()`：执行器读取上次状态。
- `pool.submit(..., resume_from_checkpoint=True)`：重试时从 checkpoint 恢复。
- `task.checkpointed` 事件纳入事件总线。
- `get_task(task_id)` 返回中体现 checkpoint 存在性。
- 三后端均已实现：SQLite 多语句事务、Redis Lua 原子更新，历史版本按 `checkpoint_keep_versions` 淘汰。
- 用法见 `examples/20_checkpoint.py`。

#### 2. 成本与耗时采集（已交付）
- 任务元数据标准化 `cost_tracking` 字段：
  `{tokens_in, tokens_out, api_calls, estimated_cost, currency}`
- `report_cost(...)`：执行器上报成本（与 `report_progress` 同风格）；执行器外使用
  `pool.report_cost(task_id, ...)`，终态任务仍可追加用量。
- 耗时拆分：`queue_wait_ms`（排队等待）+ `duration_ms`（执行耗时）。
- `pool.get_cost_summary(task_ids)`：聚合接口；币种混用标记为 `MIXED`。
- `task.completed` / `task.failed` 事件信封携带 `cost` 字段（`task.completed` 仅在
  上报过成本时包装为 `{"result": ..., "cost": ...}`，否则保持原始结果契约）。
- 用法见 `examples/18_cost_tracking.py`。

#### 3. 父子任务关联字段（已交付）
- 任务元数据增加：`parent_task_id`（未引入 `child_count` / `completed_children`）。
- 查询接口：`pool.get_children(task_id)`、`pool.get_parent(task_id)`；SQLite 建
  `idx_tasks_parent` 索引，Redis 维护父集合索引。
- **不做自动汇总触发**：父任务完成判断由调用方或控制面决定，NeoTask 不越界。
- 用法见 `examples/19_parent_child.py`。

#### 4. 自定义失败状态（已交付）
- `report_failure(reason=...)`：执行器标记业务级失败（如 `qa_failed`）。
- `task.failed` 事件携带 `reason` 字段，区别于系统异常。
- 收尾语义与持久化细节见设计文档决策 2；用法见 `examples/17_business_failure.py`。

#### 5. 本地上下文读写原语（Blackboard，已交付）
- `context_set(key, value, ttl=...)` / `context_get(key)` / `context_delete(key)`。
- 仅实现 Memory 后端（进程内 dict + 惰性 TTL 清理）；SQLite / Redis 调用时抛
  `UnsupportedFeatureError`，不静默降级。
- 仅在单进程内生效，**不承担跨服务数据传递职责**；用法见 `examples/21_blackboard.py`。

#### 6. 事件类型补全（已交付）
- 已有：`task.created` / `task.started` / `task.completed` / `task.failed` /
  `task.cancelled` / `task.retry` / `task.progress` / `task.status_changed` /
  `task.deleted` / `task.reclaimed`
- 新增：`task.checkpointed`、`task.cost_reported`
- 信封字段（`timestamp` / `node_id` / `schema_version`）已稳定，协议见契约文档第 8 节。
- 统一由 `neotask.EventType` 常量提供，禁止裸字符串。

#### 7. 外部指定 task_id（已交付）
- `pool.submit(data, task_id=..., priority=...)`：支持外部传入 ID。
- `pool.task_exists(task_id)`：幂等检查。
- 定稿：公开名沿用既有 `pool.task_exists()`（未新增 `has_task()` 别名），
  重复提交同一 ID 抛 `TaskAlreadyExistsError`；`TaskScheduler` 补齐 `task_exists_async()`。
  用法见 `examples/16_custom_task_id.py`。

### 验收标准

- Memory / SQLite / Redis 后端下所有功能可用（Blackboard 仅 Memory），每项配示例与测试，
  新增模块行覆盖率 ≥80%。
- 线协议与契约文档一致，不出现文档未覆盖的公开字段。

### 发布收尾清单

- [x] 协议契约页：[neotask-execution-contract.md](./neotask-execution-contract.md)
- [x] `src/neotask/core/checkpoint.py` 覆盖率 75.6% → 100%（补齐 sync 分支与上下文读取）
- [x] `TaskEngine` 的 `checkpoint_repo` 注入路径补测试
- [x] README / README_zh 状态同步（v1.1 已交付、架构图去掉“规划”标注、链接契约页）
- [x] release-please 依据 conventional commits 生成 1.1.0 与 CHANGELOG（不手改）
- [x] 发布说明提示 1.0.x 升级：checkpoint 表启动时自动创建，历史任务耗时回填运行
      `scripts/migrate_task_metadata.py`（v1.1.2 发布说明已覆盖）
- [ ] 官网 API 文档同步（外部站点，独立跟踪）

---

## 二点一、v1.1.1 — 稳定性与升级体验修复

**目标**：不新增公开能力，只修复 1.1.0 暴露的启动期稳定性与升级可用性问题，
语义保持向后兼容。

### 修复项

- **SQLite 老库并发升级**：同一 1.0.x 库被多仓储/多进程同时打开时，重复
  `ALTER TABLE ADD COLUMN` 会抛 `duplicate column name` 导致启动失败；改为容忍
  重复列并继续，其它错误照常抛出。
- **启动期完成 schema 升级**：`TaskRepository` / `CheckpointRepository` 增加
  `ensure_ready()`，SQLite 后端建表、补列与 `task_checkpoints` 建表提前到
  `TaskPool.start()` / `TaskEngine.initialize()`，使“新列在启动时自动补齐”成为真实
  保证，升级失败在启动阶段暴露，首个任务不再承担 DDL 延迟。
- **start() 就绪语义**：`TaskPool.start()` 等组件初始化完成后再返回，启动失败
  直接抛出原始异常，不再留下“半启动”状态。
- **依赖可用性**：`neotask[redis]` 的 redis-py 上界由 `<6` 放宽到 `<8`
  （4.5–7.x 验证可用；8.x 的 RESP3 `HELLO` 与旧版 Redis/Memurai 3.2 不兼容）。
- **指标读取并发安全**：`get_summary()` / `get_full_summary()` /
  `to_prometheus_format()` 与 worker 记录共用同一把锁，避免遍历样本 deque 时
  并发 append 抛 `deque mutated during iteration`（压测下可复现）。

### 验收标准

- 1.0.8 真实库在 1.1.1 下启动即完成补列与 checkpoint 建表，历史任务可正常读取。
- 并发初始化回归用例通过，重复补列不再抛错。
- 全量测试通过；redis-py 5.x / 7.1 下 Redis 与 checkpoint 用例均通过。

---

## 二点二、v1.1.2 — 调度精度与执行器配置修复

**目标**：不新增公开能力，修复真实环境验证中暴露的调度精度、取消语义与执行器
配置失效问题，语义保持向后兼容。

### 修复项

- **时间轮延时抖动**：槽位相位改用单调时钟 `perf_counter` 并取最近滴答；执行前
  容忍半个 tick 的提前量，消除 Windows 上 `time.time()` 约 15.6ms 粒度导致的
  整槽推迟。实测 0.2–1.0s 延时任务触发开销由 +67～+159ms 降至数十毫秒。
- **时间轮取消/删除语义**：`TaskScheduler.cancel()` / `delete()` 同步把任务移出
  轮盘；停机 drain 跳过非 PENDING 终态任务，避免已取消任务被重新入队执行。
- **执行器配置生效**：显式 `executor_type="thread" / "process" / "class"` 时，
  同步函数不再被包装成协程；字符串配置在 `TaskPoolConfig` 构造时归一化为
  `ExecutorType` 枚举。默认 `ASYNC` 行为保持不变。
- **顶层导出补齐**：`TaskEngine` / `EngineConfig` 可从 `neotask` 包根导入，
  与文档示例一致。

### 验收标准

- 时间轮单测覆盖最近槽位取整、半 tick 容忍、跨回调的槽位推进；延时任务在
  真实机器上的触发开销不超过「半 tick + 调度开销」量级。
- 取消/删除轮内任务后，轮盘计数归零且停机后状态保持 `cancelled`，任务体不执行。
- thread 执行器任务运行在 `neotask-worker` 线程；process 执行器任务运行在子进程。
- 全量测试通过；23 个示例与 Redis/SQLite 真实环境验证通过。

---

## 三、v1.2.0 — 性能优化与轻量监控

**目标**：以基准测试驱动性能优化，交付 `get_metrics()` 快照与定时推送，形成性能可观测闭环。

### 已落地基线（1.0.x / 1.1 已有，不再排期）

- 批量提交：`dispatch_many` 对任务写入与入队各做一次批量操作（SQLite `executemany_transaction`、
  Redis pipeline / `push_batch`、Memory 批量入队）。
- SQLite 索引：`idx_tasks_status`、`idx_tasks_priority`、`idx_tasks_created_at`、`idx_tasks_parent`。
- Memory 任务仓储已是 `Dict[task_id, Task]` 索引查找。
- 优先队列已是 `heapq`。
- 进度上报节流：`progress_flush_interval` / `progress_flush_delta`（事件仍实时推送）。
- SQLite：WAL + `synchronous=NORMAL` + `busy_timeout`，连接经专用 loop 复用并支持跨仓储共享。

### 基准基线（2026-11-19）

`benchmarks/run_baseline.py` 全量运行（Windows / Python 3.11 / 8 workers /
每轮 1000 任务、3 轮交替取中位）：

- 单任务提交调用：p50 0.37ms / p95 0.60ms / p99 0.83ms（200 次采样）
- 批量提交：逐条 989 tasks/s，`submit_many` 3559 tasks/s，配对加速比中位 3.60x（3.27x ~ 4.02x）
- 高并发执行（1000 任务，1/4/8/16/32/64 并发，3 轮正反交替取中位）：
  吞吐 947 → 1464 tasks/s（峰值 32 并发，1.55x）；p50 延迟 727ms → 493ms。
  并发 >1 时 EventBus 持续出现「pending events reached 1000」告警；关闭预取做对照后
  仍无扩展，瓶颈不在 Worker 数量或预取，优先排查任务完成与事件派发的串行路径。
  峰值并发档位在多次运行间会漂移（前次为 64 并发 1874 tasks/s），只看量级不看单点。
- Checkpoint 写入开销（1000 任务、8 并发、256B 状态、每任务 0/1/5 次写入）：
  - memory：0 写 1764 tasks/s；1 写 1472（0.84x），单次 `save_checkpoint` p50 75us，
    每写增量约 0.11ms；5 写 945 tasks/s（0.54x），p50 73us，每写增量约 0.10ms。
  - sqlite：0 写 373 tasks/s；1 写 198（0.53x），单次写入 p50 16.0ms / p95 24.9ms，
    每写增量约 2.38ms；5 写 62 tasks/s（0.17x），p50 19.0ms / p95 32.2ms，增量约 2.70ms。
    任务 p50 延迟 1.37s → 7.93s。
  - 结论：memory 的写入成本近似线性叠加；sqlite 单次写入是 memory 的 200+ 倍，
    且任务存储本身也慢（零写 373 vs 1764 tasks/s）。高频 Checkpoint 在 sqlite 下
    需要先解决单次提交成本与连接串行化，再谈写入频率。

> 单轮吞吐波动可达 2–3 倍，横向对比请保持机器与参数一致，并比较配对中位数。

### P0 性能优化（剩余工作）

#### 1. 基准测试套件
- 新增 `benchmarks/` 目录，覆盖：
  - [x] 单任务提交延迟
  - [x] 批量提交吞吐（1000 条，验证相对逐条提交量级提升）
  - [x] 高并发执行（1000 任务）
  - 进度上报频率对吞吐的影响
  - [x] Checkpoint 写入开销（memory / sqlite × 每任务 0/1/5 次写入）
- 每次版本发布跑基准，数据记入 CHANGELOG，防止性能回退。

#### 2. 高并发调度开销
- 同优先级任务批量出队。
- 压测验收：`worker_concurrency` 50+ 时降低每任务调度切换次数。

### P0 轻量监控

#### 1. `pool.get_metrics()` 快照接口
返回字段：
- 各状态任务数：`pending_count` / `running_count` / `completed_count` / `failed_count`
- `worker_concurrency` / `active_workers`
- 按优先级队列深度 `queue_depth_by_priority`
- `retry_queue_size`
- `oldest_pending_age_seconds`
- `storage_backend` / `storage_healthy`
- `checkpoint_count`（需新增仓储层计数接口）
- `context_key_count`（仅 Memory 后端；其他后端返回 `null`）
- 同步、无副作用。

#### 2. `pool.on_metrics(handler, interval=...)`
- 定时推送快照，避免调用方轮询。

#### 3. 监控指标纳入性能观测
- 基准测试的吞吐、延迟、队列深度通过 `get_metrics()` 暴露，形成“性能可观测”闭环。

### 验收标准
- 基准测试显示批量提交吞吐提升、查询延迟下降，无性能回退（优化前后数据记入 CHANGELOG）。
- `get_metrics()` 字段完整，示例可运行。

---

## 四、v1.3.0 — 监控导出器与可靠性补强

**目标**：提供 Prometheus 导出器（可选 extra），补强崩溃恢复边界与停机语义。

### P1 监控导出器

#### 1. Prometheus 导出器（可选 extra）
- `pip install neotask[metrics]`。
- `MetricsExporter`：
  - 用户已有 HTTP 服务：`exporter.attach(app, path="/metrics")`
  - 无 HTTP 服务：`exporter.render()` 返回文本（供 Pushgateway 或自行暴露）
- 不做独立 Web UI，不做默认监听端口（除用户显式挂载）。

### P1 可靠性补强

#### 1. 崩溃恢复边界明确化（Checkpoint 交互已补测试）
- `create_pending_task()` 未入队记录重启后不自动执行，需在文档与 `get_metrics()` 中体现
  “未入队 pending”计数。
- Checkpoint 恢复与重启交互已有用例：
  `tests/unit/test_checkpoint.py::TestCheckpointResume::test_resume_across_restart_sqlite`。

#### 2. 优雅停机（配置已存在，补文档与边界用例）
- `drain_delayed_on_shutdown`（默认 `False`）已提供：停机时是否立即执行未到期延时任务。
- 剩余工作：补齐周期任务/Cron 在停机期间的行为矩阵与回归用例。

#### 3. 重试语义（已定稿）
- 自动重试、手动重试、Checkpoint 续跑三者交互见契约文档第 9 节：
  两种重试都保留并注入最近 checkpoint；`resume_from_checkpoint=True` 仅作用于新提交，
  与 `max_retries` 相互独立。

### 验收标准
- Prometheus 导出器可挂载到 FastAPI 示例。
- 停机行为矩阵与崩溃恢复边界在文档中有明确说明。

---

## 五、v2.0.0 — NeoFlow 执行面接入（后移）

**状态**：后移。1.x 本地能力稳定、NeoFlow 控制面就绪后启动。

**目标**：让 NeoTask 作为执行面被 NeoFlow 驱动，同时不改变 1.x 公开结构与使用方式。

| 能力 | 说明 |
| :--- | :--- |
| 任务拉取 | 新增 TaskConsumer：从控制面通道拉取任务，消息体沿用契约文档第 6 节任务 JSON；候选 Redis Stream 消费者组，确认/重投语义在 2.0 定义 |
| 事件上报 | 新增 EventReporter：按契约第 8 节序列化 `task.*` 事件上报控制面，建议至少一次 + 控制面幂等 |
| 实例注册与心跳 | 复用既有 `NodeManager` / `HeartbeatManager` |
| 控制消息 | 取消、暂停等跨服务指令通道；本地 `cancel` / `pause` 已在 1.x |
| 本地重试存储 | 复用 1.x 重试与 checkpoint，执行面内失败不依赖控制面 |
| 协议文档 | 以 [neotask-execution-contract.md](./neotask-execution-contract.md) 为唯一依据，跨语言实现同步维护 |

**2.0 不做**（归 NeoFlow 控制面）：

- DAG 拆解、条件分支、并行执行
- 任务下发策略、动态路由
- 状态推进、跨服务事件监听
- 业务编排插件系统、独立 Web UI、多租户隔离

2.0 能力以可选依赖提供；不需要 DAG 编排的用户继续使用 1.x 即可。

---

## 六、文档同步要求

- 线协议变更先改 [neotask-execution-contract.md](./neotask-execution-contract.md)，再改实现。
- 版本状态变化时同步 README / README_zh 的路线图区块（当前 v1.1 已交付、v2.0 为 NeoFlow 执行面接入）。
- 每个发布版本同步 `examples/readme.txt` 索引与 CHANGELOG（由 release-please 生成）。

---

## 七、下一步行动

**v1.1 发布收尾（已完成）**：

- `TaskEngine` 注入测试已补齐；1.1.0 / 1.1.1 / 1.1.2 已由 release-please 发布。
- 基准与性能用例的低精度计时已改为 `perf_counter`，Windows 毫秒级任务不再被量成 0。

**v1.2 进行中**：

- 已建立 `benchmarks/` 目录，先落“单任务提交延迟”与“批量提交吞吐”两个基线。
- 后续以基准数据驱动高并发调度优化，再交付 `get_metrics()` 与 `on_metrics()`。

**中期**：

- v1.3 监控导出器与停机/恢复边界补强。
- NeoFlow 控制面就绪后再评估 2.0 执行面接入的启动时机。
