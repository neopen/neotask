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
5. 轻量监控与单节点操作台属于本地能力：面板随宿主进程启停、默认只监听 `127.0.0.1`，
   不做独立部署的 Web 服务或多节点控制台。
6. 人工介入属于本地能力：等待状态落库、不依赖控制面，任何节点都可 signal 唤醒。
7. 对外集成以进程内 Python API 为唯一公开契约；不内置 function-calling / tool schema，
   面向 LLM 或编排平台的工具描述由适配层提供（2.0 控制面接入时定义）。

---

## 一、版本总览

| 版本   | 主题                             | 状态                       |
| ------ | -------------------------------- | -------------------------- |
| v1.0.x | 基础任务池、定时调度、分布式基础、可观测性基础 | 已发布          |
| v1.1.0 | 本地任务能力补全                 | 已发布                     |
| v1.1.1 | 稳定性与升级体验修复             | 已发布                     |
| v1.1.2 | 调度精度与执行器配置修复         | 已发布                     |
| v1.2.0 | 性能优化与基准测试、轻量监控     | 已发布（2026-11-20）       |
| v1.2.1 | v1.2 剩余性能候选收尾            | 已发布（2026-11-22）       |
| v1.3.0 | 人工介入（等待状态与信号唤醒）   | 已发布（2026-11-26）       |
| v1.3.1 | v1.3 收尾修复（指标/校验/口径/执行器配置） | 已发布（2026-11-27） |
| v1.4.0 | 单节点操作台、任务列表语义与监控导出 | 规划中                     |
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

### 基准基线（2026-11-20 复测）

`benchmarks/run_baseline.py` 全量运行（Windows / Python 3.11 / 8 workers /
每轮 1000 任务、3 轮交替取中位）：

- 单任务提交调用：p50 0.14ms / p95 0.38ms / p99 0.46ms（200 次采样）
- 批量提交：逐条 2189 tasks/s，`submit_many` 7310 tasks/s，配对加速比中位 3.09x（2.49x ~ 3.34x）
- 高并发执行（1000 任务，1/4/8/16/32/64 并发，3 轮正反交替取中位）：
  吞吐 1912 → 3271 tasks/s（峰值 16 并发，1.71x）；p50 延迟 379ms → 215ms。
  本次整机比上一轮快（memory 零写基线 1311 → 2583 tasks/s），并发扩展比在历次
  运行间波动 1.1x ~ 1.7x、峰值档位也不固定，只能看量级；EventBus 在并发 >1 时
  仍会积压生命周期事件，任务完成与事件派发的串行路径仍是待查项。
- 高并发调度开销优化后（8/64 并发，1000 任务，5 次交错配对取中位）：
  c=8 吞吐 2587 → 3730 tasks/s、p50 277ms → 202ms；c=64 吞吐 2639 → 3506 tasks/s、
  p50 272ms → 209ms。配对加速比中位 c=8 1.64x（1.52/1.64/1.69）、c=64 1.45x
  （1.32/1.45/1.71）。归因：`Task.clone` 单独 1.45x / 1.71x；预取快路径在吞吐
  噪声内，但确定性省掉每任务一次 `wait_for` 调度（单次 12.7us → 0.16us）。
- 进度上报频率（1000 任务、8 并发、每任务 0/1/10/100 次；订阅方为只计数的空回调）：
  - 优化后 无订阅：3073 / 2419（0.79x）/ 1941（0.63x）/ 1035 tasks/s（0.34x），
    事件积压恒约 1.7k 条（仅生命周期事件），排空约 60ms，progress 投递 0 条。
  - 优化后 有订阅：3101 / 2538（0.82x）/ 1961（0.63x）/ 858 tasks/s（0.28x），
    积压 2.7k / 11.7k / 101.7k 条，排空 0.09s / 0.30s / 2.26s，投递 1k / 10k / 100k 条。
  - 优化前（无订阅，内部处理器全局订阅）：0.92x / 0.74x / 0.29x，
    积压 2.7k / 11.7k / 101.7k 条，排空 0.2s / 0.75s / 5.2s。
  - 结论：无订阅时 progress 事件不再入队，积压与排空开销消失；有订阅时事件仍逐条
    派发，但内部处理器不再空跑，排空从 5.2s 降到 2.3s。单次上报约 6us 的调用成本
    与订阅无关，100 次/任务仍只有 0.28x ~ 0.34x。后续候选：进度事件合并/节流、
    批量落库与队列有界化。
- Checkpoint 写入开销（1000 任务、8 并发、256B 状态、每任务 0/1/5 次写入）：
  - memory：0 写 2235 tasks/s；1 写 2010（0.90x），单次 `save_checkpoint` p50 49us，
    每写增量约 0.05ms；5 写 1290 tasks/s（0.58x），p50 50us，每写增量约 0.07ms。
  - sqlite（写入成本优化后）：0 写 623 tasks/s；1 写 399（0.64x），单次写入
    p50 6.64ms / p95 16.31ms，每写增量约 0.90ms；5 写 148 tasks/s（0.24x），
    p50 7.60ms / p95 14.61ms，增量约 1.03ms。
  - 优化前同参数：0 写 503 tasks/s；1 写 242（0.48x），p50 13.1ms；5 写 84（0.17x），
    p50 13.8ms。
  - 结论：sqlite 单次提交成本已降到约一半，高频 Checkpoint 的主要剩余成本是
    aiosqlite 逐语句往返（每次写约 0.2–0.4ms）与任务存储本身的串行写锁。
- 监控快照成本（1000 任务排队 + 队列暂停、100 次 `get_metrics_async`，memory 后端）：
  p50 295us / p95 542us / p99 801us；每次快照的 `queue_depth_by_priority` 总和与
  队列大小一致。memory 后端的按优先级计数与最早 PENDING 时间均为 O(队列长度) 扫描，
  按 1s 采样间隔可接受；SQLite 走 `GROUP BY` / `MIN(created_at)`，Redis 走 pipeline
  `ZCOUNT` / SMEMBERS + 分批 MGET（每 500 条一次往返）。

> 单轮吞吐波动可达 2–3 倍，横向对比请保持机器与参数一致，并比较配对中位数。

### P0 性能优化（已交付）

#### 1. 基准测试套件
- 新增 `benchmarks/` 目录，覆盖：
  - [x] 单任务提交延迟
  - [x] 批量提交吞吐（1000 条，验证相对逐条提交量级提升）
  - [x] 高并发执行（1000 任务）
  - [x] 进度上报频率对吞吐的影响（每任务 0/1/10/100 次，含事件积压与排空）
  - [x] Checkpoint 写入开销（memory / sqlite × 每任务 0/1/5 次写入）
  - [x] 监控快照成本（1000 排队任务 × 100 次 `get_metrics`，校验队列深度总和）
  - [x] 事件派发吞吐（异步/同步单订阅方，10 万事件 × 3 轮）
- 每次版本发布跑基准，数据记入 CHANGELOG，防止性能回退。

#### 2. 高并发调度开销（已交付）
- 批量出队现状：默认预取路径（`enable_prefetch` + `enable_batch_pop`）本来就按批出队，
  2000 任务只触发 105 次 `pop`、平均批 19 条；直接模式（关闭预取）仍逐条取，避免
  整批落到单个 worker。真正拖慢 50+ 并发的是每任务的拷贝与取任务调度，而不是出队次数。
- `Task.clone()`：只深拷贝 `data` / `result` / `cost_tracking`，时间戳、枚举、字符串
  直接共享。`copy.deepcopy` 对 `datetime` 走 reduce 协议（单次约 9us），而内存后端
  每个任务要整对象拷贝 3 次（提交、状态迁移、完成），单次 23us → 4us。
- `MemoryTaskRepository` 全部读写改用 `clone()`；`TaskPrefetcher.get` 在本地队列非空时
  走 `get_nowait`，只有为空才进 `wait_for`，省掉每任务一次 Task 创建 + 定时器。
- 定位依据：cProfile 下 `copy.deepcopy` 占 64 并发总耗时的约 40%（根调用 3 次/任务，
  递归调用 22 万次）；把 `deepcopy` 变恒等时 c=64 吞吐 2509 → 4471 tasks/s。
- 验证：同机 5 次交错配对，c=8 1.64x、c=64 1.45x；全量测试通过。
- 剩余候选：任务完成路径串行化（生命周期状态迁移 + 事件派发仍共享消费循环）。

#### 3. 事件派发优化（已交付）
- EventBus 在事件类型无订阅方且无全局订阅时直接丢弃，不再入队空跑消费循环。
- 内部计数器与指标处理器改为按生命周期事件类型订阅（不再全局订阅），
  `task.progress` 等高频事件在无人订阅时完全不进入事件总线。
- 验证：全量测试 866 passed / 12 skipped；基准复测见上，无订阅进度上报的事件
  积压与排空开销消失，有订阅时排空 5.2s → 2.3s。

#### 4. SQLite 写入成本优化（已交付）
- 自动提交模式（`isolation_level=None`）下 `commit()` 直接返回：每条语句自成事务，
  commit 没有可提交内容，原先每次单语句写入还要多付一次跨 loop 派发（约 0.2ms）。
  显式隔离级别与手动 `BEGIN`（`in_transaction` 为真）仍照常提交。
- `save_checkpoint` 仅在累计版本超过 `checkpoint_keep_versions` 时执行 prune，
  窗口内跳过每写一次的 DELETE 事务（SQLite 下约 0.65ms）。
- 定位依据：同线程 `sqlite3` 单事务仅约 0.04ms，成本几乎全在 aiosqlite 逐语句往返
  与跨 loop 派发；单任务 sqlite 路径有 12 次跨 loop 操作，其中约 5.4 次是空转 commit。
- 验证（同机交错 A/B，400 任务 × 3 轮中位，两次配对取比值）：0 写吞吐 1.36x / 1.41x；
  1 次 checkpoint 1.51x / 1.56x，单次写入 p50 13.2–13.5ms → 5.5–5.6ms；
  5 次 checkpoint 1.65x / 1.75x。全量套件 866 passed / 12 skipped。
- 收尾：任务创建 + 入队合并为单事务、提交路径的独立容量查询见第 5 节；
  剩余候选：直接模式批量出队（需兼顾 worker 间公平）。

#### 5. 事件派发与提交路径收尾（v1.2.1，已交付）
- EventBus 消费循环按批取事件（上限 256 条）后仍逐条处理，事件顺序契约不变；
  同步回调在消费循环内直接调用，异步回调单订阅方直接 `await`、多订阅方才 `gather`，
  省掉每个事件为回调创建 Task 的开销。
- `TaskPool` 移除无人读取的 `FutureManager` 及完成/失败/取消事件处理器；内部计数器与
  指标合并为一个订阅处理器（未启用指标时是同步函数），生命周期事件的内部分发从
  「每事件一个协程」降为直接调用。
- SQLite 新增 `save_if_absent_and_enqueue`：单个 `BEGIN IMMEDIATE` 事务内完成
  「写任务 → 容量校验 → 入队」，提交路径从 3 次跨 loop 往返降为 1 次，容量校验
  不再是独立的 `COUNT(*)` 往返。
- 验证（同机交错 A/B，Windows / Python 3.11）：
  - EventBus 微基准（12 万事件、6 轮交错）：单异步订阅 16.5k → 224k events/s
    （配对中位 12.8x），同步订阅 15.5k → 245k（16.3x），6/6 轮提升。
  - 进度场景（1000 任务 × 100 次上报、有订阅）：事件排空 3.8–6.7s → 0s
    （任务结束时 10 万条 progress 事件已全部消费），4/4 轮。
  - 内存并发（1000 任务，8 轮交替、每轮 3 次取中位）：c=8 吞吐 2103 → 2371 tasks/s
    （1.19x，7/8 轮），p50 339 → 299ms；c=64 在噪声内（配对中位 1.00，4/8 轮），
    与单轮波动 2–3 倍的方法学限制一致。
  - SQLite 顺序提交 300 任务：吞吐 64.6 → 92.8 tasks/s（配对中位 1.25x，补测 4 轮
    1.50x），p50 12.7 → 8.5ms，p95 36.5 → 20.1ms，7–8/8 轮一致改善。
  - 全量测试：unit 539 passed / 12 skipped，integration 87 passed / 1 skipped。

### P0 轻量监控（已交付）

#### 1. `pool.get_metrics()` / `get_metrics_async()` 快照接口
返回字段（同步调用无副作用；单项查询失败退化为 `None`/空值，不影响任务执行）：
- 各状态任务数：`pending` / `running` / `completed` / `failed`（事件驱动计数器）
- `worker_concurrency` / `active_workers`
- 按优先级队列深度 `queue_depth_by_priority`（四个档位恒存在，缺失补 0）
- `retry_queue_size`：自动重试已重新入队、仍在延迟窗口内等待执行的任务数
- `oldest_pending_age_seconds`：最早 PENDING 任务的等待秒数
- `storage_backend` / `storage_healthy`（健康检查未启用时为 `None`）
- `checkpoint_count`：全部 checkpoint 版本数；后端不支持时为 `None`
- `context_key_count`：仅 Memory 后端；其他后端为 `None`
- `timestamp` / `node_id`

#### 2. `pool.on_metrics(handler, interval=1.0)`
- 返回 `MetricsSubscription`；`cancel()` 线程安全、幂等。
- 首次推送立即执行，handler 支持普通函数与协程函数。
- 单次推送异常只记录告警，不中断订阅；停机时统一取消。
- `on_metrics_async()` 供已在事件循环内的调用方使用。

#### 3. 底层计数接口补齐
- `TaskRepository.oldest_pending_created_at()`：默认分页扫描；memory 遍历取最小，
  SQLite `MIN(created_at)`，Redis SMEMBERS + 分批 MGET（每 500 条往返一次）。
- `QueueRepository.count_by_priority()`：默认空字典；memory 堆扫描，SQLite `GROUP BY`
  （走 `idx_queue_order`），Redis pipeline `ZCOUNT`。
- `CheckpointRepository.count()`：默认 `None`；memory 聚合版本数、SQLite `COUNT(*)`、
  Redis SCAN `ckpt:*` + pipeline `HLEN`。
- `MemoryContextStore.count()`；`WorkerPool` 维护延迟重试任务集合，开始执行/取消/删除时清除。

#### 4. 监控指标纳入性能观测
- 基准测试新增快照成本场景，队列深度与真实队列大小交叉校验；
  用法见 `examples/22_metrics.py`。

### 验收标准
- 基准测试显示批量提交吞吐提升、查询延迟下降，无性能回退（优化前后数据记入 CHANGELOG）。
- `get_metrics()` 字段完整、示例可运行；`on_metrics` 推送可取消且异常隔离。
- memory / SQLite / Redis 三种后端的新增计数接口均有测试覆盖。

---

## 四、v1.3.0 — 人工介入（Human-in-the-loop）

**目标**：任务可以持久停在“等待外部信号”的状态（人工审批、人工校验、外部事件），信号到达后自动恢复执行；不依赖控制面，Memory / SQLite / Redis 语义一致。

**为什么单独一个 minor**：v1.3 改的是公开状态机与事件契约，v1.4 改的是导出与恢复边界；两套语义分开验证，避免状态机变更与恢复/停机语义同时落地。

**原则**：

- 等待是执行面能力：进入等待立即释放 worker 槽位，不占并发、不参与执行超时。
- 状态必须落库：进程重启、节点崩溃、换节点后仍可查询、可唤醒。
- 恢复复用 checkpoint 语义，不引入第二套持久化。
- 线协议只做增量扩展：新增一个状态值与两个事件，事件信封 `schema_version` 保持 `1`。

### 1. 状态与事件契约

- 新增 `TaskStatus.WAITING = "waiting"`，非终态。
- 状态定义规范：`TaskStatus` 统一定义为标准 `str, Enum`，成员名大写、值保持小写
  （`PENDING = "pending"`、`WAITING = "waiting"`）；显式 `__str__ = str.__str__`，
  保证 Python 3.8–3.12 下 `str(status)`、`status.value` 与 JSON 序列化一致输出小写值。
- 事件定义规范：`EventType` 保持大写常量类 + 小写线上名（`WAITING = "task.waiting"`），
  不改成 Enum——避免 3.11 `str`-Enum 格式化差异，订阅方式与裸字符串逐字节等价。
- 兼容约束：SQLite 落库值、Redis `status:*` 键名、事件 payload 的
  `old_status` / `new_status` 均保持小写；新增 `TaskStatus.from_value()` 供反序列化。
- 允许的转换：

  ```
  running → waiting      （Handler 主动挂起）
  waiting → pending      （signal 唤醒，重新入队）
  waiting → cancelled    （人工取消）
  waiting → failed       （等待超时）
  ```

- 新增事件（注册进 `EventType`，同步契约文档 8.2）：
  - `task.waiting`：`{old_status, signal, timeout_at?}`，每次进入等待发布一次。
  - `task.signal_received`：`{signal, payload}`，唤醒时发布；随后走常规 `pending → running` 与 `task.started`。
- `signal` 是业务等待键（如 `"approval"`），同一任务多次等待靠它区分。

### 2. 挂起与唤醒 API

- Handler 内：`payload = await neotask.wait_for_signal("approval", timeout=None)`
  - 首次执行：保存等待键与可选超时，状态转 `waiting`；Worker 捕获内部挂起信号后出队、释放槽位、不完成 future。
  - 唤醒后：任务以 `pending` 重新入队并重新执行 Handler；`wait_for_signal` 命中已到达的等待键时直接返回 `payload`。
  - 语义是“可重入重放”：与自动重试同一约定，Handler 从最近 checkpoint 恢复，避免重复副作用。
- Handler 外：`pool.signal(task_id, payload=None)` / `signal_async`
  - 只接受 `waiting`，先到先得；重复 signal 返回 `False`，不产生二次副作用。
  - 成功：写入 payload → 状态置 `pending` → 重新入队 → 发布 `task.signal_received`。
  - 任务已取消/已结束返回 `False`。
- 查询与取消：`list_by_status(WAITING)` 三后端通用；`cancel()` / `delete()` 接受 `waiting`，删除后迟到的 signal 返回 `False`。

### 3. 等待语义

- 进入等待自动保存一次 checkpoint（执行器未保存时以空状态占位）；signal payload 通过保留键 `task.data["__signal__"]` 注入，业务不得占用该键。
- payload 被 `wait_for_signal` 消费后清除，并记录等待序号，避免同一任务用同一等待键二次挂起时误命中旧信号。
- 默认不超时（等人没有系统超时）；显式传 `timeout` 或配置 `waiting_timeout` 时，到期转 `failed` 且 `failure_reason="waiting_timeout"`，由调度线程扫描触发。
- 等待任务不参与 RUNNING 超时回收、心跳续期与故障回收；节点下线不影响等待任务，任何节点都可 signal。
- 等待期间不触发自动重试；自动/手动重试与等待互斥。

### 4. 执行与可观测

- WorkerPool 进入等待时释放并发槽位：`running - 1`、`waiting + 1`，唤醒后反向。
- `get_stats()` / `get_metrics()` 增加 `waiting`；内置 `WebUIServer` 页面展示等待计数与 `waiting` 任务列表（`/api/tasks?status=waiting`），并提供 `list_tasks(status, limit, offset)` 查询接口。
- 并发回归：单 worker 下等待任务不阻塞其他任务。

### 5. 兼容与迁移

- 无 schema 迁移：SQLite `status` 是文本列，Redis `status:*` 集合按状态名自动索引。
- `TaskStatus` 改为 `str, Enum` 后，`str(status)` 从 `TaskStatus.PENDING` 变为 `pending`；
  这是日志/展示口径变化，`.value`、JSON 与存储值均不变，需在 CHANGELOG 注明。
- 旧版本读到 `waiting` 必须视为非终态，不得重新入队或回收；契约文档补充该约束。
- 2.0 上报自动携带新事件，控制面按 `task_id` 幂等推进。

### 验收标准

- 三后端：进入等待 → 重启进程 → signal → 任务完成；SQLite / Redis 验证跨实例唤醒。
- 状态机拒绝非法转换（signal 非 waiting、waiting 直接 success、重复 signal）。
- 等待期间不占 worker 槽位；取消/删除等待任务后 signal 无效。
- 事件顺序：`task.waiting` → `task.signal_received` → `task.started` → `task.completed`。
- 等待超时转 `failed` 且 `failure_reason="waiting_timeout"`。
- 枚举规范：成员名全部大写、值全部小写；`str()` / `.value` / JSON 三种输出一致，
  `from_value()` 能还原全部成员；旧库中的小写状态值反序列化结果不变，3.8–3.12 CI 通过。
- 契约文档（状态表、转换图、事件表、保留键）先于实现更新，ARCHITECTURE 的 `TaskStatus` 枚举表同步 `WAITING` 与 `str, Enum` 规范；新增示例 `examples/23_human_in_the_loop.py`；全量套件通过。

---

## 五、v1.4.0 — 单节点操作台、任务列表语义与监控导出

**目标**：把 v1.3 的等待语义落到可操作的本地运维面——提供嵌入式单节点操作台（查看与 signal / cancel / retry），修正任务列表的排序与分页语义，并补齐 Prometheus 导出与可靠性边界。

**为什么单独一个 minor**：新增公开 HTTP 端点与三后端创建时间索引，属于公开行为面扩展；仍以可选依赖提供，不改变核心 API 与线协议。

**原则**：

- 操作台只服务当前节点，不做多节点聚合、任务下发等控制面职责。
- 默认只监听 `127.0.0.1`；写接口强制 JSON body + 同源校验，可选 `token`。
- 不提供删除操作（不可逆）；操作集合限定为 signal / cancel / retry。
- 面板随宿主进程生命周期启停，不做独立部署的 Web 服务。

### P1 任务列表排序与分页语义

#### 1. 三后端 `list_by_status` 统一排序
- 排序键：`created_at DESC, task_id`，结果稳定、分页可复现。
- SQLite `ORDER BY created_at DESC, task_id`；memory 在锁内按 `created_at` 倒序后截断。
- Redis 新增 `neotask:tasks:created` ZSET（score = 创建时间戳）与升级回填标记，
  用 `ZREVRANGE + MGET` 读取；创建、删除与终态清理路径同步维护索引。

#### 2. `list_tasks(status=None)` 语义
- 不再“每个状态各取 `limit // 5` 拼接”；新增 `TaskRepository.list_recent(limit, offset)`：
  - memory 排序后切片；SQLite 走 `created_at` 索引；Redis 走创建时间 ZSET。
  - base 默认实现按各状态取 `offset + limit` 后合并排序，供自定义后端沿用。
- `TaskPool` / `TaskScheduler` 的 `list_tasks` 返回稳定顺序；Web `/api/tasks` 增加 `total`，支持 offset 分页。

### P1 单节点操作台

#### 1. 只读视图
- `GET /api/tasks`：状态筛选 + 分页 + `total`；`status=all` 走 `list_recent`。
- `GET /api/tasks/{task_id}`：payload、失败原因、重试次数、checkpoint 版本、父子关系。
- 页面：状态计数 chips、任务表、详情抽屉；具备空状态、结果数与轮询暂停。

#### 2. 操作端点
- `POST /api/tasks/{task_id}/signal`：仅 `waiting` 生效，body 为 payload。
- `POST /api/tasks/{task_id}/cancel`：`pending` / `running` / `waiting`。
- `POST /api/tasks/{task_id}/retry`：`failed` / `cancelled`，可选 `delay`。
- 端点复用 `pool.signal/cancel/retry` 语义并返回结构化结果；失败返回原因，操作写审计日志。
- 前端二次确认；`WebUIConfig.token` 可选，开启后写接口校验 `X-NeoTask-Token`；**不提供删除**。

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
- v1.3 的 `waiting` 任务重启后保持 `waiting`，不自动执行、不回收；signal 后按 `pending` 恢复。

#### 2. 优雅停机（配置已存在，补文档与边界用例）
- `drain_delayed_on_shutdown`（默认 `False`）已提供：停机时是否立即执行未到期延时任务。
- 剩余工作：补齐周期任务/Cron 与 `waiting` 任务在停机期间的行为矩阵与回归用例
  （等待任务不会被 drain、也不会被取消）。

#### 3. 重试语义（已定稿）
- 自动重试、手动重试、Checkpoint 续跑三者交互见契约文档第 9 节：
  两种重试都保留并注入最近 checkpoint；`resume_from_checkpoint=True` 仅作用于新提交，
  与 `max_retries` 相互独立。
- 等待中的任务不进入自动重试；取消后迟到 signal 无效。

### 验收标准

- 三后端分页顺序一致且可复现；`list_recent` 与 `list_tasks(None)` 有排序与分页用例。
- 操作台：signal / cancel / retry 在 waiting / running / failed 任务上的行为与核心 API 一致，无删除入口。
- 写接口默认仅本机可达；开启 token 后未携带返回 401，非法状态返回 400。
- Prometheus 导出器可挂载到 FastAPI 示例。
- 停机行为矩阵覆盖 `pending` / `running` / `waiting` / 周期任务，崩溃恢复边界在文档中有明确说明。

### 非目标

- 不做删除、多节点视图、远程控制台与完整鉴权/用户体系。
- 不做独立部署的 Web 服务，不默认监听公网地址。

---

## 六、v2.0.0 — NeoFlow 执行面接入（后移）

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

## 七、文档同步要求

- 线协议变更先改 [neotask-execution-contract.md](./neotask-execution-contract.md)，再改实现。
- 版本状态变化时同步 README / README_zh 的路线图区块（当前 v1.3.1 已发布、v1.4 为单节点操作台与导出/可靠性、v2.0 为 NeoFlow 执行面接入）。
- 每个发布版本同步 `examples/readme.txt` 索引与 CHANGELOG（由 release-please 生成）。

---

## 八、下一步行动

**v1.1 发布收尾（已完成）**：

- `TaskEngine` 注入测试已补齐；1.1.0 / 1.1.1 / 1.1.2 已由 release-please 发布。
- 基准与性能用例的低精度计时已改为 `perf_counter`，Windows 毫秒级任务不再被量成 0。

**v1.2 收尾（v1.2.0 已发布 2026-11-20；v1.2.1 已发布 2026-11-22）**：

- 基准套件、高并发调度/事件派发/SQLite 写入优化、`get_metrics()` 与 `on_metrics()` 已全部落地。
- v1.2.0 发布后补完事件批量派发/同步回调内联与 SQLite 原子创建入队（第 5 节），
  已随 v1.2.1 发布。
- 发布流程：推送 dev → CI 通过 → 合并 main → release-please 自动升版并发布 PyPI。

**v1.3 人工介入（v1.3.0 已发布 2026-11-26；v1.3.1 已发布 2026-11-27）**：

- 执行契约先于实现更新，`waiting` 状态与 `task.waiting` / `task.signal_received` 事件已落地；
  `TaskStatus` 统一为标准 `str, Enum`，事件保持 `EventType` 大写常量 + `task.*` 线上名。
- 1.3.1 收尾修复：信号唤醒不再重复计入排队时间、`list_tasks` 非法状态严格报错、
  `TaskEngine.get_stats().running` 统一为任务数、`executor_type` 后置赋值同样归一化。

**v1.4 单节点操作台、列表语义与导出/可靠性（下一步）**：

- 排期顺序固定为：三后端列表排序与分页（含 Redis 创建时间索引 `neotask:tasks:created`
  与升级回填）→ `list_recent(limit, offset)` 与 `list_tasks(None)` 语义统一 → 操作台
  （只读视图 + signal / cancel / retry）→ Prometheus 导出器 → 停机/崩溃恢复矩阵。
- 操作台不提供删除；默认仅 `127.0.0.1`，写接口 JSON + 同源校验，可选 token，操作写审计日志。
- 监控导出器与操作台均以可选依赖提供，不改变核心 API、线协议与三后端一致性。
- 嵌入式集成口径：对外契约就是进程内 Python API（提交、查询、signal/cancel/retry、
  事件订阅、配置对象），不内置 function-calling / tool schema；面向 LLM 的工具描述与
  MCP 封装放到 2.0 的 NeoFlow 适配层，避免核心 SDK 绑定宿主协议。

**中期**：

- NeoFlow 控制面就绪后再评估 2.0 执行面接入的启动时机。
