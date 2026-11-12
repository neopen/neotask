# NeoTask v1.1.0 设计文档 — 本地任务能力补全

**基线版本**：v1.0.8（`origin/main` == tag `v1.0.8`）
**目标版本**：v1.1.0
**状态**：设计定稿，待实施
**范围来源**：[neotask-roadmap.md](./neotask-roadmap.md) §二

本文档是 v1.1.0 的实现依据。与 roadmap 冲突处以本文档为准；本文档未覆盖处
（如各功能项的验收口径）沿用 roadmap §二。

---

## 一、范围确认

roadmap v1.1.0 的 7 项 P0 全部纳入本版本，其中：

- **第 7 项（外部指定 task_id）已在 1.0.x 落地**：`TaskPool.submit(data, task_id=..., priority=...)`
  已存在并透传至 dispatcher，`task_exists()` / `task_exists_async()` 也已提供存在性检查
  （`TaskRepository.exists()` 的公开形态）。本版本**不新增 `has_task()` 别名**——同一能力两个名字
  是长期维护成本；只补 `TaskScheduler.task_exists_async()`（该入口缺异步形态，破坏项目
  sync/async 双 API 惯例）、定稿幂等语义文档与 `examples/16_custom_task_id.py`。
- **第 5 项（Blackboard 本地上下文）范围收缩**：仅实现 **Memory 后端**，SQLite / Redis 不做。
  理由：roadmap 自身已限定它"不承担跨服务数据传递"，落库版本只是给进程内 dict 加一层持久化壳，
  却要额外背上 TTL 清理策略与容量治理，与"轻量可嵌入的执行引擎"定位冲突。
  API 签名保持不变，后续若开放落库后端属向后兼容扩展。

其余第 1–4、6 项按 roadmap 原文交付。

**当前代码起点**：`checkpoint` / `cost_tracking` / `parent_task_id` / `child_count` / `context_set` /
`has_task` 在 `src/`、`tests/`、`examples/` 中零命中，均为新增能力。

---

## 二、四项定稿决策

### 决策 1：Checkpoint 独立表 + `tasks.checkpoint_version` 标记

中间状态可达数百 KB，塞入 `tasks` 行会导致每次状态更新重写大块、列表查询与 TTL 扫描全部变慢。

- 新增 `task_checkpoints` 表，主键 `(task_id, version)`，按版本保留历史。
- `tasks` 表只加 `checkpoint_version INTEGER NOT NULL DEFAULT 0`（0 = 无 checkpoint），
  作为"存在性标记"，满足 roadmap "get_task() 返回中体现 checkpoint 存在性"。
- 保留策略：每任务默认保留最近 **5** 个版本，超出按 version 升序淘汰；上限可配置。

**Memory 后端**用 `Dict[task_id, Deque[CheckpointRecord]]`，**Redis 后端**用
`neotask:ckpt:{task_id}` hash（field = version）。Redis 仅作 checkpoint 落库跟进，与 Blackboard 无关。

### 决策 2：执行器侧 API 双形态（contextvar + 显式 task_id）

现有 `report_progress` 依赖 contextvar 绑定当前任务（`core/progress.py`），且进程执行器在独立
进程中运行、拿不到该上下文。新增 API 沿用同一机制并补齐显式形态：

| 形态 | 签名 | 适用执行器 |
| ---- | ---- | ---------- |
| contextvar | `save_checkpoint(state)` / `load_checkpoint()` / `report_cost(...)` / `report_failure(reason)` | async、thread |
| 显式 task_id | `pool.save_checkpoint(task_id, state)` / `pool.report_cost(task_id, ...)` 等 | 全部，含 process |

contextvar 形态在拿不到上下文时返回 `False`（与 `report_progress` 现有约定一致），不抛异常。
进程执行器只能使用显式形态；此限制须写进 docstring 与 README，不得静默失效。

#### 业务失败（第 4 项）收尾语义定稿

- 执行器内 `report_failure(reason, detail="")` 只**登记**原因，任务照常跑完当前执行；
  Worker 取到返回值后检查登记簿，命中则按 FAILED 收尾，不写 result、不重试、不投递死信。
- 登记 first-wins：后续调用不覆盖首条原因；`error` 合成为 `"{reason}: {detail}"`，
  无 detail 时即 `reason`；空 reason 兜底为 `business_failure`，保证 `error` 永不为空。
- 与自动重试解耦：`max_retries` 只覆盖异常与超时，业务失败的 `retry_count` 恒为 0。
- 显式形态 `pool.report_failure(task_id, reason, detail="")` 仅接受 RUNNING 任务，终态返回
  `False`；上报后执行器协程仍在运行，其后续结果回写由终态守卫拦截。
- `Task.failure_reason` 是新**字段**而非新状态：状态机仍是 `FAILED`，`error` 保持可读，
  三后端均持久化（SQLite 走决策 4 的启动期补列），`task.failed` 事件 `data` 附带 `reason`。

### 决策 3：`estimated_cost` 单列 + `cost_tracking` JSON 明细

- `cost_tracking` 明细（`tokens_in` / `tokens_out` / `api_calls` / `currency`）存 **JSON 文本列**，
  字段增减不再触发表结构变更。
- **`estimated_cost REAL` 单独成列**。`pool.get_cost_summary(task_ids)` 要跨任务聚合，
  JSON 内取数值需全量反序列化，无法下推到 SQL。
- SQLite 聚合走 `SUM(estimated_cost) ... WHERE task_id IN (...)`；Memory 后端在 Python 侧聚合。

### 决策 4：启动期自动 ALTER 与迁移脚本的分工

明确边界，避免两套机制互相越位：

**启动期自动 ALTER**（`SQLiteTaskRepository._init_db()`）当且仅当同时满足：
1. 仅新增列；
2. 新列有默认值、可为 NULL 或 `DEFAULT` 已知，无需回填业务语义；
3. 不搬运、不删除、不改写既有数据。

本版本符合的即：`checkpoint_version`、`parent_task_id`、`estimated_cost`、`cost_tracking`、
`queue_wait_ms`、`duration_ms`、`failure_reason`。ALTER 采用
`PRAGMA table_info(tasks)` 差集比对 + 逐列 `ALTER TABLE ADD COLUMN`，每库每列只执行一次。

**仍须走 `scripts/migrate_*.py` 停机迁移**：跨后端搬迁、索引重建、数据回填、表结构收缩。
本版本新增迁移脚本仅一个：`scripts/migrate_task_metadata.py`，负责 1.0.x 库升级到 1.1.0
（回填 `duration_ms`/`queue_wait_ms` 历史可由 `completed_at - started_at` / `started_at - created_at`
推导的部分，以及索引重建）。

**Redis 后端**无需迁移：任务以 hash 存储，新增字段自然缺省，读取端对缺失字段按默认值处理。

---

## 三、数据模型变更

### Task 模型（`src/neotask/models/task.py`）

新增字段，全部带默认值，保证既有构造调用不破坏：

| 字段 | 类型 | 默认 | 说明 |
| ---- | ---- | ---- | ---- |
| `checkpoint_version` | `int` | `0` | 0 表示无 checkpoint |
| `parent_task_id` | `Optional[str]` | `None` | 父任务 ID |
| `estimated_cost` | `float` | `0.0` | 可聚合成本 |
| `cost_tracking` | `Dict[str, Any]` | `{}` | 明细，落库为 JSON 文本 |
| `queue_wait_ms` | `Optional[float]` | `None` | 出队时刻 − 入队时刻 |
| `duration_ms` | `Optional[float]` | `None` | 完成时刻 − 开始时刻 |
| `failure_reason` | `Optional[str]` | `None` | 业务级失败原因 |

**不新增状态**：`TaskStatus` 枚举与状态机校验保持原样。第 4 项自定义失败只加
`failure_reason` 字段并随 `task.failed` 事件携带，语义上仍是 `FAILED`，与系统异常靠
`error` / `failure_reason` 字段区分。

### SQLite schema（`src/neotask/storage/sqlite.py`）

- `tasks` 表按决策 4 追加上述列。
- 新增索引 `idx_tasks_parent ON tasks(parent_task_id)`（`get_children()` 主查询路径）。
- 新增 `task_checkpoints(task_id TEXT, version INTEGER, state TEXT, created_at TEXT, PRIMARY KEY(task_id, version))`。

### Redis

- checkpoint：`neotask:ckpt:{task_id}`（hash，field=version）。
- 任务 hash 新增 `checkpoint_version`、`parent_task_id`、`estimated_cost`、`cost_tracking`、
  `queue_wait_ms`、`duration_ms`、`failure_reason` 字段。
- Blackboard 不落 Redis（决策见 §一）。
- 键前缀沿用 1.0.x 已统一的 `neotask:` 根前缀约定。

### 仓储接口（`src/neotask/storage/base.py`）

- `TaskRepository` 增：`list_by_parent(parent_task_id, limit, offset)`。
- 新增 `CheckpointRepository`：`save(task_id, version, state)` / `load_latest(task_id)` /
  `load(task_id, version)` / `prune(task_id, keep)` / `delete(task_id)`，三后端实现。
- 局部列更新复用现有 `save_fields(task, fields)`，避免 checkpoint/cost 写入重写整行。

### 仓储装配方式（定稿：不改 `StorageFactory.create()` 签名）

`create()` 保持返回 `(TaskRepository, QueueRepository)` 不变。checkpoint 仓储走**独立工厂函数 +
可选注入参数**，与工厂已有的 `create_task_repository()` / `create_queue_repository()` 惯例同构
（`storage/factory.py`）：

1. `StorageFactory.create_checkpoint_repo(config, connection=None) -> CheckpointRepository`。
2. `TaskEngine` / `TaskPool` / `TaskScheduler` 构造函数增加可选 `checkpoint_repo=None`；
   `TaskScheduler` 内部持有 `TaskPool`，是第 4 处需向内部 pool 透传的参数。
3. `enable_checkpoint=True` 且未注入时，由装配层调用 `create_checkpoint_repo` 自动创建，
   复用 task 仓储的连接（取值方式见下节）。
4. `StorageFactory.register(name, task_repo_class, queue_repo_class)` 增可选参数
   `ckpt_repo_class=None`；未注册时该后端调用 checkpoint API 显式报"后端不支持"，
   **不得静默降级为 no-op**，否则自定义后端用户会误以为 checkpoint 生效。

**收益**：批次 2/3 的改动范围由"改返回签名 + 三处装配"缩小为"新增工厂函数 + 四处可选参数"，
未来新增 context store 等仓储沿用同一惯例，不再触碰既有调用方。

### `connection=` 的生命周期陷阱（默认必须按 refcount 取连接）

`utils/sqlite_compat.acquire_shared_connection()` 是**按 db_path 引用计数**的共享连接
（`_SHARED_REFS` +1），`release_shared_connection()` 计数归零才真正关闭。若 checkpoint 仓储只是
持有传进来的连接对象而不持引用，task 仓储先 `close()` 就会把计数减到 0 并关闭连接，
checkpoint 仓储后续操作直接撞 `Cannot operate on a closed database`。因此：

- **默认路径（`connection=None`）**：checkpoint 仓储自行 `acquire_shared_connection(db_path)`，
  拿到与 task 仓储**同一个连接对象 + 独立引用**，`close()` 时 release 自己的引用。
  既复用连接（避免互抢写锁与 `database is locked`），又保证生命周期各自成立。
- **显式 `connection=`**：语义定义为"调用方持有生命周期"，docstring 必须写明
  **调用方须保证 checkpoint 仓储先于该连接关闭**。该路径仅供测试注入，不作为对外推荐用法。

### 事务原子性：需要新增多语句事务原语

`save_checkpoint` 的原子性 = `INSERT INTO task_checkpoints` + `UPDATE tasks SET checkpoint_version`
两条**不同 SQL** 必须落在同一事务内。现有原语做不到：
`LoopBoundSQLiteConnection.executemany_transaction(sql, params)` 只接受单条 SQL 配多组参数
（`BEGIN IMMEDIATE → executemany → COMMIT`，全程持 `_write_lock_on_loop`）。

批次 3 需在 `utils/sqlite_compat.py` 新增多语句事务原语，例如
`statements_transaction(items: List[Tuple[str, Iterable[Any]]])`，并严格沿用同一条纪律：

- 作为**一次** `_run()` 投递在专用 loop 内执行，全程持有写串行锁；
- 事务体内只用底层 `conn`，**不得回调代理的 `execute` / `commit`**——写锁非重入，
  二次加锁即死锁（该约束在 `executemany_transaction` docstring 中已明示）；
- 任一语句失败整体 `rollback` 并抛出。

**原子性实现是分后端的**，对外承诺口径统一为"`checkpoint_version` 与 checkpoint 内容
要么同时更新、要么都不更新"：

| 后端 | 实现 |
| ---- | ---- |
| SQLite | 新增 `statements_transaction`（`BEGIN IMMEDIATE` + 写串行锁） |
| Redis | `MULTI/EXEC` 或 Lua 脚本，在一次往返内完成 hash 写入 + 版本字段更新 |
| Memory | 单 loop 顺序执行，赋值序列不可分割，无需额外机制 |

该原语同时是后续批量状态写入、cost 落库等场景的复用点，不是一次性投入。

---

## 四、事件类型补全（第 6 项）

### 事件名常量注册表

当前事件名散写为裸字符串（`core/lifecycle.py`、`api/task_pool.py`、`event/bus.py` docstring 等）。
新增 `common/constants.py::EventType` 常量集，全量替换引用点。

**实际存量比 roadmap 多 3 个**：roadmap §二-6 只列了 7 项已有事件，代码里另有
`task.status_changed`、`task.deleted`、`task.reclaimed` 三个真实在发的事件，注册表一并收录，
否则新事件上线时会漏掉既有订阅路径：

`task.created` `task.started` `task.completed` `task.failed` `task.cancelled` `task.retry`
`task.progress` `task.status_changed` `task.deleted` `task.reclaimed`
**`task.checkpointed`** **`task.cost_reported`**

**取普通字符串常量类而非 `str`-Enum**：事件名会被写进日志、Web UI 展示与事件字典，
而 mixin Enum 的 `__str__` / `__format__` 语义在 Python 3.11 发生变更——实测 3.11 与 3.13 上
`str(EventType.COMPLETED)` 与 `f"{EventType.COMPLETED}"` 均**不等于** `"task.completed"`
（字典键查找则仍相等）。纯字符串常量与既有裸字符串订阅方式逐字节等价，
不必为格式化行为赌版本差异。

> 本机仅有 3.11 / 3.13 / 3.13t 可用（3.10 安装已损坏、无 3.8），3.8/3.10 的
> `__str__` 表现交由 CI 矩阵覆盖验证。

### 事件信封稳定化

`TaskEvent` 现为 `{event_type, task_id, data}`，缺时间与来源标识，无法支撑未来远程上报。扩展为：

```
event_type: str        # EventType
task_id: str
data: Optional[Any]
timestamp: float       # 事件产生时刻（epoch）
node_id: str           # 实例标识，默认取配置 node_id
schema_version: int = 1
```

`task.completed` / `task.failed` 的 `data` 信封携带 `cost` 字段；`task.failed` 额外携带 `reason`。
本版本**只做信封稳定化，不做任何远程上报**。

---

## 五、公开 API 汇总

`TaskPool` / `TaskScheduler` 同步 + 异步双形态（沿用 `method()` / `method_async()` 约定）：

- `task_exists(task_id) / task_exists_async(task_id) -> bool`（既有 API；本版本补齐
  `TaskScheduler` 的异步形态，不新增 `has_task()` 别名）
- `submit(..., parent_task_id=None)`
- `get_children(task_id) -> List[str]`
- `get_parent(task_id) -> Optional[str]`
- `get_cost_summary(task_ids=None) -> Dict[str, Any]`
- `save_checkpoint(task_id, state) / load_checkpoint(task_id)`
- `context_set(key, value, ttl=None) / context_get(key) / context_delete(key)`（仅 Memory 后端；
  非 Memory 后端调用时抛明确异常，不静默降级）
- `create_pending_task(..., resume_from_checkpoint=False)` 与重试路径接入 checkpoint 恢复
- `report_failure(task_id, reason, detail="")` / `report_failure_async(...)`：执行器外的显式失败上报
  （执行器内用顶层 `neotask.report_failure(reason, detail)`，见决策 2）

**配置项**：`enable_checkpoint: bool = True`（`TaskPoolConfig` / `EngineConfig`，`TaskScheduler` 透传）。
默认开启的理由：SQLite 建表走 `CREATE TABLE IF NOT EXISTS`、Memory 只是多一个 dict，
两者边际成本可忽略；若默认关闭，用户在执行器里调 `save_checkpoint()` 只会静默拿到 `False`，
是可观测性上的倒退。`checkpoint_keep_versions: int = 5` 同处配置。

**不做自动汇总触发**：父任务完成判定由调用方或编排层负责，NeoTask 不越界（roadmap §二-3）。

---

## 六、实施批次

| 批次 | 内容 | 预估 |
| ---- | ---- | ---- |
| 1 | 事件常量注册表 + 信封扩展；`TaskScheduler.task_exists_async()` 与幂等语义定稿；第 4 项 `report_failure(reason)` | 2 人日 |
| 2 | Task 模型扩字段 + SQLite 列与启动期 ALTER + `migrate_task_metadata.py`；第 2 项成本采集；第 3 项父子关联 | 4–5 人日 |
| 3 | 新增 `statements_transaction` 多语句事务原语；第 1 项 Checkpoint（仓储 + 独立工厂 + 双形态 API + 恢复路径 + 事件）；第 5 项 Blackboard（Memory） | 3.5–4.5 人日 |

合计 9.5–12 人日。另建 `benchmarks/` 目录骨架（roadmap §七 本周项），至少覆盖
"单任务提交延迟""批量提交吞吐""Checkpoint 写入开销"三条，作为批次 2/3 的成本基线。

---

## 七、验收标准

- 每项功能配 `examples/NN_*.py` 示例 + 单元测试 + 集成测试，模块覆盖率 ≥ 80%。
- Memory / SQLite 后端功能全通过；Redis 后端除 Blackboard 外全通过。
- 1.0.x 老 SQLite 库文件在新版本下可直接打开（启动期 ALTER 生效），并按
  `scripts/migrate_task_metadata.py` 完成回填后功能等价。
- `get_task()` 返回体现 checkpoint 存在性与耗时拆分。
- CHANGELOG 逐项记录，README 功能表与路线图同步。
- CI 全绿（Python 3.8 / 3.10 / 3.11 / 3.12 / 3.13）。

---

## 八、风险与兼容性

| 风险 | 处置 |
| ---- | ---- |
| 启动期 ALTER 在并发首次初始化时重复执行 | 沿用 `_init_db()` 现有 `_initialized` 屏障；ALTER 前先 `PRAGMA table_info` 差集比对 |
| checkpoint 历史无界增长 | 默认保留 5 版 + `prune()`，并纳入 1.0.x 已有的 TTL 终态清理路径 |
| `context_*` 在非 Memory 后端被误用 | 显式抛 `NeoTaskError` 并说明范围，不做静默 no-op |
| 双形态 API 语义漂移 | contextvar 形态内部委托到显式形态，单一实现路径 |
| 共享连接生命周期：task 仓储先 close 使 checkpoint 仓储拿到已关闭连接 | 默认路径按 db_path `acquire_shared_connection` 各自持有引用，close 时各自 release；显式 `connection=` 仅限测试注入并文档化"调用方须后关" |
| 多语句事务内误用代理加锁方法导致死锁（写锁非重入） | 新原语事务体只用底层 `conn`；补并发写 + 事务交错压力用例，覆盖 `cannot start a transaction within a transaction` |
| 自定义后端未注册 checkpoint 类却被调用 | `register()` 增可选 `ckpt_repo_class`，缺失时显式抛"后端不支持"，不静默 no-op |
| `StorageFactory.create()` 返回签名变更破坏下游装配 | 不改签名；走 `create_checkpoint_repo()` 独立工厂 + 构造函数可选参数，既有调用方零改动 |
