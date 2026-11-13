# NeoTask 执行面协议契约（v1）

**状态**：冻结。1.x 内只做增量扩展；破坏性变更必须提升 `schema_version` 并保留旧版本可读一个 minor 版本。

**适用范围**：NeoTask 1.x 已交付的全部公开数据形态，以及 2.0 与 NeoFlow 控制面交互时复用的任务与事件结构。

本页只约束**线协议**，不约束存储实现与内部模块结构。2.0 的接入方式见第 12 节。

---

## 1. 平面划分

| 平面 | 归属 | 职责 |
| :--- | :--- | :--- |
| 控制面 | NeoFlow | DAG 拆解、任务下发、事件监听、状态推进、跨服务重试决策 |
| 执行面 | NeoTask | 拉取任务、执行 Handler、本地重试与断点续跑、上报事件、实例注册与心跳 |

NeoTask 不实现 DAG 拆解、状态推进与下发策略；这些逻辑固定在 NeoFlow 控制面。1.x 在不接入任何控制面的情况下可独立使用（Memory / SQLite / Redis 后端均可）。

---

## 2. 契约范围

**属于契约（稳定）**

- 任务 JSON 结构（第 6 节）：`Task.to_dict()` 与 `task.created` 事件负载
- 任务结果视图（第 7 节）：`pool.get_result()` 返回结构
- 事件信封与事件名、各事件负载（第 8 节）
- 任务状态枚举与状态机（第 5 节）
- 失败、重试、续跑语义（第 9 节）
- 任务标识与幂等规则（第 4 节）
- Checkpoint 保留键与版本语义（第 10 节）
- 成本字段与聚合结构（第 11 节）

**不属于契约（可自由调整）**

- 存储表结构、列名、索引；Redis 键布局与序列化方式
- 本地队列的传输格式（PriorityQueue / DelayedQueue 内部结构）
- Blackboard 进程内数据
- 日志文本、监控指标命名

---

## 3. 版本与兼容规则

- 事件信封 `schema_version` 当前为 `1`。
- **只增量**：新增字段必须可选，缺省时消费方要有安全行为；消费方必须忽略未知字段。
- **破坏性变更**（删除字段、修改类型或语义）必须提升 `schema_version`，并在至少一个 minor 版本内同时接受旧结构。
- 时间格式：任务 JSON 中的时间为 ISO 8601 字符串且带时区（如 `2026-11-12T09:30:00+00:00`）；事件信封 `timestamp` 为 epoch 秒（float）。
- 序列化形态差异是本契约最易出错的一点：
  - 任务 JSON（`get_task()` 与 `task.created` 负载）中，`data` / `result` / `cost_tracking` 是 **JSON 字符串**；
  - `get_result()` 视图与 `task.completed` / `task.failed` 负载中的 `result` / `cost_tracking`（`cost`）是 **已解析对象**。

---

## 4. 任务标识与幂等

- `task_id` 可由外部指定，NeoTask 原样使用；未指定时生成 `TSK{YYMMDDHHmmssffff}{random6}`。
- `pool.task_exists(task_id)` 判断 ID 是否被占用。
- 以相同 `task_id` 再次提交会抛 `TaskAlreadyExistsError`（不覆盖、不排队）。
- 控制面下发建议：用 DAG 节点的执行 ID 作为 `task_id`，先 `task_exists()` 再决定提交；捕获 `TaskAlreadyExistsError` 同样视为幂等成功。
- `pool.report_failure(task_id, reason)` 只接受 `RUNNING` 状态的任务。

---

## 5. 任务状态机

| 状态 | 语义 | 终态 |
| :--- | :--- | :--- |
| `pending` | 已创建，等待执行 | 否 |
| `running` | 执行中 | 否 |
| `success` | 执行成功 | 是 |
| `failed` | 执行失败（系统异常或业务失败） | 是 |
| `cancelled` | 已取消 | 是 |
| `scheduled` | 保留值，NeoTask 1.x 不会产生 | — |

允许的转换：

```
pending → running → success
pending → running → failed
pending → cancelled
running → cancelled
running → pending   （自动/手动重试）
failed  → pending   （手动重试）
```

`failed` 不区分系统异常与业务失败，二者都看 `failure_reason`：

- `failure_reason` 为空：系统异常，已走自动重试；
- `failure_reason` 非空（如 `qa_failed`）：执行器主动判定的业务失败，**不进入自动重试**，`retry_count` 保持原值。

---

## 6. 任务 JSON（`Task.to_dict()`）

`pool.get_task()` 与 `task.created` 事件负载使用同一结构。

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `task_id` | string | 任务 ID |
| `data` | string | 任务输入，JSON 字符串；空对象为 `"{}"` |
| `status` | string | 见第 5 节 |
| `priority` | int | `0` 最高 → `3` 最低；默认 `2` |
| `progress` | float | `0.0` – `1.0` |
| `progress_message` | string | 最近一次进度消息，无则 `""` |
| `node_id` | string | 执行节点标识，未分配为 `""` |
| `retry_count` | int | 已重试次数 |
| `ttl` | int | 任务超时秒数，默认 `3600` |
| `created_at` | string | ISO 8601，带时区 |
| `started_at` | string | 未开始为 `""` |
| `completed_at` | string | 未结束为 `""` |
| `result` | string | 执行结果，JSON 字符串；无结果为 `""` |
| `error` | string | 错误文本，无错误为 `""` |
| `failure_reason` | string | 业务失败原因，无则为 `""` |
| `checkpoint_version` | int | 最近 checkpoint 版本，`0` 表示没有 |
| `parent_task_id` | string | 父任务 ID，无则为 `""` |
| `estimated_cost` | float | 成本金额，未上报为 `0.0` |
| `cost_tracking` | string | 成本明细 JSON 字符串，无则 `""` |
| `queue_wait_ms` | float \| null | 排队耗时（`started_at - created_at`） |
| `duration_ms` | float \| null | 执行耗时（`completed_at - started_at`） |

`cost_tracking` 解析后字段：`{tokens_in, tokens_out, api_calls, currency}`（`currency` 可缺省）。

---

## 7. 任务结果视图（`pool.get_result()`）

任务不存在返回 `null`；存在时返回：

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `task_id` | string | 任务 ID |
| `status` | string | 见第 5 节 |
| `result` | object \| null | 已解析的执行结果 |
| `error` | string \| null | 错误文本 |
| `failure_reason` | string \| null | 业务失败原因 |
| `parent_task_id` | string \| null | 父任务 ID |
| `estimated_cost` | float | 成本金额 |
| `cost_tracking` | object | 成本明细（已解析；未上报为 `{}`） |
| `queue_wait_ms` | float \| null | 排队耗时 |
| `duration_ms` | float \| null | 执行耗时 |
| `created_at` | string \| null | ISO 8601 |
| `completed_at` | string \| null | ISO 8601 |

该视图不含 `data`、`priority`、`progress`、`retry_count`、`ttl`；需要完整信息用 `get_task()`。

---

## 8. 事件契约

### 8.1 信封

1.x 在进程内以 `TaskEvent` 对象传递；2.0 上报时按下列字段序列化为 JSON，字段名与对象属性一致：

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `event_type` | string | 事件名，见 8.2 |
| `task_id` | string | 关联任务 ID |
| `data` | any | 事件负载，见 8.2 |
| `timestamp` | float | 事件产生时刻（epoch 秒） |
| `node_id` | string | 产生事件的实例标识；EventBus 未配置节点时保留事件自带值 |
| `schema_version` | int | 信封版本，当前为 `1` |

### 8.2 事件名与负载

| `event_type` | `data` |
| :--- | :--- |
| `task.created` | 任务 JSON（第 6 节） |
| `task.started` | `{node_id}` |
| `task.completed` | 执行结果对象；上报过成本时为 `{result, cost}` |
| `task.failed` | `{error, old_status, reason, cost}`；`reason` 为业务失败原因或 `null`，`cost` 为成本或 `null` |
| `task.cancelled` | `{old_status}` |
| `task.retry` | 自动重试：`{error, retry_count, max_retries, old_status}`；手动重试：`{retry_count, manual: true, old_status}` |
| `task.progress` | `{progress, message, timestamp}` |
| `task.status_changed` | `{old_status, new_status}` |
| `task.deleted` | `null` |
| `task.reclaimed` | 失败回收：`{reason, action: "failed"}`；重新入队：`{reason, retry_count, action: "requeued"}` |
| `task.checkpointed` | `{version}` |
| `task.cost_reported` | 本次上报的成本增量 `{tokens_in, tokens_out, api_calls, estimated_cost, currency?}` |

事件名常量统一由 `neotask.EventType` 提供，禁止裸字符串拼接。

### 8.3 投递语义

- 1.x 事件总线是**进程内尽力而为**的发布订阅：单个消费协程按投递顺序处理，跨任务不保证全局顺序。
- 待处理队列默认不设上限；配置 `event_queue_max_size` 后按 `drop` 策略丢弃新事件并计数告警。因此 1.x 事件**不承诺不丢**。
- 2.0 上报到控制面时由上报端与控制面共同保证可靠性，建议至少一次投递 + 控制面按 `task_id` 与状态机幂等推进（见第 12 节）。

---

## 9. 失败、重试与续跑

| 场景 | 结果 |
| :--- | :--- |
| Handler 抛异常 | 状态回 `pending`，`retry_delay * retry_count` 延迟后重试；超过 `max_retries` 转 `failed` |
| `neotask.report_failure(reason)` 后正常返回 | 直接 `failed`，`failure_reason=reason`，不自动重试 |
| `pool.report_failure(task_id, reason)` | 同上，仅接受 `RUNNING` 任务 |
| 超时（`ttl`） | 结束为 `failed` |
| 手动重试 | `failed` → `pending`，保留 checkpoint |

- 自动重试与手动重试都会把最近 checkpoint 注入 `task.data["__checkpoint__"]`。
- `resume_from_checkpoint=True` 只作用于新的提交（`submit` / `create_pending_task`），与 `max_retries` 相互独立：前者决定首次执行前是否注入历史状态，后者只决定异常后的重试次数。

---

## 10. Checkpoint 数据面

- 版本号从 `1` 开始递增，每保存一次 `checkpoint_version + 1`；任务 JSON 中的 `checkpoint_version` 为 `0` 表示没有 checkpoint。
- 单任务保留最近 `checkpoint_keep_versions` 个版本（默认 `5`），更早版本自动淘汰。
- 恢复时，最新状态被注入 `task.data["__checkpoint__"]`。**`__checkpoint__` 是保留键**，业务 `data` 不得占用同名键。
- `task.checkpointed` 事件在每次写入后发布，`data.version` 为新版本号。
- 执行器内 async 用 `neotask.save_checkpoint(state)`；线程执行器用 `*_sync` 变体；进程执行器与执行器外场景用 `pool.save_checkpoint(task_id, state)`。

---

## 11. 成本

- 上报字段：`tokens_in`、`tokens_out`、`api_calls`、`estimated_cost`、`currency`；多次上报按字段累加。
- 每次上报发布 `task.cost_reported`，负载为本次增量。
- `task.completed` 的负载在「上报过成本」时包装为 `{"result": ..., "cost": ...}`，否则保持「负载即结果」的原契约。
- `pool.get_cost_summary(task_ids=None)` 返回：

```json
{
  "task_count": 10,
  "tasks_with_cost": 6,
  "tokens_in": 1200,
  "tokens_out": 3400,
  "api_calls": 8,
  "estimated_cost": 0.42,
  "currency": "USD"
}
```

`task_count` 为参与统计的任务数，`tasks_with_cost` 为其中上报过成本的任务数；币种混用时 `currency` 为 `"MIXED"`。

---

## 12. 2.0 执行面接入（NeoFlow）

2.0 只增加「执行面对接」，不改变 1.x 的任何公开结构。

| 能力 | 说明 | 复用/新增 |
| :--- | :--- | :--- |
| 任务拉取 | NeoTask 从控制面通道拉取任务，消息体沿用第 6 节任务 JSON（`data` 为 JSON 字符串） | 新增 TaskConsumer；候选 Redis Stream 消费者组，确认/重投语义在 2.0 定义 |
| Handler 执行 | 复用现有 Executor 与 WorkerPool，按 `data` 执行 | 复用 1.x |
| 事件上报 | 按第 8 节序列化 `task.*` 事件并上报控制面 | 新增 EventReporter；建议至少一次 + 控制面幂等 |
| 实例注册与心跳 | 复用既有 `NodeManager` / `HeartbeatManager` | 复用 1.x |
| 控制消息 | 取消、暂停等指令通道 | 1.x 已有本地 `cancel` / `pause`；跨服务通道在 2.0 定义 |
| 本地重试存储 | 执行面内重试状态不依赖控制面 | 复用 1.x 重试与 checkpoint |

**2.0 不做**：DAG 拆解、状态推进、任务下发策略、跨服务事件监听。这些是 NeoFlow 控制面的职责。

安装方式上，2.0 的能力作为可选依赖提供，未安装 2.0 的 1.x 用户使用 Memory / SQLite / Redis 本地能力不受任何影响。

---

## 13. 实现检查清单

接入方（NeoFlow 或第三方）实现时逐项确认：

- [ ] 只通过任务 JSON、`get_result()` 与事件消费数据，不直接读写 NeoTask 的存储表或 Redis 键。
- [ ] 按 `task_id` 幂等推进状态，重复事件不产生二次副作用。
- [ ] 忽略不认识的字段；只在 `schema_version` 提升时调整解析。
- [ ] 区分「业务失败（`failure_reason` 非空）」与「系统异常重试中」。
- [ ] 不依赖 Blackboard 传递跨服务数据（它只在单进程内有效）。
- [ ] 恢复任务时尊重 `task.data["__checkpoint__"]` 保留键。
- [ ] 时间解析同时兼容带时区的 ISO 8601 与 epoch 秒两种形态。
