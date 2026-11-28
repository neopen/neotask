# NeoTask v1.5.0 设计文档 — 分组并发与速率限制（Group Limits）

**基线版本**：v1.3.1（v1.4.0 操作台未发布，实施顺序在其之后）
**目标版本**：v1.5.0
**状态**：设计定稿，待实施
**范围来源**：2026-11-28 功能讨论结论（尚未写入 [neotask-roadmap.md](./neotask-roadmap.md)，确认后补录）

本文档定义任务「分组（group）」以及分组级并发上限与速率限制。核心结论：**group 作为任务一等字段持久化；执行前做非阻塞准入（try-acquire），未准入则带短延迟重新入队，不在 worker 上阻塞等待。**

---

## 一、问题与现状

### 1.1 现状

| 能力 | 现状 |
| :--- | :--- |
| 全局并发 | `TaskPoolConfig.worker_concurrency` / `EngineConfig.worker_concurrency`，worker 数即执行并发上限（`worker/pool.py:146` 信号量与 worker 数一致） |
| 分组并发 | 无 |
| 速率限制（QPS/RPM） | 无 |
| 任务分组标签 | 无（`Task` 数据模型见 `models/task.py:86-118`） |
| 队列 | 只有优先级维度（`QueueScheduler` / 优先级堆），没有按业务类别分流 |

### 1.2 用户场景

- 调用 LLM / 第三方 API 的任务需要限制「同时最多 3 个」「每分钟最多 60 次」，避免触发供应商限流或超额计费。
- 浏览器/爬虫类任务需要限制同一目标的并发，避免被封禁。
- 批量导出、重计算任务需要给交互式任务让路：交互式不限，后台类限制并发。

全局并发只能回答「整个进程最多几个」，无法表达「哪一类任务最多几个、多快」。这是嵌入式任务库最常见的落地缺口之一。

---

## 二、语义定义

### 2.1 group

- `group` 是任务提交时可选填写的业务类别字符串，例如 `"llm"`、`"email"`、`"crawl:example.com"`。
- 未填写 = 不属于任何分组，行为与当前完全一致（不经过任何限流判断）。
- 分组是**纯本地调度概念**，不参与线协议状态机、不扩展任务状态、不影响重试/等待/检查点语义。

### 2.2 限制维度

| 维度 | 含义 | 计数口径 |
| :--- | :--- | :--- |
| `max_concurrency` | 同组同时处于执行中的任务数上限 | 进入执行（准入成功）到执行结束（含失败、取消、挂起） |
| `rate` / `per_seconds` | 同组单位时间内的**启动次数**上限（令牌桶） | 每次准入成功消耗 1 个令牌；与执行时长无关 |

- 两个维度可单独配置、也可同时配置，同时配置时须**同时满足**才准入。
- 令牌桶参数：`rate`（窗口内允许次数）、`per_seconds`（窗口秒数，默认 1.0）、`burst`（桶容量，默认等于 `rate`）。
- 统计口径是「启动次数」，不是完成次数：一个执行 30 秒的任务只消耗 1 个令牌。

### 2.3 保证级别

| 保证 | 说明 |
| :--- | :--- |
| 单进程内不超限 | ✅ 并发与速率均在进程内精确 |
| 多进程/多节点合计不超限 | ❌ v1.5 不提供；每个进程各自计算上限（见 §6） |
| 限流等待不计入任务超时 | ✅ `task_timeout` 只包执行阶段 |
| 限流不消耗重试预算 | ✅ 重新入队是准入延迟，不是失败 |
| 等待信号释放并发名额 | ✅ 挂起（`TaskSuspended`）时释放，唤醒后重新准入 |

---

## 三、API 设计

### 3.1 配置模型

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class GroupLimit:
    """单个分组的准入上限；两个维度均为 None 时等价于不限流"""
    max_concurrency: Optional[int] = None
    rate: Optional[int] = None
    per_seconds: float = 1.0
    burst: Optional[int] = None       # 默认等于 rate

    def __post_init__(self) -> None:
        if self.max_concurrency is not None and self.max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if self.rate is not None:
            if self.rate < 1:
                raise ValueError("rate must be >= 1")
            if self.per_seconds <= 0:
                raise ValueError("per_seconds must be > 0")
            if self.burst is not None and self.burst < 1:
                raise ValueError("burst must be >= 1")
```

配置入口（三个公开入口保持一致）：

```python
TaskPoolConfig(
    group_limits={
        "llm":   GroupLimit(max_concurrency=4, rate=60, per_seconds=60),
        "email": GroupLimit(max_concurrency=8, rate=100, per_seconds=60),
        "crawl": GroupLimit(max_concurrency=2),
    },
    group_limit_default=None,      # 未列出的分组：None = 不限流
    strict_groups=False,           # True 时提交未登记分组直接报错
    rate_limit_defer_interval=0.05 # 未准入时的重新入队延迟（秒）
)
```

- `SchedulerConfig` / `EngineConfig` 增加同名字段，并透传到内部 `TaskPoolConfig` / `WorkerPool`。
- 配置在启动时由 `WorkerPool.start()` 编译为 `GroupGate`（与信号量同生命周期，避免跨事件循环绑定问题）。
- 未知分组：
  - `strict_groups=False`（默认）：按不限流执行，每个未知分组只告警一次。
  - `strict_groups=True`：提交时抛 `UnknownGroupError`（继承 `ValueError`），在创建任务前失败。

### 3.2 提交接口

```python
# TaskPool
pool.submit(data, group="llm")
pool.submit_async(data, group="llm")
pool.submit_many(tasks, group="llm")          # 整批同一分组

# TaskScheduler
scheduler.submit_delayed(data, 60, group="llm")
scheduler.submit_interval(data, 300, group="llm")
scheduler.submit_cron(data, "0 9 * * *", group="llm")

# TaskEngine
await engine.submit(data, group="llm")
```

- `group` 参数默认 `None`，保持现有调用完全不变。
- 周期任务：`PeriodicTaskDefinition` 增加 `group: Optional[str] = None`，由 `submit_interval` / `submit_cron` 传入，`PeriodicTaskManager._submit_once` 透传给 `submit_async`。周期任务每次执行都重新准入。
- 批量提交 v1.5 只支持「整批同一分组」，不引入逐条分组（避免批量路径与原子性语义扩大）。

### 3.3 示例

```python
config = TaskPoolConfig(
    worker_concurrency=16,
    group_limits={
        "llm": GroupLimit(max_concurrency=4, rate=60, per_seconds=60),
    },
)
with TaskPool(config=config) as pool:
    for prompt in prompts:
        pool.submit({"prompt": prompt}, group="llm")
```

新增示例文件：`examples/24_group_limits.py`（并发上限 + 速率限制 + 指标读取）。

---

## 四、详细设计

### 4.1 任务字段与持久化

模型字段（`models/task.py`）：

```python
#: 业务分组；None 表示不参与分组限流
group: Optional[str] = None
```

持久化键名使用 `task_group`，避免 SQL 保留字 `GROUP` 带来的引号处理：

| 层 | 变更 |
| :--- | :--- |
| `Task.to_dict()` / `from_dict()` | 新增 `"task_group"` 键；`from_dict` 缺省为 `None`（旧数据兼容） |
| Memory | 无额外结构，`Task.clone()` 无需改动（字符串不可变） |
| SQLite | `_TASK_COLUMNS` 增加 `task_group`；`_NEW_TASK_COLUMNS` 增加 `("task_group", "TEXT")`，沿用 `sqlite.py:235` 启动期自动补列；`_row_to_task` 与 `_task_values` 增加映射；`save_if_absent` / `save_if_absent_and_enqueue` 的 INSERT 列清单同步增加一列 |
| Redis | `to_dict()` JSON 自动包含，无需改键布局；不新增索引 |
| 索引 | v1.5 不加 `idx_tasks_group`（避免每次写入多维护一个索引）；v1.4 操作台需要按分组筛选时再评估 |

`task.created` 事件载荷来自 `task.to_dict()`，因此会多出 `task_group` 字段——属于可选增量字段，事件契约 `schema_version` 保持 `1`。按照 roadmap 的文档同步要求，**先改 [neotask-execution-contract.md](./neotask-execution-contract.md) 任务 JSON 字段表，再实现**。

### 4.2 准入门控（GroupGate）

```python
class GroupGate:
    """进程内分组准入：并发计数 + 令牌桶，所有判断在同一把线程锁内完成"""

    def try_acquire(self, group: str, now: Optional[float] = None) -> bool: ...
    async def acquire(self, group: str) -> None: ...   # 仅作无法重新入队时的回退
    def release(self, group: str) -> None: ...
    def snapshot(self) -> Dict[str, Dict[str, Any]]: ...  # running / available_tokens
```

- `try_acquire` 非阻塞：
  1. 按单调时钟 `time.monotonic()` 补充令牌：`tokens = min(burst, tokens + elapsed * rate / per_seconds)`；
  2. 若配置了 `max_concurrency` 且 `running >= max_concurrency` → 返回 `False`；
  3. 若配置了 `rate` 且 `tokens < 1` → 返回 `False`；
  4. 通过：`running += 1`，`tokens -= 1`（未配置的维度不参与）。
- `release` 只做 `running = max(0, running - 1)`，**不退还令牌**（令牌代表启动次数）。
- 没有任何限流配置的分组不建档；`group=None` 或未命中的分组在 worker 里只做一次字典查找。
- 锁内不做任何 `await`；与 `WorkerPool` 现有 `_lock` 风格一致（线程锁 + 协程内短临界区）。
- `acquire()` 回退路径使用每分组 `asyncio.Condition` 等待 `release` 通知，仅在重新入队失败时使用（见 4.3）。

### 4.3 执行路径改造

准入点：`worker/pool.py:415` `_execute_task` 内，**状态检查通过之后、获取分布式锁之前**。

```python
async def _execute_task(self, worker_id: int, task_id: str) -> None:
    async with self._semaphore:
        task = await self._lifecycle.get_task(task_id)
        ...
        if task.status != TaskStatus.PENDING:
            return

        group = task.group
        permit = False
        if group and self._group_gate.has_limit(group):
            if self._group_gate.try_acquire(group):
                permit = True
            else:
                requeued = await self._queue.push(
                    task_id, task.priority.value,
                    delay=self._defer_interval,
                )
                if requeued:
                    self._group_gate.record_deferral(group)
                    return              # 释放信号量槽位，任务保持 PENDING
                # 队列已满/禁用：退化为阻塞等待，避免任务卡在队列外
                await self._group_gate.acquire(group)
                permit = True

        try:
            # ===== 现有逻辑：分布式锁、看门狗、执行、重试 =====
            ...
        finally:
            if permit:
                self._group_gate.release(group)
            # 现有 worker 统计与锁释放
```

设计要点：

1. **非阻塞准入 + 短延迟重新入队**：worker 不会停在饱和分组上等待，其他分组仍能获得执行机会。代价是同优先级内顺序可能被打乱（重新入队的任务带 `rate_limit_defer_interval` 延迟）。
2. **准入门控放在全局信号量内**：重新入队只做一次存储写入（毫秒级），随后立即返回并释放槽位。worker 数 = 信号量大小，不会出现「持有全局名额空等」。
3. **不写状态、不写 `retry_count`**：延迟期间任务仍是 `PENDING`；`queue_wait_ms` 包含这段等待（符合排队耗时定义）。
4. **重试**：自动重试保留原有的 `retry_delay × retry_count` 延迟；重新执行时再次准入并消耗令牌。限流等待本身不会触发重试。
5. **等待/唤醒**：`TaskSuspended` 分支在 `finally` 中释放并发名额；signal 唤醒后按普通 `PENDING` 重新准入并消耗一个令牌。
6. **超时**：`task_timeout` 包在 `_execute_with_progress` 外层（`worker/pool.py:683`），准入与延迟等待不计入。
7. **取消**：延迟期间取消只改存储状态；任务再次出队时因非 `PENDING` 被跳过，不会泄漏名额（延迟路径未持名额）。
8. **停机**：延迟中的任务仍在队列/存储中，重启后继续；门控无后台任务，无需单独停止。

### 4.4 与现有机制的交互

| 机制 | 行为 |
| :--- | :--- |
| 预取（prefetch） | 预取器只是把任务 ID 搬到本地队列，准入仍在执行前判断；无变化 |
| 优先级 | 先按优先级出队，再判定分组；同一分组内仍按优先级竞争 |
| 分布式锁 | 准入在锁之前；未准入的任务不会占用 `task:{id}` 锁 |
| 回收器 | 延迟中的任务是 PENDING，超时回收沿用任务 `ttl` / 回收器逻辑 |
| 手动重试 API | 重试把任务置回 PENDING 并入队，重新准入 |
| 批量提交 | 整批同一 group，逐条执行时分别准入 |

### 4.5 可观测性

- `PeriodicTaskManager` 风格的统计：`WorkerPool.get_stats()` 增加 `group_running: {group: int}`。
- `get_metrics()` 快照增加：
  - `group_running: Dict[str, int]`（当前各分组执行中数量）
  - `group_deferrals_total: int`（累计因限流重新入队次数）
- v1.4 的 Prometheus 导出器追加：
  - `neotask_group_running{group="llm"}`（gauge）
  - `neotask_group_deferrals_total{group="llm"}`（counter）
- 日志：首次出现某分组的延迟时 `debug` 记录；不逐次刷屏。

---

## 五、多进程 / 多节点边界

v1.5 的门控是**进程内**实现。使用 Redis 共享队列并启动 N 个 worker 进程时，集群合计上限 = `N × 单进程上限`。

- 文档与示例必须明确写出这一限制，避免用户误以为配置的是集群级配额。
- 单机嵌入（Memory / SQLite，单进程）是主要场景，进程内限制即最终上限。
- 集群级配额作为后续独立设计，接口预留：

```python
class GroupGateBackend(ABC):
    async def try_acquire(self, group: str, task_id: str) -> bool: ...
    async def release(self, group: str, task_id: str) -> None: ...
```

Redis 实现草案（1.6 评估，不在本期实施）：

- 键：`neotask:rl:{group}:tokens`（字符串浮点）、`neotask:rl:{group}:ts`（上次补充时刻）、`neotask:rl:{group}:leases`（ZSET，member = task_id，score = 租约到期时刻）。
- 获取：单条 Lua 内「补充令牌 → 清理过期租约 → 校验 `ZCARD(leases) < max_concurrency` 与令牌数 → `ZADD` 租约 → 扣减令牌」。
- 释放：`ZREM` 租约（校验 owner），不退还令牌。
- 崩溃回收：租约按 `lease_ttl ≥ task_timeout + 心跳余量` 过期后被清理；`task_timeout` 很长时由看门狗续租。
- Redis 不可用：fail-open（本进程放行）并累加 `group_backend_errors`，避免整条队列停摆；此偏差需写入文档。

---

## 六、方案比较

| 方案 | 说明 | 结论 |
| :--- | :--- | :--- |
| A. 分组独立队列 + 分组 worker | Celery queue 风格 | 需要新增队列维度与配置面，worker 在分组间动态分配问题大 | 超出本期范围 |
| B. worker 阻塞等待名额 | 实现最简单 | 饱和分组的任务会占住 worker，饿死其他分组 | 否决 |
| C. 非阻塞准入 + 短延迟重新入队（**采纳**） | worker 保持流动，任务保持 PENDING | 多一次存储写入与轻微乱序 | 采纳 |
| D. 仅在提交侧限流 | 提交时排队/拒绝 | 无法约束执行并发，重试与唤醒绕过 | 否决 |

---

## 七、测试计划

| 用例 | 断言 |
| :--- | :--- |
| 并发上限 | 10 个任务、`max_concurrency=2`、handler 阻塞在事件上：任意时刻执行中 ≤ 2；释放一个后下一个在 1s 内启动（有界轮询） |
| 速率限制 | `rate=2, per_seconds=0.2`、10 个短任务：启动时间戳两两间隔 ≥ 窗口（保留 20% 抖动容差），总耗时符合桶容量 |
| 双维度 | 并发与速率同时满足才启动；令牌不足时不占用并发名额 |
| 延迟不升级重试 | 延迟任务 `retry_count == 0`、状态始终 PENDING、最终成功 |
| 取消 | 延迟期间取消：任务不执行、`group_running` 归零、无名额泄漏 |
| 等待/信号 | 挂起时释放名额（其他同组任务可进入）；signal 唤醒后重新准入 |
| 超时口径 | 准入等待 0.5s + 执行 0.1s、`task_timeout=0.3` → 不超时 |
| 未知分组 | 默认不限流且只告警一次；`strict_groups=True` 提交即抛 `UnknownGroupError` |
| 回退路径 | 队列满时 `push` 失败 → 阻塞获取名额并执行，不丢任务 |
| API 透传 | pool / scheduler（delayed/interval/cron）/ engine / batch 的 group 均落库并可查询 |
| 持久化 | Memory / SQLite / Redis 往返后 `task_group` 一致；老 SQLite 库启动自动补列，旧行读出 `None` |
| 指标 | `group_running`、`group_deferrals_total` 计数正确；无分组任务不触发门控 |
| 回归 | 未配置 `group_limits` 时全量套件与基准无行为/吞吐变化（空字典快速路径） |

时序类断言遵守项目既有约定：不写固定 `sleep` 断言，使用有界轮询；吞吐相关不放进全量套件。

---

## 八、验收标准

- 单进程内并发与速率上限严格不超限（并发用「任意时刻计数」断言，速率用启动时间戳窗口断言）。
- 未配置分组的现有用户代码零改动、零行为变化，性能无可见回退。
- 三后端 `group` 往返一致；SQLite 老库自动补列；事件载荷与契约文档一致。
- 挂起、取消、重试、超时、批量、周期任务的交互语义均有对应用例。
- 文档明确「进程内限制、集群合计 = N × 上限」，示例可运行。

---

## 九、非目标

- 不做跨进程/跨节点的精确配额（接口预留，1.6 评估）。
- 不做分组级暂停/恢复、动态修改限流参数（运行中）、分组队列深度配额。
- 不做分组内抢占、加权公平或优先级老化。
- 不引入独立队列后端、不改线协议状态机与事件类型。

---

## 十、实施拆分

1. 执行契约任务 JSON 字段表补 `task_group`；`Task` 字段与三后端持久化 + 往返测试。
2. `GroupLimit` / `GroupGate`（并发 + 令牌桶）与配置校验 + 单测。
3. `WorkerPool` 准入、延迟重新入队、回退阻塞、各交互分支 + 单测。
4. 公开 API 透传（pool / scheduler / engine / periodic definition）与指标/日志。
5. `examples/24_group_limits.py`、README / roadmap / CHANGELOG 同步，全量回归与空配置基准抽查。