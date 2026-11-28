# NeoTask v1.5.0 设计文档 — 周期任务跨实例去重（Slot Claim）

**基线版本**：v1.3.1（v1.4.0 操作台未发布，实施顺序在其之后）
**目标版本**：v1.5.0
**状态**：设计定稿，待实施
**范围来源**：2026-11-28 功能讨论结论（尚未写入 [neotask-roadmap.md](./neotask-roadmap.md)，确认后补录）

本文档是周期任务跨实例去重的实现依据。核心结论：**用「slot 确定性 task_id」复用存储层唯一性完成认领（claim），不新增 Redis 键族、不做领导者选举。**

---

## 一、问题与现状

### 1.1 复现条件

同时满足以下三条时，同一个周期任务的同一次计划执行会被提交多次：

1. 两个及以上进程启用 `enable_periodic_manager=True`（默认开启）；
2. 这些进程指向同一份存储（Redis，或同一 SQLite 库文件）；
3. 周期任务定义存在于该存储中（创建后已持久化）。

### 1.2 现状证据

| 位置 | 行为 |
| :--- | :--- |
| `scheduler/periodic.py:475` `_scheduler_loop` | 每个实例各自扫描**本地** `_tasks` 字典 |
| `scheduler/periodic.py:793` `_load_tasks` | 启动时从共享存储加载**全部** ACTIVE/PAUSED 任务 |
| `scheduler/periodic.py:504` 到期判定 | 只比较 `next_run <= now`，无跨实例协调 |
| `scheduler/periodic.py:560` `_submit_once` | 直接 `submit_async` 生成**新的随机 task_id**，随后推进 `next_run` 并保存 |
| `scheduler/periodic.py:843` `_save_task` | 无条件覆盖存储中的实例状态（最后写入者胜） |
| `scheduler/periodic.py:81` | 只有进程内 `threading.Lock`，仅保护本地字典 |
| `worker/pool.py:447` 执行锁 | 锁键是 `task:{task_id}`，而各实例生成了**不同**的 task_id，锁无法去重 |

### 1.3 后果

- 业务副作用重复：每天 9 点的报表/清理/对账被执行 N 次（N = 调度实例数）。
- 状态互相覆盖：两个实例各自 `run_count += 1` 后覆盖保存，`max_runs` 可能被提前耗尽或实际执行翻倍。
- 执行记录翻倍，`success_count` / `failed_count` 口径失真。
- 错过补跑（`CATCH_UP`）会成倍放大：每个实例各自补跑一次。

---

## 二、语义定义

### 2.1 术语

- **slot（计划槽位）**：周期任务某一次计划执行，由「周期任务 ID + 该次 `next_run` 时刻」唯一确定。
- **claim（认领）**：某个调度实例取得该 slot 的提交权。一个 slot 最多被一个实例认领。
- **提交（submit）**：调用 `TaskPool.submit_async` 创建一条执行任务并入队。

### 2.2 保证级别

| 保证 | 是否提供 | 说明 |
| :--- | :--- | :--- |
| 同一 slot 集群内最多提交一次 | ✅ | 本设计目标 |
| 同一 slot 恰好执行一次 | ❌ | 沿用现有至少一次语义：worker/存储故障下的重试、回收、检查点重放不变 |
| 周期任务不重叠执行 | ❌ | 上一 slot 未完成时，下一 slot 仍会入队；`no-overlap` 属后续独立开关 |
| 补跑策略（`SKIP` / `RUN_ONCE` / `CATCH_UP`）语义不变 | ✅ | 各实例仍按同一策略判断；`CATCH_UP` 的每次补跑占独立 slot |

> 保证的是「不重复提交」。执行侧仍由既有分布式锁 `task:{task_id}` 兜底：同一 task_id 不会被两个节点并发执行。

---

## 三、方案比较

| 方案 | 机制 | 优点 | 代价 | 结论 |
| :--- | :--- | :--- | :--- | :--- |
| A. 领导者选举 | 新增租约锁，仅 leader 扫描周期任务 | 语义直观，单点调度 | 需处理租约续期、故障切换窗口、脑裂；本质是控制面职责；与「去中心化」定位冲突 | 否决 |
| B. Redis claim 键 | `SET neotask:periodic_claim:{id}:{slot} NX PX ttl` | task_id 格式保持不变 | 仅 Redis 有集群级原子性；新增键族与 TTL 调参；崩溃窗口仍需状态收敛；SQLite 多进程无收益 | 备选（附录 A） |
| C. **slot 确定性 task_id** | 执行任务的 task_id 由 `(周期任务 ID, slot)` 确定，靠存储层唯一键去重 | 三后端统一生效；无需新键族/TTL；重启天然幂等；复用 `save_if_absent` 原子语义 | 周期执行任务的 task_id 格式变化（公开可见） | **采纳** |

### 3.1 选择 C 的依据

三个后端的「创建任务」已经是原子查重写入：

- Memory：`storage/memory.py:76-82` `save_if_absent` 在同一把锁内查重 + 写入。
- SQLite：`storage/sqlite.py:356-370` `INSERT OR IGNORE`；提交快路径 `save_if_absent_and_enqueue` 位于同一事务。
- Redis：`storage/redis.py:251-271` 单条 Lua 完成「查重 + SET + 状态索引」。

把「谁认领了这个 slot」编码进 task_id，认领竞争直接变成一次主键唯一性竞争，**不需要额外状态机**。崩溃恢复也因此幂等：重启后对同一 slot 再提交一次，只会得到 `TaskAlreadyExistsError`，不会产生第二条任务。

task_id 格式变化是已知成本：文档中 `PRD_{timestamp}_{random6}` 的描述需更新，CHANGELOG 需注明「周期执行任务 ID 改为确定性格式，请勿解析 ID」。这是 minor 版本可接受的行为变更，不涉及线协议字段。

---

## 四、详细设计

### 4.1 slot 计算

```python
def _slot_key(next_run: datetime) -> str:
    """把计划时刻编码为 slot 片段（微秒精度，避免亚秒间隔碰撞）"""
    return next_run.strftime("%Y%m%d%H%M%S%f")
```

- 使用 `next_run`（本实例即将执行的那一次计划时刻），而不是 `datetime.now()`：同一 slot 在所有实例上取值一致。
- 亚秒间隔（如 `interval_seconds=0.5`）需要微秒精度，否则同秒内两个 slot 会碰撞。
- `next_run` 目前是 **naive 本地时间**（`datetime.now()`），因此本设计的前提是：**调度实例处于同一时区、时钟偏差小于扫描容忍窗口**。跨时区 Cron 属 roadmap「Cron 时区」独立项，不在本设计范围。

### 4.2 执行任务 task_id

```
PRD_{periodic_task_id}_{slot}
```

示例：

```
periodic_task_id = periodic_20261128083000_ab12cd34
slot             = 20261129090000000000
task_id          = PRD_periodic_20261128083000_ab12cd34_20261129090000000000
```

- 保留 `PRD_` 前缀，与既有周期任务命名空间一致，便于区分普通任务。
- ID 长度增加（约 50–60 字符），SQLite `TEXT` / Redis key 均无长度问题；操作台展示需保证不截断关键段（v1.4 已用等宽表格）。
- 同周期的不同 slot、不同周期任务的同一时刻，ID 均不同。

### 4.3 提交流程

```python
async def _submit_once(self, task_id, instance) -> bool:
    if definition.max_runs and instance.run_count >= definition.max_runs:
        return False

    slot = _slot_key(instance.next_run or datetime.now())
    exec_task_id = f"PRD_{task_id}_{slot}"

    try:
        task_instance_id = await self._task_pool.submit_async(
            data=definition.task_data,
            task_id=exec_task_id,          # ← 认领令牌
            priority=definition.priority,
            ttl=definition.ttl,
        )
    except TaskAlreadyExistsError:
        # 该 slot 已被其它实例认领：收编其状态，不提交、不记失败
        self._stats["claimed_elsewhere"] += 1
        await self._adopt_claimed_slot(task_id, slot)
        return True
    except Exception as e:
        # 现有语义：提交失败计入失败、推进 next_run，本 slot 不再重试
        instance.run_count += 1
        instance.failed_count += 1
        instance.last_run = datetime.now()
        instance.last_error = str(e)
        instance.next_run = self._calculate_next_run(definition, after=datetime.now())
        await self._save_task(instance)
        return True

    instance.run_count += 1
    instance.last_run = datetime.now()
    ...
    instance.next_run = self._calculate_next_run(definition, after=datetime.now())
    await self._save_task(instance)
    return True
```

要点：

1. `run_count` 只在三种确定分支里各递增一次：提交成功、提交失败（沿用现有「失败也占 max_runs」语义）、认领后本地推进。认领冲突路径**不以本地计数为准**，直接收编存储态（4.4）。
2. slot 必须在推进 `next_run` **之前**计算，保证与竞争方一致。
3. `TaskAlreadyExistsError` 必须单独捕获——现有实现把所有异常都记成 `failed_count`，会把正常的认领竞争误报为失败，且会把 loser 的 `run_count` 再叠加一次。
4. 认领失败不写 `PeriodicExecutionRecord`，避免双记录；记录由 winning 实例写入。

### 4.4 状态收敛（认领失败方）

认领失败后，本地实例状态已经陈旧（winner 可能已推进 `next_run`、递增 `run_count`）。**不得用本地旧值覆盖存储**——这是当前重复执行的放大点之一（`_save_task` 无条件覆盖）。

```python
async def _adopt_claimed_slot(self, task_id: str, slot: str) -> None:
    stored = await self._storage.load_task(task_id) if self._storage else None
    adopted = self._deserialize_instance(stored) if stored else None

    # winner 已把 next_run 推进到本 slot 之后：完全以存储为准收编
    if adopted and adopted.task_id == task_id and adopted.next_run \
            and _slot_key(adopted.next_run) > slot:
        with self._lock:
            self._tasks[task_id] = adopted
        self._update_stats()
        return

    # 存储仍是旧状态（winner 在提交后、保存前崩溃）：以存储中的 run_count 为
    # 基准推进一次；多个失败方得到同一个结果，重复保存是幂等的
    with self._lock:
        instance = self._tasks.get(task_id)
    if instance is None:
        return
    base_run_count = adopted.run_count if adopted and adopted.task_id == task_id \
        else instance.run_count
    instance.run_count = base_run_count + 1
    instance.last_run = datetime.now()
    instance.next_run = self._calculate_next_run(instance.definition, after=datetime.now())
    instance.updated_at = datetime.now()
    await self._save_task(instance)
```

收编后需同步本地执行记录缓存与统计（`_update_stats()`），不新增执行记录。

### 4.5 崩溃恢复时序

| 崩溃点 | 重启后的行为 | 结果 |
| :--- | :--- | :--- |
| 计算 slot 前 | `next_run` 仍在过去，重新执行本流程 | 正常提交一次 |
| 提交成功、实例状态未保存 | 重新提交同一 task_id → 冲突 → 收编存储态 | 不重复提交；已入队任务照常执行一次 |
| 提交成功、实例状态已保存 | `next_run` 已推进 | 正常进入下一 slot |
| winner 提交后崩溃，存储 old 状态 | 失败方按 4.4 本地推进并保存 | `next_run` 前进，不重复执行 |
| 认领成功但队列已满（提交抛 `QueueFull`） | 现有语义：计入失败并推进 next_run | 本 slot 记为失败，需业务侧监控（见 4.6） |

执行侧兜底：即使同一 task_id 在队列中出现两次（例如队列持久化重放），`worker/pool.py:447` 的 `task:{task_id}` 分布式锁保证只有一个节点并发执行；状态非 PENDING 的副本会被跳过。

### 4.6 失败与降级

- **只把 `TaskAlreadyExistsError` 视为「已认领」**。其它异常（存储不可用、队列满）保持现有语义：计入 `failed_count`、推进 `next_run`，并新增明确日志区分。
- 存储不可用时不引入新的降级路径（与现状一致：保存失败只记 debug，不影响进程）。
- 混合版本集群：旧版本实例不生成确定性 ID，仍会重复提交。**升级说明必须要求所有启用周期管理的实例一起升级**；升级期间重复执行无法避免。

### 4.7 可观测性

`PeriodicTaskManager.get_stats()` 增加：

| 字段 | 含义 |
| :--- | :--- |
| `claimed_elsewhere` | 本实例遇到「slot 已被认领」的累计次数（集群去重生效的直接证据） |
| `slot_conflicts` | 同义计数（保留一个即可，实施时二选一） |
| `last_conflict_at` | 最近一次认领冲突时间（ISO 字符串） |

- 认领冲突只打 `debug` 日志（避免高频告警），字段带 `task_id` / `slot` / `node_id`。
- 不新增事件类型：1.x 事件契约保持冻结；若后续需要 `periodic.slot_claimed` 事件，放 v1.6 重新评估。

### 4.8 时区与时钟

- slot 是 `next_run` 的字符串编码，不含时区信息；实例间时区不同时，`next_run` 本身已经不一致，slot 也随之不同（这是现有 Cron 的既有限制）。
- 时钟偏差导致某实例提前判定到期时，同一 slot 仍会算出同一 ID，因此**只多算一次调度判断，不会重复提交**。
- 时钟回拨可能导致 `next_run` 重复落到同一 slot；此时认领冲突会把它当作「已认领」，等价于该 slot 被去重——语义上可接受，文档中注明。

---

## 五、测试计划

| 用例 | 方式 |
| :--- | :--- |
| 双实例同 slot 只提交一次 | 两个 `PeriodicTaskManager` 共享同一 Redis 存储，把 `next_run` 设为过去，手动触发各自扫描；断言任务表仅 1 条、`claimed_elsewhere == 1` |
| 认领失败方状态收敛 | winner 已保存 / 未保存两种时序；断言 loser 的 `run_count` 不翻倍、`next_run` 与存储一致 |
| 崩溃恢复幂等 | 在 `_save_task` 注入异常模拟崩溃；重启后再次扫描，断言不产生第二条执行任务、`next_run` 前进 |
| catch-up | `CATCH_UP` 下补跑 N 次，各 slot ID 不同；同 slot 只提交一次 |
| skip 路径 | `SKIP`/`IGNORE` 遇到错过槽位时不提交，只推进 `next_run`；双实例并发保存结果一致 |
| 亚秒间隔 | `interval_seconds=0.5` 连续两个 slot ID 不碰撞 |
| memory / sqlite 回归 | 单实例行为不变（提交次数、执行记录、`max_runs`） |
| SQLite 双进程（可选） | 同一库文件两个进程，依赖主键唯一性去重 |
| 全量套件 | 本地 3.11 venv 跑全量；Redis 测试库沿用 db2/3/4，结束清理 `neotask:*` 自建键 |

---

## 六、验收标准

- Redis 双实例场景：同一 slot 只提交一次，`claimed_elsewhere ≥ 1`，`run_count` 与执行记录不翻倍。
- 提交成功后、状态保存前崩溃：重启不产生重复执行；`next_run` 前进到未来。
- Memory / SQLite 单实例行为与 v1.3.1 一致（用例无回归）。
- `max_runs` 不被重复计数提前耗尽，也不因去重而少执行非冲突 slot。
- 无新增 Redis 键族；无事件契约变更；文档与 CHANGELOG 注明 task_id 格式变化及混合版本升级约束。

---

## 七、非目标

- 不提供「恰好一次」执行保证；执行失败重试、回收、检查点语义不变。
- 不做周期任务不重叠（`no_overlap`）与并发上限。
- 不做跨时区 Cron、时钟同步、领导者选举。
- 不改变 `missed_policy` 与扫描间隔。

---

## 八、实施拆分

1. `_slot_key` / 执行 task_id 生成 helper + 单测（含亚秒、时区注释）。
2. `_submit_once` 冲突分支与 `_adopt_claimed_slot` 状态收敛 + 单测。
3. `get_stats()` 去重字段、日志与文档/CHANGELOG。
4. Redis 双实例集成用例、崩溃时序用例、全量回归与基准抽查（周期任务不影响热路径，预期吞吐无变化）。

---

## 附录 A：Redis claim 键备选方案

若后续确认「周期任务 ID 格式不可变」是硬约束，可退回方案 B：

```
neotask:periodic_claim:{periodic_task_id}:{slot}
```

- 认领：`SET key <node_id> NX PX <ttl>`，成功才提交。
- TTL 建议 `clamp(2 × 槽位间隔, 3600, 86400)` 秒；覆盖「认领后、保存前崩溃」的重启窗口，同时限制键数量。
- 认领失败后仍需执行 4.4 的状态收敛，逻辑与方案 C 相同。
- 代价：仅 Redis 获得跨实例原子性；SQLite 需新增 `periodic_claims` 表（`INSERT OR IGNORE`）才能获得同等语义；Memory 仍为进程内；多一个键族与 TTL 调参面。
- 因此仅作为 task_id 兼容性无法让步时的回退路径，不作为默认实现。