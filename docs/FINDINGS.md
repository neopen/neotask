# NeoTask 测试问题与不足总结

> 被测对象：`neotask==1.0.2`（PyPI，纯 Python 异步任务队列）
> 测试环境：Windows 22H2 / Python 3.13.15 / aiosqlite 0.22.1 / croniter 6.2.4
> 测试范围：9 个模块、30 个用例（8 个功能单测 + 1 个端到端集成流程），全部通过
> 说明：本文只记录**在本地实测中复现到的现象**，不含推测；每条附复现用例或源码位置与严重度。

---

## 一、总体结论

核心能力可用且稳定：立即任务、优先级、并发、批量、重试、SQLite/Memory 持久化、事件回调、
延迟/周期/Cron/定点调度、超时保护、统计、以及**「已入队任务跨重启自动恢复」**均按预期工作。

但在**关闭/崩溃恢复的健壮性**、**读写状态一致性**、**可配置性**与**官方文档准确性**四方面
发现若干问题与不足，逐条如下。

---

## 二、健壮性 / 并发缺陷（建议优先处理）

### 2.1 强制关闭时在途写库触发未捕获异常 【中等】

- **现象**：`shutdown(graceful=False)` 时若仍有 worker 正在执行任务，会出现
  `sqlite3.ProgrammingError: Cannot operate on a closed database` 及后续
  `ValueError: no active connection`，并伴随 `Task exception was never retrieved`。
- **复现**：`test_09_e2e_flows.py::test_submit_interrupt_restart_recover`
  （提交后 0.3s 即 `graceful=False`，被中断的在途任务在完成写库时撞上已关闭的连接）。
- **影响**：异常抛在后台 event loop，日志噪声大；该次任务的结果/失败状态未能落库。
- **建议**：关闭存储前应先停止/等待 worker 写出，或在 `_execute_task` 对 `save()`
  捕获连接关闭异常并降级告警，而非让 future 异常无人回收。

### 2.2 `graceful=True` 不执行「排队中(pending)」的任务，且会空等满超时 【中等】

- **现象**：`shutdown(graceful=True, timeout=15)` 时，尚未被 worker 取走的 pending 任务
  不会被执行，全部停留 `pending`，而关闭流程仍等待到 `timeout` 才返回（实测耗时约 15.5s）。
- **复现**：集成用例第一版提交 3 个 pending 任务立即优雅关闭，结果 `{'pending': 3}` 且阻塞 15s。
- **影响**：与「优雅关闭=把手头的活儿干完」的直觉不符，易造成关机脚本无谓卡住。
- **建议**：明确文档语义（graceful 仅 drain 在途 running，不含排队），并在无在途任务时
  提前返回，而不是等满超时。

### 2.3 强制关闭遗留的 `running` 孤儿任务恢复慢且间隔不可配 【中等】

- **现象**：崩溃/强关会让正在执行的任务长期停留 `running`；恢复靠 `TaskReclaimer`
  的孤儿回收，但其回收间隔在源码中被写死为 `30s`（`api/task_pool.py`：
  `ReclaimerConfig(interval=30, ...)`），`TaskPoolConfig` 未暴露该参数。
- **影响**：① 生产环境孤儿任务最长需 ~30s+ 才被回收重跑；② 集成/单元测试无法在秒级验证
  孤儿回收链路（本测试因此不对孤儿回收做断言）。
- **建议**：将 reclaimer interval 提升为 `TaskPoolConfig` 可配项（如 `reclaim_interval`）。

### 2.4 未声明 Redis 协议，新版 redis-py 连旧版 Redis 直接失败 【中等】

- **现象**：`redis-py 8.1.0` 默认以 RESP3 握手（发 `HELLO`），对不支持 RESP3 的
  旧 Redis（<6.0）报 `unknown command 'HELLO'`；而 neotask 内部
  `redis.asyncio.ConnectionPool.from_url(...)` 多处均**未传 `protocol`**（见
  `storage/redis.py`、`lock/redis.py`、`core/heartbeat.py` 等），导致整个 Redis
  后端在旧服务端上无法连接。
- **复现**：本机 `redis://localhost:6379`（旧版）直接初始化 `TaskPool(storage_type="redis")` 即失败；
  手动将 `ConnectionPool.from_url` 默认改为 `protocol=2` 后全部连通（见 `testutil._patch_protocol2`）。
- **影响**：旧 Redis 环境下 neotask 开箱不可用，且错误信息（HELLO）对用户不直观。
- **建议**：建连时允许通过配置传递 `protocol`/连接参数，或默认 `protocol=2` 以扩大兼容面。

---

## 三、状态读写一致性问题

### 3.1 跨重启恢复后 `get_status()` 返回陈旧的 `running` 【中等】

- **现象**：新池实例已把恢复任务执行完毕（executor 已返回），但 `pool.get_status(task_id)`
  仍返回 `running`，而直接查 SQLite 却显示 `success`。
- **复现**：`test_09` 恢复用例最初用 `get_status` 判定终态，`状态=['running','success',...]`
  长时间不翻转；改为直接查库后立刻正确。
- **影响**：`get_status` 与持久层不一致，业务方以 `get_status` 判终态会误判/卡等。
- **建议**：核对 `get_status` 的缓存/读源；跨实例场景应保证从存储读取最新状态。

### 3.2 `running` 中间态未必持久化，轮询难以观测到执行中 【轻微】

- **现象**：SQLite 存储下，一个明显处于执行的 0.8s 任务，`get_status` 轮询只观测到
  `{'pending','success'}`，几乎抓不到 `running`。
- **复现**：`test_09_e2e_flows.py::test_full_lifecycle_single_pool`（因此改用 `get_task` 的
  `started_at` 字段佐证任务确实进入过执行阶段）。
- **影响**：依赖状态轮询做进度展示的场景不可靠。

### 3.3 纯 `create_pending_task()` 的任务重启后不自动执行（与 submit 恢复行为不一致） 【轻微/存疑】

- **现象**：`submit()` 入队后中断的 PENDING 任务，重启会被自动恢复执行；
  但仅 `create_pending_task()`（从未 `enqueue_task`）落库的 PENDING 任务，重启后
  保持 `pending` 达 8s 未被自动执行。
- **复现**：机制探针：池1 `create_pending_task`×6 → 关闭；池2 `start()` 后状态仍 `{'pending':6}`。
- **影响**：两条「产生 PENDING」路径的恢复语义不同且不直观，易被误用。
- **建议**：文档明确「自动恢复只针对已入队任务」；或统一恢复策略对 PENDING 一并重新入队。

### 3.4 周期/定时任务持久化为**未实现存根**，`enable_persistence` 名不副实 【高】

- **现象**：`TaskScheduler` 注册的 interval/cron/at 周期任务，**不会跨重启恢复**。
  即便 `SchedulerConfig(enable_persistence=True, storage_type="sqlite")`，新调度器
  实例的 `get_periodic_tasks()` 仍为空，重启后不再触发。
- **根因（源码直接证据）**：`scheduler/periodic.py` 的 `_load_tasks`（L770）、
  `_save_task`（L790）、`_delete_task`、`_save_execution_record` 内部均为
  `# TODO: 实现…` + `pass`（**从未实现读写存储**）；且 `api/task_scheduler.py:L133`
  构造 `PeriodicTaskManager(task_pool=..., **storage=None**)` 直接写死 None，
  使 `if not self._storage: return` 提前短路。
- **复现**：`test_09_e2e_flows.py::test_scheduled_tasks_not_recovered_across_restart`
  （表征测试，如实固化该行为；若官方日后实现持久化，该用例会失败提醒更新）。
- **影响**：配置项 `enable_persistence` / `storage_type` 对调度器周期任务完全无效，
  与“支持持久化”的预期严重不符；崩溃/重启后所有定时任务静默丢失，需上层自行重建。
- **建议**：要么实现 `_load_tasks/_save_task` 并接入存储，要么移除/置灰 `enable_persistence`
  配置与相关文档描述，避免误导。

---

## 四、可配置性 / 精度限制

### 4.1 周期任务间隔受 `scan_interval` 下限约束 【轻微】

- **现象**：`SchedulerConfig.scan_interval` 默认 1.0s；`submit_interval(interval_seconds<1)`
  会被扫描粒度吞掉导致漏跑（实测 0.4s 间隔只跑 1 次）。
- **复现**：`test_07_scheduler.py::test_interval_task_repeats`（改用 1.0s 间隔后精确跑 3 次）。
- **建议**：文档标注最小有效间隔；或提供更高精度定时（如启用 `enable_time_wheel`）的默认路径。

### 4.2 周期任务活跃期 `shutdown(graceful=True)` 会等满 30s 超时 【轻微】

- **复现**：`test_07` 首版优雅关闭耗时约 30s；改 `shutdown(graceful=False)`/`cancel_periodic` 即时返回。

---

## 五、官方文档与实现不一致（README / Quick Start）

| # | 文档写法 | 实际行为（1.0.2） | 影响 |
|---|---|---|---|
| 1 | Quick Start 直接 `pool.submit(...)` → `wait_for_result(...)`，**未调用 `start()`** | 必须先 `pool.start()`（或 `with` 上下文）否则任务不被处理 | 照抄即失败 |
| 2 | `get_result` 未说明返回结构 | 返回包装字典 `{task_id,status,result,error,created_at,completed_at}`，真结果在 `["result"]`；而 `wait_for_result` 直接返回 executor 值 | 取值取错 |
| 3 | 事件回调示例以 `event.data` 笼统带过 | 事件类型串是 `task.created/started/completed/failed`（非 `created` 等） | 监听器按错 key 过滤会漏事件 |
| 4 | 未提示优雅关闭语义边界 | `graceful=True` 不执行排队 pending（见 2.2） | 关机脚本行为与预期不符 |
| 5 | 未提示恢复/一致性限制 | 见 2.1/3.1/3.3 | 崩溃恢复场景踩坑 |
| 6 | 配置项 `enable_persistence`（及 `storage_type`）暗示周期任务可持久化 | 实为 `# TODO` 存根且 `storage=None`，完全无效（见 3.4） | 定时任务静默丢失 |

---

## 六、其他观察（非缺陷）

- 日志使用独立 `NeoTask` logger，且会在运行目录创建 `logs/` 与默认 `neotask.db`；
  测试项目已用 `.gitignore` 忽略。
- Windows 下 `aiosqlite` 连接在进程内释放有延迟，测试脚本删库需重试/容错（已在用例中处理）。
- Redis 分布式后端已部署本地 Redis 后验证（见 `test_10_distributed.py`）：多节点共享队列
  抢占消费（无丢失/无重复）、节点注册/心跳、跨节点分布式锁互斥均**已测通过**。
- **孤儿任务跨节点回收（reclaim）也已确定性验证**（`test_10::test_orphan_task_reclaim_by_alive_node`）：
  向共享 Redis 注入一条 `status=RUNNING, node_id=幽灵节点` 的任务，存活节点显式调
  `reclaim_now()` 返回 `reason=orphan` 的 `ReclaimResult(success=True)`，任务被重排并由存活节点
  重跑至 `success`。（注：因回收间隔硬编码 30s（见 2.3），靠真实崩溃+心跳超时来验证时序不稳，
  故采用“注入孤儿 + 显式驱动 reclaim_now”的确定性方式，这本身也印证了 2.3 间隔不可配的不便。）
- **仍未纳入验证的分布式链路**：Web UI 未测。


---

## 七、优先级建议

0. 【高】调度器周期任务持久化为未实现存根（3.4）：`enable_persistence` 开箱无效，
   崩溃/重启后定时任务全丢——要么实现，要么从配置/文档下架。
1. 【中】修复强制关闭写库未捕获异常（2.1）与 `get_status` 跨实例/跨节点陈旧（3.1）——影响可靠性与可用性判断。
2. 【中】Redis 建连支持可配 `protocol`（或默认 RESP2），避免旧版 Redis（<6.0）+ 新版 redis-py 环开箱不可用（2.4）。
3. 【中】明确并统一 graceful shutdown / 崩溃恢复语义（2.2、2.3、3.3），补充文档。
4. 【轻】将 reclaimer interval、interval 最小粒度做成可配并在文档标注（2.3、4.1）。
5. 【文档】修订 Quick Start 与 API 参考（第五节 1~5）。

> 以上均为在本地 `1.0.2` 上实测复现；对应可运行复现脚本见同目录各 `test_*.py` 用例。
