"""
NeoTask 1.0.5 性能基准套件（E:\\neotask-demo）

口径：
- submit_rate：从第一个 submit 开始，到所有 submit 协程返回为止；包含任务创建、
  持久化与入队，不代表任务执行完成。
- complete_rate：从第一个 submit 开始，到第 N 个 task.completed 事件为止；包含
  排队、执行、生命周期落库与事件通知。事件时间戳由事件总线处理器记录。
- latency：单任务 submit 前 -> 该任务 task.completed 事件，包含排队等待。
- 所有吞吐均以实际壁钟为分母；不写死名义时长。
- 每个场景独立建池、预热，重复若干轮取中位数。
"""

import asyncio
import json
import logging
import math
import statistics
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from neotask import TaskPool
from neotask.models.config import TaskPoolConfig
from neotask.queue.priority_queue import PriorityQueue
from neotask.storage.memory import MemoryQueueRepository

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)

REPS = 3
IO_REPS = 2


async def noop(data):
    return {"ok": True}


async def io20(data):
    await asyncio.sleep(0.020)
    return {"ok": True}


async def cpu5000(data):
    x = 0
    for i in range(5000):
        x += i * i
    return {"x": x}


def make_pool(
    executor,
    workers: int = 10,
    storage: str = "memory",
    prefetch: bool = False,
    prefetch_size: int = 1,
    sqlite_path: Optional[Path] = None,
) -> TaskPool:
    cfg = TaskPoolConfig(
        storage_type=storage,
        sqlite_path=str(sqlite_path) if sqlite_path else str(DATA / "bench.db"),
        worker_concurrency=workers,
        prefetch_size=prefetch_size,
        enable_prefetch=prefetch,
        enable_metrics=False,
        enable_health_check=False,
        enable_reporter=False,
        enable_reclaimer=False,
        enable_heartbeat=False,
        enable_dead_letter=False,
        max_retries=0,
    )
    return TaskPool(executor=executor, config=cfg)


@dataclass
class RunResult:
    scenario: str
    n: int
    workers: int
    storage: str
    prefetch: str
    wall_s: float
    submit_rate: float
    complete_rate: float
    wait_rate: Optional[float]
    p50_ms: float
    p95_ms: float
    p99_ms: float
    success: int
    failed: int
    notes: Dict[str, Any] = field(default_factory=dict)


def _percentile(sorted_values: List[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, max(0, int(len(sorted_values) * q) - 1))
    return sorted_values[idx]


async def run_batch(
    pool: TaskPool,
    n: int,
    wait_mode: str = "events",
    timeout: float = 180.0,
) -> Dict[str, Any]:
    """提交 N 个任务并测量提交/完成/等待口径与延迟分布。"""
    completed: Dict[str, float] = {}
    failed: Dict[str, float] = {}
    starts: Dict[str, float] = {}
    done = asyncio.Event()

    async def on_ok(event):
        if event.task_id in starts:
            completed[event.task_id] = time.perf_counter()
        if len(completed) + len(failed) >= n:
            done.set()

    async def on_fail(event):
        if event.task_id in starts:
            failed[event.task_id] = time.perf_counter()
        if len(completed) + len(failed) >= n:
            done.set()

    pool._event_bus.subscribe("task.completed", on_ok)
    pool._event_bus.subscribe("task.failed", on_fail)

    # 预热
    warm = [await pool.submit_async({"warm": i}) for i in range(10)]
    await asyncio.gather(*[pool.wait_for_result_async(t, timeout) for t in warm])
    completed.clear()
    failed.clear()

    t0 = time.perf_counter()
    ids: List[str] = []
    for i in range(n):
        starts[i] = time.perf_counter()
        ids.append(await pool.submit_async({"i": i}))
    for i, task_id in enumerate(ids):
        starts[task_id] = starts.pop(i)
    t_submit = time.perf_counter()

    wait_rate = None
    if wait_mode == "waiters":
        async def one(task_id: str):
            await pool.wait_for_result_async(task_id, timeout)

        await asyncio.gather(*[one(t) for t in ids])
        t_end = time.perf_counter()
        wait_rate = n / (t_end - t0)
    else:
        await asyncio.wait_for(done.wait(), timeout)
        t_end = time.perf_counter()

    latencies = sorted(completed[t] - starts[t] for t in ids if t in completed)
    return {
        "wall_s": t_end - t0,
        "submit_s": t_submit - t0,
        "submit_rate": n / (t_submit - t0),
        "complete_rate": n / (t_end - t0),
        "wait_rate": wait_rate,
        "p50_ms": _percentile(latencies, 0.50) * 1000,
        "p95_ms": _percentile(latencies, 0.95) * 1000,
        "p99_ms": _percentile(latencies, 0.99) * 1000,
        "success": len(completed),
        "failed": len(failed),
    }


async def run_single_latency(pool: TaskPool, n: int, timeout: float = 30.0) -> Dict[str, Any]:
    """无并发积压下的单任务往返延迟（submit -> wait 返回）。"""
    warm = [await pool.submit_async({"warm": i}) for i in range(10)]
    await asyncio.gather(*[pool.wait_for_result_async(t, timeout) for t in warm])
    latencies = []
    t0 = time.perf_counter()
    for i in range(n):
        start = time.perf_counter()
        task_id = await pool.submit_async({"i": i})
        await pool.wait_for_result_async(task_id, timeout)
        latencies.append(time.perf_counter() - start)
    wall = time.perf_counter() - t0
    latencies.sort()
    return {
        "wall_s": wall,
        "submit_rate": n / wall,
        "complete_rate": n / wall,
        "p50_ms": _percentile(latencies, 0.50) * 1000,
        "p95_ms": _percentile(latencies, 0.95) * 1000,
        "p99_ms": _percentile(latencies, 0.99) * 1000,
        "success": n,
        "failed": 0,
    }


async def run_submit_only(pool: TaskPool, n: int) -> Dict[str, Any]:
    """只测生产者提交窗口；提交结束后由 shutdown 排空执行。"""
    warm = [await pool.submit_async({"warm": i}) for i in range(10)]
    await asyncio.gather(*[pool.wait_for_result_async(t, 30) for t in warm])
    t0 = time.perf_counter()
    for i in range(n):
        await pool.submit_async({"i": i})
    wall = time.perf_counter() - t0
    return {
        "wall_s": wall,
        "submit_rate": n / wall,
        "complete_rate": float("nan"),
        "p50_ms": float("nan"),
        "p95_ms": float("nan"),
        "p99_ms": float("nan"),
        "success": n,
        "failed": 0,
    }


async def run_sustained(
    pool: TaskPool,
    seconds: float = 5.0,
    warmup: float = 1.0,
    max_inflight: int = 200,
) -> Dict[str, Any]:
    """有界在途任务的持续提交：先预热 1s，再统计 5s 窗口内的完成速率。"""
    completed_times: List[float] = []
    failed_count = 0
    submitted = 0

    async def on_ok(event):
        completed_times.append(time.perf_counter())

    async def on_fail(event):
        nonlocal failed_count
        failed_count += 1

    pool._event_bus.subscribe("task.completed", on_ok)
    pool._event_bus.subscribe("task.failed", on_fail)

    start = time.perf_counter()
    window_start = start + warmup
    window_end = window_start + seconds

    while time.perf_counter() < window_end:
        inflight = submitted - len(completed_times) - failed_count
        if inflight >= max_inflight:
            await asyncio.sleep(0.0005)
            continue
        await pool.submit_async({"t": submitted})
        submitted += 1

    # 停止生产，等待在途任务排空
    deadline = time.perf_counter() + 60
    while len(completed_times) + failed_count < submitted and time.perf_counter() < deadline:
        await asyncio.sleep(0.005)

    in_window = [t for t in completed_times if window_start <= t <= window_end]
    return {
        "window_rate": len(in_window) / seconds,
        "total_submitted": submitted,
        "success": len(completed_times),
        "failed": failed_count,
        "wall_s": time.perf_counter() - start,
    }


def _run_async(pool: TaskPool, coro):
    return pool._run_coroutine(coro)


def median(values: List[float]) -> float:
    return float(statistics.median(values))


def run_scenario(entry: Dict[str, Any]) -> RunResult:
    scenario = entry["scenario"]
    executor = entry["executor"]
    n = entry["n"]
    workers = entry.get("workers", 10)
    storage = entry.get("storage", "memory")
    prefetch = entry.get("prefetch", False)
    size = entry.get("prefetch_size", 1)
    mode = entry.get("mode", "events")
    reps = entry.get("reps", REPS)
    db = DATA / f"{scenario}.db" if storage == "sqlite" else None

    runs = []
    for _ in range(reps):
        if db:
            for suffix in ("", "-wal", "-shm"):
                Path(str(db) + suffix).unlink(missing_ok=True)
        pool = make_pool(executor, workers, storage, prefetch, size, db)
        pool.start()
        try:
            if mode == "single":
                data = _run_async(pool, run_single_latency(pool, n))
            elif mode == "submit":
                data = _run_async(pool, run_submit_only(pool, n))
            elif mode == "sustained":
                data = _run_async(pool, run_sustained(pool, n, entry.get("warmup", 1.0), entry.get("inflight", 200)))
                data["complete_rate"] = data["window_rate"]
                data["submit_rate"] = data["total_submitted"] / data["wall_s"]
                data["p50_ms"] = data["p95_ms"] = data["p99_ms"] = float("nan")
            else:
                data = _run_async(pool, run_batch(pool, n, wait_mode=mode))
            runs.append(data)
            entry.setdefault("notes", {})["last"] = data
        finally:
            pool.shutdown()
        if entry.get("sleep_between"):
            time.sleep(entry["sleep_between"])

    def med(key: str) -> float:
        vals = [r[key] for r in runs if r.get(key) is not None and not _is_nan(r[key])]
        return median(vals) if vals else float("nan")

    return RunResult(
        scenario=scenario,
        n=n,
        workers=workers,
        storage=storage,
        prefetch=("on" if prefetch else "off") + f"/{size}",
        wall_s=med("wall_s"),
        submit_rate=med("submit_rate"),
        complete_rate=med("complete_rate"),
        wait_rate=med("wait_rate") if mode == "waiters" else None,
        p50_ms=med("p50_ms"),
        p95_ms=med("p95_ms"),
        p99_ms=med("p99_ms"),
        success=int(med("success")),
        failed=int(med("failed")),
        notes={"runs": runs},
    )


def _is_nan(value: Any) -> bool:
    try:
        return value != value
    except Exception:
        return False


def _clean_json(value: Any) -> Any:
    """把 NaN 转成 null，保证输出是严格 JSON（jq 等工具可直接解析）。"""
    if isinstance(value, float):
        return None if math.isnan(value) else value
    if isinstance(value, dict):
        return {key: _clean_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean_json(item) for item in value]
    return value


async def queue_microbenchmark(reps: int = 3) -> Dict[str, Any]:
    results = []
    n = 5000
    for _ in range(reps):
        q = PriorityQueue(MemoryQueueRepository(), max_size=200000)
        t0 = time.perf_counter()
        for i in range(n):
            await q.push(f"t{i}", i % 4)
        t1 = time.perf_counter()
        for _ in range(n):
            await q.pop(1)
        t2 = time.perf_counter()
        push_rate = n / (t1 - t0)
        pop_rate = n / (t2 - t1)

        # 同一存量、同一 1,000 条目分子，对比 pop(1) 与 pop(10)
        q = PriorityQueue(MemoryQueueRepository(), max_size=200000)
        for i in range(n):
            await q.push(f"b{i}", i % 4)
        t0 = time.perf_counter()
        for _ in range(1000):
            await q.pop(1)
        t1 = time.perf_counter()
        single_items = 1000 / (t1 - t0)

        q = PriorityQueue(MemoryQueueRepository(), max_size=200000)
        for i in range(n):
            await q.push(f"c{i}", i % 4)
        t0 = time.perf_counter()
        for _ in range(100):
            await q.pop(10)
        t1 = time.perf_counter()
        batch_items = 1000 / (t1 - t0)
        batch_calls = 100 / (t1 - t0)

        results.append({
            "push_rate": push_rate,
            "pop_rate": pop_rate,
            "single_items": single_items,
            "batch_items": batch_items,
            "batch_calls": batch_calls,
            "speedup": batch_items / single_items,
        })
    keys = results[0].keys()
    return {k: median([r[k] for r in results]) for k in keys}


SCENARIOS = [
    # 提交端：内存、direct、10 workers
    {"scenario": "submit_noop_mem_w10", "executor": noop, "n": 2000, "mode": "submit"},
    # 完成端：内存、direct、10 workers
    {"scenario": "complete_noop_mem_w10", "executor": noop, "n": 3000, "mode": "events"},
    # 无积压单任务往返
    {"scenario": "single_noop_mem_w10", "executor": noop, "n": 200, "mode": "single"},
    # IO 扩展性
    *[
        {"scenario": f"complete_io20_mem_w{w}", "executor": io20, "n": 400, "workers": w,
         "mode": "events", "reps": IO_REPS}
        for w in (1, 2, 4, 8)
    ],
    # CPU 密集任务（异步执行器内同步累加）
    *[
        {"scenario": f"complete_cpu5000_mem_w{w}", "executor": cpu5000, "n": 500, "workers": w,
         "mode": "events", "reps": IO_REPS}
        for w in (1, 4, 10)
    ],
    # 预取 A/B：同任务、同存量、同消费者视角
    {"scenario": "complete_noop_prefetch_off_s1", "executor": noop, "n": 3000,
     "prefetch": False, "prefetch_size": 1, "mode": "events", "reps": IO_REPS},
    {"scenario": "complete_noop_prefetch_on_s20", "executor": noop, "n": 3000,
     "prefetch": True, "prefetch_size": 20, "mode": "events", "reps": IO_REPS},
    {"scenario": "complete_noop_prefetch_on_s100", "executor": noop, "n": 3000,
     "prefetch": True, "prefetch_size": 100, "mode": "events", "reps": IO_REPS},
    # 等待口径：3000 个并发 waiter（对比事件口径）
    {"scenario": "waiters_noop_mem_w10", "executor": noop, "n": 3000, "mode": "waiters", "reps": IO_REPS},
    # 5s 持续负载
    {"scenario": "sustained_noop_mem_w10", "executor": noop, "n": 5.0,
     "mode": "sustained", "warmup": 1.0, "inflight": 200, "reps": IO_REPS},
]


def main():
    logging.getLogger("NeoTask").setLevel(logging.WARNING)
    started = time.perf_counter()
    queue = asyncio.run(queue_microbenchmark())
    results = []
    for entry in SCENARIOS:
        try:
            results.append(run_scenario(entry))
        except Exception as exc:  # 单个场景异常不阻断整轮
            results.append(RunResult(
                scenario=entry["scenario"], n=entry["n"], workers=entry.get("workers", 10),
                storage=entry.get("storage", "memory"),
                prefetch=("on" if entry.get("prefetch") else "off") + f"/{entry.get('prefetch_size', 1)}",
                wall_s=float("nan"), submit_rate=float("nan"), complete_rate=float("nan"),
                wait_rate=None, p50_ms=float("nan"), p95_ms=float("nan"), p99_ms=float("nan"),
                success=0, failed=0, notes={"error": repr(exc)},
            ))
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "python": __import__("sys").version.split()[0],
        "neotask": __import__("neotask").__version__,
        "queue": queue,
        "scenarios": [asdict(r) for r in results],
        "elapsed_s": round(time.perf_counter() - started, 2),
    }
    out = BASE / "results.json"
    out.write_text(
        json.dumps(_clean_json(payload), ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )

    print(f"\nNeoTask {payload['neotask']} benchmark / Python {payload['python']}")
    print(f"queue: push={queue['push_rate']:.0f}/s pop={queue['pop_rate']:.0f}/s "
          f"pop(10)={queue['batch_items']:.0f} items/s vs pop(1)={queue['single_items']:.0f} "
          f"({queue['speedup']:.2f}x)")
    for r in results:
        print(f"{r.scenario:34s} submit={r.submit_rate:8.0f}/s complete={r.complete_rate:8.0f}/s "
              f"p50={r.p50_ms:8.2f} p95={r.p95_ms:8.2f} p99={r.p99_ms:8.2f} "
              f"ok={r.success} fail={r.failed}")
    print(f"elapsed {payload['elapsed_s']}s -> {out}")


if __name__ == "__main__":
    main()
