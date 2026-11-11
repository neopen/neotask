"""性能优化专项探针。

补充主套件未覆盖的两组对照：
1. 关闭预取时，direct 模式的 prefetch_size 对极短任务的影响。
2. 大批量结果回收时，全量并发 waiter 与分片 waiter 的差异。

用法：python bench/optimization_probe.py
输出：bench/optimization_results.json
"""

from __future__ import annotations

import asyncio
import json
import logging
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List

from bench_runner import _percentile, io20, make_pool, noop, run_batch

BASE = Path(__file__).resolve().parent
REPS = 2
WAIT_REPS = 3


async def run_bounded_waiters(
    pool,
    n: int,
    batch_size: int,
    timeout: float = 180.0,
) -> Dict[str, Any]:
    """提交 N 个任务后，按 batch_size 分批等待结果。"""
    completed: Dict[str, float] = {}
    starts: Dict[str, float] = {}

    async def on_ok(event):
        if event.task_id in starts:
            completed[event.task_id] = time.perf_counter()

    pool._event_bus.subscribe("task.completed", on_ok)

    # 预热
    warm = [await pool.submit_async({"warm": i}) for i in range(10)]
    await asyncio.gather(*[pool.wait_for_result_async(t, timeout) for t in warm])
    completed.clear()

    t0 = time.perf_counter()
    ids: List[str] = []
    for i in range(n):
        starts[i] = time.perf_counter()
        ids.append(await pool.submit_async({"i": i}))
    for i, task_id in enumerate(ids):
        starts[task_id] = starts.pop(i)

    for start in range(0, n, batch_size):
        chunk = ids[start:start + batch_size]
        await asyncio.gather(*[pool.wait_for_result_async(t, timeout) for t in chunk])

    wall = time.perf_counter() - t0
    latencies = sorted(completed[t] - starts[t] for t in ids if t in completed)
    return {
        "n": n,
        "batch_size": batch_size,
        "wall_s": round(wall, 3),
        "rate": round(n / wall, 2),
        "p50_ms": round(_percentile(latencies, 0.50) * 1000, 2),
        "p95_ms": round(_percentile(latencies, 0.95) * 1000, 2),
        "completed": len(completed),
    }


def median_runs(runs: List[Dict[str, Any]], keys: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in keys:
        vals = [r[key] for r in runs if isinstance(r.get(key), (int, float))]
        out[key] = round(statistics.median(vals), 2) if vals else None
    return out


def main() -> None:
    logging.getLogger("NeoTask").setLevel(logging.WARNING)
    payload: Dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "prefetch": [],
        "waiting": [],
    }

    # 1) 关闭预取时，direct 模式每次 pop 的数量
    for size in (1, 20, 50):
        runs = []
        for _ in range(REPS):
            pool = make_pool(
                noop, workers=10, prefetch=False, prefetch_size=size
            )
            pool.start()
            try:
                runs.append(pool._run_coroutine(run_batch(pool, 3000)))
            finally:
                pool.shutdown()
        summary = {"prefetch_size": size, "workers": 10, "n": 3000}
        summary.update(
            median_runs(
                runs, ["complete_rate", "p50_ms", "p95_ms", "p99_ms"]
            )
        )
        payload["prefetch"].append(summary)
        print("prefetch", summary, flush=True)

    # 2) IO 任务下，预取开关与批量大小的对照
    for workers in (4, 8):
        for prefetch, size in ((False, 1), (True, 20), (True, 100)):
            runs = []
            for _ in range(REPS):
                pool = make_pool(
                    io20, workers=workers, prefetch=prefetch,
                    prefetch_size=size,
                )
                pool.start()
                try:
                    runs.append(pool._run_coroutine(run_batch(pool, 400)))
                finally:
                    pool.shutdown()
            summary = {
                "workers": workers,
                "n": 400,
                "prefetch": "on" if prefetch else "off",
                "prefetch_size": size,
            }
            summary.update(
                median_runs(
                    runs, ["complete_rate", "p50_ms", "p95_ms"]
                )
            )
            payload.setdefault("io_prefetch", []).append(summary)
            print("io", summary, flush=True)

    # 3) 全量 waiter 与分片 waiter
    for batch_size in (3000, 500, 100, 50):
        runs = []
        for _ in range(WAIT_REPS):
            pool = make_pool(noop, workers=10, prefetch=False, prefetch_size=1)
            pool.start()
            try:
                runs.append(
                    pool._run_coroutine(
                        run_bounded_waiters(pool, 3000, batch_size)
                    )
                )
            finally:
                pool.shutdown()
        summary = {"batch_size": batch_size, "workers": 10, "n": 3000}
        summary.update(median_runs(runs, ["rate", "p50_ms", "p95_ms"]))
        summary["runs"] = runs
        payload["waiting"].append(summary)
        print("waiting", summary, flush=True)

    out = BASE / "optimization_results.json"
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"written: {out}")


if __name__ == "__main__":
    main()
