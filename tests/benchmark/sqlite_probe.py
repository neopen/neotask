"""SQLite 后端低并发探针。

目的：给出 SQLite 后端在“单任务串行提交”和“突发提交”两种口径下的
可复现数据，并如实记录 Windows 下的 database is locked 现象。

用法：python bench/sqlite_probe.py
输出：bench/sqlite_results.json
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List

from neotask import TaskPool
from neotask.models.config import TaskPoolConfig

logging.getLogger("NeoTask").setLevel(logging.CRITICAL)

DATA = Path(__file__).parent / "data"
DATA.mkdir(parents=True, exist_ok=True)


async def noop(data):
    return {"ok": True}


def make_pool(workers: int, db: Path) -> TaskPool:
    cfg = TaskPoolConfig(
        storage_type="sqlite",
        sqlite_path=str(db),
        worker_concurrency=workers,
        prefetch_size=1,
        enable_prefetch=False,
        enable_metrics=False,
        enable_health_check=False,
        enable_reporter=False,
        enable_reclaimer=False,
        enable_heartbeat=False,
        enable_dead_letter=False,
        max_retries=0,
    )
    return TaskPool(executor=noop, config=cfg)


def reset_db(db: Path) -> None:
    for p in (db, Path(str(db) + "-wal"), Path(str(db) + "-shm")):
        p.unlink(missing_ok=True)


async def serial_run(pool: TaskPool, n: int, deadline_s: float = 120.0) -> Dict[str, Any]:
    """逐条提交并等待结果：用于测量稳定的单任务延迟。"""
    latencies: List[float] = []
    failed = 0
    start = time.perf_counter()
    for i in range(n):
        t0 = time.perf_counter()
        task_id = await pool.submit_async({"i": i})
        try:
            await asyncio.wait_for(
                pool.wait_for_result_async(task_id, timeout=30), timeout=30
            )
            latencies.append((time.perf_counter() - t0) * 1000)
        except Exception:
            failed += 1
        if time.perf_counter() - start > deadline_s:
            break
    wall = time.perf_counter() - start
    latencies.sort()
    return {
        "mode": "serial",
        "submitted": len(latencies) + failed,
        "success": len(latencies),
        "failed": failed,
        "wall_s": round(wall, 3),
        "rate": round(len(latencies) / wall, 2) if wall else 0,
        "p50_ms": round(latencies[len(latencies) // 2], 2) if latencies else None,
        "p95_ms": round(latencies[int(len(latencies) * 0.95)], 2) if latencies else None,
    }


async def burst_run(pool: TaskPool, n: int, deadline_s: float = 60.0) -> Dict[str, Any]:
    """一次性提交 n 个任务，统计完成率与锁错误。"""
    errors: List[str] = []
    completed = 0

    async def on_ok(event):
        nonlocal completed
        completed += 1

    async def on_fail(event):
        nonlocal completed
        completed += 1

    pool._event_bus.subscribe("task.completed", on_ok)
    pool._event_bus.subscribe("task.failed", on_fail)

    t0 = time.perf_counter()
    submitted = 0
    try:
        for i in range(n):
            await pool.submit_async({"i": i})
            submitted += 1
    except Exception as exc:  # noqa: BLE001 - 探针需要记录原始异常
        errors.append(f"{type(exc).__name__}: {exc}")

    deadline = time.perf_counter() + deadline_s
    while completed < submitted and time.perf_counter() < deadline:
        await asyncio.sleep(0.05)

    wall = time.perf_counter() - t0
    return {
        "mode": "burst",
        "submitted": submitted,
        "completed": completed,
        "wall_s": round(wall, 3),
        "rate": round(completed / wall, 2) if wall else 0,
        "errors": errors[:3],
    }


def run_case(workers: int, n: int, mode: str, tag: str) -> Dict[str, Any]:
    # 每个用例独立库文件，避免上一轮连接未完全释放时干扰下一轮
    db = DATA / f"sqlite_{tag}_w{workers}.db"
    reset_db(db)
    pool = make_pool(workers, db)
    pool.start()
    try:
        runner = serial_run if mode == "serial" else burst_run
        result = pool._run_coroutine(runner(pool, n))
        result.update({"workers": workers, "n": n, "tag": tag})
        return result
    finally:
        pool.shutdown(graceful=True, timeout=10)


def main() -> None:
    cases = [
        (1, 100, "serial"),
        (1, 100, "burst"),
        (2, 100, "burst"),
        (4, 100, "burst"),
    ]
    out: List[Dict[str, Any]] = []
    for idx, (workers, n, mode) in enumerate(cases):
        try:
            out.append(run_case(workers, n, mode, tag=f"{idx}_{mode}"))
            print(out[-1], flush=True)
        except Exception as exc:  # noqa: BLE001 - 保证后续用例继续执行
            out.append(
                {"workers": workers, "n": n, "mode": mode,
                 "error": f"{type(exc).__name__}: {exc}"}
            )
            print(out[-1], flush=True)

    path = Path(__file__).parent / "sqlite_results.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"written: {path}")


if __name__ == "__main__":
    main()
