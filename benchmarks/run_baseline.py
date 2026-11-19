"""
@FileName: run_baseline.py
@Description: v1.2 基准基线 - 单任务提交延迟与批量提交吞吐
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/19
"""

import argparse
import asyncio
import json
import logging
import statistics
import time
from typing import Any, Dict, List, Optional

from neotask.api.task_pool import TaskPool
from neotask.models.config import TaskPoolConfig

DEFAULT_TASKS = 1000
DEFAULT_SINGLE = 200
DEFAULT_ROUNDS = 3
DEFAULT_CONCURRENCY = 8


async def noop_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """只返回数据的空执行器，避免执行耗时污染提交路径测量"""
    return {"index": data.get("index")}


def _new_pool(concurrency: int, queue_size: int) -> TaskPool:
    config = TaskPoolConfig(
        storage_type="memory",
        worker_concurrency=concurrency,
        enable_prefetch=True,
        max_retries=0,
        enable_metrics=False,
        enable_health_check=False,
        enable_reporter=False,
        queue_max_size=queue_size,
    )
    pool = TaskPool(executor=noop_executor, config=config)
    pool.start()
    return pool


def _percentiles(values: List[float]) -> Dict[str, float]:
    ordered = sorted(values)
    if not ordered:
        return {"avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}

    def pick(ratio: float) -> float:
        rank = ratio * (len(ordered) - 1)
        low = int(rank)
        high = min(low + 1, len(ordered) - 1)
        weight = rank - low
        return ordered[low] * (1.0 - weight) + ordered[high] * weight

    return {
        "avg": statistics.mean(ordered),
        "p50": pick(0.50),
        "p95": pick(0.95),
        "p99": pick(0.99),
        "max": ordered[-1],
    }


async def measure_single_submit(count: int) -> Dict[str, Any]:
    """单任务提交延迟：逐条提交并等待完成，分别记录提交与端到端耗时"""
    pool = _new_pool(concurrency=4, queue_size=count + 64)
    try:
        for i in range(min(20, count)):
            task_id = await pool.submit_async({"index": -1 - i})
            await pool.wait_for_result_async(task_id, timeout=30)

        submit_ms: List[float] = []
        end_to_end_ms: List[float] = []
        for i in range(count):
            started = time.perf_counter()
            task_id = await pool.submit_async({"index": i})
            submitted = time.perf_counter()
            await pool.wait_for_result_async(task_id, timeout=30)
            finished = time.perf_counter()
            submit_ms.append((submitted - started) * 1000.0)
            end_to_end_ms.append((finished - started) * 1000.0)

        return {
            "count": count,
            "submit_ms": _percentiles(submit_ms),
            "end_to_end_ms": _percentiles(end_to_end_ms),
        }
    finally:
        pool.shutdown(graceful=True, timeout=10)


async def _submit_one_by_one(pool: TaskPool, tasks: int) -> List[str]:
    task_ids: List[str] = []
    for i in range(tasks):
        task_ids.append(await pool.submit_async({"index": i}))
    return task_ids


async def _submit_batch(pool: TaskPool, tasks: int) -> List[str]:
    payload = [{"index": i} for i in range(tasks)]
    return await pool.submit_many_async(payload)


async def measure_bulk_once(mode: str, tasks: int, concurrency: int) -> float:
    """测一轮批量提交耗时；每次用新池，避免上一轮任务状态影响"""
    pool = _new_pool(concurrency=concurrency, queue_size=tasks + 64)
    try:
        warmup_ids = [await pool.submit_async({"index": -1}) for _ in range(10)]
        await pool.wait_all_async(warmup_ids, timeout=30)

        started = time.perf_counter()
        if mode == "single":
            task_ids = await _submit_one_by_one(pool, tasks)
        else:
            task_ids = await _submit_batch(pool, tasks)
        elapsed = time.perf_counter() - started

        await pool.wait_all_async(task_ids, timeout=60)
        return elapsed
    finally:
        pool.shutdown(graceful=True, timeout=10)


async def measure_batch_submit(
    tasks: int,
    rounds: int,
    concurrency: int,
) -> Dict[str, Any]:
    """交替执行逐条提交与批量提交，取多轮配对加速比的中位数"""
    round_results: List[Dict[str, float]] = []
    for index in range(rounds):
        if index % 2 == 0:
            single_s = await measure_bulk_once("single", tasks, concurrency)
            batch_s = await measure_bulk_once("batch", tasks, concurrency)
        else:
            batch_s = await measure_bulk_once("batch", tasks, concurrency)
            single_s = await measure_bulk_once("single", tasks, concurrency)

        single_ops = tasks / single_s
        batch_ops = tasks / batch_s
        round_results.append(
            {
                "round": index + 1,
                "single_s": single_s,
                "batch_s": batch_s,
                "single_ops": single_ops,
                "batch_ops": batch_ops,
                "speedup": single_s / batch_s if batch_s else 0.0,
            }
        )

    speedups = [item["speedup"] for item in round_results]
    return {
        "tasks_per_round": tasks,
        "rounds": rounds,
        "single_ops_median": statistics.median(item["single_ops"] for item in round_results),
        "batch_ops_median": statistics.median(item["batch_ops"] for item in round_results),
        "speedup_median": statistics.median(speedups),
        "speedup_min": min(speedups),
        "speedup_max": max(speedups),
        "details": round_results,
    }


def _format_latency(values: Dict[str, float]) -> str:
    return (
        f"p50 {values['p50']:.3f} ms / p95 {values['p95']:.3f} ms / "
        f"p99 {values['p99']:.3f} ms / max {values['max']:.3f} ms"
    )


def _print_report(result: Dict[str, Any]) -> None:
    single_submit = result["single_submit"]
    bulk = result["batch_submit"]
    print("NeoTask v1.2 基准基线")
    print("-" * 60)
    print(f"单任务提交延迟（{single_submit['count']} 次）")
    print(f"  提交调用 : {_format_latency(single_submit['submit_ms'])}")
    print(f"  端到端   : {_format_latency(single_submit['end_to_end_ms'])}")
    print("-" * 60)
    print(f"批量提交吞吐（每轮 {bulk['tasks_per_round']} 任务，{bulk['rounds']} 轮交替取中位）")
    print(f"  逐条提交 : {bulk['single_ops_median']:.0f} tasks/s")
    print(f"  批量提交 : {bulk['batch_ops_median']:.0f} tasks/s")
    print(
        f"  配对加速比: 中位 {bulk['speedup_median']:.2f}x"
        f"（{bulk['speedup_min']:.2f}x ~ {bulk['speedup_max']:.2f}x）"
    )
    print("-" * 60)
    print("提示：保持参数与机器一致再做横向对比；memory 存储，仅测提交路径")


async def run(
    tasks: int,
    single: int,
    rounds: int,
    concurrency: int,
) -> Dict[str, Any]:
    single_result = await measure_single_submit(single)
    bulk_result = await measure_batch_submit(tasks, rounds, concurrency)
    return {
        "environment": {
            "storage": "memory",
            "worker_concurrency": concurrency,
            "prefetch": True,
            "tasks_per_round": tasks,
            "single_samples": single,
            "rounds": rounds,
        },
        "single_submit": single_result,
        "batch_submit": bulk_result,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="NeoTask v1.2 基准基线：单任务提交延迟与批量提交吞吐"
    )
    parser.add_argument("--tasks", type=int, default=DEFAULT_TASKS, help="每轮批量提交任务数")
    parser.add_argument("--single", type=int, default=DEFAULT_SINGLE, help="单任务延迟采样数")
    parser.add_argument("--rounds", type=int, default=DEFAULT_ROUNDS, help="批量基准轮数")
    parser.add_argument(
        "--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Worker 并发数"
    )
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    args = parser.parse_args(argv)

    for name, value in (("tasks", args.tasks), ("single", args.single), ("rounds", args.rounds)):
        if value <= 0:
            parser.error(f"--{name} 必须大于 0")
    if args.concurrency <= 0:
        parser.error("--concurrency 必须大于 0")

    logging.getLogger("NeoTask").setLevel(logging.WARNING)
    result = asyncio.run(run(args.tasks, args.single, args.rounds, args.concurrency))

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        _print_report(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
