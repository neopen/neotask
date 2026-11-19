"""
@FileName: run_baseline.py
@Description: v1.2 基准基线 - 单任务提交延迟、批量提交吞吐与高并发执行
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
DEFAULT_HC_LEVELS = (1, 4, 8, 16, 32, 64)


async def noop_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """只返回数据的空执行器，避免执行耗时污染提交路径测量"""
    return {"index": data.get("index")}


def _new_pool(
    concurrency: int,
    queue_size: int,
    executor: Any = noop_executor,
) -> TaskPool:
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
    pool = TaskPool(executor=executor, config=config)
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


async def _measure_concurrency_once(level: int, tasks: int) -> Dict[str, Any]:
    """测一轮固定并发度下的 1000 任务端到端耗时与执行完成延迟

    延迟在执行器返回时用 ``perf_counter`` 采样，不经事件总线，避免高并发下
    事件积压把完成时间整体后移。
    """
    completed_perf: Dict[int, float] = {}

    async def timed_executor(data: Dict[str, Any]) -> Dict[str, Any]:
        completed_perf[data["index"]] = time.perf_counter()
        return {"index": data["index"]}

    pool = _new_pool(
        concurrency=level,
        queue_size=tasks + 64,
        executor=timed_executor,
    )
    try:
        warmup_ids = [await pool.submit_async({"index": -1 - i}) for i in range(10)]
        await pool.wait_all_async(warmup_ids, timeout=30)
        completed_perf.clear()

        payload = [{"index": i} for i in range(tasks)]
        started = time.perf_counter()
        task_ids = await pool.submit_many_async(payload)
        await pool.wait_all_async(task_ids, timeout=60)
        elapsed = time.perf_counter() - started

        latencies_ms = [(finished - started) * 1000.0 for finished in completed_perf.values()]
        return {
            "elapsed_s": elapsed,
            "latency_samples": len(latencies_ms),
            "latencies_ms": latencies_ms,
        }
    finally:
        pool.shutdown(graceful=True, timeout=10)


async def measure_high_concurrency(
    tasks: int,
    rounds: int,
    levels: List[int],
) -> Dict[str, Any]:
    """按并发度扫描吞吐与延迟；每轮正反交替，取多轮中位数"""
    samples: Dict[int, List[Dict[str, Any]]] = {level: [] for level in levels}
    for index in range(rounds):
        order = list(levels) if index % 2 == 0 else list(reversed(levels))
        for level in order:
            samples[level].append(await _measure_concurrency_once(level, tasks))

    summaries: List[Dict[str, Any]] = []
    for level in levels:
        level_samples = samples[level]
        latency_runs = [_percentiles(item["latencies_ms"]) for item in level_samples]
        summaries.append(
            {
                "concurrency": level,
                "throughput_median": statistics.median(
                    tasks / item["elapsed_s"] for item in level_samples
                ),
                "p50_ms_median": statistics.median(item["p50"] for item in latency_runs),
                "p95_ms_median": statistics.median(item["p95"] for item in latency_runs),
                "latency_samples": min(item["latency_samples"] for item in level_samples),
            }
        )

    baseline = next((item for item in summaries if item["concurrency"] == 1), summaries[0])
    for item in summaries:
        throughput = item["throughput_median"]
        item["speedup_vs_1"] = (
            throughput / baseline["throughput_median"] if baseline["throughput_median"] else 0.0
        )

    best = max(summaries, key=lambda item: item["throughput_median"])
    return {
        "tasks_per_round": tasks,
        "rounds": rounds,
        "levels": summaries,
        "best_concurrency": best["concurrency"],
        "peak_throughput": best["throughput_median"],
    }


def _format_latency(values: Dict[str, float]) -> str:
    return (
        f"p50 {values['p50']:.3f} ms / p95 {values['p95']:.3f} ms / "
        f"p99 {values['p99']:.3f} ms / max {values['max']:.3f} ms"
    )


def _print_report(result: Dict[str, Any]) -> None:
    single_submit = result["single_submit"]
    bulk = result["batch_submit"]
    high_concurrency = result["high_concurrency"]
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
    print(
        f"高并发执行（每轮 {high_concurrency['tasks_per_round']} 任务，"
        f"{high_concurrency['rounds']} 轮正反交替取中位）"
    )
    print(f"  {'并发':>4}  {'吞吐(tasks/s)':>14}  {'p50(ms)':>9}  {'p95(ms)':>9}  {'相对1并发':>9}")
    for item in high_concurrency["levels"]:
        print(
            f"  {item['concurrency']:>4}  {item['throughput_median']:>14.0f}  "
            f"{item['p50_ms_median']:>9.2f}  {item['p95_ms_median']:>9.2f}  "
            f"{item['speedup_vs_1']:>8.2f}x"
        )
    print(
        f"  峰值：{high_concurrency['best_concurrency']} 并发 "
        f"{high_concurrency['peak_throughput']:.0f} tasks/s"
    )
    print("  延迟口径：批提交起点 → 执行器返回（不经事件总线）")
    print("-" * 60)
    print("提示：保持参数与机器一致再做横向对比；memory 存储，仅测提交与调度路径")


async def run(
    tasks: int,
    single: int,
    rounds: int,
    concurrency: int,
    hc_levels: List[int],
) -> Dict[str, Any]:
    single_result = await measure_single_submit(single)
    bulk_result = await measure_batch_submit(tasks, rounds, concurrency)
    high_concurrency = await measure_high_concurrency(tasks, rounds, hc_levels)
    return {
        "environment": {
            "storage": "memory",
            "worker_concurrency": concurrency,
            "prefetch": True,
            "tasks_per_round": tasks,
            "single_samples": single,
            "rounds": rounds,
            "high_concurrency_levels": hc_levels,
        },
        "single_submit": single_result,
        "batch_submit": bulk_result,
        "high_concurrency": high_concurrency,
    }


def _parse_levels(text: str) -> List[int]:
    try:
        levels = [int(part.strip()) for part in text.split(",") if part.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("并发度必须是逗号分隔的整数") from exc
    if not levels or any(level <= 0 for level in levels):
        raise argparse.ArgumentTypeError("并发度必须包含至少一个正整数")
    return levels


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="NeoTask v1.2 基准基线：单任务提交延迟、批量提交吞吐与高并发执行"
    )
    parser.add_argument("--tasks", type=int, default=DEFAULT_TASKS, help="每轮批量提交任务数")
    parser.add_argument("--single", type=int, default=DEFAULT_SINGLE, help="单任务延迟采样数")
    parser.add_argument("--rounds", type=int, default=DEFAULT_ROUNDS, help="批量基准轮数")
    parser.add_argument(
        "--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="批量提交基准的 Worker 并发数"
    )
    parser.add_argument(
        "--hc-levels",
        type=_parse_levels,
        default=list(DEFAULT_HC_LEVELS),
        help="高并发扫描的并发度，逗号分隔（默认 1,4,8,16,32,64）",
    )
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    args = parser.parse_args(argv)

    for name, value in (("tasks", args.tasks), ("single", args.single), ("rounds", args.rounds)):
        if value <= 0:
            parser.error(f"--{name} 必须大于 0")
    if args.concurrency <= 0:
        parser.error("--concurrency 必须大于 0")

    logging.getLogger("NeoTask").setLevel(logging.CRITICAL if args.json else logging.WARNING)
    result = asyncio.run(
        run(args.tasks, args.single, args.rounds, args.concurrency, args.hc_levels)
    )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        _print_report(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
