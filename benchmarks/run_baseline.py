"""
@FileName: run_baseline.py
@Description: v1.2 基准基线 - 单任务提交延迟、批量提交吞吐、高并发执行与 Checkpoint 写入开销
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/19
"""

import argparse
import asyncio
import json
import logging
import os
import shutil
import statistics
import tempfile
import time
from typing import Any, Dict, List, Optional

from neotask import save_checkpoint
from neotask.api.task_pool import TaskPool
from neotask.models.config import TaskPoolConfig

DEFAULT_TASKS = 1000
DEFAULT_SINGLE = 200
DEFAULT_ROUNDS = 3
DEFAULT_CONCURRENCY = 8
DEFAULT_HC_LEVELS = (1, 4, 8, 16, 32, 64)
DEFAULT_CKPT_BACKENDS = ("memory", "sqlite")
DEFAULT_CKPT_WRITES = (0, 1, 5)
DEFAULT_CKPT_PAYLOAD = 256


async def noop_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """只返回数据的空执行器，避免执行耗时污染提交路径测量"""
    return {"index": data.get("index")}


def _new_pool(
    concurrency: int,
    queue_size: int,
    executor: Any = noop_executor,
    storage_type: str = "memory",
    sqlite_path: Optional[str] = None,
) -> TaskPool:
    config = TaskPoolConfig(
        storage_type=storage_type,
        sqlite_path=sqlite_path or "/data/neotask.db",
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


async def _measure_checkpoint_once(
    backend: str,
    writes: int,
    tasks: int,
    concurrency: int,
    payload_bytes: int,
) -> Dict[str, Any]:
    """测一轮指定后端与每任务写入次数，采集单次写入延迟与端到端耗时"""
    db_dir = tempfile.mkdtemp(prefix="neotask_ckpt_") if backend == "sqlite" else None
    sqlite_path = os.path.join(db_dir, "bench.db") if db_dir else None
    write_ms: List[float] = []
    completed_perf: Dict[int, float] = {}

    async def checkpoint_executor(data: Dict[str, Any]) -> Dict[str, Any]:
        state_tail = "x" * payload_bytes
        for step in range(writes):
            started = time.perf_counter()
            saved = await save_checkpoint(
                {"index": data["index"], "step": step, "payload": state_tail}
            )
            if not saved:
                raise RuntimeError("checkpoint 未写入：执行器上下文未绑定，基准结果无效")
            write_ms.append((time.perf_counter() - started) * 1000.0)
        completed_perf[data["index"]] = time.perf_counter()
        return {"index": data["index"]}

    pool = _new_pool(
        concurrency=concurrency,
        queue_size=tasks + 64,
        executor=checkpoint_executor,
        storage_type=backend,
        sqlite_path=sqlite_path,
    )
    try:
        warmup_ids = [await pool.submit_async({"index": -1 - i}) for i in range(10)]
        await pool.wait_all_async(warmup_ids, timeout=30)
        write_ms.clear()
        completed_perf.clear()

        payload = [{"index": i} for i in range(tasks)]
        started = time.perf_counter()
        task_ids = await pool.submit_many_async(payload)
        await pool.wait_all_async(task_ids, timeout=120)
        elapsed = time.perf_counter() - started

        return {
            "elapsed_s": elapsed,
            "write_latencies_ms": write_ms,
            "task_latencies_ms": [
                (finished - started) * 1000.0 for finished in completed_perf.values()
            ],
        }
    finally:
        pool.shutdown(graceful=True, timeout=30)
        if db_dir:
            shutil.rmtree(db_dir, ignore_errors=True)


async def measure_checkpoint_overhead(
    tasks: int,
    rounds: int,
    concurrency: int,
    backends: List[str],
    write_counts: List[int],
    payload_bytes: int,
) -> Dict[str, Any]:
    """按后端 × 写入次数扫描，正反交替多轮取中位"""
    samples: Dict[Any, List[Dict[str, Any]]] = {
        (backend, writes): [] for backend in backends for writes in write_counts
    }
    for index in range(rounds):
        pairs = [(backend, writes) for backend in backends for writes in write_counts]
        order = pairs if index % 2 == 0 else list(reversed(pairs))
        for backend, writes in order:
            samples[(backend, writes)].append(
                await _measure_checkpoint_once(backend, writes, tasks, concurrency, payload_bytes)
            )

    backend_results: List[Dict[str, Any]] = []
    for backend in backends:
        baseline_elapsed = None
        if 0 in write_counts:
            baseline_elapsed = statistics.median(
                item["elapsed_s"] for item in samples[(backend, 0)]
            )

        scenarios: List[Dict[str, Any]] = []
        for writes in write_counts:
            runs = samples[(backend, writes)]
            elapsed_median = statistics.median(item["elapsed_s"] for item in runs)
            throughput = tasks / elapsed_median if elapsed_median else 0.0
            task_latencies = [value for item in runs for value in item["task_latencies_ms"]]
            scenario: Dict[str, Any] = {
                "writes_per_task": writes,
                "throughput_median": throughput,
                "task_latency_ms": _percentiles(task_latencies),
            }

            if writes:
                write_latencies = [value for item in runs for value in item["write_latencies_ms"]]
                write_stats = _percentiles(write_latencies)
                scenario["write_p50_us"] = write_stats["p50"] * 1000.0
                scenario["write_p95_us"] = write_stats["p95"] * 1000.0
                if baseline_elapsed:
                    scenario["throughput_ratio_vs_0"] = (
                        baseline_elapsed / elapsed_median if elapsed_median else 0.0
                    )
                    scenario["wall_overhead_per_write_ms"] = (
                        (elapsed_median - baseline_elapsed) * 1000.0 / (tasks * writes)
                    )
            elif baseline_elapsed:
                scenario["throughput_ratio_vs_0"] = 1.0

            scenarios.append(scenario)

        backend_results.append({"backend": backend, "scenarios": scenarios})

    return {
        "tasks_per_round": tasks,
        "rounds": rounds,
        "worker_concurrency": concurrency,
        "payload_bytes": payload_bytes,
        "backends": backend_results,
    }


def _format_latency(values: Dict[str, float]) -> str:
    return (
        f"p50 {values['p50']:.3f} ms / p95 {values['p95']:.3f} ms / "
        f"p99 {values['p99']:.3f} ms / max {values['max']:.3f} ms"
    )


def _format_us(value: float) -> str:
    if value >= 1000.0:
        return f"{value / 1000.0:.2f}ms"
    return f"{value:.0f}us"


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

    checkpoint = result["checkpoint_overhead"]
    print(
        f"Checkpoint 写入开销（每轮 {checkpoint['tasks_per_round']} 任务，"
        f"{checkpoint['worker_concurrency']} 并发，{checkpoint['rounds']} 轮交替取中位）"
    )
    print(
        f"  {'backend':<8} {'writes':>6} {'tasks/s':>10} {'相对0写':>8} "
        f"{'write p50':>10} {'write p95':>10} {'增量/写':>9}"
    )
    for backend in checkpoint["backends"]:
        for item in backend["scenarios"]:
            ratio = item.get("throughput_ratio_vs_0")
            extra = item.get("wall_overhead_per_write_ms")
            ratio_text = f"{ratio:.2f}x" if ratio is not None else "-"
            extra_text = f"{extra:.3f}ms" if extra is not None else "-"
            write_p50 = _format_us(item["write_p50_us"]) if item.get("write_p50_us") else "-"
            write_p95 = _format_us(item["write_p95_us"]) if item.get("write_p95_us") else "-"
            print(
                f"  {backend['backend']:<8} {item['writes_per_task']:>6} "
                f"{item['throughput_median']:>10.0f} {ratio_text:>8} "
                f"{write_p50:>10} {write_p95:>10} {extra_text:>9}"
            )
    print("-" * 60)
    print("写入延迟口径：save_checkpoint 调用返回；增量/写 = 端到端耗时差 / (任务数 × 写入次数)")
    print("提示：保持参数与机器一致再做横向对比；Checkpoint 场景会同时测任务存储开销")


async def run(
    tasks: int,
    single: int,
    rounds: int,
    concurrency: int,
    hc_levels: List[int],
    ckpt_backends: List[str],
    ckpt_writes: List[int],
    ckpt_payload: int,
) -> Dict[str, Any]:
    single_result = await measure_single_submit(single)
    bulk_result = await measure_batch_submit(tasks, rounds, concurrency)
    high_concurrency = await measure_high_concurrency(tasks, rounds, hc_levels)
    checkpoint_overhead = await measure_checkpoint_overhead(
        tasks, rounds, concurrency, ckpt_backends, ckpt_writes, ckpt_payload
    )
    return {
        "environment": {
            "storage": "memory",
            "worker_concurrency": concurrency,
            "prefetch": True,
            "tasks_per_round": tasks,
            "single_samples": single,
            "rounds": rounds,
            "high_concurrency_levels": hc_levels,
            "checkpoint_backends": ckpt_backends,
            "checkpoint_writes": ckpt_writes,
            "checkpoint_payload_bytes": ckpt_payload,
        },
        "single_submit": single_result,
        "batch_submit": bulk_result,
        "high_concurrency": high_concurrency,
        "checkpoint_overhead": checkpoint_overhead,
    }


def _parse_levels(text: str) -> List[int]:
    try:
        levels = [int(part.strip()) for part in text.split(",") if part.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("并发度必须是逗号分隔的整数") from exc
    if not levels or any(level <= 0 for level in levels):
        raise argparse.ArgumentTypeError("并发度必须包含至少一个正整数")
    return levels


def _parse_backends(text: str) -> List[str]:
    backends: List[str] = []
    for part in text.split(","):
        backend = part.strip().lower()
        if not backend:
            continue
        if backend not in ("memory", "sqlite"):
            raise argparse.ArgumentTypeError("Checkpoint 后端只支持 memory / sqlite")
        if backend not in backends:
            backends.append(backend)
    if not backends:
        raise argparse.ArgumentTypeError("至少需要指定一个 Checkpoint 后端")
    return backends


def _parse_writes(text: str) -> List[int]:
    try:
        counts = [int(part.strip()) for part in text.split(",") if part.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("写入次数必须是逗号分隔的整数") from exc
    if not counts or any(count < 0 for count in counts):
        raise argparse.ArgumentTypeError("写入次数必须包含至少一个非负整数")
    return sorted(set(counts))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="NeoTask v1.2 基准基线：单任务提交延迟、批量提交吞吐、高并发执行与 Checkpoint 写入开销"
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
    parser.add_argument(
        "--ckpt-backends",
        type=_parse_backends,
        default=list(DEFAULT_CKPT_BACKENDS),
        help="Checkpoint 情景的后端，逗号分隔（默认 memory,sqlite）",
    )
    parser.add_argument(
        "--ckpt-writes",
        type=_parse_writes,
        default=list(DEFAULT_CKPT_WRITES),
        help="每任务 Checkpoint 写入次数，逗号分隔（默认 0,1,5）",
    )
    parser.add_argument(
        "--ckpt-payload",
        type=int,
        default=DEFAULT_CKPT_PAYLOAD,
        help="每次 Checkpoint 状态的负载字节数（默认 256）",
    )
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    args = parser.parse_args(argv)

    for name, value in (("tasks", args.tasks), ("single", args.single), ("rounds", args.rounds)):
        if value <= 0:
            parser.error(f"--{name} 必须大于 0")
    if args.concurrency <= 0:
        parser.error("--concurrency 必须大于 0")
    if args.ckpt_payload < 0:
        parser.error("--ckpt-payload 不能为负数")

    logging.getLogger("NeoTask").setLevel(logging.CRITICAL if args.json else logging.WARNING)
    result = asyncio.run(
        run(
            args.tasks,
            args.single,
            args.rounds,
            args.concurrency,
            args.hc_levels,
            args.ckpt_backends,
            args.ckpt_writes,
            args.ckpt_payload,
        )
    )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        _print_report(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
