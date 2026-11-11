"""
@FileName: benchmark.py
@Description: 轻量性能自测入口 - 用自己的 executor 快速验证吞吐与延迟
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

import argparse
import asyncio
import importlib
import importlib.util
import inspect
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from neotask.api.task_pool import TaskPool
from neotask.executor.base import TaskExecutor
from neotask.models.config import TaskPoolConfig

# 默认任务数与并发度刻意保持“几十毫秒到几秒”的体量：用户改几行就能跑完一轮，
# 而不是像 tests/benchmark 那样覆盖完整基准矩阵。
DEFAULT_TASKS = 200
DEFAULT_CONCURRENCY = 10
DEFAULT_TIMEOUT = 120.0


async def demo_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """内置演示执行器：模拟 5ms 的 I/O 等待"""
    await asyncio.sleep(0.005)
    return {"ok": True, "index": data.get("index")}


@dataclass
class BenchmarkResult:
    """一次轻量基准测试的结果

    延迟口径为 ``created_at -> completed_at``，即从任务创建（提交）到终态的
    端到端耗时；只统计成功任务，失败/取消的任务单独计数。
    """

    executor: str
    tasks: int
    succeeded: int
    failed: int
    unfinished: int
    duration_s: float
    submit_s: float
    throughput: float
    success_rate: float
    latency_avg_ms: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latency_min_ms: float
    latency_max_ms: float
    worker_concurrency: int
    prefetch: bool
    storage: str = "memory"
    executor_mode: str = "async"
    warmup: int = 0
    timeout_s: float = DEFAULT_TIMEOUT
    payload_template: Optional[Dict[str, Any]] = None
    error_samples: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """转换为可直接序列化的字典（数值保留适量小数）"""
        return {
            "executor": self.executor,
            "tasks": self.tasks,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "unfinished": self.unfinished,
            "duration_s": round(self.duration_s, 3),
            "submit_s": round(self.submit_s, 3),
            "throughput": round(self.throughput, 2),
            "success_rate": round(self.success_rate, 4),
            "latency_avg_ms": round(self.latency_avg_ms, 2),
            "latency_p50_ms": round(self.latency_p50_ms, 2),
            "latency_p95_ms": round(self.latency_p95_ms, 2),
            "latency_p99_ms": round(self.latency_p99_ms, 2),
            "latency_min_ms": round(self.latency_min_ms, 2),
            "latency_max_ms": round(self.latency_max_ms, 2),
            "worker_concurrency": self.worker_concurrency,
            "prefetch": self.prefetch,
            "storage": self.storage,
            "executor_mode": self.executor_mode,
            "warmup": self.warmup,
            "timeout_s": round(self.timeout_s, 1),
            "payload_template": (
                dict(self.payload_template)
                if self.payload_template is not None
                else None
            ),
            "error_samples": list(self.error_samples),
        }

    def format(self) -> str:
        """生成人类可读的文本报告"""
        lines = [
            "NeoTask 轻量基准测试",
            "-" * 48,
            f"执行器        : {self.executor}（{self.executor_mode}）",
            f"任务数        : {self.tasks}",
            f"并发度        : {self.worker_concurrency}",
            f"预热          : {self.warmup}",
            f"超时          : {self.timeout_s:.1f} s",
            f"预取          : {'开启' if self.prefetch else '关闭'}",
            f"存储          : {self.storage}",
            f"数据模板      : {self._payload_text()}",
            "-" * 48,
            f"完成          : {self.succeeded + self.failed}"
            f"（成功 {self.succeeded}, 失败 {self.failed}, 未完成 {self.unfinished}）",
            f"提交耗时      : {self.submit_s:.3f} s",
            f"总耗时        : {self.duration_s:.3f} s",
            f"吞吐量        : {self.throughput:.2f} tasks/s",
            f"成功率        : {self.success_rate:.1%}",
            "延迟（成功任务）",
            f"  avg / p50   : {self.latency_avg_ms:.2f} / {self.latency_p50_ms:.2f} ms",
            f"  p95 / p99   : {self.latency_p95_ms:.2f} / {self.latency_p99_ms:.2f} ms",
            f"  min / max   : {self.latency_min_ms:.2f} / {self.latency_max_ms:.2f} ms",
        ]
        if self.error_samples:
            lines.append("错误示例")
            for message in self.error_samples:
                lines.append(f"  - {message}")
        return "\n".join(lines)

    def _payload_text(self) -> str:
        """渲染任务数据模板；默认模板逐条注入 index"""
        if self.payload_template is None:
            return '{"index": i}'
        return json.dumps(self.payload_template, ensure_ascii=False)


def run_benchmark(
        executor: Union[TaskExecutor, Callable, None] = None,
        *,
        tasks: int = DEFAULT_TASKS,
        concurrency: int = DEFAULT_CONCURRENCY,
        prefetch: bool = True,
        payload: Optional[Dict[str, Any]] = None,
        warmup: int = 0,
        timeout: float = DEFAULT_TIMEOUT,
) -> BenchmarkResult:
    """同步运行一次轻量基准测试

    注意：本函数内部使用 ``asyncio.run``，不能在已运行的事件循环中调用；
    异步场景请使用 :func:`run_benchmark_async`。

    Args:
        executor: 业务执行器。``None`` 时使用内置 5ms 演示执行器；
            同步函数会自动放到线程池执行，避免阻塞 Worker 事件循环
        tasks: 正式测量的任务数
        concurrency: Worker 并发数（``worker_concurrency``）
        prefetch: 是否启用 Worker 预取
        payload: 任务数据模板；默认每条任务为 ``{"index": i}``，
            传入字典时会拷贝一份并补上 ``index``（已有 ``index`` 则保留）
        warmup: 正式测量前的预热任务数（结果不计入统计）
        timeout: 等待所有任务完成的超时时间（秒）

    Returns:
        :class:`BenchmarkResult`

    Examples:
        >>> from neotask.benchmark import run_benchmark
        >>> async def my_task(data):
        ...     await asyncio.sleep(0.01)
        ...     return {"ok": True}
        >>> result = run_benchmark(my_task, tasks=100, concurrency=8)
        >>> print(result.format())
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError(
            "检测到正在运行的事件循环，请改用 await run_benchmark_async(...)"
        )

    return asyncio.run(
        run_benchmark_async(
            executor,
            tasks=tasks,
            concurrency=concurrency,
            prefetch=prefetch,
            payload=payload,
            warmup=warmup,
            timeout=timeout,
        )
    )


async def run_benchmark_async(
        executor: Union[TaskExecutor, Callable, None] = None,
        *,
        tasks: int = DEFAULT_TASKS,
        concurrency: int = DEFAULT_CONCURRENCY,
        prefetch: bool = True,
        payload: Optional[Dict[str, Any]] = None,
        warmup: int = 0,
        timeout: float = DEFAULT_TIMEOUT,
) -> BenchmarkResult:
    """异步运行一次轻量基准测试

    使用 memory 存储与 ``max_retries=0``，避免持久化 I/O 和重试放大掩盖真实
    性能；关闭健康检查/指标/Reporter，只保留任务执行主链路。同步执行器会
    通过线程池运行（``executor_mode="thread"``），异步执行器保持原样。
    """
    if tasks <= 0:
        raise ValueError("tasks 必须大于 0")
    if concurrency <= 0:
        raise ValueError("concurrency 必须大于 0")
    if warmup < 0:
        raise ValueError("warmup 不能为负数")
    if timeout <= 0:
        raise ValueError("timeout 必须大于 0")

    executor = executor or demo_executor
    executor_name = _executor_name(executor)
    executor_mode = "async"
    # TaskPool 会把同步函数直接包成 async 包装器，从而阻塞 Worker 事件循环；
    # 基准里改成线程执行，否则同步 I/O 执行器会被串行化，指标完全失真。
    if not isinstance(executor, TaskExecutor) and not _is_async_callable(executor):
        executor = _wrap_sync_executor(executor)
        executor_mode = "thread"

    config = TaskPoolConfig(
        storage_type="memory",
        worker_concurrency=concurrency,
        enable_prefetch=prefetch,
        max_retries=0,
        enable_metrics=False,
        enable_health_check=False,
        enable_reporter=False,
        # 队列容量要覆盖本次提交量，否则会因队列满而提交失败，污染测试结果
        queue_max_size=max(10000, tasks + warmup + concurrency + 64),
    )

    pool = TaskPool(executor=executor, config=config)
    pool.start()

    unfinished = tasks  # 异常中断时按“仍有未完成任务”处理，走非优雅停机
    try:
        if warmup:
            warmup_ids = [
                await pool.submit_async(_make_payload(payload, i))
                for i in range(warmup)
            ]
            await pool.wait_all_async(warmup_ids, timeout=timeout)

        submit_start = time.perf_counter()
        task_ids = [
            await pool.submit_async(_make_payload(payload, i))
            for i in range(tasks)
        ]
        submit_s = time.perf_counter() - submit_start

        await pool.wait_all_async(task_ids, timeout=timeout)
        finished_at = time.perf_counter()

        # 等待结束后再逐条读取结果：状态与时间戳都已是终态，且不计入测量窗口
        records = [await pool.get_result_async(task_id) for task_id in task_ids]
        result = _build_result(
            executor_name=executor_name,
            records=records,
            tasks=tasks,
            duration_s=finished_at - submit_start,
            submit_s=submit_s,
            concurrency=concurrency,
            prefetch=prefetch,
            executor_mode=executor_mode,
            warmup=warmup,
            timeout_s=timeout,
            payload_template=payload,
        )
        unfinished = result.unfinished
        return result
    finally:
        pool.shutdown(
            graceful=unfinished == 0,
            timeout=min(30.0, max(5.0, timeout)),
        )


def load_executor(target: str) -> Callable:
    """按 ``模块:函数`` 或 ``路径.py:函数`` 加载执行器

    Examples:
        >>> load_executor("my_tasks:handle")
        >>> load_executor("./bench.py:my_task")
    """
    if not target or ":" not in target:
        raise ValueError(
            f"executor 需要写成 '模块:函数' 或 '路径.py:函数'，收到: {target!r}"
        )

    # 用 rsplit：Windows 绝对路径自带盘符冒号（C:\path\bench.py:func）
    source, attr_path = target.rsplit(":", 1)
    source = source.strip()
    attr_path = attr_path.strip()
    if not source or not attr_path:
        raise ValueError(f"executor 描述不完整: {target!r}")

    if source.endswith(".py") or os.path.isfile(source):
        if not os.path.isfile(source):
            raise ValueError(f"executor 文件不存在: {source}")
        module_name = f"_neotask_benchmark_{abs(hash(os.path.abspath(source)))}"
        spec = importlib.util.spec_from_file_location(module_name, source)
        if spec is None or spec.loader is None:
            raise ValueError(f"无法加载 executor 文件: {source}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    else:
        module = importlib.import_module(source)

    obj: Any = module
    for part in attr_path.split("."):
        if not hasattr(obj, part):
            raise ValueError(f"找不到 executor: {target!r}")
        obj = getattr(obj, part)

    if not callable(obj):
        raise ValueError(f"executor 不可调用: {target!r}")
    return obj


def main(
        argv: Optional[Sequence[str]] = None,
        *,
        prog: Optional[str] = None,
) -> int:
    """命令行入口：``python -m neotask.benchmark``（也可经 ``neotask --benchmark``）"""
    parser = argparse.ArgumentParser(
        prog=prog or "python -m neotask.benchmark",
        description="用你自己的 executor 快速验证 NeoTask 在当前场景下的吞吐与延迟",
    )
    parser.add_argument(
        "--executor",
        metavar="MODULE:FUNC",
        help="执行器，如 my_tasks:handle 或 ./bench.py:my_task；缺省使用内置演示执行器",
    )
    parser.add_argument(
        "--tasks",
        type=int,
        default=DEFAULT_TASKS,
        help=f"测量的任务数（默认 {DEFAULT_TASKS}）",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Worker 并发数（默认 {DEFAULT_CONCURRENCY}）",
    )
    parser.add_argument(
        "--warmup", type=int, default=0, help="预热任务数（不计入统计，默认 0）"
    )
    parser.add_argument(
        "--payload",
        metavar="JSON",
        help='任务数据模板，如 \'{"size": 100}\'；默认 {"index": i}',
    )
    parser.add_argument(
        "--no-prefetch", action="store_true", help="关闭 Worker 预取（默认开启）"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"等待完成的超时时间（秒，默认 {DEFAULT_TIMEOUT:g}）",
    )
    parser.add_argument("--json", action="store_true", help="以 JSON 输出，便于 CI/脚本消费")
    parser.add_argument("--verbose", action="store_true", help="保留 NeoTask 的 INFO 日志")
    args = parser.parse_args(argv)

    # 默认压掉 INFO 日志，保证报告（尤其是 --json）不被框架日志污染
    logger_level = logging.CRITICAL if args.json else (
        logging.DEBUG if args.verbose else logging.WARNING
    )
    neotask_logger = logging.getLogger("NeoTask")
    previous_level = neotask_logger.level
    neotask_logger.setLevel(logger_level)

    try:
        executor = load_executor(args.executor) if args.executor else demo_executor
        payload = json.loads(args.payload) if args.payload else None
        if payload is not None and not isinstance(payload, dict):
            raise ValueError("--payload 必须是 JSON 对象")

        result = run_benchmark(
            executor,
            tasks=args.tasks,
            concurrency=args.concurrency,
            prefetch=not args.no_prefetch,
            payload=payload,
            warmup=args.warmup,
            timeout=args.timeout,
        )
    except Exception as exc:
        print(f"基准测试失败: {exc}", file=sys.stderr)
        return 1
    finally:
        neotask_logger.setLevel(previous_level)

    if args.json:
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(result.format())
        print("-" * 48)
        print("提示：")
        print("  - 自定义执行器：python -m neotask.benchmark --executor my_module:my_task")
        print("  - 统一 CLI：neotask --benchmark --executor my_module:my_task")
        print("  - 机器可读：在上面的命令后追加 --json")
        print("  - 结果只反映当前机器、memory 存储与当前并发；横向对比请保持参数一致")
        print("  - 首次突发提交会包含 Worker 空闲退避开销，建议加 --warmup 10 再对比")
    return 0


# ========== 内部辅助 ==========

def _make_payload(payload: Optional[Dict[str, Any]], index: int) -> Dict[str, Any]:
    """生成单条任务数据，避免多条任务共享同一个可变字典"""
    if payload is None:
        return {"index": index}
    data = dict(payload)
    data.setdefault("index", index)
    return data


def _executor_name(executor: Union[TaskExecutor, Callable]) -> str:
    """提取可读的执行器名称"""
    if isinstance(executor, TaskExecutor):
        return type(executor).__name__
    name = getattr(executor, "__name__", None)
    return name or type(executor).__name__


def _is_async_callable(executor: Callable) -> bool:
    """判断执行器是否本身可 await（含 async __call__ 的实例）"""
    if inspect.iscoroutinefunction(executor):
        return True
    return inspect.iscoroutinefunction(getattr(executor, "__call__", None))


def _wrap_sync_executor(executor: Callable) -> Callable:
    """把同步执行器放到线程池执行，避免阻塞共享事件循环"""

    async def wrapper(data: Dict[str, Any]) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, executor, data)

    return wrapper


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    """解析任务记录里的 ISO 时间戳"""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def _percentile(sorted_values: List[float], ratio: float) -> float:
    """线性插值百分位；空列表返回 0.0"""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]

    rank = ratio * (len(sorted_values) - 1)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return sorted_values[low]

    weight = rank - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def _build_result(
        *,
        executor_name: str,
        records: List[Optional[Dict[str, Any]]],
        tasks: int,
        duration_s: float,
        submit_s: float,
        concurrency: int,
        prefetch: bool,
        executor_mode: str,
        warmup: int,
        timeout_s: float,
        payload_template: Optional[Dict[str, Any]],
) -> BenchmarkResult:
    """根据终态任务记录汇总指标"""
    latencies_ms: List[float] = []
    succeeded = 0
    failed = 0
    unfinished = 0
    error_samples: List[str] = []

    for record in records:
        if not record:
            unfinished += 1
            continue

        status = record.get("status")
        if status == "success":
            succeeded += 1
            created_at = _parse_time(record.get("created_at"))
            completed_at = _parse_time(record.get("completed_at"))
            if created_at and completed_at:
                latencies_ms.append(
                    (completed_at - created_at).total_seconds() * 1000.0
                )
        elif status in ("failed", "cancelled"):
            failed += 1
        else:
            unfinished += 1

        error = record.get("error")
        if error and len(error_samples) < 3:
            error_samples.append(str(error))

    latencies_ms.sort()
    finished = succeeded + failed
    throughput = finished / duration_s if duration_s > 0 else 0.0
    success_rate = succeeded / tasks if tasks else 0.0

    if latencies_ms:
        latency_avg = sum(latencies_ms) / len(latencies_ms)
        latency_p50 = _percentile(latencies_ms, 0.50)
        latency_p95 = _percentile(latencies_ms, 0.95)
        latency_p99 = _percentile(latencies_ms, 0.99)
        latency_min = latencies_ms[0]
        latency_max = latencies_ms[-1]
    else:
        latency_avg = latency_p50 = latency_p95 = latency_p99 = 0.0
        latency_min = latency_max = 0.0

    return BenchmarkResult(
        executor=executor_name,
        tasks=tasks,
        succeeded=succeeded,
        failed=failed,
        unfinished=unfinished,
        duration_s=duration_s,
        submit_s=submit_s,
        throughput=throughput,
        success_rate=success_rate,
        latency_avg_ms=latency_avg,
        latency_p50_ms=latency_p50,
        latency_p95_ms=latency_p95,
        latency_p99_ms=latency_p99,
        latency_min_ms=latency_min,
        latency_max_ms=latency_max,
        worker_concurrency=concurrency,
        prefetch=prefetch,
        executor_mode=executor_mode,
        warmup=warmup,
        timeout_s=timeout_s,
        payload_template=payload_template,
        error_samples=error_samples,
    )


if __name__ == "__main__":
    raise SystemExit(main())
