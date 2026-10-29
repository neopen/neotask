"""
@FileName: 13_benchmark.py
@Description: 轻量性能自测示例 — 用真实 executor 验证当前并发度下的吞吐与延迟
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

import asyncio
import logging
from typing import Any, Dict

from neotask.benchmark import run_benchmark_async

# 基准报告需要清爽的输出，抑制框架启动/停机日志（与 CLI 默认行为一致）
logging.getLogger("NeoTask").setLevel(logging.WARNING)


async def io_task(data: Dict[str, Any]) -> Dict[str, Any]:
    """模拟一次 20ms 的外部调用（HTTP / 数据库 / 文件读取）"""
    await asyncio.sleep(data.get("latency", 0.02))
    return {"index": data["index"]}


async def main():
    print("场景：模拟 20ms I/O 的任务，对比不同并发度下的表现")
    print("=" * 56)

    for concurrency in (2, 8):
        result = await run_benchmark_async(
            io_task,
            tasks=40,
            concurrency=concurrency,
            payload={"latency": 0.02},
            warmup=5,  # 预热，排除首次空闲退避的影响
            timeout=30,
        )
        print(
            f"并发 {concurrency:>2}: "
            f"吞吐 {result.throughput:6.2f} tasks/s | "
            f"p50 {result.latency_p50_ms:7.2f} ms | "
            f"p95 {result.latency_p95_ms:7.2f} ms | "
            f"成功 {result.succeeded}/{result.tasks}"
        )

    print("=" * 56)
    print("延迟为端到端耗时（含排队），并发调大后吞吐提升、单任务延迟可能上升。")
    print("换成自己的执行器：")
    print("  python -m neotask.benchmark --executor my_tasks:handle --json")
    print("  neotask --benchmark --executor my_tasks:handle --json")


if __name__ == "__main__":
    asyncio.run(main())
