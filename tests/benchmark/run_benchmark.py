"""
@FileName: run_benchmarks.py
@Description: NeoTask 性能基准测试独立运行脚本（支持结果自动导出为 JSON）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
"""

import asyncio
import sys
import os
import json
import time
import argparse
from typing import Dict, Any, List

import psutil

sys.path.insert(0, 'src')

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from test_performance import (
    TestBasicThroughput,
    TestQueuePerformance,
    TestConcurrencyPerformance,
    TestTaskTypePerformance,
    TestStressTest
)


class BenchmarkReporter:
    """基准测试结果收集与报告器"""
    def __init__(self):
        self.results: List[Dict[str, Any]] = []

    def add_result(self, suite_name: str, test_name: str, duration: float, success: bool, error: str = ""):
        self.results.append({
            "suite": suite_name,
            "test": test_name,
            "duration": round(duration, 3),
            "success": success,
            "error": error
        })

    def export_to_json(self, filename: str = "benchmark_results.json"):
        """将测试结果导出到 data 目录下的 JSON 文件"""
        data_dir = "/data"
        os.makedirs(data_dir, exist_ok=True)

        file_path = os.path.join(data_dir, filename)
        output_data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_tests": len(self.results),
            "passed_tests": sum(1 for r in self.results if r["success"]),
            "results": self.results
        }

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, ensure_ascii=False, indent=4)
            print(f"\n  📁 测试报告已成功导出到: {file_path}")
        except Exception as e:
            print(f"\n  ❌ 导出 JSON 报告失败: {e}")

    def print_summary(self):
        print("\n" + "=" * 70)
        print("📊 性能基准测试总结")
        print("=" * 70)

        passed = sum(1 for r in self.results if r["success"])
        total = len(self.results)

        for r in self.results:
            status = "✅" if r["success"] else "❌"
            print(f"  {status} [{r['suite']}] {r['test']} (耗时: {r['duration']:.3f}s)")
            if not r["success"]:
                print(f"      错误: {r['error']}")

        print("-" * 70)
        print(f"总计: {passed}/{total} 测试成功完成")
        print("=" * 70)

        # 自动触发结果导出
        self.export_to_json()


async def run_suite(reporter: BenchmarkReporter, suite_name: str, test_instance, test_methods: List[str]):
    """安全运行一组测试"""
    print(f"\n--- 🚀 运行 {suite_name} ---")
    for method_name in test_methods:
        if not hasattr(test_instance, method_name):
            continue

        method = getattr(test_instance, method_name)
        start_time = time.time()
        success = True
        error_msg = ""

        try:
            print(f"  ▶ 执行 {method_name}...", end="", flush=True)
            if asyncio.iscoroutinefunction(method):
                await method()
            else:
                method()
            print(" 完成")
        except Exception as e:
            success = False
            error_msg = str(e)
            print(f" 失败: {e}")

        duration = time.time() - start_time
        reporter.add_result(suite_name, method_name, duration, success, error_msg)


async def run_all_benchmarks(args):
    print("=" * 70)
    print("NeoTask 性能基准测试")
    print("=" * 70)

    reporter = BenchmarkReporter()

    # 1. 基础吞吐量测试
    if args.suite in ["all", "throughput"]:
        basic_test = TestBasicThroughput()
        await run_suite(reporter, "基础吞吐量测试", basic_test, [
            "test_task_pool_throughput",
            "test_task_pool_latency_distribution"
        ])

    # 2. 队列性能测试
    if args.suite in ["all", "queue"]:
        queue_test = TestQueuePerformance()
        await run_suite(reporter, "队列性能测试", queue_test, [
            "test_priority_queue_throughput",
            "test_prefetcher_performance",
            "test_batch_pop_performance"
        ])

    # 3. 并发性能测试
    if args.suite in ["all", "concurrency"]:
        concurrency_test = TestConcurrencyPerformance()
        await run_suite(reporter, "并发性能测试", concurrency_test, [
            "test_worker_scalability_light_task",
            "test_worker_scalability_heavy_task",
            "test_high_concurrency_submit"
        ])

    # 4. 任务类型性能测试
    if args.suite in ["all", "task_type"]:
        task_type_test = TestTaskTypePerformance()
        await run_suite(reporter, "任务类型性能测试", task_type_test, [
            "test_light_task_performance",
            "test_io_task_performance"
        ])

    # 5. 压力测试
    if args.suite in ["all", "stress"]:
        stress_test = TestStressTest()
        await run_suite(reporter, "压力测试", stress_test, [
            "test_sustained_load",
            "test_burst_load"
        ])

    reporter.print_summary()


def print_environment_specs():
    print("=" * 70)
    print("🖥️ NeoTask 性能基准测试 - 运行环境口径")
    print("=" * 70)

    # 系统与 Python
    import platform
    print(f" - 系统平台: {platform.system()} {platform.release()} ({platform.machine()})")
    print(f" - Python 版本: {sys.version.split()[0]}")

    # 硬件信息 (如果安装了 psutil)
    if psutil:
        mem_gb = round(psutil.virtual_memory().total / (1024 ** 3), 2)
        print(f" - CPU 逻辑核心: {psutil.cpu_count(logical=True)} 核")
        print(f" - 物理内存: {mem_gb} GB")
    else:
        print(f" - 硬件提示: 安装 'psutil' 库可自动获取详细 CPU/内存配置 (pip install psutil)")

    print(f" - NeoTask 版本: v1.0.2")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NeoTask 性能基准测试运行器")
    print_environment_specs()
    parser.add_argument(
        "--suite",
        type=str,
        default="all",
        choices=["all", "throughput", "queue", "concurrency", "task_type", "stress"],
        help="指定运行的测试套件"
    )
    args = parser.parse_args()

    asyncio.run(run_all_benchmarks(args))