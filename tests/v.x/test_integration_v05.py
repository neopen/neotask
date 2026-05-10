"""
@FileName: test_integration_v05.py
@Description: NeoTask v0.5 完整集成测试 - 性能优化
@Author: HiPeng
@Time: 2026/5/10

运行方式：
    python -m pytest tests/v.x/test_integration_v05.py -v -s
    或
    python tests/v.x/test_integration_v05.py

测试覆盖：
    v0.5.1: 预取机制 - 批量任务预取、本地队列缓存
    v0.5.2: 批量操作 - 批量提交、批量状态更新
    v0.5.3: 任务进度上报 - 长时间任务进度跟踪
    v0.5.4: 性能基准 - 吞吐量测试
"""

import asyncio
import os
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, List

from neotask import TaskExecutor

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from neotask.api.task_pool import TaskPool
from neotask.models.config import TaskPoolConfig
from neotask.models.task import TaskPriority


# ============================================================
# 测试配置
# ============================================================

class TestStatus(Enum):
    """测试状态"""
    PASSED = "✅"
    FAILED = "❌"
    SKIPPED = "⚠️"


@dataclass
class TestResult:
    """测试结果"""
    name: str
    status: TestStatus
    message: str = ""
    duration: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)


class TestReporter:
    """测试报告器"""

    def __init__(self):
        self.results: List[TestResult] = []
        self.current_suite: str = ""
        self.suite_start: float = 0.0

    def start_suite(self, name: str):
        self.current_suite = name
        self.suite_start = time.time()
        print(f"\n{'=' * 80}")
        print(f"🧪 {name}")
        print(f"{'=' * 80}")

    def end_suite(self):
        duration = time.time() - self.suite_start
        suite_results = [r for r in self.results if r.name.startswith(self.current_suite[:20])]
        passed = sum(1 for r in suite_results if r.status == TestStatus.PASSED)
        total = len(suite_results)
        print(f"\n📊 {self.current_suite}: {passed}/{total} 通过 (耗时: {duration:.2f}s)")

    def add_result(self, result: TestResult):
        self.results.append(result)
        status_icon = result.status.value
        print(f"  {status_icon} {result.name}: {result.message}")
        if result.details:
            for key, value in result.details.items():
                print(f"      {key}: {value}")

    def print_summary(self):
        print(f"\n{'=' * 80}")
        print("📊 测试总结")
        print(f"{'=' * 80}")

        passed = sum(1 for r in self.results if r.status == TestStatus.PASSED)
        failed = sum(1 for r in self.results if r.status == TestStatus.FAILED)
        skipped = sum(1 for r in self.results if r.status == TestStatus.SKIPPED)
        total = len(self.results)

        print(f"\n  总计: {passed} 通过, {failed} 失败, {skipped} 跳过 (共 {total})")

        if failed > 0:
            print("\n❌ 失败的测试:")
            for r in self.results:
                if r.status == TestStatus.FAILED:
                    print(f"   - {r.name}: {r.message}")

        print(f"\n{'=' * 80}")


# ============================================================
# v0.5.1 预取机制测试
# ============================================================

async def test_prefetch_mechanism(reporter: TestReporter):
    """测试预取机制"""
    reporter.start_suite("v0.5.1 预取机制测试")

    execution_times = []
    progress_updates = []

    async def prefetch_executor(data: Dict) -> Dict:
        start = time.time()
        await asyncio.sleep(data.get("duration", 0.05))
        execution_times.append(time.time() - start)
        return {"result": "ok", "task_id": data.get("task_id")}

    # 1. 预取器基本功能测试
    print("\n📌 测试 1.1: 预取器基本功能")
    config = TaskPoolConfig.memory()
    config.enable_prefetch = True
    config.prefetch_size = 10

    pool = TaskPool(executor=prefetch_executor, config=config)
    pool.start()
    await asyncio.sleep(0.5)

    # 提交一批任务
    task_count = 50
    for i in range(task_count):
        await pool.submit_async({
            "task_id": f"prefetch-{i}",
            "duration": 0.02
        })

    await asyncio.sleep(3)

    # 获取预取器统计
    prefetch_stats = {}
    if pool._worker_pool._prefetcher:
        prefetch_stats = pool._worker_pool._prefetcher.get_stats()
        total_fetched = prefetch_stats.get("total_fetched", 0)
        total_prefetch = prefetch_stats.get("total_prefetch", 0)

        reporter.add_result(TestResult(
            name="预取器基本功能",
            status=TestStatus.PASSED if total_fetched > 0 else TestStatus.FAILED,
            message=f"预取次数: {total_prefetch}, 获取任务数: {total_fetched}",
            details={
                "avg_batch_size": f"{prefetch_stats.get('avg_batch_size', 0):.2f}",
                "avg_latency_ms": f"{prefetch_stats.get('avg_latency_ms', 0):.2f}"
            }
        ))
    else:
        reporter.add_result(TestResult(
            name="预取器基本功能",
            status=TestStatus.SKIPPED,
            message="预取器未启用"
        ))

    pool.shutdown()
    await asyncio.sleep(0.5)

    # 2. 预取 vs 非预取性能对比
    print("\n📌 测试 1.2: 预取性能对比")

    async def run_test(enable_prefetch: bool, task_count: int = 100) -> float:
        config = TaskPoolConfig.memory()
        config.enable_prefetch = enable_prefetch
        config.prefetch_size = 20

        pool = TaskPool(executor=prefetch_executor, config=config)
        pool.start()
        await asyncio.sleep(0.5)

        start = time.time()
        for i in range(task_count):
            await pool.submit_async({
                "task_id": f"perf-{enable_prefetch}-{i}",
                "duration": 0.01
            })

        # 等待所有任务完成
        await asyncio.sleep(task_count * 0.015)
        elapsed = time.time() - start

        pool.shutdown()
        await asyncio.sleep(0.3)
        return elapsed

    try:
        # 运行预取模式测试
        prefetch_time = await run_test(True, 50)
        # 运行非预取模式测试
        no_prefetch_time = await run_test(False, 50)

        improvement = ((no_prefetch_time - prefetch_time) / no_prefetch_time) * 100

        reporter.add_result(TestResult(
            name="预取性能对比",
            status=TestStatus.PASSED,
            message=f"预取模式: {prefetch_time:.2f}s, 普通模式: {no_prefetch_time:.2f}s",
            details={"性能提升": f"{improvement:.1f}%"}
        ))
    except Exception as e:
        reporter.add_result(TestResult(
            name="预取性能对比",
            status=TestStatus.FAILED,
            message=str(e)
        ))

    reporter.end_suite()


# ============================================================
# v0.5.2 批量操作测试
# ============================================================

async def test_batch_operations(reporter: TestReporter):
    """测试批量操作"""
    reporter.start_suite("v0.5.2 批量操作测试")

    async def batch_executor(data: Dict) -> Dict:
        await asyncio.sleep(data.get("duration", 0.01))
        return {"result": "ok", "task_id": data.get("task_id")}

    # 1. 批量提交测试
    print("\n📌 测试 2.1: 批量任务提交")
    config = TaskPoolConfig.memory()
    pool = TaskPool(executor=batch_executor, config=config)
    pool.start()
    await asyncio.sleep(0.5)

    batch_tasks = [
        {"task_id": f"batch-{i}", "duration": 0.01}
        for i in range(20)
    ]

    start = time.time()
    task_ids = pool.submit_batch(batch_tasks, priority=TaskPriority.NORMAL)
    elapsed = time.time() - start

    # 验证所有任务都提交成功
    all_submitted = len(task_ids) == len(batch_tasks)

    reporter.add_result(TestResult(
        name="批量任务提交",
        status=TestStatus.PASSED if all_submitted else TestStatus.FAILED,
        message=f"提交 {len(task_ids)} 个任务, 耗时 {elapsed:.3f}s",
        details={"吞吐量": f"{len(task_ids) / elapsed:.1f} 任务/秒"}
    ))

    # 2. 批量状态查询
    print("\n📌 测试 2.2: 批量状态查询")

    # 等待任务完成
    await asyncio.sleep(1)

    statuses = {}
    for task_id in task_ids[:10]:
        status = await pool.get_status_async(task_id)
        statuses[task_id] = status

    completed_count = sum(1 for s in statuses.values() if s == "success")

    reporter.add_result(TestResult(
        name="批量状态查询",
        status=TestStatus.PASSED if completed_count >= 5 else TestStatus.FAILED,
        message=f"查询 {len(statuses)} 个任务, {completed_count} 个已完成"
    ))

    # 3. 批量等待
    print("\n📌 测试 2.3: 批量等待")

    start = time.time()
    results = pool.wait_all(task_ids[:10], timeout=5)
    elapsed = time.time() - start

    success_count = sum(1 for r in results.values() if r is not None and "error" not in str(r))

    reporter.add_result(TestResult(
        name="批量等待",
        status=TestStatus.PASSED if success_count >= 8 else TestStatus.FAILED,
        message=f"等待 {len(results)} 个任务, {success_count} 成功, 耗时 {elapsed:.2f}s"
    ))

    pool.shutdown()
    await asyncio.sleep(0.5)

    # 4. 批量重试测试
    print("\n📌 测试 2.4: 批量重试")

    retry_count = 0

    async def flaky_executor(data: Dict) -> Dict:
        nonlocal retry_count
        if data.get("should_fail", False) and retry_count < 2:
            retry_count += 1
            raise ValueError("Temporary failure")
        return {"result": "ok"}

    config2 = TaskPoolConfig.memory()
    pool2 = TaskPool(executor=flaky_executor, config=config2)
    pool2.start()
    await asyncio.sleep(0.5)

    # 提交会失败的任务
    task_id = await pool2.submit_async({
        "task_id": "flaky-task",
        "should_fail": True,
        "duration": 0.05
    })

    # 手动重试
    await asyncio.sleep(1)
    retry_success = pool2.retry(task_id)

    reporter.add_result(TestResult(
        name="批量重试",
        status=TestStatus.PASSED if retry_success else TestStatus.FAILED,
        message=f"重试任务: {task_id}"
    ))

    pool2.shutdown()
    await asyncio.sleep(0.5)

    reporter.end_suite()


# ============================================================
# v0.5.3 任务进度上报测试
# ============================================================

async def test_progress_reporting(reporter: TestReporter):
    """测试任务进度上报"""
    reporter.start_suite("v0.5.3 任务进度上报测试")

    progress_history = []

    class ProgressExecutor(TaskExecutor):
        def __init__(self):
            self.progress_callback = None

        async def execute(self, data: Dict) -> Dict:
            """标准执行方法"""
            # 调用带进度的版本
            return await self.execute_with_progress(data, self._dummy_callback)

        async def execute_with_progress(self, data: Dict, callback) -> Dict:
            """带进度回调的执行方法"""
            self.progress_callback = callback
            total_steps = 10
            for i in range(total_steps + 1):
                progress = i / total_steps
                if self.progress_callback:
                    await self.progress_callback(progress, f"步骤 {i}/{total_steps}")
                await asyncio.sleep(0.05)
            return {"result": "completed", "steps": total_steps}

        async def _dummy_callback(self, progress, message):
            pass

    # 1. 进度上报基本功能
    print("\n📌 测试 3.1: 进度上报基本功能")

    progress_executor = ProgressExecutor()
    config = TaskPoolConfig.memory()
    pool = TaskPool(executor=progress_executor, config=config)
    pool.start()
    await asyncio.sleep(0.5)

    # 注册进度事件监听
    def on_progress(event):
        if event.event_type == "task.progress":
            progress_history.append(event.data)

    if hasattr(pool, 'on_progress'):
        pool.on_progress(on_progress)
    else:
        # 方案B：直接订阅事件总线
        pool._event_bus.subscribe("task.progress", on_progress)

    task_id = await pool.submit_async({"task_id": "progress-test"})

    # 等待任务完成
    result = await pool.wait_for_result_async(task_id, timeout=10)

    # 检查是否收到进度更新
    progress_received = len(progress_history) > 0

    reporter.add_result(TestResult(
        name="进度上报基本功能",
        status=TestStatus.PASSED if progress_received else TestStatus.FAILED,
        message=f"收到 {len(progress_history)} 次进度更新",
        details={"最终进度": str(progress_history[-1]) if progress_history else "无"}
    ))

    pool.shutdown()
    await asyncio.sleep(0.5)

    # 2. 任务进度查询
    print("\n📌 测试 3.2: 任务进度查询")

    async def slow_executor(data: Dict) -> Dict:
        for i in range(1, 6):
            await asyncio.sleep(0.2)
        return {"result": "done"}

    config2 = TaskPoolConfig.memory()
    pool2 = TaskPool(executor=slow_executor, config=config2)
    pool2.start()
    await asyncio.sleep(0.5)

    task_id = await pool2.submit_async({"task_id": "slow-task"})

    # 查询任务进度
    await asyncio.sleep(0.5)
    task_info = await pool2.get_task_async(task_id)

    pool2.shutdown()
    await asyncio.sleep(0.5)

    reporter.add_result(TestResult(
        name="任务进度查询",
        status=TestStatus.PASSED if task_info else TestStatus.FAILED,
        message=f"任务信息: {task_info.get('status', 'unknown') if task_info else '不存在'}"
    ))

    reporter.end_suite()


# ============================================================
# v0.5.4 性能基准测试
# ============================================================

async def test_performance_benchmark(reporter: TestReporter):
    """性能基准测试"""
    reporter.start_suite("v0.5.4 性能基准测试")

    async def fast_executor(data: Dict) -> Dict:
        return {"result": "ok"}

    async def medium_executor(data: Dict) -> Dict:
        await asyncio.sleep(0.01)
        return {"result": "ok"}

    # 1. 吞吐量测试
    print("\n📌 测试 4.1: 吞吐量测试")

    config = TaskPoolConfig.memory()
    config.worker_concurrency = 10
    config.enable_prefetch = True
    config.prefetch_size = 50

    pool = TaskPool(executor=fast_executor, config=config)
    pool.start()
    await asyncio.sleep(0.5)

    task_count = 500
    start = time.time()

    for i in range(task_count):
        await pool.submit_async({"task_id": f"throughput-{i}"})

    await asyncio.sleep(2)
    elapsed = time.time() - start

    throughput = task_count / elapsed

    reporter.add_result(TestResult(
        name="吞吐量测试",
        status=TestStatus.PASSED,
        message=f"处理 {task_count} 个任务, 耗时 {elapsed:.2f}s",
        details={"吞吐量": f"{throughput:.0f} 任务/秒"}
    ))

    pool.shutdown()
    await asyncio.sleep(0.5)

    # 2. 延迟测试（P50/P95/P99）
    print("\n📌 测试 4.2: 延迟分布测试")

    latencies = []

    async def latency_executor(data: Dict) -> Dict:
        start_time = time.time()
        await asyncio.sleep(0.02)
        latencies.append(time.time() - start_time)
        return {"result": "ok"}

    config2 = TaskPoolConfig.memory()
    config2.worker_concurrency = 5
    pool2 = TaskPool(executor=latency_executor, config=config2)
    pool2.start()
    await asyncio.sleep(0.5)

    task_count = 100
    for i in range(task_count):
        await pool2.submit_async({"task_id": f"latency-{i}"})

    await asyncio.sleep(3)
    pool2.shutdown()

    if latencies:
        sorted_latencies = sorted(latencies)
        p50 = sorted_latencies[int(len(sorted_latencies) * 0.5)]
        p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
        p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]

        reporter.add_result(TestResult(
            name="延迟分布测试",
            status=TestStatus.PASSED,
            message=f"采样 {len(latencies)} 个任务",
            details={
                "P50 延迟": f"{p50 * 1000:.1f}ms",
                "P95 延迟": f"{p95 * 1000:.1f}ms",
                "P99 延迟": f"{p99 * 1000:.1f}ms"
            }
        ))
    else:
        reporter.add_result(TestResult(
            name="延迟分布测试",
            status=TestStatus.FAILED,
            message="无延迟数据"
        ))

    await asyncio.sleep(0.5)

    # 3. 并发压力测试
    print("\n📌 测试 4.3: 并发压力测试")

    async def concurrent_executor(data: Dict) -> Dict:
        await asyncio.sleep(0.05)
        return {"result": "ok"}

    config3 = TaskPoolConfig.memory()
    config3.worker_concurrency = 20

    pool3 = TaskPool(executor=concurrent_executor, config=config3)
    pool3.start()
    await asyncio.sleep(0.5)

    start = time.time()
    tasks = []
    for i in range(200):
        tasks.append(pool3.submit_async({"task_id": f"concurrent-{i}"}))

    task_ids = await asyncio.gather(*tasks)
    pool3.wait_all(task_ids, timeout=30)
    elapsed = time.time() - start

    concurrency_rate = 200 / elapsed

    reporter.add_result(TestResult(
        name="并发压力测试",
        status=TestStatus.PASSED,
        message=f"200 个并发任务, 耗时 {elapsed:.2f}s",
        details={"并发处理率": f"{concurrency_rate:.1f} 任务/秒"}
    ))

    pool3.shutdown()
    await asyncio.sleep(0.5)

    reporter.end_suite()


# ============================================================
# v0.5 集成测试入口
# ============================================================

async def run_v05_integration_test():
    """运行 v0.5 完整集成测试"""

    print("\n" + "=" * 80)
    print("🚀 NeoTask v0.5 集成测试 - 性能优化")
    print("=" * 80)
    print("\n测试内容:")
    print("  1. 预取机制 - 批量任务预取、本地队列缓存")
    print("  2. 批量操作 - 批量提交、批量状态更新")
    print("  3. 进度上报 - 长时间任务进度跟踪")
    print("  4. 性能基准 - 吞吐量、延迟、并发测试")

    reporter = TestReporter()

    # 运行所有测试
    await test_prefetch_mechanism(reporter)
    await asyncio.sleep(0.5)

    await test_batch_operations(reporter)
    await asyncio.sleep(0.5)

    await test_progress_reporting(reporter)
    await asyncio.sleep(0.5)

    await test_performance_benchmark(reporter)

    # 打印总结
    reporter.print_summary()

    # 返回测试是否全部通过
    passed = sum(1 for r in reporter.results if r.status == TestStatus.PASSED)
    failed = sum(1 for r in reporter.results if r.status == TestStatus.FAILED)

    return failed == 0


# ============================================================
# 快速测试入口
# ============================================================

async def quick_v05_test():
    """快速测试 v0.5 核心功能"""

    print("\n" + "=" * 60)
    print("⚡ NeoTask v0.5 快速测试")
    print("=" * 60)

    results = []

    # 1. 预取器快速测试
    print("\n📌 [v0.5.1] 预取器测试")

    async def fast_executor(data):
        return {"result": "ok"}

    config = TaskPoolConfig.memory()
    config.enable_prefetch = True
    config.prefetch_size = 10

    pool = TaskPool(executor=fast_executor, config=config)
    pool.start()
    await asyncio.sleep(0.3)

    # 批量提交任务
    for i in range(50):
        await pool.submit_async({"task_id": f"quick-{i}"})

    await asyncio.sleep(1)

    if pool._worker_pool._prefetcher:
        stats = pool._worker_pool._prefetcher.get_stats()
        results.append(("预取器", stats.get("total_fetched", 0) > 0))
        print(f"  ✅ 预取统计: 获取 {stats.get('total_fetched', 0)} 个任务")
    else:
        results.append(("预取器", False))
        print(f"  ❌ 预取器未启用")

    pool.shutdown()
    await asyncio.sleep(0.3)

    # 2. 批量操作快速测试
    print("\n📌 [v0.5.2] 批量操作测试")

    config2 = TaskPoolConfig.memory()
    pool2 = TaskPool(executor=fast_executor, config=config2)
    pool2.start()
    await asyncio.sleep(0.3)

    batch_tasks = [{"task_id": f"batched-{i}"} for i in range(20)]
    task_ids = pool2.submit_batch(batch_tasks)

    results.append(("批量提交", len(task_ids) == 20))
    print(f"  ✅ 批量提交: {len(task_ids)} 个任务")

    pool2.shutdown()
    await asyncio.sleep(0.3)

    # 3. 性能快速测试
    print("\n📌 [v0.5.3] 性能测试")

    config3 = TaskPoolConfig.memory()
    config3.worker_concurrency = 5
    pool3 = TaskPool(executor=fast_executor, config=config3)
    pool3.start()
    await asyncio.sleep(0.3)

    start = time.time()
    for i in range(100):
        await pool3.submit_async({"task_id": f"perf-{i}"})

    await asyncio.sleep(1)
    elapsed = time.time() - start

    throughput = 100 / elapsed
    results.append(("性能", throughput > 50))
    print(f"  ✅ 吞吐量: {throughput:.0f} 任务/秒")

    pool3.shutdown()

    # 总结
    print("\n" + "=" * 60)
    print("📊 快速测试结果:")
    passed = sum(1 for _, p in results if p)
    total = len(results)

    for name, passed_flag in results:
        status = "✅" if passed_flag else "❌"
        print(f"  {status} {name}")

    print(f"\n总计: {passed}/{total} 通过")
    print("=" * 60)

    return passed == total


# ============================================================
# 主入口

# 快速测试（核心功能）：python tests/v.x/test_integration_v05.py --quick

# 完整测试（详细性能基准）：python tests/v.x/test_integration_v05.py --full

# 使用 pytest 运行：python -m pytest tests/v.x/test_integration_v05.py -v -s

# 只运行特定测试套件：python -m pytest tests/v.x/test_integration_v05.py::test_prefetch_mechanism -v -s
# ============================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NeoTask v0.5 集成测试")
    parser.add_argument("--quick", action="store_true", help="运行快速测试")
    parser.add_argument("--full", action="store_true", help="运行完整测试")

    args = parser.parse_args()

    if args.quick:
        success = asyncio.run(quick_v05_test())
    else:
        success = asyncio.run(run_v05_integration_test())

    sys.exit(0 if success else 1)
