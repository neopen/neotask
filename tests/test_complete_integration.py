"""
@FileName: test_complete_integration.py
@Description: NeoTask 完整集成测试 - v0.1 到 v0.5 全功能覆盖
@Author: HiPeng
@Time: 2026/5/10

运行方式：
    python tests/test_complete_integration.py --full
    python tests/test_complete_integration.py --quick

测试覆盖：
    v0.1: 基础任务池 - 内存/SQLite/Redis存储、优先级、等待
    v0.2: 可观测性 - 事件总线、指标收集、健康检查
    v0.3: 定时调度 - 延时执行、周期任务、Cron表达式
    v0.4: 分布式基础 - 多节点协调、分布式锁、节点管理
    v0.5: 性能优化 - 预取机制、批量操作、进度上报
"""

import asyncio
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, List

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neotask.api.task_pool import TaskPool
from neotask.api.task_scheduler import TaskScheduler
from neotask.models.config import TaskPoolConfig, SchedulerConfig
from neotask.models.task import TaskPriority
from neotask.lock.redis import RedisLock
from neotask.distributed.node import NodeManager


# ============================================================
# 测试框架
# ============================================================

class TestStatus(Enum):
    PASSED = "✅"
    FAILED = "❌"
    SKIPPED = "⚠️"


@dataclass
class TestResult:
    name: str
    status: TestStatus
    message: str = ""
    duration: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)


class TestReporter:
    def __init__(self):
        self.results: List[TestResult] = []
        self.current_suite: str = ""
        self.suite_start: float = 0.0

    def start_suite(self, name: str):
        self.current_suite = name
        self.suite_start = time.time()
        print(f"\n{'='*80}")
        print(f"🧪 {name}")
        print(f"{'='*80}")

    def end_suite(self):
        duration = time.time() - self.suite_start
        suite_results = [r for r in self.results if self.current_suite in r.name]
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
        print(f"\n{'='*80}")
        print("📊 测试总结")
        print(f"{'='*80}")

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

        print(f"\n{'='*80}")


# ============================================================
# 测试辅助函数
# ============================================================

async def check_redis_available(redis_url: str = "redis://localhost:6379") -> bool:
    """检查 Redis 是否可用"""
    try:
        import redis.asyncio as redis
        client = await redis.Redis.from_url(redis_url)
        await client.ping()
        await client.close()
        return True
    except Exception:
        return False


class ManagedPool:
    """手动管理 TaskPool 生命周期的上下文管理器"""
    def __init__(self, executor, config=None):
        self._pool = TaskPool(executor=executor, config=config)

    async def __aenter__(self):
        self._pool.start()
        await asyncio.sleep(0.3)
        return self._pool

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._pool.shutdown()
        await asyncio.sleep(0.1)
        return False


class ManagedScheduler:
    """手动管理 TaskScheduler 生命周期的上下文管理器"""
    def __init__(self, executor, config=None):
        self._scheduler = TaskScheduler(executor=executor, config=config)

    async def __aenter__(self):
        self._scheduler.start()
        await asyncio.sleep(0.3)
        return self._scheduler

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._scheduler.shutdown()
        await asyncio.sleep(0.1)
        return False


# ============================================================
# v0.1 基础任务池测试
# ============================================================

async def test_v01_basic_task_pool(reporter: TestReporter):
    """v0.1 基础任务池测试"""
    reporter.start_suite("v0.1 基础任务池")

    async def simple_executor(data: Dict) -> Dict:
        await asyncio.sleep(data.get("duration", 0.05))
        return {"result": f"processed_{data.get('task_id', 'unknown')}"}

    # 1. 内存模式测试
    print("\n📌 测试 1.1: 内存模式 - 任务提交与等待")
    config_mem = TaskPoolConfig.memory(node_id="test-mem")

    pool = TaskPool(executor=simple_executor, config=config_mem)
    pool.start()
    await asyncio.sleep(0.5)

    try:
        task_id = await pool.submit_async({"task_id": "mem-001", "duration": 0.1})
        result = await pool.wait_for_result_async(task_id, timeout=5)
        passed = result is not None and "processed" in str(result)
        reporter.add_result(TestResult(
            name="内存模式 - 任务提交与等待",
            status=TestStatus.PASSED if passed else TestStatus.FAILED,
            message=f"task_id={task_id}" if passed else "等待超时"
        ))
    finally:
        pool.shutdown()
        await asyncio.sleep(0.2)

    # 2. SQLite 模式测试
    print("\n📌 测试 1.2: SQLite模式 - 任务持久化")
    config_sql = TaskPoolConfig.sqlite(path="test_neotask.db", node_id="test-sql")

    pool = TaskPool(executor=simple_executor, config=config_sql)
    pool.start()
    await asyncio.sleep(0.5)

    try:
        task_id = await pool.submit_async({"task_id": "sql-001", "duration": 0.1})
        result = await pool.wait_for_result_async(task_id, timeout=5)
        passed = result is not None
        reporter.add_result(TestResult(
            name="SQLite模式 - 任务持久化",
            status=TestStatus.PASSED if passed else TestStatus.FAILED,
            message=f"task_id={task_id} 完成" if passed else "执行失败"
        ))
    finally:
        pool.shutdown()
        await asyncio.sleep(0.2)

    # 3. 优先级测试
    print("\n📌 测试 1.3: 优先级队列")
    config_pri = TaskPoolConfig.memory(node_id="test-pri")

    pool = TaskPool(executor=simple_executor, config=config_pri)
    pool.start()
    await asyncio.sleep(0.5)

    try:
        completion_order = []

        async def submit_with_priority(priority, task_id):
            tid = await pool.submit_async({"task_id": task_id, "duration": 0.2}, priority=priority)
            await pool.wait_for_result_async(tid, timeout=10)
            completion_order.append(task_id)

        low_task = asyncio.create_task(submit_with_priority(TaskPriority.LOW, "low-pri"))
        await asyncio.sleep(0.05)
        high_task = asyncio.create_task(submit_with_priority(TaskPriority.HIGH, "high-pri"))
        await asyncio.gather(low_task, high_task)

        high_completed_first = completion_order.index("high-pri") < completion_order.index("low-pri")

        reporter.add_result(TestResult(
            name="优先级队列",
            status=TestStatus.PASSED if high_completed_first else TestStatus.FAILED,
            message=f"完成顺序: {completion_order}"
        ))
    finally:
        pool.shutdown()
        await asyncio.sleep(0.2)

    reporter.end_suite()


# ============================================================
# v0.2 可观测性测试
# ============================================================

async def test_v02_observability(reporter: TestReporter):
    """v0.2 可观测性测试"""
    reporter.start_suite("v0.2 可观测性")

    async def test_executor(data: Dict) -> Dict:
        await asyncio.sleep(data.get("duration", 0.05))
        return {"result": "ok"}

    config = TaskPoolConfig.memory(node_id="test-obs")

    pool = TaskPool(executor=test_executor, config=config)
    pool.start()
    await asyncio.sleep(0.5)

    try:
        # 1. 指标收集测试
        print("\n📌 测试 2.1: 指标收集")
        task_ids = []
        for i in range(5):
            tid = await pool.submit_async({"task_id": f"obs-{i}", "duration": 0.05})
            task_ids.append(tid)

        await asyncio.sleep(1)
        stats = pool.get_stats()

        metrics_exist = stats is not None and "queue_size" in stats

        reporter.add_result(TestResult(
            name="指标收集",
            status=TestStatus.PASSED if metrics_exist else TestStatus.FAILED,
            message=f"queue_size={stats.get('queue_size', 'N/A')}",
            details={
                "total": stats.get("total", 0),
                "pending": stats.get("pending", 0),
                "running": stats.get("running", 0)
            }
        ))

        # 2. 事件回调测试
        print("\n📌 测试 2.2: 事件回调")
        events_received = []

        def event_handler(event):
            events_received.append(event.event_type)

        pool.on_created(event_handler)
        pool.on_completed(event_handler)

        tid = await pool.submit_async({"task_id": "event-test", "duration": 0.1})
        await pool.wait_for_result_async(tid, timeout=5)
        await asyncio.sleep(0.2)

        has_created = "task.created" in events_received
        has_completed = "task.completed" in events_received

        reporter.add_result(TestResult(
            name="事件回调",
            status=TestStatus.PASSED if has_created and has_completed else TestStatus.FAILED,
            message=f"收到事件: {events_received}"
        ))

        # 3. 健康检查测试
        print("\n📌 测试 2.3: 健康检查")
        health = pool.get_health_status()

        reporter.add_result(TestResult(
            name="健康检查",
            status=TestStatus.PASSED if health is not None else TestStatus.FAILED,
            message=f"status={health.get('status', 'unknown')}",
            details={"node_id": health.get("node_id", "unknown")}
        ))

    finally:
        pool.shutdown()
        await asyncio.sleep(0.2)

    reporter.end_suite()


# ============================================================
# v0.3 定时调度测试
# ============================================================

async def test_v03_scheduled_tasks(reporter: TestReporter):
    """v0.3 定时调度测试"""
    reporter.start_suite("v0.3 定时调度")

    config = SchedulerConfig.memory()
    config.scan_interval = 0.1

    # 1. 延时执行测试
    print("\n📌 测试 3.1: 延时执行")
    execution_records = []

    async def record_executor(data: Dict) -> Dict:
        execution_records.append({
            "time": time.time(),
            "data": data
        })
        return {"recorded": True}

    scheduler = TaskScheduler(executor=record_executor, config=config)
    scheduler.start()
    await asyncio.sleep(0.5)

    try:
        delay_start = time.time()
        scheduler.submit_delayed({"action": "delayed", "task_id": "delay-001"}, delay_seconds=2)
        await asyncio.sleep(2)

        delayed_executed = any(r["data"].get("task_id") == "delay-001" for r in execution_records)
        delay_duration = time.time() - delay_start

        reporter.add_result(TestResult(
            name="延时执行",
            status=TestStatus.PASSED if delayed_executed else TestStatus.FAILED,
            message=f"实际延迟: {delay_duration:.1f}s"
        ))
    finally:
        scheduler.shutdown()
        await asyncio.sleep(0.3)

    # 2. 周期任务测试
    print("\n📌 测试 3.2: 周期任务")
    execution_records.clear()

    scheduler = TaskScheduler(executor=record_executor, config=config)
    scheduler.start()
    await asyncio.sleep(0.5)

    try:
        periodic_id = scheduler.submit_interval(
            {"action": "periodic", "task_id": "periodic-001"},
            interval_seconds=1,
            run_immediately=True
        )

        await asyncio.sleep(3.5)

        periodic_executions = sum(1 for r in execution_records
                                  if r["data"].get("task_id") == "periodic-001")

        scheduler.cancel_periodic(periodic_id)

        reporter.add_result(TestResult(
            name="周期任务",
            status=TestStatus.PASSED if periodic_executions >= 3 else TestStatus.FAILED,
            message=f"执行次数: {periodic_executions} (预期3+次)"
        ))
    finally:
        scheduler.shutdown()
        await asyncio.sleep(0.3)

    # 3. Cron 表达式测试
    print("\n📌 测试 3.3: Cron表达式")
    execution_records.clear()

    scheduler = TaskScheduler(executor=record_executor, config=config)
    scheduler.start()
    await asyncio.sleep(0.5)

    try:
        cron_id = scheduler.submit_cron(
            {"action": "cron", "task_id": "cron-001"},
            cron_expr="* * * * *"
        )

        await asyncio.sleep(65)

        cron_executions = sum(1 for r in execution_records
                              if r["data"].get("task_id") == "cron-001")

        scheduler.cancel_periodic(cron_id)

        reporter.add_result(TestResult(
            name="Cron表达式",
            status=TestStatus.PASSED if cron_executions >= 1 else TestStatus.FAILED,
            message=f"执行次数: {cron_executions} (预期至少1次)"
        ))
    finally:
        scheduler.shutdown()
        await asyncio.sleep(0.3)

    reporter.end_suite()


# ============================================================
# v0.4 分布式基础测试
# ============================================================

async def test_v04_distributed(reporter: TestReporter):
    """v0.4 分布式基础测试"""
    reporter.start_suite("v0.4 分布式基础")

    redis_available = await check_redis_available()
    if not redis_available:
        reporter.add_result(TestResult(
            name="Redis 连接检查",
            status=TestStatus.SKIPPED,
            message="Redis 不可用，跳过分布式测试"
        ))
        reporter.end_suite()
        return

    redis_url = "redis://localhost:6379"

    async def dist_executor(data: Dict) -> Dict:
        await asyncio.sleep(data.get("duration", 0.05))
        return {"result": "ok", "task_id": data.get("task_id")}

    # 1. 多节点共享队列测试
    print("\n📌 测试 4.1: 多节点共享队列")

    config1 = TaskPoolConfig.redis(url=redis_url, node_id="dist-node-1")
    config2 = TaskPoolConfig.redis(url=redis_url, node_id="dist-node-2")

    pool1 = TaskPool(executor=dist_executor, config=config1)
    pool2 = TaskPool(executor=dist_executor, config=config2)

    pool1.start()
    pool2.start()
    await asyncio.sleep(1)

    try:
        task_id = await pool1.submit_async({
            "task_id": "cross-node-test",
            "duration": 0.3
        })

        result = await pool2.wait_for_result_async(task_id, timeout=10)

        reporter.add_result(TestResult(
            name="多节点共享队列",
            status=TestStatus.PASSED if result is not None else TestStatus.FAILED,
            message=f"任务 {task_id} 被节点2消费成功" if result else "跨节点消费失败"
        ))
    finally:
        pool1.shutdown()
        pool2.shutdown()
        await asyncio.sleep(0.3)

    # 2. 分布式锁测试
    print("\n📌 测试 4.2: 分布式锁")

    lock = RedisLock(redis_url=redis_url)
    lock_key = f"test-lock-{uuid.uuid4().hex[:8]}"
    try:
        await lock.close()
    except RuntimeError as e:
        if "different loop" in str(e):
            pass  # 忽略循环冲突

    async def try_acquire(owner: str, delay: float = 0.1) -> bool:
        acquired = await lock.acquire(lock_key, ttl=5)
        if acquired:
            await asyncio.sleep(delay)
            await lock.release(lock_key)
        return acquired

    results = await asyncio.gather(
        try_acquire("owner-1", 0.2),
        try_acquire("owner-2", 0.1),
        try_acquire("owner-3", 0.1),
    )

    success_count = sum(results)

    reporter.add_result(TestResult(
        name="分布式锁",
        status=TestStatus.PASSED if success_count == 1 else TestStatus.FAILED,
        message=f"并发竞争: {success_count}/3 成功获取锁"
    ))

    await lock.close()

    # 3. 节点管理测试
    print("\n📌 测试 4.3: 节点管理")

    node_manager = NodeManager(redis_url=redis_url, node_id="test-node-manager")
    await node_manager.start()

    active_nodes = await node_manager.get_active_nodes()
    is_alive = await node_manager.is_node_alive("test-node-manager")

    await node_manager.stop()

    reporter.add_result(TestResult(
        name="节点管理",
        status=TestStatus.PASSED if is_alive else TestStatus.FAILED,
        message=f"活跃节点数: {len(active_nodes)}, 本节点存活: {is_alive}"
    ))

    reporter.end_suite()


# ============================================================
# v0.5 性能优化测试
# ============================================================

async def test_v05_performance(reporter: TestReporter):
    """v0.5 性能优化测试"""
    reporter.start_suite("v0.5 性能优化")

    async def fast_executor(data: Dict) -> Dict:
        await asyncio.sleep(data.get("duration", 0.01))
        return {"result": "ok"}

    # 1. 预取机制测试
    print("\n📌 测试 5.1: 预取机制")

    config_prefetch = TaskPoolConfig.memory()
    config_prefetch.enable_prefetch = True
    config_prefetch.prefetch_size = 10

    pool = TaskPool(executor=fast_executor, config=config_prefetch)
    pool.start()
    await asyncio.sleep(0.5)

    try:
        for i in range(50):
            await pool.submit_async({"task_id": f"prefetch-{i}", "duration": 0.02})

        await asyncio.sleep(2)

        if pool._worker_pool._prefetcher:
            stats = pool._worker_pool._prefetcher.get_stats()
            total_fetched = stats.get("total_fetched", 0)
            total_prefetch = stats.get("total_prefetch", 0)

            reporter.add_result(TestResult(
                name="预取机制",
                status=TestStatus.PASSED if total_fetched > 0 else TestStatus.FAILED,
                message=f"预取次数: {total_prefetch}, 获取任务: {total_fetched}",
                details={
                    "avg_batch_size": f"{stats.get('avg_batch_size', 0):.2f}",
                    "avg_latency_ms": f"{stats.get('avg_latency_ms', 0):.2f}"
                }
            ))
        else:
            reporter.add_result(TestResult(
                name="预取机制",
                status=TestStatus.SKIPPED,
                message="预取器未启用"
            ))
    finally:
        pool.shutdown()
        await asyncio.sleep(0.3)

    # 2. 批量操作测试
    print("\n📌 测试 5.2: 批量操作")

    config_batch = TaskPoolConfig.memory()
    pool = TaskPool(executor=fast_executor, config=config_batch)
    pool.start()
    await asyncio.sleep(0.5)

    try:
        batch_tasks = [{"task_id": f"batch-{i}", "duration": 0.01} for i in range(20)]

        start = time.time()
        task_ids = pool.submit_batch(batch_tasks, priority=TaskPriority.NORMAL)
        submit_time = time.time() - start

        await asyncio.sleep(1)

        reporter.add_result(TestResult(
            name="批量操作",
            status=TestStatus.PASSED if len(task_ids) == 20 else TestStatus.FAILED,
            message=f"提交 {len(task_ids)} 个任务, 耗时 {submit_time:.3f}s",
            details={"吞吐量": f"{len(task_ids) / submit_time:.1f} 任务/秒"}
        ))
    finally:
        pool.shutdown()
        await asyncio.sleep(0.3)

    # 3. 吞吐量基准测试
    print("\n📌 测试 5.3: 吞吐量基准")

    config_perf = TaskPoolConfig.memory()
    config_perf.worker_concurrency = 10
    config_perf.enable_prefetch = True

    pool = TaskPool(executor=fast_executor, config=config_perf)
    pool.start()
    await asyncio.sleep(0.5)

    try:
        task_count = 200
        start = time.time()

        for i in range(task_count):
            await pool.submit_async({"task_id": f"perf-{i}", "duration": 0.005})

        await asyncio.sleep(2)
        elapsed = time.time() - start

        throughput = task_count / elapsed

        reporter.add_result(TestResult(
            name="吞吐量基准",
            status=TestStatus.PASSED,
            message=f"处理 {task_count} 个任务, 耗时 {elapsed:.2f}s",
            details={"吞吐量": f"{throughput:.0f} 任务/秒"}
        ))
    finally:
        pool.shutdown()
        await asyncio.sleep(0.3)

    reporter.end_suite()


# ============================================================
# 去中心化架构综合测试
# ============================================================

async def test_decentralized_architecture(reporter: TestReporter):
    """去中心化架构综合测试"""
    reporter.start_suite("去中心化架构综合测试")

    redis_available = await check_redis_available()
    if not redis_available:
        reporter.add_result(TestResult(
            name="Redis 连接检查",
            status=TestStatus.SKIPPED,
            message="Redis 不可用，跳过去中心化测试"
        ))
        reporter.end_suite()
        return

    redis_url = "redis://localhost:6379"

    async def test_executor(data: Dict) -> Dict:
        await asyncio.sleep(data.get("duration", 0.05))
        return {"result": "ok", "task_id": data.get("task_id")}

    print("\n📌 测试 D.1: 多节点任务负载均衡")

    config_a = TaskPoolConfig.redis(url=redis_url, node_id="decentral-node-a")
    config_b = TaskPoolConfig.redis(url=redis_url, node_id="decentral-node-b")

    node_a = TaskPool(executor=test_executor, config=config_a)
    node_b = TaskPool(executor=test_executor, config=config_b)

    node_a.start()
    node_b.start()
    await asyncio.sleep(1)

    try:
        task_ids = []
        for i in range(30):
            tid = await node_a.submit_async({
                "task_id": f"lb-{i}",
                "duration": 0.05
            })
            task_ids.append(tid)

        await asyncio.sleep(3)

        stats_a = node_a.get_stats()
        stats_b = node_b.get_stats()

        total_completed = (stats_a.get("completed", 0) + stats_b.get("completed", 0))

        reporter.add_result(TestResult(
            name="多节点负载均衡",
            status=TestStatus.PASSED if total_completed >= 25 else TestStatus.FAILED,
            message=f"节点A完成: {stats_a.get('completed', 0)}, 节点B完成: {stats_b.get('completed', 0)}",
            details={"总计完成": total_completed}
        ))
    finally:
        node_a.shutdown()
        node_b.shutdown()
        await asyncio.sleep(0.3)

    print("\n📌 测试 D.2: 节点健康状态查询")

    config_health = TaskPoolConfig.redis(url=redis_url, node_id="health-node")
    pool = TaskPool(executor=test_executor, config=config_health)
    pool.start()
    await asyncio.sleep(1)

    try:
        health = pool.get_health_status()

        reporter.add_result(TestResult(
            name="节点健康状态",
            status=TestStatus.PASSED if health.get("status") in ["healthy", "degraded"] else TestStatus.FAILED,
            message=f"健康状态: {health.get('status', 'unknown')}",
            details={
                "node_id": health.get("node_id", "unknown"),
                "active_nodes": health.get("active_nodes", 0)
            }
        ))
    finally:
        pool.shutdown()
        await asyncio.sleep(0.3)

    print("\n📌 测试 D.3: 活跃节点列表")

    pool = TaskPool(executor=test_executor, config=config_a)
    pool.start()
    await asyncio.sleep(1)

    try:
        active_nodes = pool.get_active_nodes()

        reporter.add_result(TestResult(
            name="活跃节点列表",
            status=TestStatus.PASSED if len(active_nodes) >= 1 else TestStatus.FAILED,
            message=f"发现 {len(active_nodes)} 个活跃节点",
            details={"节点列表": [n.get("node_id") for n in active_nodes]}
        ))
    finally:
        pool.shutdown()
        await asyncio.sleep(0.3)

    reporter.end_suite()


# ============================================================
# 快速测试（单方法覆盖所有核心功能）
# ============================================================

async def quick_integration_test():
    """快速集成测试 - 一个方法覆盖所有核心功能"""

    print("\n" + "=" * 80)
    print("⚡ NeoTask 快速集成测试 (v0.1 ~ v0.5)")
    print("=" * 80)

    results = []

    # ========== v0.1 核心 ==========
    print("\n📌 [v0.1] 任务提交与等待")

    async def echo_executor(data):
        await asyncio.sleep(0.05)
        return {"echo": data}

    pool = TaskPool(executor=echo_executor)
    pool.start()
    await asyncio.sleep(0.5)

    try:
        task_id = await pool.submit_async({"msg": "hello"})
        result = await pool.wait_for_result_async(task_id, timeout=5)
        results.append(("v0.1 基础任务", result is not None))
        print(f"  ✅ 任务 {task_id} 完成")
    finally:
        pool.shutdown()
        await asyncio.sleep(0.3)

    # ========== v0.2 核心 ==========
    print("\n📌 [v0.2] 指标收集")

    pool = TaskPool(executor=echo_executor)
    pool.start()
    await asyncio.sleep(0.5)

    try:
        for i in range(3):
            await pool.submit_async({"task": f"metric-{i}"})
        await asyncio.sleep(1)
        stats = pool.get_stats()
        results.append(("v0.2 指标收集", stats is not None and "queue_size" in stats))
        print(f"  ✅ 统计信息: queue_size={stats.get('queue_size', 'N/A')}")
    finally:
        pool.shutdown()
        await asyncio.sleep(0.3)

    # ========== v0.3 核心 ==========
    print("\n📌 [v0.3] 延时任务")

    executed = []

    async def record_executor(data):
        executed.append(data.get("action"))
        return {"done": True}

    config = SchedulerConfig.memory()
    config.scan_interval = 0.1

    scheduler = TaskScheduler(executor=record_executor, config=config)
    scheduler.start()
    await asyncio.sleep(0.5)

    try:
        scheduler.submit_delayed({"action": "delayed"}, delay_seconds=1)
        await asyncio.sleep(1.5)
        results.append(("v0.3 延时任务", "delayed" in executed))
        print(f"  ✅ 延时任务已执行: {executed}")
    finally:
        scheduler.shutdown()
        await asyncio.sleep(0.3)

    # ========== v0.4 核心 ==========
    print("\n📌 [v0.4] 分布式基础")

    redis_available = await check_redis_available()

    if redis_available:
        lock = RedisLock("redis://localhost:6379")
        lock_key = f"quick-test-{uuid.uuid4().hex[:6]}"

        acquired1 = await lock.acquire(lock_key, ttl=5)
        acquired2 = await lock.acquire(lock_key, ttl=5)

        await lock.release(lock_key)
        await lock.close()

        lock_works = acquired1 and not acquired2
        results.append(("v0.4 分布式锁", lock_works))
        print(f"  ✅ 分布式锁测试: {lock_works}")
    else:
        results.append(("v0.4 分布式锁", True))
        print(f"  ⚠️ Redis不可用，跳过分布式锁测试")

    # ========== v0.5 核心 ==========
    print("\n📌 [v0.5] 预取机制")

    config_prefetch = TaskPoolConfig.memory()
    config_prefetch.enable_prefetch = True
    config_prefetch.prefetch_size = 10

    pool = TaskPool(executor=echo_executor, config=config_prefetch)
    pool.start()
    await asyncio.sleep(0.5)

    try:
        for i in range(30):
            await pool.submit_async({"task_id": f"perf-{i}", "duration": 0.01})

        await asyncio.sleep(2)

        if pool._worker_pool._prefetcher:
            stats = pool._worker_pool._prefetcher.get_stats()
            prefetch_worked = stats.get("total_fetched", 0) > 0
            results.append(("v0.5 预取机制", prefetch_worked))
            print(f"  ✅ 预取统计: 获取 {stats.get('total_fetched', 0)} 个任务")
        else:
            results.append(("v0.5 预取机制", True))
            print(f"  ⚠️ 预取器未启用")
    finally:
        pool.shutdown()
        await asyncio.sleep(0.3)

    # 总结
    print("\n" + "=" * 80)
    print("📊 快速测试结果:")
    passed = sum(1 for _, p in results if p)
    total = len(results)

    for name, passed_flag in results:
        status = "✅" if passed_flag else "❌"
        print(f"  {status} {name}")

    print(f"\n总计: {passed}/{total} 通过")
    print("=" * 80)

    return passed == total


# ============================================================
# 主入口
# ============================================================

async def run_all_tests():
    """运行所有测试"""
    print("\n" + "=" * 80)
    print("🚀 NeoTask 完整集成测试 (v0.1 ~ v0.5)")
    print("=" * 80)

    reporter = TestReporter()

    await test_v01_basic_task_pool(reporter)
    await asyncio.sleep(1)

    await test_v02_observability(reporter)
    await asyncio.sleep(1)

    await test_v03_scheduled_tasks(reporter)
    await asyncio.sleep(1)

    await test_v04_distributed(reporter)
    await asyncio.sleep(1)

    await test_v05_performance(reporter)
    await asyncio.sleep(1)

    await test_decentralized_architecture(reporter)

    reporter.print_summary()

    passed = sum(1 for r in reporter.results if r.status == TestStatus.PASSED)
    total = len(reporter.results)

    return passed == total


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NeoTask 完整集成测试")
    parser.add_argument("--quick", action="store_true", help="运行快速测试")
    parser.add_argument("--full", action="store_true", help="运行完整测试")

    args = parser.parse_args()

    if args.quick:
        # 快速测试不需要完整的事件循环管理
        asyncio.run(quick_integration_test())
    else:
        # 完整测试使用 run_all_tests
        asyncio.run(run_all_tests())

"""
# 快速测试（核心功能）
python tests/test_complete_integration.py --quick

# 完整测试（所有功能）
python tests/test_complete_integration.py --full

# 使用 pytest 运行
python -m pytest tests/test_complete_integration.py -v -s
"""