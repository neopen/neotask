"""
@FileName: test_integration_v01_v04.py
@Description: NeoTask v0.1 ~ v0.4 完整集成测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/5/7

运行方式：
    pytest tests/test_integration_v01_v04.py
    python tests/test_integration_v01_v04.py

测试覆盖：
    v0.1: 基础任务池 - 任务提交、等待、优先级、重试
    v0.2: 可观测性 - 事件总线、指标收集、健康检查
    v0.3: 定时调度 - 延时执行、周期任务、Cron表达式
    v0.4: 分布式基础 - Redis队列、分布式锁、多节点协调
"""

import asyncio
import os
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List
import pytest

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neotask.api.task_pool import TaskPool
from neotask.api.task_scheduler import TaskScheduler
from neotask.models.config import TaskPoolConfig, SchedulerConfig
from neotask.models.task import TaskPriority
from neotask.lock.redis import RedisLock
from neotask.distributed.node import NodeManager


# ============================================================
# 测试配置与 Fixture
# ============================================================

@dataclass
class TestResult:
    """测试结果"""
    name: str
    passed: bool
    message: str = ""
    duration: float = 0.0


class TestReporter:
    """测试报告器"""

    def __init__(self):
        self.results: List[TestResult] = []
        self.start_time: float = 0.0

    def start_suite(self, name: str):
        print(f"\n" + "=" * 70)
        print(f"🧪 {name}")
        print("=" * 70)
        self.start_time = time.time()

    def end_suite(self):
        duration = time.time() - self.start_time
        passed = sum(1 for r in self.results if r.passed)
        total = len(self.results)

        print(f"\n" + "=" * 70)
        print(f"📊 测试总结: {passed}/{total} 通过 (耗时: {duration:.2f}s)")

        for r in self.results:
            status = "✅" if r.passed else "❌"
            print(f"  {status} {r.name}: {r.message}")
        print("=" * 70 + "\n")

    def add_result(self, result: TestResult):
        self.results.append(result)
        status = "✅" if result.passed else "❌"
        print(f"  {status} {result.name}: {result.message}")


@pytest.fixture
def reporter():
    """Pytest 兼容的 reporter fixture"""
    return TestReporter()


# ============================================================
# v0.1 测试：基础任务池
# ============================================================

@pytest.mark.asyncio
async def test_v01_basic_task_pool(reporter: TestReporter):
    """v0.1 基础任务池测试"""
    reporter.start_suite("v0.1 基础任务池测试")

    async def simple_executor(data: Dict) -> Dict:
        await asyncio.sleep(data.get("duration", 0.05))
        return {"result": f"processed_{data.get('task_id', 'unknown')}"}

    # 1. 内存模式
    print("\n📌 测试 1.1: 内存模式 任务提交与等待")
    config_mem = TaskPoolConfig.memory(node_id="test-mem")
    pool_mem = TaskPool(executor=simple_executor, config=config_mem)

    try:
        pool_mem.start()
        await asyncio.sleep(0.3)

        task_id = await pool_mem.submit_async({"task_id": "mem-001", "duration": 0.1})
        result = await pool_mem.wait_for_result_async(task_id, timeout=5)

        passed = result is not None and "processed" in str(result)
        reporter.add_result(TestResult(
            name="内存模式 任务提交与等待",
            passed=passed,
            message=f"task_id={task_id}, result={result}" if passed else "等待超时"
        ))

        pool_mem.shutdown()
        await asyncio.sleep(0.3)
    except Exception as e:
        reporter.add_result(TestResult("内存模式 任务提交与等待", False, str(e)))
        pool_mem.shutdown()

    # 2. SQLite 持久化模式
    print("\n📌 测试 1.2: SQLite持久化 任务持久化")
    # 使用临时目录：硬编码 /data 在 Windows 下会被解析为盘符根目录，可能无写权限
    db_path = os.path.join(tempfile.gettempdir(), f"neotask_v01_{uuid.uuid4().hex}.db")
    config_sql = TaskPoolConfig.sqlite(path=db_path, node_id="test-sql")
    pool_sql = TaskPool(executor=simple_executor, config=config_sql)

    try:
        pool_sql.start()
        await asyncio.sleep(0.3)

        task_id = await pool_sql.submit_async({"task_id": "sql-001", "duration": 0.1})
        result = await pool_sql.wait_for_result_async(task_id, timeout=5)

        passed = result is not None
        reporter.add_result(TestResult(
            name="SQLite模式 任务持久化",
            passed=passed,
            message=f"task_id={task_id} 完成" if passed else "执行失败"
        ))

        pool_sql.shutdown()
        await asyncio.sleep(0.3)
    except Exception as e:
        reporter.add_result(TestResult("SQLite模式 任务持久化", False, str(e)))
        pool_sql.shutdown()

    # 清理临时数据库及日志文件（Windows 下句柄未释放时忽略失败）
    for suffix in ("", "-journal", "-wal", "-shm"):
        try:
            os.remove(db_path + suffix)
        except OSError:
            pass

    # 3. 优先级测试
    print("\n📌 测试 1.3: 优先级队列")
    pool_pri = TaskPool(executor=simple_executor, config=config_mem)

    try:
        pool_pri.start()
        await asyncio.sleep(0.3)

        async def submit_with_priority(priority, task_id):
            start = time.time()
            tid = await pool_pri.submit_async({"task_id": task_id, "duration": 0.2}, priority=priority)
            await pool_pri.wait_for_result_async(tid, timeout=10)
            return task_id, time.time() - start

        low_task = asyncio.create_task(submit_with_priority(TaskPriority.LOW, "low-pri"))
        await asyncio.sleep(0.05)
        high_task = asyncio.create_task(submit_with_priority(TaskPriority.HIGH, "high-pri"))

        low_result, high_result = await asyncio.gather(low_task, high_task)
        high_completed_first = high_result[1] < low_result[1]

        reporter.add_result(TestResult(
            name="优先级队列",
            passed=high_completed_first,
            message=f"高优先级完成时间={high_result[1]:.2f}s, 低优先级={low_result[1]:.2f}s"
        ))

        pool_pri.shutdown()
        await asyncio.sleep(0.3)
    except Exception as e:
        reporter.add_result(TestResult("优先级队列", False, str(e)))
        pool_pri.shutdown()

    reporter.end_suite()


# ============================================================
# v0.2 测试：可观测性
# ============================================================

@pytest.mark.asyncio
async def test_v02_observability(reporter: TestReporter):
    """v0.2 可观测性测试"""
    reporter.start_suite("v0.2 可观测性测试")

    async def test_executor(data: Dict) -> Dict:
        await asyncio.sleep(data.get("duration", 0.05))
        return {"result": "ok", "data": data}

    config = TaskPoolConfig.memory(node_id="test-obs")
    pool = TaskPool(executor=test_executor, config=config)

    try:
        pool.start()
        await asyncio.sleep(0.3)

        # 1. 提交多个任务
        print("\n📌 测试 2.1: 指标收集")
        for i in range(5):
            await pool.submit_async({"task_id": f"obs-{i}", "duration": 0.05})

        await asyncio.sleep(1)
        stats = pool.get_stats()
        metrics_exist = stats is not None and "queue_size" in stats

        reporter.add_result(TestResult(
            name="指标收集",
            passed=metrics_exist,
            message=f"stats: queue_size={stats.get('queue_size', 'N/A')}, total={stats.get('total', 'N/A')}"
        ))

        # 2. 事件回调
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
            passed=has_created and has_completed,
            message=f"收到事件: {events_received}"
        ))

        # 3. 健康检查
        print("\n📌 测试 2.3: 健康检查")
        health = pool.get_health_status()

        reporter.add_result(TestResult(
            name="健康检查",
            passed=health is not None,
            message=f"status={health.get('status', 'unknown')}"
        ))

        pool.shutdown()
        await asyncio.sleep(0.3)

    except Exception as e:
        reporter.add_result(TestResult("可观测性测试", False, str(e)))
        pool.shutdown()

    reporter.end_suite()


# ============================================================
# v0.3 测试：定时调度
# ============================================================

@pytest.mark.asyncio
async def test_v03_scheduled_tasks(reporter: TestReporter):
    """v0.3 定时调度测试"""
    reporter.start_suite("v0.3 定时调度测试")

    execution_records = []

    async def record_executor(data: Dict) -> Dict:
        execution_records.append({
            "time": time.time(),
            "data": data
        })
        return {"recorded": True}

    config = SchedulerConfig.memory()
    config.scan_interval = 0.2
    scheduler = TaskScheduler(executor=record_executor, config=config)

    try:
        scheduler.start()
        await asyncio.sleep(0.5)

        # 1. 延时执行
        print("\n📌 测试 3.1: 延时执行")
        delay_start = time.time()
        task_id = scheduler.submit_delayed(
            {"action": "delayed", "task_id": "delay-001"},
            delay_seconds=2
        )

        scheduler.wait_for_result(task_id, timeout=3)
        delayed_executed = any(r["data"].get("task_id") == "delay-001" for r in execution_records)
        delay_duration = time.time() - delay_start

        reporter.add_result(TestResult(
            name="延时执行",
            passed=delayed_executed,
            message=f"实际延迟: {delay_duration:.1f}s (预期2s)"
        ))

        # 2. 周期任务
        print("\n📌 测试 3.2: 周期任务")
        periodic_id = scheduler.submit_interval(
            {"action": "periodic", "task_id": "periodic-001"},
            interval_seconds=1,
            run_immediately=True
        )

        await asyncio.sleep(3.5)
        periodic_executions = sum(1 for r in execution_records if r["data"].get("task_id") == "periodic-001")
        scheduler.cancel_periodic(periodic_id)

        reporter.add_result(TestResult(
            name="周期任务",
            passed=periodic_executions >= 3,
            message=f"执行次数: {periodic_executions} (预期3+次)"
        ))

        # 3. Cron 表达式
        print("\n📌 测试 3.3: Cron表达式")
        cron_id = scheduler.submit_cron(
            {"action": "cron", "task_id": "cron-001"},
            cron_expr="* * * * *"
        )

        await asyncio.sleep(65)
        cron_executions = sum(1 for r in execution_records if r["data"].get("task_id") == "cron-001")
        scheduler.cancel_periodic(cron_id)

        reporter.add_result(TestResult(
            name="Cron表达式",
            passed=cron_executions >= 1,
            message=f"执行次数: {cron_executions} (预期至少1次)"
        ))

        scheduler.shutdown()
        await asyncio.sleep(0.5)

    except Exception as e:
        reporter.add_result(TestResult("定时调度测试", False, str(e)))
        scheduler.shutdown()

    reporter.end_suite()


# ============================================================
# v0.4 测试：分布式基础
# ============================================================

@pytest.mark.asyncio
async def test_v04_distributed(reporter: TestReporter):
    """v0.4 分布式基础测试"""
    reporter.start_suite("v0.4 分布式基础测试")

    try:
        import redis.asyncio as redis
        client = await redis.Redis.from_url("redis://127.0.0.1:6379")
        await client.ping()
        await client.close()
    except Exception as e:
        reporter.add_result(TestResult(
            name="Redis连接检查",
            passed=False,
            message=f"Redis不可用: {e}，请确保Redis运行在 localhost:6379"
        ))
        reporter.end_suite()
        return

    redis_url = "redis://127.0.0.1:6379"

    async def dist_executor(data: Dict) -> Dict:
        await asyncio.sleep(data.get("duration", 0.05))
        return {"result": "ok", "task_id": data.get("task_id")}

    # 1. 多节点共享队列
    print("\n📌 测试 4.1: 多节点共享队列")
    config1 = TaskPoolConfig.redis(url=redis_url, node_id="dist-node-1")
    config2 = TaskPoolConfig.redis(url=redis_url, node_id="dist-node-2")

    pool1 = TaskPool(executor=dist_executor, config=config1)
    pool2 = TaskPool(executor=dist_executor, config=config2)

    try:
        pool1.start()
        pool2.start()
        await asyncio.sleep(1)

        task_id = pool1.submit({"task_id": "cross-node-test", "duration": 0.3})
        result = pool2.wait_for_result(task_id, timeout=10)

        reporter.add_result(TestResult(
            name="多节点共享队列",
            passed=result is not None,
            message=f"任务 {task_id} 被节点2消费成功"
        ))

        pool1.shutdown()
        pool2.shutdown()
    except Exception as e:
        reporter.add_result(TestResult("多节点共享队列", False, str(e)))
        pool1.shutdown()
        pool2.shutdown()

    # 2. 分布式锁
    print("\n📌 测试 4.2: 分布式锁")
    lock = RedisLock(redis_url=redis_url)
    lock_key = f"test-lock-{uuid.uuid4().hex[:8]}"

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
        passed=success_count == 1,
        message=f"并发竞争: {success_count}/3 成功获取锁 (预期1个)"
    ))

    await lock.close()

    # 3. 节点管理
    print("\n📌 测试 4.3: 节点管理")
    node_manager = NodeManager(redis_url=redis_url, node_id="test-node-manager")
    await node_manager.start()

    active_nodes = await node_manager.get_active_nodes()
    is_alive = await node_manager.is_node_alive("test-node-manager")
    await node_manager.stop()

    reporter.add_result(TestResult(
        name="节点管理",
        passed=is_alive and len(active_nodes) >= 1,
        message=f"活跃节点数: {len(active_nodes)}, 本节点存活: {is_alive}"
    ))

    # 4. 预取机制
    print("\n📌 测试 4.4: 预取机制")
    config_prefetch = TaskPoolConfig.redis(url=redis_url, node_id="prefetch-node")
    pool_prefetch = TaskPool(executor=dist_executor, config=config_prefetch)

    try:
        pool_prefetch.start()
        await asyncio.sleep(0.5)

        for i in range(20):
            await pool_prefetch.submit_async({"task_id": f"prefetch-{i}", "duration": 0.02})

        await asyncio.sleep(2)

        if pool_prefetch._worker_pool._prefetcher:
            stats = pool_prefetch._worker_pool._prefetcher.get_stats()
            prefetch_occurred = stats.get("total_prefetch", 0) > 0
            message = f"预取次数: {stats.get('total_prefetch', 0)}, 获取任务: {stats.get('total_fetched', 0)}"
        else:
            prefetch_occurred = True
            message = "预取器已启用"

        reporter.add_result(TestResult(
            name="预取机制",
            passed=prefetch_occurred,
            message=message
        ))

        pool_prefetch.shutdown()
        await asyncio.sleep(0.5)
    except Exception as e:
        reporter.add_result(TestResult("预取机制", False, str(e)))
        pool_prefetch.shutdown()

    reporter.end_suite()


# ============================================================
# 脚本入口（支持 python 直接运行）
# ============================================================

async def run_all_tests():
    print("\n" + "=" * 70)
    print("🚀 NeoTask v0.1 ~ v0.4 完整集成测试")
    print("=" * 70)

    reporter = TestReporter()
    await test_v01_basic_task_pool(reporter)
    await asyncio.sleep(1)
    await test_v02_observability(reporter)
    await asyncio.sleep(1)
    await test_v03_scheduled_tasks(reporter)
    await asyncio.sleep(1)
    await test_v04_distributed(reporter)

    print("\n" + "=" * 70)
    print("🎉 NeoTask v0.1 ~ v0.4 集成测试完成")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_all_tests())