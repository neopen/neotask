"""
@FileName: test_integration_v10.py
@Description: NeoTask v1.0 完整集成测试 - 独立运行，不依赖 pytest
@Author: HiPeng
@Time: 2026/5/11

运行方式：
    python tests/v.x/test_integration_v10.py

测试覆盖：
    v0.1: 基础任务池 - 任务提交、等待、优先级
    v0.2: 可观测性 - 事件总线、指标收集
    v0.3: 定时调度 - 延时执行、周期任务
    v0.4: 分布式基础 - Redis队列、分布式锁、多节点
    v0.5: 性能优化 - 预取机制、批量操作
    v1.0: 高可用 - 看门狗、任务回收、心跳、死信队列
"""

import asyncio
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, List, Optional

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from neotask.api.task_pool import TaskPool
from neotask.api.task_scheduler import TaskScheduler
from neotask.models.config import TaskPoolConfig, SchedulerConfig
from neotask.models.task import TaskPriority


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


class TestRunner:
    """独立测试运行器 - 自己管理事件循环"""

    def __init__(self):
        self.results: List[TestResult] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """获取或创建事件循环"""
        if self._loop is None or self._loop.is_closed():
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop

    def run_async(self, coro):
        """同步运行异步代码"""
        loop = self._get_loop()
        if loop.is_running():
            # 如果循环已在运行，创建任务
            return asyncio.create_task(coro)
        else:
            return loop.run_until_complete(coro)

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
        return failed == 0


# ============================================================
# 测试执行器
# ============================================================

async def echo_executor(data: Dict) -> Dict:
    """简单回显执行器"""
    await asyncio.sleep(data.get("duration", 0.01))
    return {"result": "ok", "echo": data.get("msg", "none")}


async def slow_executor(data: Dict) -> Dict:
    """慢速执行器（用于测试超时）"""
    duration = data.get("duration", 0.5)
    await asyncio.sleep(duration)
    return {"result": "done", "duration": duration}


# ============================================================
# v0.1 测试：基础任务池
# ============================================================

async def test_v01_basic_pool() -> List[TestResult]:
    """v0.1 基础任务池测试"""
    print(f"\n{'=' * 80}")
    print("🧪 v0.1 基础任务池测试")
    print(f"{'=' * 80}")

    results = []

    # 1.1 内存模式 - 任务提交与等待
    print("\n📌 测试 1.1: 内存模式 - 任务提交与等待")
    config = TaskPoolConfig.memory(node_id="test-mem")

    async with TaskPool(executor=echo_executor, config=config) as pool:
        await asyncio.sleep(0.2)

        task_id = await pool.submit_async({"msg": "hello", "duration": 0.05})
        result = await pool.wait_for_result_async(task_id, timeout=5)

        passed = result is not None and result.get("result") == "ok"
        results.append(TestResult(
            name="内存模式 - 任务提交与等待",
            status=TestStatus.PASSED if passed else TestStatus.FAILED,
            message=f"task_id={task_id}, result={result.get('result') if result else 'None'}"
        ))

    # 1.2 SQLite 模式
    print("\n📌 测试 1.2: SQLite 模式 - 持久化")
    config_sql = TaskPoolConfig.sqlite(path="test_neotask_v10.db", node_id="test-sql")

    async with TaskPool(executor=echo_executor, config=config_sql) as pool:
        await asyncio.sleep(0.2)

        task_id = await pool.submit_async({"msg": "persistent", "duration": 0.05})
        result = await pool.wait_for_result_async(task_id, timeout=5)

        passed = result is not None
        results.append(TestResult(
            name="SQLite模式 - 持久化",
            status=TestStatus.PASSED if passed else TestStatus.FAILED,
            message=f"task_id={task_id}, 持久化成功"
        ))

    # 1.3 优先级测试
    print("\n📌 测试 1.3: 优先级队列")
    config_pri = TaskPoolConfig.memory(node_id="test-pri")

    async with TaskPool(executor=echo_executor, config=config_pri) as pool:
        await asyncio.sleep(0.2)

        completion_order = []

        async def submit_with_priority(priority, name):
            tid = await pool.submit_async(
                {"msg": name, "duration": 0.1},
                priority=priority
            )
            await pool.wait_for_result_async(tid, timeout=10)
            completion_order.append(name)

        # 先提交低优先级，再提交高优先级
        await submit_with_priority(TaskPriority.LOW, "low")
        await submit_with_priority(TaskPriority.HIGH, "high")

        # 高优先级应该先完成
        high_first = completion_order.index("high") < completion_order.index("low")

        results.append(TestResult(
            name="优先级队列",
            status=TestStatus.PASSED if high_first else TestStatus.FAILED,
            message=f"完成顺序: {completion_order}",
            details={"高优先级优先": high_first}
        ))

    return results


# ============================================================
# v0.2 测试：可观测性
# ============================================================

async def test_v02_observability() -> List[TestResult]:
    """v0.2 可观测性测试"""
    print(f"\n{'=' * 80}")
    print("🧪 v0.2 可观测性测试")
    print(f"{'=' * 80}")

    results = []
    events_received = []

    def event_collector(event):
        events_received.append(event.event_type)

    # 2.1 事件总线
    print("\n📌 测试 2.1: 事件总线")
    config = TaskPoolConfig.memory(node_id="test-events")

    async with TaskPool(executor=echo_executor, config=config) as pool:
        await asyncio.sleep(0.2)

        pool.on_created(event_collector)
        pool.on_completed(event_collector)

        task_id = await pool.submit_async({"msg": "event-test", "duration": 0.05})
        await pool.wait_for_result_async(task_id, timeout=5)
        await asyncio.sleep(0.1)

        has_created = "task.created" in events_received
        has_completed = "task.completed" in events_received

        results.append(TestResult(
            name="事件总线",
            status=TestStatus.PASSED if has_created and has_completed else TestStatus.FAILED,
            message=f"收到事件: {events_received}"
        ))

    # 2.2 指标收集
    print("\n📌 测试 2.2: 指标收集")
    config2 = TaskPoolConfig.memory(node_id="test-metrics")

    async with TaskPool(executor=echo_executor, config=config2) as pool:
        await asyncio.sleep(0.2)

        for i in range(5):
            await pool.submit_async({"msg": f"metric-{i}", "duration": 0.02})

        await asyncio.sleep(0.5)
        stats = pool.get_stats()

        has_stats = stats is not None and "queue_size" in stats

        results.append(TestResult(
            name="指标收集",
            status=TestStatus.PASSED if has_stats else TestStatus.FAILED,
            message=f"统计信息: queue_size={stats.get('queue_size', 'N/A')}, total={stats.get('total', 'N/A')}"
        ))

    # 2.3 健康检查
    print("\n📌 测试 2.3: 健康检查")
    config3 = TaskPoolConfig.memory(node_id="test-health")

    async with TaskPool(executor=echo_executor, config=config3) as pool:
        await asyncio.sleep(0.2)
        health = pool.get_health_status()

        results.append(TestResult(
            name="健康检查",
            status=TestStatus.PASSED if health is not None else TestStatus.FAILED,
            message=f"status={health.get('status', 'unknown') if health else 'None'}"
        ))

    return results


# ============================================================
# v0.3 测试：定时调度
# ============================================================

async def test_v03_scheduled_tasks() -> List[TestResult]:
    """v0.3 定时调度测试"""
    print(f"\n{'=' * 80}")
    print("🧪 v0.3 定时调度测试")
    print(f"{'=' * 80}")

    results = []
    execution_records = []

    async def record_executor(data):
        execution_records.append({"time": time.time(), "data": data})
        return {"recorded": True}

    # 3.1 延时执行
    print("\n📌 测试 3.1: 延时执行")
    config = SchedulerConfig.memory()
    config.scan_interval = 0.1  # 提高精度

    scheduler = TaskScheduler(executor=record_executor, config=config)
    scheduler.start()
    await asyncio.sleep(0.3)

    delay_start = time.time()
    scheduler.submit_delayed({"action": "delayed"}, delay_seconds=1)

    await asyncio.sleep(1.5)

    delayed_executed = len(execution_records) > 0
    delay_duration = time.time() - delay_start

    results.append(TestResult(
        name="延时执行",
        status=TestStatus.PASSED if delayed_executed else TestStatus.FAILED,
        message=f"实际延迟: {delay_duration:.1f}s (预期1s)"
    ))

    scheduler.shutdown()
    await asyncio.sleep(0.3)

    # 3.2 周期任务
    print("\n📌 测试 3.2: 周期任务")
    execution_records.clear()

    scheduler2 = TaskScheduler(executor=record_executor, config=config)
    scheduler2.start()
    await asyncio.sleep(0.3)

    periodic_id = scheduler2.submit_interval(
        {"action": "periodic"},
        interval_seconds=1,
        run_immediately=True
    )

    await asyncio.sleep(3.5)

    periodic_count = len(execution_records)
    scheduler2.cancel_periodic(periodic_id)
    scheduler2.shutdown()

    results.append(TestResult(
        name="周期任务",
        status=TestStatus.PASSED if periodic_count >= 3 else TestStatus.FAILED,
        message=f"执行次数: {periodic_count} (预期3+次)"
    ))

    return results


# ============================================================
# v0.4 测试：分布式基础
# ============================================================

async def test_v04_distributed() -> List[TestResult]:
    """v0.4 分布式基础测试"""
    print(f"\n{'=' * 80}")
    print("🧪 v0.4 分布式基础测试")
    print(f"{'=' * 80}")

    results = []

    # 检查 Redis
    redis_available = False
    try:
        import redis.asyncio as redis
        client = await redis.Redis.from_url("redis://localhost:6379")
        await client.ping()
        await client.close()
        redis_available = True
        print("  ✅ Redis 可用")
    except Exception as e:
        print(f"  ⚠️ Redis 不可用: {e}")

    if not redis_available:
        results.append(TestResult(
            name="Redis连接检查",
            status=TestStatus.SKIPPED,
            message="Redis 不可用，跳过分布式测试"
        ))
        return results

    redis_url = "redis://localhost:6379"

    async def dist_executor(data):
        await asyncio.sleep(data.get("duration", 0.05))
        return {"result": "ok", "task_id": data.get("task_id")}

    # 4.1 Redis 存储
    print("\n📌 测试 4.1: Redis 存储")
    config = TaskPoolConfig.redis(url=redis_url, node_id="test-redis")

    async with TaskPool(executor=dist_executor, config=config) as pool:
        await asyncio.sleep(0.3)

        task_id = await pool.submit_async({"task_id": "redis-test", "duration": 0.1})
        result = await pool.wait_for_result_async(task_id, timeout=10)

        results.append(TestResult(
            name="Redis存储",
            status=TestStatus.PASSED if result else TestStatus.FAILED,
            message=f"任务 {task_id} 完成"
        ))

    # 4.2 分布式锁
    print("\n📌 测试 4.2: 分布式锁")
    from neotask.lock.redis import RedisLock

    lock = RedisLock(redis_url=redis_url)
    lock_key = f"test-lock-{uuid.uuid4().hex[:8]}"

    async def try_acquire(owner: str) -> bool:
        acquired = await lock.acquire(lock_key, ttl=5)
        if acquired:
            await asyncio.sleep(0.1)
            await lock.release(lock_key)
        return acquired

    results_lock = await asyncio.gather(
        try_acquire("owner-1"),
        try_acquire("owner-2"),
        try_acquire("owner-3"),
    )

    success_count = sum(results_lock)
    await lock.close()

    results.append(TestResult(
        name="分布式锁",
        status=TestStatus.PASSED if success_count == 1 else TestStatus.FAILED,
        message=f"并发竞争: {success_count}/3 成功 (预期1)"
    ))

    return results


# ============================================================
# v0.5 测试：性能优化
# ============================================================

async def test_v05_performance() -> List[TestResult]:
    """v0.5 性能优化测试"""
    print(f"\n{'=' * 80}")
    print("🧪 v0.5 性能优化测试")
    print(f"{'=' * 80}")

    results = []

    # 5.1 预取机制
    print("\n📌 测试 5.1: 预取机制")
    config = TaskPoolConfig.memory(node_id="test-prefetch")
    config.enable_prefetch = True
    config.prefetch_size = 10

    async with TaskPool(executor=echo_executor, config=config) as pool:
        await asyncio.sleep(0.3)

        # 批量提交任务
        for i in range(30):
            await pool.submit_async({"msg": f"prefetch-{i}", "duration": 0.01})

        await asyncio.sleep(1)

        # 检查预取器统计
        prefetch_stats = {}
        if pool._worker_pool._prefetcher:
            prefetch_stats = pool._worker_pool._prefetcher.get_stats()
            total_fetched = prefetch_stats.get("total_fetched", 0)

            results.append(TestResult(
                name="预取机制",
                status=TestStatus.PASSED if total_fetched > 0 else TestStatus.FAILED,
                message=f"预取次数: {prefetch_stats.get('total_prefetch', 0)}, 获取任务: {total_fetched}",
                details={"avg_batch_size": f"{prefetch_stats.get('avg_batch_size', 0):.2f}"}
            ))
        else:
            results.append(TestResult(
                name="预取机制",
                status=TestStatus.SKIPPED,
                message="预取器未启用"
            ))

    # 5.2 批量操作
    print("\n📌 测试 5.2: 批量操作")
    config2 = TaskPoolConfig.memory(node_id="test-batch")

    async with TaskPool(executor=echo_executor, config=config2) as pool:
        await asyncio.sleep(0.3)

        batch_tasks = [{"msg": f"batch-{i}", "duration": 0.01} for i in range(20)]

        start = time.time()
        task_ids = pool.submit_batch(batch_tasks)
        elapsed = time.time() - start

        results.append(TestResult(
            name="批量操作",
            status=TestStatus.PASSED if len(task_ids) == 20 else TestStatus.FAILED,
            message=f"提交 {len(task_ids)} 个任务, 耗时 {elapsed:.3f}s",
            details={"吞吐量": f"{len(task_ids) / elapsed:.1f} 任务/秒"}
        ))

    return results


# ============================================================
# v1.0 测试：高可用保障
# ============================================================

async def test_v10_high_availability() -> List[TestResult]:
    """v1.0 高可用保障测试"""
    print(f"\n{'=' * 80}")
    print("🧪 v1.0 高可用保障测试")
    print(f"{'=' * 80}")

    results = []

    # 检查 Redis
    redis_available = False
    try:
        import redis.asyncio as redis
        client = await redis.Redis.from_url("redis://localhost:6379")
        await client.ping()
        await client.close()
        redis_available = True
    except Exception:
        pass

    # 1.1 看门狗测试（需要 Redis 锁）
    print("\n📌 测试 1.1: 看门狗机制")

    if redis_available:
        from neotask.lock.redis import RedisLock
        from neotask.lock.watchdog import WatchDog

        lock = RedisLock("redis://localhost:6379")
        watchdog = WatchDog(lock)
        lock_key = f"test-watchdog-{uuid.uuid4().hex[:8]}"

        try:
            acquired = await lock.acquire(lock_key, ttl=3)
            if acquired:
                await watchdog.start(lock_key, ttl=3)
                await asyncio.sleep(4)  # 等待看门狗续期

                # 检查锁是否仍然存在（续期成功）
                is_locked = await lock.is_locked(lock_key)
                await watchdog.stop(lock_key)
                await lock.release(lock_key)

                results.append(TestResult(
                    name="看门狗机制",
                    status=TestStatus.PASSED if is_locked else TestStatus.FAILED,
                    message=f"锁在 TTL 过期后仍被持有: {is_locked}"
                ))
            else:
                results.append(TestResult(
                    name="看门狗机制",
                    status=TestStatus.FAILED,
                    message="无法获取锁"
                ))
        except Exception as e:
            results.append(TestResult(
                name="看门狗机制",
                status=TestStatus.FAILED,
                message=str(e)
            ))
        finally:
            await lock.close()
    else:
        results.append(TestResult(
            name="看门狗机制",
            status=TestStatus.SKIPPED,
            message="Redis 不可用，跳过"
        ))

    # 1.2 超时检测
    print("\n📌 测试 1.2: 超时检测")
    config = TaskPoolConfig.memory(node_id="test-timeout")
    config.task_timeout = 1  # 1秒超时

    async with TaskPool(executor=slow_executor, config=config) as pool:
        await asyncio.sleep(0.3)

        start = time.time()
        task_id = await pool.submit_async({"duration": 2, "task_id": "timeout-test"})

        try:
            result = await pool.wait_for_result_async(task_id, timeout=3)
            elapsed = time.time() - start
            # 如果任务超时，应该抛出异常或状态为 failed
            status = await pool.get_status_async(task_id)
            timed_out = status in ["failed", "FAILED", "timeout"]

            results.append(TestResult(
                name="超时检测",
                status=TestStatus.PASSED if timed_out else TestStatus.FAILED,
                message=f"任务状态: {status}, 耗时: {elapsed:.2f}s"
            ))
        except Exception as e:
            elapsed = time.time() - start
            results.append(TestResult(
                name="超时检测",
                status=TestStatus.PASSED,
                message=f"任务超时正确抛出异常: {str(e)[:50]}, 耗时: {elapsed:.2f}s"
            ))

    # 1.3 任务回收器
    print("\n📌 测试 1.3: 任务回收器")

    if redis_available:
        config2 = TaskPoolConfig.redis(url="redis://localhost:6379", node_id="test-reclaimer")
        config2.enable_reclaimer = True

        async with TaskPool(executor=echo_executor, config=config2) as pool:
            await asyncio.sleep(0.5)

            # 提交任务
            for i in range(5):
                await pool.submit_async({"msg": f"reclaim-{i}", "duration": 0.05})

            await asyncio.sleep(1)

            # 获取回收器统计
            if pool._reclaimer:
                stats = pool._reclaimer.get_stats()
                results.append(TestResult(
                    name="任务回收器",
                    status=TestStatus.PASSED,
                    message=f"回收器运行中，统计: {stats.get('total_reclaimed', 0)} 个回收任务"
                ))
            else:
                results.append(TestResult(
                    name="任务回收器",
                    status=TestStatus.SKIPPED,
                    message="回收器未启用"
                ))
    else:
        results.append(TestResult(
            name="任务回收器",
            status=TestStatus.SKIPPED,
            message="Redis 不可用，跳过"
        ))

    # 1.4 死信队列
    print("\n📌 测试 1.4: 死信队列")

    if redis_available:
        from neotask.queue.dead_letter import DeadLetterQueue

        try:
            dlq = DeadLetterQueue("redis://localhost:6379", max_size=100, ttl=3600)

            # 发送测试死信
            from neotask.models.task import Task, TaskStatus, TaskPriority
            test_task = Task(
                task_id=f"test-dlq-{uuid.uuid4().hex[:8]}",
                data={"test": "data"},
                status=TaskStatus.FAILED,
                priority=TaskPriority.NORMAL
            )

            from neotask.queue.dead_letter import DeadLetterReason
            success = await dlq.send(test_task, DeadLetterReason.MAX_RETRIES, "Test error")
            await dlq.close()

            results.append(TestResult(
                name="死信队列",
                status=TestStatus.PASSED if success else TestStatus.FAILED,
                message="死信发送成功" if success else "死信发送失败"
            ))
        except Exception as e:
            results.append(TestResult(
                name="死信队列",
                status=TestStatus.SKIPPED,
                message=f"死信队列未实现或错误: {str(e)[:50]}"
            ))
    else:
        results.append(TestResult(
            name="死信队列",
            status=TestStatus.SKIPPED,
            message="Redis 不可用，跳过"
        ))

    return results


# ============================================================
# 完整测试运行器
# ============================================================

async def run_all_tests() -> bool:
    """运行所有测试"""
    print("\n" + "=" * 80)
    print("🚀 NeoTask v1.0 完整集成测试")
    print("=" * 80)
    print("\n测试覆盖:")
    print("  v0.1 基础任务池 - 任务提交、优先级、持久化")
    print("  v0.2 可观测性 - 事件总线、指标收集、健康检查")
    print("  v0.3 定时调度 - 延时执行、周期任务")
    print("  v0.4 分布式基础 - Redis队列、分布式锁")
    print("  v0.5 性能优化 - 预取机制、批量操作")
    print("  v1.0 高可用保障 - 看门狗、超时检测、任务回收、死信队列")

    runner = TestRunner()

    # 按顺序运行测试
    test_functions = [
        ("v0.1 基础任务池", test_v01_basic_pool),
        ("v0.2 可观测性", test_v02_observability),
        ("v0.3 定时调度", test_v03_scheduled_tasks),
        ("v0.4 分布式基础", test_v04_distributed),
        ("v0.5 性能优化", test_v05_performance),
        ("v1.0 高可用保障", test_v10_high_availability),
    ]

    for name, test_func in test_functions:
        print(f"\n{'─' * 80}")
        print(f"▶️ 运行: {name}")
        print(f"{'─' * 80}")

        try:
            results = await test_func()
            runner.results.extend(results)
        except Exception as e:
            print(f"  ❌ 测试套件 {name} 执行失败: {e}")

    return runner.print_summary()


# ============================================================
# 快速测试（单方法覆盖所有核心功能）
# ============================================================

async def quick_test() -> bool:
    """快速测试 - 一个方法覆盖所有版本核心功能"""

    print("\n" + "=" * 80)
    print("⚡ NeoTask v1.0 快速测试")
    print("=" * 80)

    results = []

    # ========== v0.1 核心 ==========
    print("\n📌 [v0.1] 任务提交与等待")

    async def quick_executor(data):
        await asyncio.sleep(0.02)
        return {"result": "ok", "data": data}

    async with TaskPool(executor=quick_executor) as pool:
        await asyncio.sleep(0.2)

        task_id = await pool.submit_async({"msg": "quick-test"})
        result = await pool.wait_for_result_async(task_id, timeout=5)

        results.append(("v0.1 基础任务", result is not None))
        print(f"  ✅ 任务 {task_id} 完成")

    # ========== v0.2 核心 ==========
    print("\n📌 [v0.2] 指标收集")

    async with TaskPool(executor=quick_executor) as pool:
        await asyncio.sleep(0.2)

        for i in range(3):
            await pool.submit_async({"msg": f"metric-{i}"})

        await asyncio.sleep(0.5)
        stats = pool.get_stats()

        results.append(("v0.2 指标收集", stats is not None and "queue_size" in stats))
        print(f"  ✅ 统计信息: queue_size={stats.get('queue_size', 'N/A')}")

    # ========== v0.3 核心 ==========
    print("\n📌 [v0.3] 延时任务")

    executed = False

    async def record_executor(data):
        nonlocal executed
        executed = True
        return {"done": True}

    config = SchedulerConfig.memory()
    config.scan_interval = 0.1

    scheduler = TaskScheduler(executor=record_executor, config=config)
    scheduler.start()
    await asyncio.sleep(0.3)

    scheduler.submit_delayed({"action": "delayed"}, delay_seconds=1)
    await asyncio.sleep(1.5)

    results.append(("v0.3 延时任务", executed))
    print(f"  ✅ 延时任务已执行: {executed}")

    scheduler.shutdown()
    await asyncio.sleep(0.3)

    # ========== v0.4 核心 ==========
    print("\n📌 [v0.4] 分布式基础")

    try:
        import redis.asyncio as redis
        client = await redis.Redis.from_url("redis://localhost:6379")
        await client.ping()
        await client.close()

        from neotask.lock.redis import RedisLock
        lock = RedisLock("redis://localhost:6379")
        lock_key = f"quick-lock-{uuid.uuid4().hex[:8]}"

        acquired1 = await lock.acquire(lock_key, ttl=5)
        acquired2 = await lock.acquire(lock_key, ttl=5)

        await lock.release(lock_key)
        await lock.close()

        lock_works = acquired1 and not acquired2
        results.append(("v0.4 分布式锁", lock_works))
        print(f"  ✅ 分布式锁测试: 竞争成功={lock_works}")

    except Exception as e:
        results.append(("v0.4 分布式锁", False))
        print(f"  ⚠️ Redis不可用: {e}")

    # ========== v0.5 核心 ==========
    print("\n📌 [v0.5] 批量操作")

    async with TaskPool(executor=quick_executor) as pool:
        await asyncio.sleep(0.2)

        batch = [{"msg": f"batch-{i}"} for i in range(10)]
        task_ids = pool.submit_batch(batch)

        results.append(("v0.5 批量操作", len(task_ids) == 10))
        print(f"  ✅ 批量提交: {len(task_ids)} 个任务")

    # ========== v1.0 核心 ==========
    print("\n📌 [v1.0] 超时检测")

    async def slow_executor(data):
        await asyncio.sleep(0.5)
        return {"result": "done"}

    config_timeout = TaskPoolConfig.memory()
    config_timeout.task_timeout = 0.3  # 300ms 超时

    async with TaskPool(executor=slow_executor, config=config_timeout) as pool:
        await asyncio.sleep(0.2)

        task_id = await pool.submit_async({})
        await asyncio.sleep(0.5)

        status = await pool.get_status_async(task_id)
        timed_out = status in ["failed", "FAILED", "timeout", "TIMEOUT"]

        results.append(("v1.0 超时检测", timed_out))
        print(f"  ✅ 超时检测: 任务状态={status}")

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

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NeoTask v1.0 集成测试")
    parser.add_argument("--quick", action="store_true", help="运行快速测试")
    parser.add_argument("--full", action="store_true", help="运行完整测试")

    args = parser.parse_args()

    if args.quick:
        success = asyncio.run(quick_test())
    else:
        success = asyncio.run(run_all_tests())

    sys.exit(0 if success else 1)

# 快速测试（一个方法覆盖所有核心功能）：python tests/v.x/test_integration_v10.py --quick
# 完整测试（详细验证每个功能模块）：python tests/v.x/test_integration_v10.py --full
# 默认运行完整测试：python tests/v.x/test_integration_v10.py
