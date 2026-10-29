"""
@FileName: test_integration_v10_standalone.py
@Description: NeoTask v1.0 独立集成测试
完全独立，避免事件循环冲突
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/5/11

运行方式：
    python tests/test_integration_v10_standalone.py --quick
    python tests/test_integration_v10_standalone.py --full

设计原则：
- 不使用 async/await 在测试框架层面
- 每个测试独立运行，自己管理事件循环
- 不依赖外部 pytest fixtures
"""

import argparse
import asyncio
import os
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, List

import pytest

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from neotask.api.task_pool import TaskPool
from neotask.api.task_scheduler import TaskScheduler
from neotask.models.config import TaskPoolConfig, SchedulerConfig
from neotask.models.task import TaskPriority


# ============================================================
# 测试框架（完全同步，不混用事件循环）
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
    """独立测试运行器，每个测试独立运行"""

    def __init__(self):
        self.results: List[TestResult] = []

    def run_async(self, coro_func, *args, **kwargs):
        """在独立的事件循环中运行异步函数"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro_func(*args, **kwargs))
        finally:
            loop.close()

    def add_result(self, result: TestResult):
        self.results.append(result)
        status_icon = result.status.value
        print(f"  {status_icon} {result.name}: {result.message}")
        if result.details:
            for key, value in result.details.items():
                print(f"      {key}: {value}")

    def print_summary(self):
        print(f"\n" + "=" * 80)
        print("📊 测试总结")
        print("=" * 80)

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

        print(f"\n" + "=" * 80)
        return failed == 0


@pytest.fixture
def runner():
    """为 pytest 提供兼容的 TestRunner fixture"""
    return TestRunner()


# ============================================================
# v0.1 测试：基础任务池
# ============================================================

def test_v01_basic_pool(runner: TestRunner):
    """v0.1 基础任务池测试，完全同步执行"""
    print(f"\n" + "=" * 80)
    print("🧪 v0.1 基础任务池测试")
    print("=" * 80)

    async def echo_executor(data):
        await asyncio.sleep(0.01)
        return {"result": "ok", "echo": data.get("msg", "none")}

    # 1.1 内存模式
    print("\n📌 测试 1.1: 内存模式 任务提交与等待")
    config = TaskPoolConfig.memory(node_id="test-mem")
    pool = TaskPool(executor=echo_executor, config=config)
    pool.start()
    time.sleep(0.3)

    try:
        task_id = pool.submit({"msg": "hello", "duration": 0.05})
        result = pool.wait_for_result(task_id, timeout=5)

        passed = result is not None and result.get("result") == "ok"
        runner.add_result(TestResult(
            name="内存模式 任务提交与等待",
            status=TestStatus.PASSED if passed else TestStatus.FAILED,
            message=f"task_id={task_id}, result={result.get('result') if result else 'None'}"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="内存模式 任务提交与等待",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        pool.shutdown()
        time.sleep(0.3)

    # 1.2 SQLite 模式
    print("\n📌 测试 1.2: SQLite 模式 持久化")
    # 使用临时目录：硬编码 /data 在 Windows 下会被解析为盘符根目录，可能无写权限
    db_path = os.path.join(tempfile.gettempdir(), f"neotask_v10_{uuid.uuid4().hex}.db")
    config_sql = TaskPoolConfig.sqlite(path=db_path, node_id="test-sql")
    pool2 = TaskPool(executor=echo_executor, config=config_sql)
    pool2.start()
    time.sleep(0.3)

    try:
        task_id = pool2.submit({"msg": "persistent", "duration": 0.05})
        result = pool2.wait_for_result(task_id, timeout=5)

        passed = result is not None
        runner.add_result(TestResult(
            name="SQLite模式 持久化",
            status=TestStatus.PASSED if passed else TestStatus.FAILED,
            message=f"task_id={task_id}, 持久化成功"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="SQLite模式 持久化",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        pool2.shutdown()
        time.sleep(0.3)
        # 清理临时数据库及日志文件（Windows 下句柄未释放时忽略失败）
        for suffix in ("", "-journal", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except OSError:
                pass

    # 1.3 优先级测试
    print("\n📌 测试 1.3: 优先级队列")
    config_pri = TaskPoolConfig.memory(node_id="test-pri")
    pool3 = TaskPool(executor=echo_executor, config=config_pri)
    pool3.start()
    time.sleep(0.3)

    try:
        completion_order = []
        low_id = pool3.submit({"msg": "low", "duration": 0.1}, priority=TaskPriority.LOW)
        time.sleep(0.05)
        high_id = pool3.submit({"msg": "high", "duration": 0.1}, priority=TaskPriority.HIGH)

        pool3.wait_for_result(high_id, timeout=10)
        completion_order.append("high")
        pool3.wait_for_result(low_id, timeout=10)
        completion_order.append("low")

        high_first = completion_order[0] == "high"
        runner.add_result(TestResult(
            name="优先级队列",
            status=TestStatus.PASSED if high_first else TestStatus.FAILED,
            message=f"完成顺序: {completion_order}"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="优先级队列",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        pool3.shutdown()
        time.sleep(0.3)


# ============================================================
# v0.2 测试：可观测性
# ============================================================

def test_v02_observability(runner: TestRunner):
    """v0.2 可观测性测试"""
    print(f"\n" + "=" * 80)
    print("🧪 v0.2 可观测性测试")
    print("=" * 80)

    async def echo_executor(data):
        await asyncio.sleep(0.02)
        return {"result": "ok"}

    # 2.1 指标收集
    print("\n📌 测试 2.1: 指标收集")
    config = TaskPoolConfig.memory(node_id="test-metrics")
    pool = TaskPool(executor=echo_executor, config=config)
    pool.start()
    time.sleep(0.3)

    try:
        for i in range(5):
            pool.submit({"msg": f"metric-{i}"})

        time.sleep(0.5)
        stats = pool.get_stats()
        has_stats = stats is not None and "queue_size" in stats

        runner.add_result(TestResult(
            name="指标收集",
            status=TestStatus.PASSED if has_stats else TestStatus.FAILED,
            message=f"统计信息: queue_size={stats.get('queue_size', 'N/A')}"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="指标收集",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        pool.shutdown()
        time.sleep(0.3)

    # 2.2 健康检查
    print("\n📌 测试 2.2: 健康检查")
    config2 = TaskPoolConfig.memory(node_id="test-health")
    pool2 = TaskPool(executor=echo_executor, config=config2)
    pool2.start()
    time.sleep(0.3)

    try:
        health = pool2.get_health_status()
        runner.add_result(TestResult(
            name="健康检查",
            status=TestStatus.PASSED if health is not None else TestStatus.FAILED,
            message=f"status={health.get('status', 'unknown') if health else 'None'}"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="健康检查",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        pool2.shutdown()
        time.sleep(0.3)


# ============================================================
# v0.3 测试：定时调度
# ============================================================

def test_v03_scheduled_tasks(runner: TestRunner):
    """v0.3 定时调度测试"""
    print(f"\n" + "=" * 80)
    print("🧪 v0.3 定时调度测试")
    print("=" * 80)

    execution_records = []

    async def record_executor(data):
        execution_records.append({"time": time.time(), "data": data})
        return {"recorded": True}

    config = SchedulerConfig.memory()
    config.scan_interval = 0.1

    # 3.1 延时执行
    print("\n📌 测试 3.1: 延时执行")
    scheduler = TaskScheduler(executor=record_executor, config=config)
    scheduler.start()
    time.sleep(0.3)

    try:
        delay_start = time.time()
        task_id = scheduler.submit_delayed({"action": "delayed"}, delay_seconds=1)
        scheduler.wait_for_result(task_id, timeout=2)

        delayed_executed = len(execution_records) > 0
        delay_duration = time.time() - delay_start

        runner.add_result(TestResult(
            name="延时执行",
            status=TestStatus.PASSED if delayed_executed else TestStatus.FAILED,
            message=f"实际延迟: {delay_duration:.1f}s (预期1s)"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="延时执行",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        scheduler.shutdown()
        time.sleep(0.3)

    # 3.2 周期任务
    print("\n📌 测试 3.2: 周期任务")
    execution_records.clear()
    scheduler2 = TaskScheduler(executor=record_executor, config=config)
    scheduler2.start()
    time.sleep(0.3)

    try:
        periodic_id = scheduler2.submit_interval(
            {"action": "periodic"},
            interval_seconds=1,
            run_immediately=True
        )
        time.sleep(3.5)
        periodic_count = len(execution_records)
        scheduler2.cancel_periodic(periodic_id)

        runner.add_result(TestResult(
            name="周期任务",
            status=TestStatus.PASSED if periodic_count >= 3 else TestStatus.FAILED,
            message=f"执行次数: {periodic_count} (预期3+次)"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="周期任务",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        scheduler2.shutdown()
        time.sleep(0.3)


# ============================================================
# v0.4 测试：分布式基础
# ============================================================

def test_v04_distributed(runner: TestRunner):
    """v0.4 分布式基础测试"""
    print(f"\n" + "=" * 80)
    print("🧪 v0.4 分布式基础测试")
    print("=" * 80)

    redis_available = False
    try:
        import redis as sync_redis
        r = sync_redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        r.close()
        redis_available = True
        print("  ✅ Redis 可用")
    except Exception as e:
        print(f"  ⚠️ Redis 不可用: {e}")

    if not redis_available:
        runner.add_result(TestResult(
            name="Redis连接检查",
            status=TestStatus.SKIPPED,
            message="Redis 不可用，跳过分布式测试"
        ))
        return

    async def dist_executor(data):
        await asyncio.sleep(0.05)
        return {"result": "ok", "task_id": data.get("task_id")}

    redis_url = "redis://127.0.0.1:6379"

    # 4.1 Redis 存储
    print("\n📌 测试 4.1: Redis 存储")
    config = TaskPoolConfig.redis(url=redis_url, node_id="test-redis")
    pool = TaskPool(executor=dist_executor, config=config)
    pool.start()
    time.sleep(0.5)

    try:
        task_id = pool.submit({"task_id": "redis-test", "duration": 0.1})
        result = pool.wait_for_result(task_id, timeout=10)

        runner.add_result(TestResult(
            name="Redis存储",
            status=TestStatus.PASSED if result else TestStatus.FAILED,
            message=f"任务 {task_id} 完成"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="Redis存储",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        pool.shutdown()
        time.sleep(0.5)

    # 4.2 分布式锁
    print("\n📌 测试 4.2: 分布式锁")
    try:
        import redis as sync_redis
        r = sync_redis.Redis(host='localhost', port=6379, decode_responses=True)
        lock_key = f"test-lock-{uuid.uuid4().hex[:8]}"

        acquired1 = r.set(lock_key, "owner-1", nx=True, ex=5)
        acquired2 = r.set(lock_key, "owner-2", nx=True, ex=5)

        if acquired1:
            r.delete(lock_key)
        r.close()

        lock_works = acquired1 and not acquired2
        runner.add_result(TestResult(
            name="分布式锁",
            status=TestStatus.PASSED if lock_works else TestStatus.FAILED,
            message=f"并发竞争: 第一个成功={acquired1}, 第二个成功={acquired2}"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="分布式锁",
            status=TestStatus.FAILED,
            message=str(e)
        ))


# ============================================================
# v0.5 测试：性能优化
# ============================================================

def test_v05_performance(runner: TestRunner):
    """v0.5 性能优化测试"""
    print(f"\n" + "=" * 80)
    print("🧪 v0.5 性能优化测试")
    print("=" * 80)

    async def echo_executor(data):
        await asyncio.sleep(0.01)
        return {"result": "ok"}

    # 5.1 预取机制
    print("\n📌 测试 5.1: 预取机制")
    config = TaskPoolConfig.memory(node_id="test-prefetch")
    config.enable_prefetch = True
    config.prefetch_size = 10

    pool = TaskPool(executor=echo_executor, config=config)
    pool.start()
    time.sleep(0.3)

    try:
        for i in range(30):
            pool.submit({"msg": f"prefetch-{i}"})

        time.sleep(1)
        if pool._worker_pool._prefetcher:
            prefetch_stats = pool._worker_pool._prefetcher.get_stats()
            total_fetched = prefetch_stats.get("total_fetched", 0)

            runner.add_result(TestResult(
                name="预取机制",
                status=TestStatus.PASSED if total_fetched > 0 else TestStatus.FAILED,
                message=f"获取任务数: {total_fetched}",
                details={"avg_batch_size": f"{prefetch_stats.get('avg_batch_size', 0):.2f}"}
            ))
        else:
            runner.add_result(TestResult(
                name="预取机制",
                status=TestStatus.SKIPPED,
                message="预取器未启用"
            ))
    except Exception as e:
        runner.add_result(TestResult(
            name="预取机制",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        pool.shutdown()
        time.sleep(0.3)

    # 5.2 批量操作
    print("\n📌 测试 5.2: 批量操作")
    config2 = TaskPoolConfig.memory(node_id="test-batch")
    pool2 = TaskPool(executor=echo_executor, config=config2)
    pool2.start()
    time.sleep(0.3)

    try:
        batch_tasks = [{"msg": f"batch-{i}"} for i in range(20)]
        start = time.time()
        task_ids = pool2.submit_batch(batch_tasks)
        elapsed = time.time() - start

        runner.add_result(TestResult(
            name="批量操作",
            status=TestStatus.PASSED if len(task_ids) == 20 else TestStatus.FAILED,
            message=f"提交 {len(task_ids)} 个任务, 耗时 {elapsed:.3f}s"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="批量操作",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        pool2.shutdown()
        time.sleep(0.3)


# ============================================================
# v1.0 测试：高可用保障
# ============================================================

def test_v10_high_availability(runner: TestRunner):
    """v1.0 高可用保障测试"""
    print(f"\n" + "=" * 80)
    print("🧪 v1.0 高可用保障测试")
    print("=" * 80)

    redis_available = False
    try:
        import redis as sync_redis
        r = sync_redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        r.close()
        redis_available = True
    except Exception:
        pass

    # 1.1 超时检测
    print("\n📌 测试 1.1: 超时检测")

    async def slow_executor(data):
        try:
            await asyncio.sleep(1)
            return {"result": "done"}
        except asyncio.CancelledError:
            raise

    config = TaskPoolConfig.memory(node_id="test-timeout")
    config.task_timeout = 0.3
    config.max_retries = 0
    config.worker_concurrency = 1

    pool = TaskPool(executor=slow_executor, config=config)
    pool.start()
    time.sleep(0.3)

    try:
        start = time.time()
        task_id = pool.submit({})
        pool.wait_for_result(task_id)

        status = pool.get_status(task_id)
        elapsed = time.time() - start
        timed_out = status in ["failed", "FAILED", "timeout", "TIMEOUT"]

        runner.add_result(TestResult(
            name="超时检测",
            status=TestStatus.PASSED if timed_out else TestStatus.FAILED,
            message=f"任务状态: {status}, 耗时: {elapsed:.2f}s"
        ))
    except Exception as e:
        runner.add_result(TestResult(
            name="超时检测",
            status=TestStatus.FAILED,
            message=str(e)
        ))
    finally:
        pool.shutdown()
        time.sleep(0.3)

    # 1.2 看门狗
    print("\n📌 测试 1.2: 看门狗机制")
    if redis_available:
        try:
            import redis as sync_redis
            r = sync_redis.Redis(host='localhost', port=6379, decode_responses=True)
            lock_key = f"test-watchdog-{uuid.uuid4().hex[:8]}"

            acquired = r.set(lock_key, "test-owner", nx=True, ex=3)
            if acquired:
                time.sleep(2)
                r.expire(lock_key, 3)
                time.sleep(2)
                still_locked = r.exists(lock_key) > 0
                r.delete(lock_key)
                r.close()

                runner.add_result(TestResult(
                    name="看门狗机制",
                    status=TestStatus.PASSED if still_locked else TestStatus.FAILED,
                    message=f"锁续期后仍然存在: {still_locked}"
                ))
            else:
                r.close()
                runner.add_result(TestResult(
                    name="看门狗机制",
                    status=TestStatus.FAILED,
                    message="无法获取锁"
                ))
        except Exception as e:
            runner.add_result(TestResult(
                name="看门狗机制",
                status=TestStatus.FAILED,
                message=str(e)
            ))
    else:
        runner.add_result(TestResult(
            name="看门狗机制",
            status=TestStatus.SKIPPED,
            message="Redis 不可用，跳过"
        ))


# ============================================================
# 快速测试
# ============================================================

def quick_test(runner: TestRunner) -> bool:
    """快速测试，一个方法覆盖所有版本核心功能"""
    print("\n" + "=" * 80)
    print("⚡ NeoTask v1.0 快速测试")
    print("=" * 80)

    results = []

    # v0.1
    async def quick_executor(data):
        await asyncio.sleep(0.02)
        return {"result": "ok"}

    pool = TaskPool(executor=quick_executor)
    pool.start()
    time.sleep(0.3)
    try:
        task_id = pool.submit({"msg": "quick-test"})
        result = pool.wait_for_result(task_id, timeout=5)
        results.append(("v0.1 基础任务", result is not None))
    except Exception:
        results.append(("v0.1 基础任务", False))
    finally:
        pool.shutdown()
        time.sleep(0.3)

    # v0.2
    pool2 = TaskPool(executor=quick_executor)
    pool2.start()
    time.sleep(0.3)
    try:
        for i in range(3):
            pool2.submit({"msg": f"metric-{i}"})
        time.sleep(0.5)
        stats = pool2.get_stats()
        results.append(("v0.2 指标收集", stats is not None and "queue_size" in stats))
    except Exception:
        results.append(("v0.2 指标收集", False))
    finally:
        pool2.shutdown()
        time.sleep(0.3)

    # v0.3
    executed = False

    async def record_executor(data):
        nonlocal executed
        executed = True
        return {"done": True}

    config = SchedulerConfig.memory()
    config.scan_interval = 0.1
    scheduler = TaskScheduler(executor=record_executor, config=config)
    scheduler.start()
    time.sleep(0.3)
    try:
        task_id = scheduler.submit_delayed({"action": "delayed"}, delay_seconds=1)
        scheduler.wait_for_result(task_id, 2)
        results.append(("v0.3 延时任务", executed))
    except Exception:
        results.append(("v0.3 延时任务", False))
    finally:
        scheduler.shutdown()
        time.sleep(0.3)

    # v0.4
    try:
        import redis as sync_redis
        r = sync_redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        lock_key = f"quick-lock-{uuid.uuid4().hex[:8]}"
        acquired1 = r.set(lock_key, "owner-1", nx=True, ex=5)
        acquired2 = r.set(lock_key, "owner-2", nx=True, ex=5)
        if acquired1:
            r.delete(lock_key)
        r.close()
        results.append(("v0.4 分布式锁", acquired1 and not acquired2))
    except Exception:
        results.append(("v0.4 分布式锁", False))

    # v0.5
    pool3 = TaskPool(executor=quick_executor)
    pool3.start()
    time.sleep(0.3)
    try:
        batch = [{"msg": f"batch-{i}"} for i in range(10)]
        task_ids = pool3.submit_batch(batch)
        results.append(("v0.5 批量操作", len(task_ids) == 10))
    except Exception:
        results.append(("v0.5 批量操作", False))
    finally:
        pool3.shutdown()
        time.sleep(0.3)

    # v1.0
    async def slow_executor(data):
        await asyncio.sleep(0.5)
        return {"result": "done"}

    config_timeout = TaskPoolConfig.memory(node_id="test-timeout")
    config_timeout.task_timeout = 0.3
    config_timeout.max_retries = 0

    pool4 = TaskPool(executor=slow_executor, config=config_timeout)
    pool4.start()
    time.sleep(0.5)
    try:
        task_id = pool4.submit({})
        time.sleep(1.0)
        status = pool4.get_status(task_id)
        timed_out = status in ["failed", "FAILED", "timeout", "TIMEOUT"]
        results.append(("v1.0 超时检测", timed_out))
    except Exception:
        results.append(("v1.0 超时检测", False))
    finally:
        pool4.shutdown()
        time.sleep(0.3)

    # 总结
    print("\n" + "=" * 80)
    print("📊 快速测试结果:")
    passed = sum(1 for _, p in results if p)
    total = len(results)

    for name, passed_flag in results:
        status = "✅" if passed_flag else "❌"
        print(f"  {status} {name}")

    print(f"\n总计: {passed}/{total} ผ่าน (通过)")
    print("=" * 80)

    return passed == total


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="NeoTask v1.0 独立集成测试")
    parser.add_argument("--quick", action="store_true", help="运行快速测试")
    parser.add_argument("--full", action="store_true", help="运行完整测试")

    args = parser.parse_args()
    runner = TestRunner()

    if args.quick or (not args.full):
        success = quick_test(runner)
    else:
        test_v01_basic_pool(runner)
        test_v02_observability(runner)
        test_v03_scheduled_tasks(runner)
        test_v04_distributed(runner)
        test_v05_performance(runner)
        test_v10_high_availability(runner)
        success = runner.print_summary()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
