"""
@FileName: test_distributed_v04_sync.py
@Description: v0.4 分布式功能测试 - 使用同步 API（推荐）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29

运行方式：
    python tests/v.x/test_distributed_v04_sync.py

测试覆盖：
    1. 多节点部署与共享队列
    2. 预取机制
    3. 分布式锁
    4. 节点心跳
    5. 任务重试
    6. 批量任务
"""

import time
import os
import sys
import uuid
import threading
from typing import Dict, Any, List

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from neotask.api.task_pool import TaskPool
from neotask.models.config import TaskPoolConfig
from neotask.distributed.node import NodeManager
from neotask.lock.redis import RedisLock

# ============================================================
# 测试配置
# ============================================================

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
TIMEOUT = 30


def check_redis():
    """检查 Redis 是否可用"""
    try:
        import redis
        r = redis.Redis.from_url(REDIS_URL)
        r.ping()
        r.close()
        print("✅ Redis connection successful")
        return True
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        print("\nPlease start Redis with:")
        print("  docker run -d -p 6379:6379 redis")
        return False


# ============================================================
# 执行器函数（同步版本）
# ============================================================

def simple_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """简单同步执行器"""
    import time
    duration = data.get("duration", 0.05)
    if duration > 0:
        time.sleep(duration)

    if data.get("should_fail", False):
        raise ValueError(f"Intentional failure for task {data.get('task_id', 'unknown')}")

    return {
        "status": "success",
        "task_id": data.get("task_id", "unknown"),
        "processed_at": time.time()
    }


# ============================================================
# 测试类
# ============================================================

class DistributedTest:
    """分布式功能测试"""

    def __init__(self):
        self.results = {}

    def test_single_task(self) -> bool:
        """测试1：单个任务提交和执行"""
        print("\n" + "=" * 60)
        print("Test 1: Single Task Submission")
        print("=" * 60)

        config = TaskPoolConfig(
            storage_type="redis",
            redis_url=REDIS_URL,
            node_id=f"test-single-{uuid.uuid4().hex[:6]}",
            worker_concurrency=2
        )

        pool = TaskPool(executor=simple_executor, config=config)
        pool.start()

        try:
            task_id = pool.submit({
                "task_id": "single-test",
                "duration": 0.1
            })
            print(f"Submitted task: {task_id}")

            result = pool.wait_for_result(task_id, timeout=TIMEOUT)
            print(f"Result: {result}")

            success = result is not None and result.get("status") == "success"
            print(f"✅ Single task test: {'PASSED' if success else 'FAILED'}")
            return success

        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False
        finally:
            pool.shutdown()

    def test_multiple_tasks(self) -> bool:
        """测试2：多个任务批量执行"""
        print("\n" + "=" * 60)
        print("Test 2: Multiple Tasks")
        print("=" * 60)

        config = TaskPoolConfig(
            storage_type="redis",
            redis_url=REDIS_URL,
            node_id=f"test-multi-{uuid.uuid4().hex[:6]}",
            worker_concurrency=5
        )

        pool = TaskPool(executor=simple_executor, config=config)
        pool.start()

        try:
            task_count = 10
            task_ids = []

            for i in range(task_count):
                task_id = pool.submit({
                    "task_id": f"multi-{i}",
                    "duration": 0.05
                })
                task_ids.append(task_id)
                print(f"Submitted task {i}: {task_id}")

            success_count = 0
            for task_id in task_ids:
                try:
                    result = pool.wait_for_result(task_id, timeout=TIMEOUT)
                    if result and result.get("status") == "success":
                        success_count += 1
                except Exception as e:
                    print(f"Task {task_id} failed: {e}")

            print(f"Success rate: {success_count}/{task_count}")
            success = success_count == task_count
            print(f"✅ Multiple tasks test: {'PASSED' if success else 'FAILED'}")
            return success

        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False
        finally:
            pool.shutdown()

    def test_cross_node(self) -> bool:
        """测试3：跨节点任务消费（分布式核心）"""
        print("\n" + "=" * 60)
        print("Test 3: Cross-Node Task Consumption (Distributed)")
        print("=" * 60)

        # 节点1：提交任务
        config1 = TaskPoolConfig(
            storage_type="redis",
            redis_url=REDIS_URL,
            node_id="cross-node-1",
            worker_concurrency=2
        )

        # 节点2：消费任务
        config2 = TaskPoolConfig(
            storage_type="redis",
            redis_url=REDIS_URL,
            node_id="cross-node-2",
            worker_concurrency=2
        )

        pool1 = TaskPool(executor=simple_executor, config=config1)
        pool2 = TaskPool(executor=simple_executor, config=config2)

        pool1.start()
        pool2.start()

        # 等待节点注册
        time.sleep(1)

        try:
            # 从节点1提交任务
            task_id = pool1.submit({
                "task_id": "cross-node-test",
                "duration": 0.2
            })
            print(f"Node 1 submitted task: {task_id}")

            # 从节点2等待任务（验证共享队列）
            result = pool2.wait_for_result(task_id, timeout=TIMEOUT)
            print(f"Node 2 consumed task: {result}")

            success = result is not None and result.get("status") == "success"
            print(f"✅ Cross-node test: {'PASSED' if success else 'FAILED'}")
            return success

        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False
        finally:
            pool1.shutdown()
            pool2.shutdown()

    def test_distributed_lock(self) -> bool:
        """测试4：分布式锁"""
        print("\n" + "=" * 60)
        print("Test 4: Distributed Lock")
        print("=" * 60)

        import threading

        lock = RedisLock(redis_url=REDIS_URL)
        lock_key = f"test-lock-{uuid.uuid4().hex[:8]}"

        acquired_count = 0
        lock_acquired = threading.Lock()

        def try_acquire(owner: str):
            nonlocal acquired_count
            import asyncio

            async def _acquire():
                return await lock.acquire(lock_key, ttl=5)

            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                acquired = loop.run_until_complete(_acquire())
                loop.close()

                if acquired:
                    with lock_acquired:
                        acquired_count += 1
                    time.sleep(0.1)
                    loop2 = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop2)
                    loop2.run_until_complete(lock.release(lock_key))
                    loop2.close()
            except Exception as e:
                print(f"Error for {owner}: {e}")

        # 并发获取锁
        threads = []
        for i in range(5):
            t = threading.Thread(target=try_acquire, args=(f"owner-{i}",))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        success = acquired_count == 1
        print(f"Acquired count: {acquired_count} (expected 1)")
        print(f"✅ Distributed lock test: {'PASSED' if success else 'FAILED'}")

        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(lock.close())
        loop.close()

        return success

    def test_node_heartbeat(self) -> bool:
        """测试5：节点心跳"""
        print("\n" + "=" * 60)
        print("Test 5: Node Heartbeat")
        print("=" * 60)

        import asyncio

        async def _test():
            manager = NodeManager(redis_url=REDIS_URL, node_id=f"heartbeat-{uuid.uuid4().hex[:6]}")
            await manager.start()

            # 等待心跳注册
            await asyncio.sleep(1)

            active_nodes = await manager.get_active_nodes()
            node_ids = [n.node_id for n in active_nodes]

            is_alive = manager.node_id in node_ids

            await manager.stop()
            return is_alive

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(_test())
        loop.close()

        print(f"Node alive in registry: {result}")
        print(f"✅ Node heartbeat test: {'PASSED' if result else 'FAILED'}")
        return result

    def test_retry_mechanism(self) -> bool:
        """测试6：任务重试"""
        print("\n" + "=" * 60)
        print("Test 6: Retry Mechanism")
        print("=" * 60)

        config = TaskPoolConfig(
            storage_type="redis",
            redis_url=REDIS_URL,
            node_id=f"test-retry-{uuid.uuid4().hex[:6]}",
            max_retries=3,
            retry_delay=0.5,
            worker_concurrency=2
        )

        pool = TaskPool(executor=simple_executor, config=config)
        pool.start()

        try:
            # 提交会失败的任务
            task_id = pool.submit({
                "task_id": f"fail-{uuid.uuid4().hex[:6]}",
                "duration": 0.05,
                "should_fail": True
            })
            print(f"Submitted failing task: {task_id}")

            # 等待重试完成
            time.sleep(5)

            # 检查最终状态
            status = pool.get_status(task_id)
            print(f"Final status: {status}")

            # 应该是 FAILED（超过重试次数）
            success = status in ["failed", "FAILED"]
            print(f"✅ Retry test: {'PASSED' if success else 'FAILED'}")
            return success

        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False
        finally:
            pool.shutdown()

    def test_batch_tasks_performance(self) -> bool:
        """测试7：批量任务性能"""
        print("\n" + "=" * 60)
        print("Test 7: Batch Tasks Performance")
        print("=" * 60)

        config = TaskPoolConfig(
            storage_type="redis",
            redis_url=REDIS_URL,
            node_id=f"test-batch-{uuid.uuid4().hex[:6]}",
            worker_concurrency=10,
            prefetch_size=20,
            enable_prefetch=True
        )

        pool = TaskPool(executor=simple_executor, config=config)
        pool.start()

        try:
            batch_size = 50
            start_time = time.time()

            task_ids = []
            for i in range(batch_size):
                task_id = pool.submit({
                    "task_id": f"batch-{i}",
                    "duration": 0.01
                })
                task_ids.append(task_id)

            # 等待所有任务完成
            success_count = 0
            for task_id in task_ids:
                try:
                    result = pool.wait_for_result(task_id, timeout=10)
                    if result and result.get("status") == "success":
                        success_count += 1
                except Exception:
                    pass

            elapsed = time.time() - start_time
            throughput = batch_size / elapsed

            print(f"Batch size: {batch_size}")
            print(f"Success count: {success_count}/{batch_size}")
            print(f"Elapsed: {elapsed:.2f}s")
            print(f"Throughput: {throughput:.2f} tasks/sec")

            success = success_count == batch_size
            print(f"✅ Batch test: {'PASSED' if success else 'FAILED'}")
            return success

        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False
        finally:
            pool.shutdown()

    def run_all(self) -> Dict[str, bool]:
        """运行所有测试"""
        print("\n" + "=" * 60)
        print("🚀 NeoTask v0.4 Distributed Feature Test Suite")
        print("=" * 60)

        if not check_redis():
            print("\n⚠️ Redis not available, skipping distributed tests")
            return {"redis_available": False}

        tests = [
            ("single_task", self.test_single_task),
            ("multiple_tasks", self.test_multiple_tasks),
            ("cross_node", self.test_cross_node),
            ("distributed_lock", self.test_distributed_lock),
            ("node_heartbeat", self.test_node_heartbeat),
            ("retry_mechanism", self.test_retry_mechanism),
            ("batch_tasks", self.test_batch_tasks_performance),
        ]

        for name, test_func in tests:
            try:
                self.results[name] = test_func()
                time.sleep(0.5)  # 间隔
            except Exception as e:
                print(f"❌ {name} crashed: {e}")
                self.results[name] = False

        self._print_summary()
        return self.results

    def _print_summary(self):
        """打印测试总结"""
        print("\n" + "=" * 60)
        print("📊 Test Summary")
        print("=" * 60)

        passed = sum(1 for v in self.results.values() if v)
        total = len(self.results)

        print(f"\nResults: {passed}/{total} tests passed")

        print("\nDetailed Results:")
        for name, result in self.results.items():
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"  - {name}: {status}")

        print("\n" + "=" * 60)
        if passed == total:
            print("🎉 All tests passed! v0.4 distributed features are working!")
        else:
            print("⚠️ Some tests failed. Please check the logs above.")
        print("=" * 60)


# ============================================================
# 简化版集成测试（单方法）
# ============================================================

def quick_integration_test():
    """快速集成测试 - 一个方法验证核心功能"""
    print("\n" + "=" * 60)
    print("🔧 Quick Integration Test")
    print("=" * 60)

    if not check_redis():
        return False

    config = TaskPoolConfig(
        storage_type="redis",
        redis_url=REDIS_URL,
        node_id="quick-test",
        worker_concurrency=3
    )

    pool = TaskPool(executor=simple_executor, config=config)
    pool.start()

    try:
        # 1. 提交多个任务
        print("\n1. Submitting 5 tasks...")
        task_ids = []
        for i in range(5):
            task_id = pool.submit({
                "task_id": f"quick-{i}",
                "duration": 0.05
            })
            task_ids.append(task_id)
            print(f"   Submitted: {task_id}")

        # 2. 等待所有任务完成
        print("\n2. Waiting for results...")
        success_count = 0
        for task_id in task_ids:
            try:
                result = pool.wait_for_result(task_id, timeout=10)
                if result and result.get("status") == "success":
                    success_count += 1
                    print(f"   ✅ {task_id}: success")
                else:
                    print(f"   ❌ {task_id}: failed")
            except Exception as e:
                print(f"   ❌ {task_id}: {e}")

        # 3. 获取统计
        print("\n3. Statistics:")
        stats = pool.get_stats()
        print(f"   Queue size: {stats.get('queue_size', 0)}")
        print(f"   Completed: {stats.get('completed', 0)}")

        success = success_count == 5
        print(f"\n{'✅' if success else '❌'} Quick test: {success_count}/5 tasks completed")

        return success

    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False
    finally:
        pool.shutdown()


# ============================================================
# 主入口
# ============================================================

# python tests/v.x/test_distributed_v04_sync.py
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        # 快速测试
        quick_integration_test()
    else:
        # 完整测试
        test = DistributedTest()
        test.run_all()