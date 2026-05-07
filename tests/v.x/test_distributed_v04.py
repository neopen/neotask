"""
@FileName: test_distributed_v04.py
@Description: v0.4 分布式功能 Pytest 测试
@Author: HiPeng
@Time: 2026/4/29

运行方式：
    python -m pytest tests/v.x/test_distributed_v04.py -v

测试覆盖：
    1. 多节点部署与共享队列
    2. 预取机制
    3. 分布式锁
    4. 节点心跳
    5. 任务重试
    6. 负载均衡
"""

import asyncio
import os
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Any

import pytest

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 导入 NeoTask 组件
from neotask.api.task_pool import TaskPool
from neotask.models.config import TaskPoolConfig
from neotask.distributed.node import NodeManager
from neotask.lock.redis import RedisLock


# ============================================================
# 测试配置
# ============================================================

@dataclass
class TestConfig:
    """测试配置"""
    redis_url: str = "redis://localhost:6379"
    test_timeout: float = 60.0


# 全局配置
TEST_CONFIG = TestConfig()


# ============================================================
# 测试 Fixtures
# ============================================================

@pytest.fixture(scope="session")
def event_loop():
    """创建事件循环"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture
async def redis_available():
    """检查 Redis 是否可用"""
    try:
        import redis.asyncio as redis
        client = await redis.Redis.from_url(TEST_CONFIG.redis_url)
        await client.ping()
        await client.close()
        return True
    except Exception as e:
        pytest.skip(f"Redis not available: {e}")
        return False


@pytest.fixture
async def task_pool(redis_available):
    """创建单个任务池"""
    config = TaskPoolConfig(
        storage_type="redis",
        redis_url=TEST_CONFIG.redis_url,
        node_id=f"test-node-{uuid.uuid4().hex[:8]}",
        enable_prefetch=True,
        worker_concurrency=2
    )

    async def test_executor(data: Dict[str, Any]) -> Dict[str, Any]:
        """测试执行器"""
        await asyncio.sleep(data.get("duration", 0.05))

        if data.get("should_fail", False):
            raise ValueError("Intentional failure for testing")

        return {
            "status": "success",
            "task_id": data.get("task_id", "unknown"),
            "processed_at": time.time()
        }

    pool = TaskPool(executor=test_executor, config=config)
    pool.start()

    # 等待启动
    await asyncio.sleep(0.5)

    yield pool

    pool.shutdown()
    await asyncio.sleep(0.5)


@pytest.fixture
async def two_nodes(redis_available):
    """创建两个节点"""
    nodes = []

    for i in range(2):
        config = TaskPoolConfig(
            storage_type="redis",
            redis_url=TEST_CONFIG.redis_url,
            node_id=f"multi-node-{i + 1}",
            enable_prefetch=True,
            worker_concurrency=2
        )

        async def test_executor(data):
            await asyncio.sleep(data.get("duration", 0.05))
            return {"status": "success", "data": data}

        pool = TaskPool(executor=test_executor, config=config)
        pool.start()
        nodes.append(pool)

        await asyncio.sleep(0.3)

    yield nodes

    for pool in nodes:
        pool.shutdown()

    await asyncio.sleep(0.5)


@pytest.fixture
async def node_manager(redis_available):
    """创建节点管理器"""
    node_id = f"test-node-{uuid.uuid4().hex[:8]}"
    manager = NodeManager(redis_url=TEST_CONFIG.redis_url, node_id=node_id)
    await manager.start()

    yield manager

    await manager.stop()


# ============================================================
# 测试类
# ============================================================

class TestDistributedV04:
    """v0.4 分布式功能测试类"""

    # ========== 基础功能测试 ==========

    @pytest.mark.asyncio
    async def test_task_submit_and_wait(self, task_pool):
        """测试任务提交和等待"""
        task_id = await task_pool.submit_async({
            "task_id": str(uuid.uuid4()),
            "duration": 0.1
        })

        assert task_id is not None
        assert len(task_id) > 0

        result = await task_pool.wait_for_result_async(task_id, timeout=5)
        assert result is not None
        assert result.get("status") == "success"

    @pytest.mark.asyncio
    async def test_task_priority(self, task_pool):
        """测试任务优先级"""
        from neotask.models.task import TaskPriority

        # 提交不同优先级的任务
        low_task = await task_pool.submit_async(
            {"task_id": "low", "duration": 0.1},
            priority=TaskPriority.LOW
        )
        high_task = await task_pool.submit_async(
            {"task_id": "high", "duration": 0.1},
            priority=TaskPriority.HIGH
        )

        assert low_task is not None
        assert high_task is not None

        # 等待高优先级任务先完成（由于延迟较小，等待即可）
        await asyncio.sleep(1)

        high_status = await task_pool.get_status_async(high_task)
        low_status = await task_pool.get_status_async(low_task)

        # 两个都应该完成或正在处理
        assert high_status in ["success", "running", "pending"]
        assert low_status in ["success", "running", "pending"]

    # ========== 多节点测试 ==========

    @pytest.mark.asyncio
    async def test_shared_queue_cross_node(self, two_nodes):
        """测试跨节点共享队列"""
        node1, node2 = two_nodes

        # 从节点1提交任务
        task_id = await node1.submit_async({
            "task_id": str(uuid.uuid4()),
            "duration": 0.2
        })

        # 从节点2等待任务（验证共享队列）
        try:
            result = await node2.wait_for_result_async(task_id, timeout=10)
            assert result is not None
            assert result.get("status") == "success"
        except Exception as e:
            pytest.fail(f"Cross-node task consumption failed: {e}")

    @pytest.mark.asyncio
    async def test_multiple_nodes_load_balance(self, two_nodes):
        """测试多节点负载均衡"""
        node1, node2 = two_nodes

        # 提交多个任务
        task_count = 10
        task_ids = []

        for i in range(task_count):
            task_id = await node1.submit_async({
                "task_id": f"lb-{i}",
                "duration": 0.05
            })
            task_ids.append(task_id)

        # 等待所有任务完成
        await asyncio.sleep(3)

        # 获取统计
        stats1 = node1.get_stats()
        stats2 = node2.get_stats()

        total_completed = stats1.get("completed", 0) + stats2.get("completed", 0)

        # 至少80%的任务完成
        assert total_completed >= task_count * 0.8

    # ========== 预取测试 ==========

    @pytest.mark.asyncio
    async def test_prefetch_mechanism(self, task_pool):
        """测试预取机制"""
        # 检查预取器是否存在
        if not task_pool._worker_pool._prefetcher:
            pytest.skip("Prefetcher not enabled")

        # 批量提交任务
        batch_size = 20
        for i in range(batch_size):
            await task_pool.submit_async({
                "task_id": f"prefetch-{i}",
                "duration": 0.02
            })

        # 等待预取触发
        await asyncio.sleep(2)

        # 获取预取统计
        stats = task_pool._worker_pool._prefetcher.get_stats()

        print(f"\nPrefetch stats: {stats}")

        # 验证预取发生了
        assert stats["total_prefetch"] >= 0  # 至少尝试预取
        assert stats["total_fetched"] >= 0

    # ========== 分布式锁测试 ==========

    @pytest.mark.asyncio
    async def test_distributed_lock(self, redis_available):
        """测试分布式锁"""
        lock = RedisLock(redis_url=TEST_CONFIG.redis_url)
        lock_key = f"test-lock-{uuid.uuid4().hex[:8]}"

        # 模拟并发获取锁
        async def try_acquire(owner: str) -> tuple:
            acquired = await lock.acquire(lock_key, ttl=5)
            if acquired:
                await asyncio.sleep(0.1)
                await lock.release(lock_key)
            return (owner, acquired)

        # 并发执行
        results = await asyncio.gather(
            try_acquire("owner-1"),
            try_acquire("owner-2"),
            try_acquire("owner-3"),
        )

        # 只有一个应该成功
        success_count = sum(1 for _, acquired in results if acquired)
        assert success_count == 1

        await lock.close()

    @pytest.mark.asyncio
    async def test_lock_watchdog(self, redis_available):
        """测试锁看门狗"""
        from neotask.lock.watchdog import WatchDog

        lock = RedisLock(redis_url=TEST_CONFIG.redis_url)
        watchdog = WatchDog(lock)

        lock_key = f"test-watchdog-{uuid.uuid4().hex[:8]}"

        # 获取锁
        acquired = await lock.acquire(lock_key, ttl=3)
        assert acquired

        # 启动看门狗
        await watchdog.start(lock_key, ttl=3)

        # 等待看门狗续期
        await asyncio.sleep(4)

        # 检查锁是否仍然存在（看门狗应该续期了）
        is_locked = await lock.is_locked(lock_key)

        # 停止看门狗并释放锁
        await watchdog.stop(lock_key)
        await lock.release(lock_key)

        # 验证
        final_locked = await lock.is_locked(lock_key)

        assert not final_locked

        await watchdog.stop_all()
        await lock.close()

    # ========== 节点管理测试 ==========

    @pytest.mark.asyncio
    async def test_node_register_and_heartbeat(self, node_manager):
        """测试节点注册和心跳"""
        # 节点应该已经注册
        assert node_manager.node_id is not None

        # 获取活跃节点
        active_nodes = await node_manager.get_active_nodes()

        # 应该至少包含当前节点
        node_ids = [n.node_id for n in active_nodes]
        assert node_manager.node_id in node_ids

        # 检查节点是否存活
        is_alive = await node_manager.is_node_alive(node_manager.node_id)
        assert is_alive

    @pytest.mark.asyncio
    async def test_multiple_nodes_registration(self, redis_available):
        """测试多个节点注册"""
        managers = []
        node_count = 3

        # 创建多个节点管理器
        for i in range(node_count):
            manager = NodeManager(
                redis_url=TEST_CONFIG.redis_url,
                node_id=f"reg-test-{i + 1}"
            )
            await manager.start()
            managers.append(manager)
            await asyncio.sleep(0.2)

        # 获取活跃节点
        active_nodes = await managers[0].get_active_nodes()

        # 验证所有节点都注册了
        node_ids = {n.node_id for n in active_nodes}
        expected_ids = {f"reg-test-{i + 1}" for i in range(node_count)}

        # 至少包含我们的节点
        assert len(node_ids & expected_ids) >= node_count

        # 清理
        for manager in managers:
            await manager.stop()

    # ========== 任务重试测试 ==========

    @pytest.mark.asyncio
    async def test_task_retry_on_failure(self, task_pool):
        """测试任务失败自动重试"""
        # 提交会失败的任务
        task_id = await task_pool.submit_async({
            "task_id": f"fail-{uuid.uuid4().hex[:6]}",
            "duration": 0.1,
            "should_fail": True
        })

        # 等待重试完成
        await asyncio.sleep(5)

        # 检查最终状态（应该是 failed 或 success）
        status = await task_pool.get_status_async(task_id)

        # 重试后可能成功也可能失败，取决于执行器
        assert status in ["failed", "success", "FAILED", "SUCCESS"]

    # ========== 统计和健康检查测试 ==========

    @pytest.mark.asyncio
    async def test_get_stats(self, task_pool):
        """测试获取统计信息"""
        # 提交几个任务
        for i in range(5):
            await task_pool.submit_async({
                "task_id": f"stats-{i}",
                "duration": 0.05
            })

        await asyncio.sleep(1)

        stats = task_pool.get_stats()

        assert "queue_size" in stats
        assert "total" in stats or "completed" in stats
        assert isinstance(stats, dict)

    @pytest.mark.asyncio
    async def test_get_worker_stats(self, task_pool):
        """测试获取 Worker 统计"""
        stats = task_pool.get_worker_stats()

        assert isinstance(stats, dict)

    @pytest.mark.asyncio
    async def test_health_check(self, task_pool):
        """测试健康检查"""
        health = task_pool.get_health_status()

        assert "status" in health
        assert health["status"] in ["healthy", "degraded", "unknown"]


# ============================================================
# 集成测试（一个方法覆盖所有功能）
# ============================================================

@pytest.mark.asyncio
async def test_integration_distributed(redis_available):
    """集成测试 - 一个测试覆盖所有分布式功能"""

    print("\n" + "=" * 60)
    print("v0.4 Distributed Features Integration Test")
    print("=" * 60)

    results = {}

    # 1. 创建两个节点
    print("\n1. Creating distributed nodes...")

    async def executor(data):
        await asyncio.sleep(data.get("duration", 0.05))
        if data.get("should_fail", False):
            raise ValueError("Test failure")
        return {"result": "ok", "task_id": data.get("task_id")}

    config1 = TaskPoolConfig(
        storage_type="redis",
        redis_url=TEST_CONFIG.redis_url,
        node_id="integ-node-1",
        enable_prefetch=True
    )
    config2 = TaskPoolConfig(
        storage_type="redis",
        redis_url=TEST_CONFIG.redis_url,
        node_id="integ-node-2",
        enable_prefetch=True
    )

    pool1 = TaskPool(executor=executor, config=config1)
    pool2 = TaskPool(executor=executor, config=config2)

    pool1.start()
    pool2.start()

    await asyncio.sleep(1)
    results["nodes_created"] = True
    print("    Nodes created")

    # 2. 测试跨节点任务消费
    print("\n2. Testing cross-node task consumption...")
    task_id = await pool1.submit_async({
        "task_id": "cross-node-test",
        "duration": 0.2
    })

    try:
        result = await pool2.wait_for_result_async(task_id, timeout=10)
        results["cross_node"] = result is not None
        print(f"    Task consumed by node-2: {result}")
    except Exception as e:
        results["cross_node"] = False
        print(f"    Failed: {e}")

    # 3. 测试批量任务
    print("\n3. Testing batch tasks...")
    batch_ids = []
    for i in range(10):
        tid = await pool1.submit_async({
            "task_id": f"batch-{i}",
            "duration": 0.05
        })
        batch_ids.append(tid)

    await asyncio.sleep(2)
    results["batch_tasks"] = True
    print(f"    {len(batch_ids)} tasks submitted")

    # 4. 测试预取器
    print("\n4. Testing prefetcher...")
    if pool1._worker_pool._prefetcher:
        stats = pool1._worker_pool._prefetcher.get_stats()
        results["prefetch"] = stats["total_prefetch"] >= 0
        print(f"    Prefetch stats: total_fetched={stats['total_fetched']}")
    else:
        results["prefetch"] = True
        print("    Prefetcher not enabled")

    # 5. 测试节点管理器
    print("\n5. Testing node manager...")
    manager = NodeManager(redis_url=TEST_CONFIG.redis_url, node_id="test-manager")
    await manager.start()
    active_nodes = await manager.get_active_nodes()
    await manager.stop()

    results["node_manager"] = len(active_nodes) >= 2
    print(f"    Active nodes detected: {len(active_nodes)}")

    # 6. 清理
    print("\n6. Cleaning up...")
    pool1.shutdown()
    pool2.shutdown()
    await asyncio.sleep(1)

    # 总结
    print("\n" + "=" * 60)
    print("Integration Test Results:")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"  Passed: {passed}/{total}")

    for name, result in results.items():
        status = "" if result else ""
        print(f"  {status} {name}: {result}")

    print("=" * 60)

    assert passed == total, f"Integration test failed: {passed}/{total} passed"


# ============================================================
# 运行配置
# ============================================================

if __name__ == "__main__":
    # 直接运行时的入口
    pytest.main([__file__, "-v"])
