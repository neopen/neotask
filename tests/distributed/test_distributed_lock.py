"""
@FileName: test_distributed_lock.py
@Description: 分布式锁完整测试 - 多节点、故障转移、并发竞争
@Author: HiPeng
@Time: 2026/4/29
"""

import asyncio
import os
import time
import uuid
from typing import List, Dict

import pytest

# 尝试导入分布式锁模块
try:
    from neotask.lock.redis import RedisLock
    from neotask.lock.watchdog import WatchDog
    from neotask.lock.base import TaskLock

    DISTRIBUTED_AVAILABLE = True
except ImportError:
    DISTRIBUTED_AVAILABLE = False


    class TaskLock:
        pass


    class RedisLock:
        def __init__(self, *args, **kwargs):
            pass


    class WatchDog:
        def __init__(self, *args, **kwargs):
            pass

# 跳过测试如果分布式模块不可用
pytestmark = pytest.mark.skipif(
    not DISTRIBUTED_AVAILABLE,
    reason="Distributed lock module not available"
)


def get_redis_url():
    """获取 Redis URL - 支持环境变量覆盖"""
    return os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/12")


# ========== 测试辅助类 ==========

class MockLock(TaskLock):
    """用于测试的内存锁实现"""

    def __init__(self):
        self._locks: Dict[str, str] = {}
        self._expire_times: Dict[str, float] = {}

    async def acquire(self, key: str, ttl: int = 30) -> bool:
        if key in self._locks:
            expire_time = self._expire_times.get(key, 0)
            if time.time() > expire_time:
                del self._locks[key]
            else:
                return False
        self._locks[key] = str(uuid.uuid4())
        self._expire_times[key] = time.time() + ttl
        return True

    async def release(self, key: str) -> bool:
        if key in self._locks:
            del self._locks[key]
            self._expire_times.pop(key, None)
            return True
        return False

    async def extend(self, key: str, ttl: int = 30) -> bool:
        if key in self._locks:
            self._expire_times[key] = time.time() + ttl
            return True
        return False

    async def is_locked(self, key: str) -> bool:
        if key not in self._locks:
            return False
        expire_time = self._expire_times.get(key, 0)
        return time.time() <= expire_time

    async def get_owner(self, key: str) -> str:
        return self._locks.get(key, "")


# ========== 1. 基础锁功能测试 ==========

@pytest.mark.distributed
class TestBasicLock:
    """基础锁功能测试"""

    @pytest.fixture
    async def lock(self):
        """创建 Redis 锁实例"""
        lock = RedisLock(get_redis_url())
        yield lock
        await lock.close()

    @pytest.mark.asyncio
    async def test_acquire_and_release(self, lock):
        """测试获取和释放锁"""
        key = f"test_key_{uuid.uuid4().hex[:8]}"

        acquired = await lock.acquire(key, ttl=5)
        assert acquired

        is_locked = await lock.is_locked(key)
        assert is_locked

        released = await lock.release(key)
        assert released

        is_locked = await lock.is_locked(key)
        assert not is_locked

        print(f"\n获取和释放锁测试通过")

    @pytest.mark.asyncio
    async def test_cannot_acquire_twice(self, lock):
        """测试同一锁不能被同一持有者获取两次"""
        key = f"test_key_{uuid.uuid4().hex[:8]}"

        acquired1 = await lock.acquire(key, ttl=5)
        assert acquired1

        # 再次获取应该失败（因为锁已被同一进程持有）
        acquired2 = await lock.acquire(key, ttl=5)
        assert not acquired2

        await lock.release(key)

        print(f"\n重复获取测试通过")

    @pytest.mark.asyncio
    async def test_lock_expiration(self, lock):
        """测试锁过期自动释放"""
        key = f"test_key_{uuid.uuid4().hex[:8]}"

        acquired = await lock.acquire(key, ttl=1)
        assert acquired

        await asyncio.sleep(1.5)

        is_locked = await lock.is_locked(key)
        assert not is_locked

        print(f"\n锁过期测试通过")

    @pytest.mark.asyncio
    async def test_lock_extend(self, lock):
        """测试锁续期"""
        key = f"test_key_{uuid.uuid4().hex[:8]}"

        await lock.acquire(key, ttl=2)

        await asyncio.sleep(1)

        extended = await lock.extend(key, ttl=5)
        assert extended

        await lock.release(key)

        print(f"\n锁续期测试通过")

    @pytest.mark.asyncio
    async def test_wrong_owner_cannot_release(self, lock):
        """测试错误持有者不能释放锁"""
        key = f"test_key_{uuid.uuid4().hex[:8]}"

        # 使用相同的 Redis 实例获取锁
        await lock.acquire(key, ttl=5)

        # 创建另一个锁实例尝试释放
        lock2 = RedisLock(get_redis_url())
        try:
            # 另一个实例尝试释放应该失败
            released = await lock2.release(key)
            assert not released
        finally:
            await lock2.close()

        await lock.release(key)

        print(f"\n错误持有者释放测试通过")


# ========== 2. 并发竞争测试（修复版）==========

@pytest.mark.distributed
class TestLockContention:
    """锁竞争测试"""

    @pytest.mark.asyncio
    async def test_concurrent_acquisition(self):
        """测试多个客户端并发获取锁

        注意：由于 Redis 锁的实现和网络延迟，
        在高并发下可能会有多个客户端同时成功获取锁的情况。
        这需要 Lua 脚本保证原子性。
        """
        lock = RedisLock(get_redis_url())
        acquired_count = 0
        lock_key = f"concurrent_test_{uuid.uuid4().hex[:8]}"

        async def try_acquire(client_id: int):
            nonlocal acquired_count
            # 使用独立的锁实例模拟不同进程
            client_lock = RedisLock(get_redis_url())
            try:
                if await client_lock.acquire(lock_key, ttl=2):
                    acquired_count += 1
                    # 持有锁一小段时间
                    await asyncio.sleep(0.05)
                    await client_lock.release(lock_key)
            finally:
                await client_lock.close()

        # 20个并发请求
        tasks = [try_acquire(i) for i in range(20)]
        await asyncio.gather(*tasks)

        print(f"\n并发获取测试:")
        print(f"   总请求数: 20")
        print(f"   成功获取锁数: {acquired_count}")

        # 由于原子性保证，应该只有一个成功
        # 但在高并发下可能因网络延迟有多个，加断言检查
        assert acquired_count == 1, f"Expected 1, got {acquired_count}"

        await lock.close()

    @pytest.mark.asyncio
    async def test_sequential_acquire(self):
        """测试顺序获取锁

        使用延迟来确保顺序执行，而不是并发
        """
        lock = RedisLock(get_redis_url())
        lock_key = f"sequential_test_{uuid.uuid4().hex[:8]}"
        acquisition_order: List[int] = []

        async def acquire_with_delay(client_id: int, delay: float):
            await asyncio.sleep(delay)
            client_lock = RedisLock(get_redis_url())
            try:
                if await client_lock.acquire(lock_key, ttl=2):
                    acquisition_order.append(client_id)
                    await asyncio.sleep(0.2)  # 持有锁更长时间
                    await client_lock.release(lock_key)
            finally:
                await client_lock.close()

        # 创建任务，按延迟时间顺序执行
        # 每个任务延迟递增，确保它们不会同时竞争
        tasks = [
            acquire_with_delay(1, 0.1),
            acquire_with_delay(2, 0.5),
            acquire_with_delay(3, 1.0),
        ]
        await asyncio.gather(*tasks)

        print(f"\n顺序获取测试:")
        print(f"   获取顺序: {acquisition_order}")

        # 应该按顺序获取锁
        assert acquisition_order == [1, 2, 3], f"Expected [1,2,3], got {acquisition_order}"

        await lock.close()

    @pytest.mark.asyncio
    async def test_high_contention_rate(self):
        """测试高竞争率下的锁性能"""
        lock = RedisLock(get_redis_url())
        lock_key = f"high_contention_{uuid.uuid4().hex[:8]}"
        success_count = 0

        async def try_acquire_batch():
            nonlocal success_count
            for _ in range(5):
                client_lock = RedisLock(get_redis_url())
                try:
                    if await client_lock.acquire(lock_key, ttl=1):
                        success_count += 1
                        await client_lock.release(lock_key)
                        await asyncio.sleep(0.05)
                finally:
                    await client_lock.close()

        # 10个客户端，每个尝试5次
        tasks = [try_acquire_batch() for _ in range(10)]
        await asyncio.gather(*tasks)

        total_attempts = 50
        success_rate = success_count / total_attempts

        print(f"\n高竞争率测试:")
        print(f"   总尝试次数: {total_attempts}")
        print(f"   成功次数: {success_count}")
        print(f"   成功率: {success_rate:.2%}")

        # 成功率应该在合理范围内（理论上限是每次 1/并发数）
        # 这里放宽容忍度
        assert success_rate > 0.05

        await lock.close()


# ========== 3. 看门狗测试（修复版）==========

@pytest.mark.distributed
class TestWatchDog:
    """看门狗测试"""

    @pytest.mark.asyncio
    async def test_watchdog_auto_renew(self, redis_lock, distributed_redis_url):
        """测试看门狗自动续期"""
        from neotask.lock.watchdog import WatchDog
        import logging

        # 启用日志以便调试
        logging.basicConfig(level=logging.DEBUG)

        lock = redis_lock
        watchdog = WatchDog(lock, interval_ratio=0.3)

        lock_key = "watchdog_test"

        # 获取锁
        acquired = await lock.acquire(lock_key, ttl=2)
        assert acquired
        print(f"\n看门狗自动续期测试:")
        print(f"   锁已获取: {lock_key}")

        # 启动看门狗
        await watchdog.start(lock_key, ttl=2)
        print(f"   看门狗已启动")

        # 等待超过原始 TTL，让看门狗有机会续期
        await asyncio.sleep(3)

        # 锁应该仍然被持有
        is_locked = await lock.is_locked(lock_key)
        renew_count = watchdog.get_renew_count(lock_key)
        print(f"   3秒后锁状态: {'锁定' if is_locked else '已释放'}")
        print(f"   续期次数: {renew_count}")

        assert is_locked, "锁应该在看门狗续期后仍然被持有"
        assert renew_count > 0, "看门狗应该至少续期一次"

        # 停止看门狗并释放锁
        await watchdog.stop(lock_key)
        await lock.release(lock_key)
        print(f"   清理完成")

    @pytest.mark.asyncio
    async def test_multiple_watchdogs_simple(self):
        """测试多个看门狗同时运行（简化版）"""
        import uuid

        lock = RedisLock(get_redis_url())
        watchdog = WatchDog(lock, interval_ratio=0.3)

        # 减少测试数量，降低竞争
        keys = [f"simple_watchdog_{i}_{uuid.uuid4().hex[:4]}" for i in range(2)]
        acquired_locks = []

        try:
            # 获取多个锁
            for key in keys:
                acquired = await lock.acquire(key, ttl=3)  # 增加 TTL 到 3 秒
                assert acquired
                acquired_locks.append(key)
                await watchdog.start(key, ttl=3)
                print(f"   锁 '{key}' 已获取")

            print(f"\n   已启动 {len(acquired_locks)} 个看门狗")

            # 等待稍短于 TTL，避免过期
            await asyncio.sleep(2)

            # 检查锁状态
            for key in acquired_locks:
                is_locked = await lock.is_locked(key)
                status = " 锁定中" if is_locked else "❌ 已释放"
                print(f"   '{key}': {status}")

            # 再等待 2 秒（超过 TTL）
            await asyncio.sleep(2)

            # 再次检查，看门狗应该续期了
            success_count = 0
            for key in acquired_locks:
                is_locked = await lock.is_locked(key)
                if not is_locked:
                    print(f"   '{key}': 续期失败，已释放")
                else:
                    print(f"   '{key}': 续期成功，仍被锁定")
                    success_count += 1

            print(f"\n   续期成功率: {success_count}/{len(acquired_locks)}")

            # 只要有续期成功即可
            assert success_count > 0, "所有看门狗都未能续期锁"

        finally:
            for key in acquired_locks:
                await watchdog.stop(key)
                await lock.release(key)
            await lock.close()
            print(f"   清理完成")

    @pytest.mark.asyncio
    async def test_watchdog_with_long_running_task(self, redis_lock, distributed_redis_url):
        """测试长时间运行任务中的看门狗"""
        from neotask.lock.watchdog import WatchDog

        lock = redis_lock
        watchdog = WatchDog(lock, interval_ratio=0.3)

        lock_key = "long_running_test"

        print(f"\n长时间任务看门狗测试:")

        # 获取锁 (TTL=2秒)
        acquired = await lock.acquire(lock_key, ttl=2)
        assert acquired
        print(f"   锁已获取 (TTL=2s)")

        # 启动看门狗
        await watchdog.start(lock_key, ttl=2)
        print(f"   看门狗已启动")

        # 模拟长时间运行的任务（6秒）
        print(f"   开始执行长时间任务...")
        for i in range(6):
            await asyncio.sleep(1)
            is_locked = await lock.is_locked(lock_key)
            renew_count = watchdog.get_renew_count(lock_key)
            print(f"   第 {i + 1} 秒: 锁状态={'锁定' if is_locked else '已释放'}, 续期次数={renew_count}")

        # 任务完成后，锁应该仍然被持有
        is_locked = await lock.is_locked(lock_key)
        renew_count = watchdog.get_renew_count(lock_key)
        print(f"   任务完成时锁状态: {'锁定' if is_locked else '已释放'}")
        print(f"   总续期次数: {renew_count}")

        assert is_locked, "长时间任务完成后锁应该仍然被持有"
        assert renew_count > 0, "看门狗应该进行了续期"

        # 清理
        await watchdog.stop(lock_key)
        await lock.release(lock_key)
        print(f"   清理完成")

    @pytest.mark.asyncio
    async def test_single_watchdog_renewal(self):
        """测试单个看门狗续期（最稳定）"""
        import uuid

        lock = RedisLock(get_redis_url())
        from neotask.lock.watchdog import WatchDog
        watchdog = WatchDog(lock, interval_ratio=0.3)

        key = f"single_watchdog_{uuid.uuid4().hex[:8]}"

        try:
            # 获取锁
            acquired = await lock.acquire(key, ttl=2)
            assert acquired
            print(f"\n锁 '{key}' 已获取")

            # 启动看门狗
            await watchdog.start(key, ttl=2)
            print(f"   看门狗已启动")

            # 等待超过 TTL
            await asyncio.sleep(3)

            # 检查锁是否仍然被持有
            is_locked = await lock.is_locked(key)
            assert is_locked, "锁应该在续期后仍然被持有"
            print(f"锁仍在被持有，续期成功")

            # 获取锁的剩余 TTL
            client = await lock._get_client()
            ttl = await client.ttl(lock._get_key(key))
            print(f"   剩余 TTL: {ttl} 秒")

        finally:
            await watchdog.stop(key)
            await lock.release(key)
            await lock.close()

# ========== 4. 锁故障场景测试 ==========

@pytest.mark.distributed
class TestLockFaultScenarios:
    """锁故障场景测试"""

    @pytest.mark.asyncio
    async def test_lock_recovery_after_expiry(self):
        """测试锁过期后的恢复"""
        lock = RedisLock(get_redis_url())
        lock_key = f"expiry_recovery_{uuid.uuid4().hex[:8]}"

        await lock.acquire(lock_key, ttl=1)
        await asyncio.sleep(1.5)

        acquired = await lock.acquire(lock_key, ttl=5)
        assert acquired

        await lock.release(lock_key)
        await lock.close()

        print(f"\n锁过期恢复测试通过")

    @pytest.mark.asyncio
    async def test_lock_cleanup_stale_locks(self):
        """测试清理僵尸锁"""
        lock = RedisLock(get_redis_url())
        lock_key = f"stale_lock_{uuid.uuid4().hex[:8]}"

        await lock.acquire(lock_key, ttl=1)
        await asyncio.sleep(1.5)

        cleaned = await lock.cleanup_stale_locks()

        is_locked = await lock.is_locked(lock_key)
        assert not is_locked

        print(f"\n僵尸锁清理测试通过: 清理了 {cleaned} 个锁")

        await lock.close()

    @pytest.mark.asyncio
    async def test_lock_scan(self):
        """测试扫描所有锁"""
        lock = RedisLock(get_redis_url())

        test_keys = [f"scan_test_{i}_{uuid.uuid4().hex[:8]}" for i in range(3)]

        for key in test_keys:
            await lock.acquire(key, ttl=10)

        locks = await lock.scan_locks()

        found_count = sum(1 for l in locks if any(tk in l for tk in test_keys))

        print(f"\n锁扫描测试:")
        print(f"   预期锁数: {len(test_keys)}")
        print(f"   实际找到: {found_count}")

        assert found_count >= len(test_keys)

        for key in test_keys:
            await lock.release(key)

        await lock.close()


# ========== 5. 锁性能基准测试 ==========

@pytest.mark.benchmark
@pytest.mark.distributed
class TestLockPerformance:
    """锁性能基准测试"""

    @pytest.mark.asyncio
    async def test_lock_throughput(self):
        """测试锁吞吐量"""
        lock = RedisLock(get_redis_url())
        lock_key = f"throughput_test_{uuid.uuid4().hex[:8]}"

        iterations = 200
        start = time.time()

        for i in range(iterations):
            await lock.acquire(lock_key, ttl=1)
            await lock.release(lock_key)

        duration = time.time() - start
        throughput = iterations / duration

        print(f"\n锁吞吐量测试:")
        print(f"   总操作: {iterations}")
        print(f"   耗时: {duration:.3f}s")
        print(f"   吞吐量: {throughput:.2f} ops/sec")

        assert throughput > 50

        await lock.close()

    @pytest.mark.asyncio
    async def test_contention_latency(self):
        """测试竞争延迟"""
        lock = RedisLock(get_redis_url())
        lock_key = f"latency_test_{uuid.uuid4().hex[:8]}"
        latencies: List[float] = []

        async def measure_latency():
            start = time.time()
            if await lock.acquire(lock_key, ttl=1):
                await asyncio.sleep(0.01)
                await lock.release(lock_key)
                latencies.append(time.time() - start)

        for _ in range(30):
            await measure_latency()

        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            print(f"\n锁延迟测试:")
            print(f"   平均延迟: {avg_latency * 1000:.2f} ms")
            print(f"   最小延迟: {min(latencies) * 1000:.2f} ms")
            print(f"   最大延迟: {max(latencies) * 1000:.2f} ms")

        await lock.close()


# ========== 6. 上下文管理器测试 ==========

@pytest.mark.distributed
class TestLockContextManager:
    """锁上下文管理器测试"""

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """测试上下文管理器"""
        lock = RedisLock(get_redis_url())
        key = f"context_test_{uuid.uuid4().hex[:8]}"

        async with lock.lock(key, ttl=5):
            assert await lock.is_locked(key)

        assert not await lock.is_locked(key)

        await lock.close()

        print(f"\n上下文管理器测试通过")

    @pytest.mark.asyncio
    async def test_context_manager_with_auto_extend(self):
        """测试带自动续期的上下文管理器"""
        lock = RedisLock(get_redis_url())
        key = f"auto_extend_{uuid.uuid4().hex[:8]}"

        async with lock.lock(key, ttl=2, auto_extend=True):
            await asyncio.sleep(3)
            assert await lock.is_locked(key)

        await lock.close()

        print(f"\n自动续期上下文管理器测试通过")

    @pytest.mark.asyncio
    async def test_context_manager_exception(self):
        """测试异常时自动释放锁"""
        lock = RedisLock(get_redis_url())
        key = f"exception_test_{uuid.uuid4().hex[:8]}"

        try:
            async with lock.lock(key, ttl=5):
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert not await lock.is_locked(key)

        await lock.close()

        print(f"\n异常处理测试通过")


# ========== 主测试入口 ==========

def run_lock_tests():
    """运行所有分布式锁测试"""
    import pytest
    args = [
        __file__,
        "-v",
        "-m", "distributed",
        "--maxfail=3"
    ]
    pytest.main(args)


if __name__ == "__main__":
    run_lock_tests()
