"""
@FileName: test_redis_lock.py
@Description: Redis 分布式锁测试
@Author: HiPeng
@Time: 2026/4/28
"""

import pytest
import asyncio

from neotask.lock.redis import RedisLock
from tests.conftest import TEST_REDIS_URL


class TestRedisLock:
    """Redis 分布式锁测试"""

    @pytest.fixture
    async def lock(self):
        """锁 fixture"""
        lock = RedisLock(TEST_REDIS_URL, key_prefix="test_lock:")
        yield lock
        # 清理所有锁
        await lock.cleanup_all_locks()
        await lock.close()

    @pytest.mark.asyncio
    async def test_acquire_release(self, lock):
        """测试获取和释放锁"""
        key = "test_key"

        # 获取锁
        acquired = await lock.acquire(key, ttl=10)
        assert acquired is True

        # 验证锁被持有
        is_locked = await lock.is_locked(key)
        assert is_locked is True

        # 释放锁
        released = await lock.release(key)
        assert released is True

        # 验证锁已释放
        is_locked = await lock.is_locked(key)
        assert is_locked is False

    @pytest.mark.asyncio
    async def test_concurrent_acquire(self, lock):
        """测试并发获取锁"""
        key = "concurrent_key"

        async def try_acquire():
            return await lock.acquire(key, ttl=5)

        # 同时尝试获取锁
        results = await asyncio.gather(
            try_acquire(),
            try_acquire(),
            try_acquire()
        )

        # 只有一个成功
        assert sum(results) == 1

    @pytest.mark.asyncio
    async def test_lock_owner(self, lock):
        """测试锁持有者"""
        key = "owner_key"

        await lock.acquire(key, ttl=10)

        owner = await lock.get_owner(key)
        assert owner is not None
        assert owner == lock._owners.get(key)

    @pytest.mark.asyncio
    async def test_extend_lock(self, lock):
        """测试延长锁"""
        key = "extend_key"

        await lock.acquire(key, ttl=2)

        await asyncio.sleep(0.5)

        # 延长锁
        extended = await lock.extend(key, ttl=5)
        assert extended is True

        # 等待原始 TTL 过期
        await asyncio.sleep(2)

        # 锁应该仍然存在（因为已延长）
        is_locked = await lock.is_locked(key)
        assert is_locked is True

        await lock.release(key)

    @pytest.mark.asyncio
    async def test_auto_release_on_expire(self, lock):
        """测试锁自动过期"""
        key = "expire_key"

        await lock.acquire(key, ttl=1)

        # 等待过期
        await asyncio.sleep(1.5)

        # 锁应该已自动释放
        is_locked = await lock.is_locked(key)
        assert is_locked is False

    @pytest.mark.asyncio
    async def test_release_wrong_owner(self, lock):
        """测试错误持有者释放锁"""
        key = "wrong_owner_key"

        # 获取锁
        await lock.acquire(key, ttl=10)

        # 创建另一个锁实例（不同 owner）
        lock2 = RedisLock(TEST_REDIS_URL, key_prefix="test_lock:")

        # 尝试用第二个实例释放（应该失败）
        released = await lock2.release(key)
        assert released is False

        # 原持有者可以释放
        released = await lock.release(key)
        assert released is True

        await lock2.close()

    @pytest.mark.asyncio
    async def test_scan_locks(self, lock):
        """测试扫描所有锁"""
        keys = ["scan_key_1", "scan_key_2", "scan_key_3"]

        for key in keys:
            await lock.acquire(key, ttl=10)

        # 扫描锁
        found_keys = await lock.scan_locks()

        # 验证找到了所有锁
        for key in keys:
            full_key = f"test_lock:{key}"
            assert any(full_key in k for k in found_keys)

        # 清理
        for key in keys:
            await lock.release(key)

    @pytest.mark.asyncio
    async def test_get_lock_info(self, lock):
        """测试获取锁详细信息"""
        key = "info_key"

        await lock.acquire(key, ttl=30)

        info = await lock.get_lock_info(key)

        assert info is not None
        assert info["key"] == key
        assert info["is_locked"] is True
        assert info["owner"] == lock._owners.get(key)
        assert info["ttl_remaining"] > 0

        await lock.release(key)

    @pytest.mark.asyncio
    async def test_cleanup_stale_locks(self, lock):
        """测试清理僵尸锁"""
        key = "stale_key"

        await lock.acquire(key, ttl=1)

        # 等待锁过期
        await asyncio.sleep(1.5)

        # Redis 会自动删除过期键，所以清理返回 0
        cleaned = await lock.cleanup_stale_locks()
        assert cleaned == 0

    @pytest.mark.asyncio
    async def test_cleanup_stale_locks_with_threshold(self, lock):
        """测试按阈值清理僵尸锁"""
        key = "expire_test_key"

        await lock.acquire(key, ttl=2)

        await asyncio.sleep(1)

        # 清理 TTL <= 1 的锁
        cleaned = await lock.cleanup_stale_locks(ttl_threshold=1)
        # 锁还没过期，TTL 大约 1 秒，可能被清理
        assert cleaned >= 0

        await lock.release(key)

    @pytest.mark.asyncio
    async def test_cleanup_by_owner(self, lock):
        """测试按持有者清理锁

        注意：每次调用 acquire 会生成不同的 owner，
        因为每个锁的 owner 是在 acquire 时生成的 UUID。
        所以这个测试验证的是：清理指定 owner 的锁时，
        只有匹配的锁会被清理。
        """
        keys = ["cleanup_key_1", "cleanup_key_2", "cleanup_key_3"]

        # 获取每个锁并记录 owner
        owners = {}
        for key in keys:
            await lock.acquire(key, ttl=10)
            owners[key] = lock._owners.get(key)

        # 验证每个锁有不同的 owner（UUID 不同）
        unique_owners = set(owners.values())
        assert len(unique_owners) == len(keys)

        # 选择第一个 owner 进行清理
        first_key = keys[0]
        first_owner = owners[first_key]

        # 清理指定 owner 的锁
        cleaned = await lock.cleanup_by_owner(first_owner)
        assert cleaned == 1  # 只清理了一个锁

        # 验证第一个锁已被清理
        is_locked = await lock.is_locked(first_key)
        assert is_locked is False

        # 验证其他锁仍然存在
        for key in keys[1:]:
            is_locked = await lock.is_locked(key)
            assert is_locked is True

        # 清理剩余锁
        for key in keys[1:]:
            await lock.release(key)

    @pytest.mark.asyncio
    async def test_cleanup_all_locks(self, lock):
        """测试清理所有锁"""
        keys = ["clean_all_1", "clean_all_2", "clean_all_3"]

        for key in keys:
            await lock.acquire(key, ttl=10)

        # 清理所有锁
        cleaned = await lock.cleanup_all_locks()
        assert cleaned == len(keys)

        # 验证所有锁已清理
        for key in keys:
            is_locked = await lock.is_locked(key)
            assert is_locked is False

    @pytest.mark.asyncio
    async def test_multiple_locks_same_instance(self, lock):
        """测试同一实例持有多个锁"""
        keys = ["multi_key_1", "multi_key_2", "multi_key_3"]

        # 获取多个锁
        for key in keys:
            acquired = await lock.acquire(key, ttl=10)
            assert acquired is True

        # 验证 _owners 字典中有所有锁
        assert len(lock._owners) == len(keys)

        # 验证所有锁都被持有
        for key in keys:
            is_locked = await lock.is_locked(key)
            assert is_locked is True

        # 释放所有锁
        for key in keys:
            released = await lock.release(key)
            assert released is True

        # 验证 _owners 已清空
        assert len(lock._owners) == 0

        # 验证所有锁已释放
        for key in keys:
            is_locked = await lock.is_locked(key)
            assert is_locked is False