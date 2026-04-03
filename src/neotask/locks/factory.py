"""
@FileName: factory.py
@Description: 锁工厂 - 创建分布式锁实例
@Author: HiPeng
@Time: 2026/4/3 22:28
"""
from contextlib import asynccontextmanager
from typing import Optional

from neotask.locks.base import TaskLock
from neotask.locks.memory import MemoryLock
from neotask.locks.redis import RedisLock
from neotask.locks.watchdog import WatchDog


class LockType:
    """锁类型常量。"""
    MEMORY = "memory"
    REDIS = "redis"


class LockFactory:
    """锁工厂，负责创建分布式锁实例。

    使用示例：
        >>> # 创建内存锁
        >>> lock = LockFactory.create_memory()
        >>> 
        >>> # 创建 Redis 锁
        >>> lock = LockFactory.create_redis("redis://localhost:6379")
        >>> 
        >>> # 根据配置创建
        >>> from neotask.models.config import LockConfig
        >>> config = LockConfig.redis("redis://localhost:6379")
        >>> lock = LockFactory.create(config)
    """

    @staticmethod
    def create_memory() -> MemoryLock:
        """创建内存锁实例。

        Returns:
            MemoryLock 实例
        """
        return MemoryLock()

    @staticmethod
    def create_redis(redis_url: str, key_prefix: str = None) -> RedisLock:
        """创建 Redis 锁实例。

        Args:
            redis_url: Redis 连接 URL
            key_prefix: 键前缀

        Returns:
            RedisLock 实例
        """
        return RedisLock(redis_url, key_prefix)

    @staticmethod
    def create(config) -> TaskLock:
        """根据配置创建锁实例。

        Args:
            config: LockConfig 配置对象

        Returns:
            TaskLock 实例

        Raises:
            ValueError: 不支持的锁类型
        """
        if config.type == LockType.MEMORY:
            return LockFactory.create_memory()

        elif config.type == LockType.REDIS:
            if not config.redis_url:
                raise ValueError("Redis URL is required for Redis lock")
            return LockFactory.create_redis(config.redis_url)

        else:
            raise ValueError(f"Unknown lock type: {config.type}")

    @staticmethod
    def create_watchdog(lock: TaskLock, interval_ratio: float = 0.3) -> WatchDog:
        """创建看门狗实例。

        Args:
            lock: 分布式锁实例
            interval_ratio: 续期间隔与 TTL 的比例

        Returns:
            WatchDog 实例
        """
        return WatchDog(lock, interval_ratio)


class LockManager:
    """锁管理器，统一管理锁的获取和释放。

    使用示例：
        >>> manager = LockManager(TaskLock())
        >>> async with manager.lock("my_key", ttl=30, auto_extend=True):
        ...     # 临界区代码
        ...     pass
    """

    def __init__(self, lock: TaskLock):
        """初始化锁管理器。

        Args:
            lock: 分布式锁实例
        """
        self._lock = lock
        self._watchdog = WatchDog(lock)

    async def acquire(self, key: str, ttl: int = 30, auto_extend: bool = False) -> bool:
        """获取锁。

        Args:
            key: 锁的键名
            ttl: 锁的生存时间（秒）
            auto_extend: 是否自动续期

        Returns:
            是否成功获取锁
        """
        success = await self._lock.acquire(key, ttl)

        if success and auto_extend:
            await self._watchdog.start(key, ttl)

        return success

    async def release(self, key: str) -> bool:
        """释放锁。"""
        await self._watchdog.stop(key)
        return await self._lock.release(key)

    @asynccontextmanager
    async def lock(self, key: str, ttl: int = 30, auto_extend: bool = False):
        """上下文管理器。"""
        acquired = await self.acquire(key, ttl, auto_extend)
        try:
            yield acquired
        finally:
            if acquired:
                await self.release(key)

    async def extend(self, key: str, ttl: int = 30) -> bool:
        """延长锁。"""
        return await self._lock.extend(key, ttl)

    async def is_locked(self, key: str) -> bool:
        """检查锁状态。"""
        return await self._lock.is_locked(key)

    async def get_owner(self, key: str) -> Optional[str]:
        """获取锁持有者。"""
        return await self._lock.get_owner(key)

    async def shutdown(self):
        """关闭管理器。"""
        await self._watchdog.stop_all()
        if hasattr(self._lock, 'close'):
            await self._lock.close()
