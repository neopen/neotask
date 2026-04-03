"""
@FileName: base.py
@Description: 锁抽象基类
@Author: HiPeng
@Time: 2026/4/1 19:07
"""

import asyncio
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator


class TaskLock(ABC):
    """分布式锁抽象接口。

    使用示例：
        >>> lock = TaskLock()
        >>> async with lock.lock("my_key"):
        ...     # 临界区代码
        ...     pass
    """

    @abstractmethod
    async def acquire(self, key: str, ttl: int = 30) -> bool:
        """获取锁。

        Args:
            key: 锁的键名
            ttl: 锁的生存时间（秒）

        Returns:
            是否成功获取锁
        """
        pass

    @abstractmethod
    async def release(self, key: str) -> bool:
        """释放锁。

        Args:
            key: 锁的键名

        Returns:
            是否成功释放锁
        """
        pass

    @abstractmethod
    async def extend(self, key: str, ttl: int = 30) -> bool:
        """延长锁的生存时间。

        Args:
            key: 锁的键名
            ttl: 新的生存时间（秒）

        Returns:
            是否成功延长
        """
        pass

    @abstractmethod
    async def is_locked(self, key: str) -> bool:
        """检查锁是否被持有。

        Args:
            key: 锁的键名

        Returns:
            是否被锁定
        """
        pass

    @abstractmethod
    async def get_owner(self, key: str) -> Optional[str]:
        """获取锁的持有者。

        Args:
            key: 锁的键名

        Returns:
            持有者标识，如果没有锁则返回 None
        """
        pass

    @asynccontextmanager
    async def lock(self, key: str, ttl: int = 30, auto_extend: bool = False) -> AsyncGenerator[bool, None]:
        """上下文管理器，自动获取和释放锁。

        Args:
            key: 锁的键名
            ttl: 锁的生存时间（秒）
            auto_extend: 是否自动延长锁（看门狗）

        Yields:
            是否成功获取锁
        """
        acquired = await self.acquire(key, ttl)

        # 启动看门狗
        watchdog_task = None
        if acquired and auto_extend:
            watchdog_task = asyncio.create_task(self._watchdog(key, ttl))

        try:
            yield acquired
        finally:
            if watchdog_task:
                watchdog_task.cancel()
                try:
                    await watchdog_task
                except asyncio.CancelledError:
                    pass

            if acquired:
                await self.release(key)

    async def _watchdog(self, key: str, ttl: int, interval: float = None):
        """看门狗，定期延长锁。

        Args:
            key: 锁的键名
            ttl: 锁的生存时间（秒）
            interval: 延长间隔（秒），默认为 ttl/3
        """
        interval = interval or (ttl / 3)
        while True:
            await asyncio.sleep(interval)
            if not await self.extend(key, ttl):
                break
