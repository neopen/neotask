"""
@FileName: base.py
@Description: 锁抽象基类 - 扩展扫描功能
@Author: HiPeng
@Time: 2026/4/15
"""

import asyncio
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator, List, Dict, Any


class TaskLock(ABC):
    """分布式锁抽象接口。"""

    @abstractmethod
    async def acquire(self, key: str, ttl: int = 30) -> bool:
        """获取锁。"""
        pass

    @abstractmethod
    async def release(self, key: str) -> bool:
        """释放锁。"""
        pass

    @abstractmethod
    async def extend(self, key: str, ttl: int = 30) -> bool:
        """延长锁的生存时间。"""
        pass

    @abstractmethod
    async def is_locked(self, key: str) -> bool:
        """检查锁是否被持有。"""
        pass

    @abstractmethod
    async def get_owner(self, key: str) -> Optional[str]:
        """获取锁的持有者。"""
        pass

    # ========== 新增：扫描和清理方法 ==========

    @abstractmethod
    async def scan_locks(self, pattern: str = "lock:*", count: int = 100) -> List[str]:
        """扫描所有锁键。

        Args:
            pattern: 键名匹配模式
            count: 每次扫描数量

        Returns:
            锁键列表
        """
        pass

    @abstractmethod
    async def get_lock_info(self, key: str) -> Optional[Dict[str, Any]]:
        """获取锁的详细信息。

        Args:
            key: 锁键名

        Returns:
            锁信息字典，包含 owner, ttl, created_at 等
        """
        pass

    @abstractmethod
    async def cleanup_stale_locks(self, ttl_threshold: Optional[int] = None) -> int:
        """清理僵尸锁。

        Args:
            ttl_threshold: TTL 阈值（秒），超过此时间的锁被视为僵尸

        Returns:
            清理的锁数量
        """
        pass

    @asynccontextmanager
    async def lock(self, key: str, ttl: int = 30, auto_extend: bool = False) -> AsyncGenerator[bool, None]:
        """上下文管理器，自动获取和释放锁。"""
        acquired = await self.acquire(key, ttl)

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
        """看门狗，定期延长锁。"""
        interval = interval or (ttl / 3)
        while True:
            await asyncio.sleep(interval)
            if not await self.extend(key, ttl):
                break
