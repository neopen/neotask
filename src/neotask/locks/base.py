"""
@FileName: base.py
@Description: 锁接口。Distributed lock abstraction.
@Author: HiPeng
@Time: 2026/4/1 19:07
"""

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncGenerator


class DistributedLock(ABC):
    """Abstract distributed lock."""

    @abstractmethod
    async def acquire(self, key: str, ttl: int = 30) -> bool:
        """Acquire lock, returns True if successful."""
        pass

    @abstractmethod
    async def release(self, key: str) -> bool:
        """Release lock, returns True if successful."""
        pass

    @abstractmethod
    async def extend(self, key: str, ttl: int = 30) -> bool:
        """Extend lock TTL."""
        pass

    @asynccontextmanager
    async def lock(self, key: str, ttl: int = 30) -> AsyncGenerator[bool, None]:
        """Context manager for lock acquisition."""
        acquired = await self.acquire(key, ttl)
        try:
            yield acquired
        finally:
            if acquired:
                await self.release(key)
