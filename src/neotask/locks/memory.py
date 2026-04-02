"""
@FileName: memory.py
@Description: 内存锁。In-memory lock implementation.
@Author: HiPeng
@Time: 2026/4/1 19:08
"""

import asyncio
from typing import Dict
from neotask.locks.base import DistributedLock


class MemoryLock(DistributedLock):
    """In-memory distributed lock (for single node)."""

    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def _get_lock(self, key: str) -> asyncio.Lock:
        async with self._global_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    async def acquire(self, key: str, ttl: int = 30) -> bool:
        lock = await self._get_lock(key)
        return await lock.acquire()

    async def release(self, key: str) -> bool:
        lock = await self._get_lock(key)
        lock.release()
        return True

    async def extend(self, key: str, ttl: int = 30) -> bool:
        # Memory lock doesn't need extension
        return True