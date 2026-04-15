"""
@FileName: memory.py
@Description: 内存锁实现 - 添加扫描和清理功能
@Author: HiPeng
@Time: 2026/4/15
"""

import asyncio
import time
import uuid
from typing import Dict, Optional, List, Any

from neotask.lock.base import TaskLock


class MemoryLock(TaskLock):
    """内存锁 - 支持扫描和清理"""

    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._owners: Dict[str, str] = {}
        self._expire_times: Dict[str, float] = {}
        self._created_times: Dict[str, float] = {}
        self._global_lock = asyncio.Lock()

    def _get_lock(self, key: str) -> asyncio.Lock:
        """获取或创建锁"""
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def acquire(self, key: str, ttl: int = 30) -> bool:
        """获取锁"""
        lock = self._get_lock(key)

        async with self._global_lock:
            # 检查是否已被持有且未过期
            if key in self._owners and self._owners[key] != "":
                expire_time = self._expire_times.get(key, 0)
                if time.time() < expire_time:
                    return False

            # 获取锁
            await lock.acquire()
            self._owners[key] = str(uuid.uuid4())
            self._expire_times[key] = time.time() + ttl
            self._created_times[key] = time.time()
            return True

    async def release(self, key: str) -> bool:
        """释放锁"""
        async with self._global_lock:
            if key not in self._locks:
                return False

            lock = self._locks[key]
            if lock.locked():
                lock.release()
                self._owners.pop(key, None)
                self._expire_times.pop(key, None)
                self._created_times.pop(key, None)
                return True
        return False

    async def extend(self, key: str, ttl: int = 30) -> bool:
        """延长锁的生存时间"""
        async with self._global_lock:
            if key not in self._owners:
                return False

            expire_time = self._expire_times.get(key, 0)
            if time.time() > expire_time:
                return False

            self._expire_times[key] = time.time() + ttl
            return True

    async def is_locked(self, key: str) -> bool:
        """检查锁是否被持有"""
        async with self._global_lock:
            if key not in self._locks:
                return False
            return self._locks[key].locked()

    async def get_owner(self, key: str) -> Optional[str]:
        """获取锁的持有者"""
        async with self._global_lock:
            return self._owners.get(key)

    # ========== 新增：扫描和清理方法 ==========

    async def scan_locks(self, pattern: str = "lock:*", count: int = 100) -> List[str]:
        """扫描所有锁键

        Args:
            pattern: 键名匹配模式（内存锁忽略 pattern，返回所有锁）
            count: 每次扫描数量

        Returns:
            锁键列表
        """
        async with self._global_lock:
            # 内存锁返回所有锁的键名
            keys = list(self._locks.keys())
            return keys[:count]

    async def get_lock_info(self, key: str) -> Optional[Dict[str, Any]]:
        """获取锁的详细信息

        Args:
            key: 锁键名

        Returns:
            锁信息字典
        """
        async with self._global_lock:
            if key not in self._locks:
                return None

            now = time.time()
            expire_time = self._expire_times.get(key, 0)
            created_time = self._created_times.get(key, now)

            return {
                "key": key,
                "owner": self._owners.get(key),
                "is_locked": self._locks[key].locked(),
                "created_at": created_time,
                "expire_at": expire_time,
                "ttl_remaining": max(0.0, expire_time - now) if expire_time > 0 else 0,
                "is_stale": expire_time > 0 and now > expire_time
            }

    async def cleanup_stale_locks(self, ttl_threshold: Optional[int] = None) -> int:
        """清理僵尸锁

        Args:
            ttl_threshold: TTL 阈值（秒），超过此时间的锁被视为僵尸

        Returns:
            清理的锁数量
        """
        cleaned = 0
        async with self._global_lock:
            now = time.time()
            stale_keys = []

            for key, expire_time in self._expire_times.items():
                is_stale = False

                if ttl_threshold is not None:
                    # 基于创建时间判断
                    created_time = self._created_times.get(key, now)
                    if now - created_time > ttl_threshold:
                        is_stale = True
                else:
                    # 基于过期时间判断
                    if expire_time > 0 and now > expire_time:
                        is_stale = True

                if is_stale:
                    stale_keys.append(key)

            for key in stale_keys:
                if key in self._locks and self._locks[key].locked():
                    self._locks[key].release()
                self._owners.pop(key, None)
                self._expire_times.pop(key, None)
                self._created_times.pop(key, None)
                cleaned += 1

        return cleaned
