"""
@FileName: memory.py
@Description: 内存锁实现（单机环境）
@Author: HiPeng
@Time: 2026/4/1 19:08
"""

import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional

from neotask.locks.base import TaskLock


class MemoryLock(TaskLock):
    """内存锁实现，适用于单机环境。

    特点：
    - 无外部依赖
    - 高性能
    - 不支持多进程/多节点
    """

    def __init__(self):
        self._locks: Dict[str, str] = {}  # key -> owner
        self._expires: Dict[str, datetime] = {}  # key -> expire_time
        self._lock = asyncio.Lock()

    async def acquire(self, key: str, ttl: int = 30) -> bool:
        """获取内存锁。"""
        async with self._lock:
            # 检查锁是否存在且未过期
            if key in self._locks:
                expire_time = self._expires.get(key)
                if expire_time and expire_time > datetime.now():
                    return False
                # 锁已过期，清理
                del self._locks[key]
                del self._expires[key]

            # 获取锁
            owner = str(uuid.uuid4())
            self._locks[key] = owner
            self._expires[key] = datetime.now() + timedelta(seconds=ttl)
            return True

    async def release(self, key: str) -> bool:
        """释放内存锁。"""
        async with self._lock:
            if key in self._locks:
                del self._locks[key]
                del self._expires[key]
                return True
            return False

    async def extend(self, key: str, ttl: int = 30) -> bool:
        """延长内存锁。"""
        async with self._lock:
            if key not in self._locks:
                return False

            self._expires[key] = datetime.now() + timedelta(seconds=ttl)
            return True

    async def is_locked(self, key: str) -> bool:
        """检查锁是否被持有。"""
        async with self._lock:
            if key not in self._locks:
                return False

            expire_time = self._expires.get(key)
            if expire_time and expire_time <= datetime.now():
                # 锁已过期
                return False

            return True

    async def get_owner(self, key: str) -> Optional[str]:
        """获取锁的持有者。"""
        async with self._lock:
            if key not in self._locks:
                return None

            expire_time = self._expires.get(key)
            if expire_time and expire_time <= datetime.now():
                return None

            return self._locks[key]

    async def cleanup_expired(self) -> int:
        """清理过期的锁。"""
        async with self._lock:
            now = datetime.now()
            expired_keys = [
                key for key, expire_time in self._expires.items()
                if expire_time <= now
            ]
            for key in expired_keys:
                del self._locks[key]
                del self._expires[key]
            return len(expired_keys)
