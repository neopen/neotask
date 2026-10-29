"""
@FileName: memory.py
@Description: 内存锁实现 - 添加扫描和清理功能
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/15
"""

import threading
import time
import uuid
from typing import Dict, Optional, List, Any

from neotask.lock.base import TaskLock


class MemoryLock(TaskLock):
    """内存锁 - 支持扫描和清理

    实现要点：以「所有者 + 过期时间」字典记录锁状态，acquire 为非阻塞的
    原子写入，因此不存在跨事件循环等待。早期版本使用 ``asyncio.Lock``
    队列等待，当 TTL 过期后仍会在原 loop 上阻塞，且跨 loop 使用会直接
    报错；这里改用 ``threading.RLock`` 保护共享字典，保证同一进程内任意
    线程/事件循环调用都安全。
    """

    def __init__(self):
        # 保护以下字典的线程锁；所有临界区都不含 await，不会阻塞事件循环
        self._lock = threading.RLock()
        self._owners: Dict[str, str] = {}
        self._expire_times: Dict[str, float] = {}
        self._created_times: Dict[str, float] = {}

    async def acquire(self, key: str, ttl: int = 30) -> bool:
        """获取锁（非阻塞）

        同一 key 已被持有且未过期时立即返回 False，而不是排队等待，
        避免调用方在另一个事件循环上被长期挂起。
        """
        now = time.time()
        with self._lock:
            if key in self._owners:
                if now < self._expire_times.get(key, 0):
                    return False
                # 已过期：回收后重新持有
            self._owners[key] = str(uuid.uuid4())
            self._expire_times[key] = now + ttl
            self._created_times[key] = now
            return True

    async def release(self, key: str) -> bool:
        """释放锁"""
        with self._lock:
            if key not in self._owners:
                return False
            self._owners.pop(key, None)
            self._expire_times.pop(key, None)
            self._created_times.pop(key, None)
            return True

    async def extend(self, key: str, ttl: int = 30) -> bool:
        """延长锁的生存时间"""
        now = time.time()
        with self._lock:
            if key not in self._owners:
                return False

            expire_time = self._expire_times.get(key, 0)
            if now > expire_time:
                return False

            self._expire_times[key] = now + ttl
            return True

    async def is_locked(self, key: str) -> bool:
        """检查锁是否被持有（已过期的锁视为未持有）"""
        now = time.time()
        with self._lock:
            if key not in self._owners:
                return False
            return now < self._expire_times.get(key, 0)

    async def get_owner(self, key: str) -> Optional[str]:
        """获取锁的持有者"""
        with self._lock:
            return self._owners.get(key)

    # ========== 扫描和清理方法 ==========

    async def scan_locks(self, pattern: str = "lock:*", count: int = 100) -> List[str]:
        """扫描所有锁键

        Args:
            pattern: 键名匹配模式（内存锁忽略 pattern，返回所有锁）
            count: 每次扫描数量

        Returns:
            锁键列表
        """
        with self._lock:
            keys = list(self._owners.keys())
            return keys[:count]

    async def get_lock_info(self, key: str, ttl_threshold: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """获取锁的详细信息

        Args:
            key: 锁键名

        Returns:
            锁信息字典
        """
        with self._lock:
            if key not in self._owners:
                return None

            now = time.time()
            expire_time = self._expire_times.get(key, 0)
            created_time = self._created_times.get(key, now)

            return {
                "key": key,
                "owner": self._owners.get(key),
                "is_locked": expire_time > 0 and now < expire_time,
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
        with self._lock:
            now = time.time()
            stale_keys = []

            for key, expire_time in self._expire_times.items():
                if ttl_threshold is not None:
                    # 基于创建时间判断
                    created_time = self._created_times.get(key, now)
                    if now - created_time > ttl_threshold:
                        stale_keys.append(key)
                elif expire_time > 0 and now > expire_time:
                    # 基于过期时间判断
                    stale_keys.append(key)

            for key in stale_keys:
                self._owners.pop(key, None)
                self._expire_times.pop(key, None)
                self._created_times.pop(key, None)
                cleaned += 1

        return cleaned
