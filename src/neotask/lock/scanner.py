"""
@FileName: scanner.py
@Description: 锁扫描器 - 扫描和清理僵尸锁
@Author: HiPeng
@Time: 2026/4/15
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any

from neotask.lock.base import TaskLock


@dataclass
class LockScannerConfig:
    """锁扫描器配置"""
    scan_interval: float = 60.0  # 扫描间隔（秒）
    stale_ttl_threshold: int = 300  # 僵尸锁 TTL 阈值（秒）
    max_scan_per_cycle: int = 1000  # 每周期最大扫描数量
    auto_cleanup: bool = True  # 是否自动清理


class LockScanner:
    """锁扫描器

    负责扫描和清理僵尸锁：
    - 扫描所有锁键
    - 检测已过期的锁
    - 自动清理僵尸锁

    设计模式：Scheduler Pattern - 定期扫描

    使用示例：
        >>> scanner = LockScanner(redis_lock)
        >>> await scanner.start()
        >>>
        >>> # 手动扫描
        >>> cleaned = await scanner.scan_now()
        >>>
        >>> # 获取统计
        >>> stats = scanner.get_stats()
    """

    def __init__(
            self,
            lock: TaskLock,
            config: Optional[LockScannerConfig] = None
    ):
        self._lock = lock
        self._config = config or LockScannerConfig()

        self._running = False
        self._scan_task: Optional[asyncio.Task] = None

        # 统计信息
        self._total_scans = 0
        self._total_cleaned = 0
        self._last_scan_time: Optional[float] = None
        self._last_scan_duration: float = 0.0
        self._last_scan_result: int = 0

    async def start(self) -> None:
        """启动锁扫描器"""
        if self._running:
            return

        self._running = True
        self._scan_task = asyncio.create_task(self._scan_loop())

    async def stop(self) -> None:
        """停止锁扫描器"""
        self._running = False

        if self._scan_task:
            self._scan_task.cancel()
            try:
                await self._scan_task
            except asyncio.CancelledError:
                pass

    async def scan_now(self) -> int:
        """立即执行一次扫描

        Returns:
            清理的锁数量
        """
        start_time = time.time()
        cleaned = 0

        try:
            # 获取扫描方法（根据锁类型）
            if hasattr(self._lock, 'scan_locks'):
                # Redis 锁支持扫描
                lock_keys = await self._lock.scan_locks(
                    count=self._config.max_scan_per_cycle
                )

                for lock_key in lock_keys:
                    if await self._is_stale_lock(lock_key):
                        # 清理僵尸锁
                        await self._cleanup_lock(lock_key)
                        cleaned += 1

            elif self._config.auto_cleanup:
                # 内存锁：清理已过期的锁
                cleaned = await self._cleanup_memory_locks()

        except Exception as e:
            # 记录错误但不中断
            pass

        # 更新统计
        self._total_scans += 1
        self._total_cleaned += cleaned
        self._last_scan_time = time.time()
        self._last_scan_duration = (time.time() - start_time) * 1000  # ms
        self._last_scan_result = cleaned

        return cleaned

    async def _scan_loop(self) -> None:
        """扫描循环"""
        while self._running:
            try:
                await self.scan_now()
                await asyncio.sleep(self._config.scan_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self._config.scan_interval)

    async def _is_stale_lock(self, lock_key: str) -> bool:
        """判断锁是否为僵尸锁

        Args:
            lock_key: 锁键名

        Returns:
            是否为僵尸锁
        """
        try:
            # 获取锁的 TTL
            if hasattr(self._lock, 'get_ttl'):
                ttl = await self._lock.get_ttl(lock_key)
                if ttl is not None and ttl <= 0:
                    return True

            # 检查锁是否已被释放
            if hasattr(self._lock, 'is_locked'):
                is_locked = await self._lock.is_locked(lock_key)
                if not is_locked:
                    return True

            # 检查锁是否存在
            if hasattr(self._lock, 'exists'):
                exists = await self._lock.exists(lock_key)
                if not exists:
                    return True

        except Exception:
            return False

        return False

    async def _cleanup_lock(self, lock_key: str) -> None:
        """清理锁

        Args:
            lock_key: 锁键名
        """
        try:
            # 尝试释放锁
            if hasattr(self._lock, 'release'):
                await self._lock.release(lock_key)

            # 尝试删除键
            if hasattr(self._lock, 'delete'):
                await self._lock.delete(lock_key)

        except Exception:
            pass

    async def _cleanup_memory_locks(self) -> int:
        """清理内存锁中的过期锁

        Returns:
            清理的锁数量
        """
        cleaned = 0

        try:
            # 获取所有锁键
            if hasattr(self._lock, '_locks'):
                import time
                now = time.time()

                # 遍历内存锁字典
                keys_to_remove = []
                for key, lock_info in self._lock._locks.items():
                    # 检查是否过期
                    if hasattr(self._lock, '_expire_times'):
                        expire_time = self._lock._expire_times.get(key, 0)
                        if now > expire_time:
                            keys_to_remove.append(key)

                # 清理过期锁
                for key in keys_to_remove:
                    if hasattr(self._lock, '_locks'):
                        if key in self._lock._locks:
                            del self._lock._locks[key]
                    if hasattr(self._lock, '_owners'):
                        self._lock._owners.pop(key, None)
                    if hasattr(self._lock, '_expire_times'):
                        self._lock._expire_times.pop(key, None)
                    cleaned += 1

        except Exception:
            pass

        return cleaned

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "total_scans": self._total_scans,
            "total_cleaned": self._total_cleaned,
            "last_scan_time": self._last_scan_time,
            "last_scan_duration_ms": round(self._last_scan_duration, 2),
            "last_scan_result": self._last_scan_result,
            "is_running": self._running,
            "config": {
                "scan_interval": self._config.scan_interval,
                "stale_ttl_threshold": self._config.stale_ttl_threshold,
                "auto_cleanup": self._config.auto_cleanup
            }
        }

    def reset_stats(self) -> None:
        """重置统计信息"""
        self._total_scans = 0
        self._total_cleaned = 0
        self._last_scan_time = None
        self._last_scan_duration = 0.0
        self._last_scan_result = 0
