"""
@FileName: scanner.py
@Description: 锁扫描器 - 定期扫描和清理僵尸锁
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
    stale_ttl_threshold: int = 300  # 僵尸锁TTL阈值（秒）
    max_scan_per_cycle: int = 1000  # 每周期最大扫描数量


class LockScanner:
    """锁扫描器

    定期扫描并清理僵尸锁。

    使用示例：
        >>> scanner = LockScanner(lock)
        >>> await scanner.start()
        >>>
        >>> # 手动触发扫描
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
        self._last_cleaned_count = 0

    async def start(self) -> None:
        """启动扫描器"""
        if self._running:
            return

        self._running = True
        self._scan_task = asyncio.create_task(self._scan_loop())

    async def stop(self) -> None:
        """停止扫描器"""
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
        self._last_scan_time = time.time()

        try:
            cleaned = await self._lock.cleanup_stale_locks(
                ttl_threshold=self._config.stale_ttl_threshold
            )

            self._total_scans += 1
            self._total_cleaned += cleaned
            self._last_cleaned_count = cleaned

            return cleaned
        except Exception as e:
            return 0

    async def _scan_loop(self) -> None:
        """扫描循环"""
        while self._running:
            try:
                await self.scan_now()
                await asyncio.sleep(self._config.scan_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "total_scans": self._total_scans,
            "total_cleaned": self._total_cleaned,
            "last_scan_time": self._last_scan_time,
            "last_cleaned_count": self._last_cleaned_count,
            "is_running": self._running,
            "config": {
                "scan_interval": self._config.scan_interval,
                "stale_ttl_threshold": self._config.stale_ttl_threshold
            }
        }

    def reset_stats(self) -> None:
        """重置统计信息"""
        self._total_scans = 0
        self._total_cleaned = 0
        self._last_cleaned_count = 0
