"""
@FileName: watchdog.py
@Description: 看门狗 - 自动续期锁
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import logging
from typing import Dict

from neotask.lock.base import TaskLock

logger = logging.getLogger(__name__)


class WatchDog:
    """看门狗

    自动为锁续期，防止锁在任务执行期间过期。

    使用示例：
        >>> lock = RedisLock("redis://localhost")
        >>> watchdog = WatchDog(lock)
        >>>
        >>> if await lock.acquire("my_key", ttl=30):
        ...     await watchdog.start("my_key", ttl=30)
        ...     try:
        ...         # 长时间执行的任务
        ...         await long_running_task()
        ...     finally:
        ...         await watchdog.stop("my_key")
        ...         await lock.release("my_key")
    """

    def __init__(self, lock: TaskLock, interval_ratio: float = 0.3):
        """初始化看门狗

        Args:
            lock: 分布式锁实例
            interval_ratio: 续期间隔与 TTL 的比例（默认 0.3，即每 TTL/3 续期一次）
        """
        self._lock = lock
        self._interval_ratio = interval_ratio
        self._tasks: Dict[str, asyncio.Task] = {}
        self._running: Dict[str, bool] = {}
        self._ttl: Dict[str, int] = {}
        self._renew_count: Dict[str, int] = {}

    async def start(self, key: str, ttl: int) -> None:
        """启动看门狗

        Args:
            key: 锁的键名
            ttl: 锁的生存时间（秒）
        """
        if key in self._tasks and not self._tasks[key].done():
            logger.debug(f"Watchdog already running for key: {key}")
            return

        self._running[key] = True
        self._ttl[key] = ttl
        self._renew_count[key] = 0
        interval = max(0.5, ttl * self._interval_ratio)

        self._tasks[key] = asyncio.create_task(
            self._watchdog_loop(key, ttl, interval)
        )
        logger.debug(f"Watchdog started for key: {key}, ttl={ttl}, interval={interval:.2f}s")

    async def stop(self, key: str) -> None:
        """停止看门狗

        Args:
            key: 锁的键名
        """
        logger.debug(f"Stopping watchdog for key: {key}")
        self._running[key] = False

        if key in self._tasks:
            self._tasks[key].cancel()
            try:
                await self._tasks[key]
            except asyncio.CancelledError:
                pass
            finally:
                self._tasks.pop(key, None)
                self._running.pop(key, None)
                self._ttl.pop(key, None)
                self._renew_count.pop(key, None)
                logger.debug(f"Watchdog stopped for key: {key}")

    async def stop_all(self) -> None:
        """停止所有看门狗"""
        for key in list(self._tasks.keys()):
            await self.stop(key)

    async def _watchdog_loop(self, key: str, ttl: int, interval: float) -> None:
        """看门狗循环"""
        logger.debug(f"Watchdog loop started for key: {key}")

        while self._running.get(key, False):
            try:
                await asyncio.sleep(interval)

                if not self._running.get(key, False):
                    break

                # 续期锁
                success = await self._lock.extend(key, ttl)
                self._renew_count[key] = self._renew_count.get(key, 0) + 1

                if success:
                    logger.debug(f"Watchdog renewed lock for key: {key}, count={self._renew_count[key]}")
                else:
                    # 锁已丢失，停止续期
                    logger.warning(f"Watchdog failed to renew lock for key: {key}, stopping")
                    self._running[key] = False
                    break

            except asyncio.CancelledError:
                logger.debug(f"Watchdog cancelled for key: {key}")
                break
            except Exception as e:
                logger.error(f"Watchdog error for key {key}: {e}")
                # 发生错误，继续尝试
                continue

        logger.debug(f"Watchdog loop ended for key: {key}")

    def is_running(self, key: str) -> bool:
        """检查看门狗是否在运行

        Args:
            key: 锁的键名

        Returns:
            是否在运行
        """
        return self._running.get(key, False)

    def get_renew_count(self, key: str) -> int:
        """获取续期次数

        Args:
            key: 锁的键名

        Returns:
            续期次数
        """
        return self._renew_count.get(key, 0)

    def __repr__(self) -> str:
        return f"WatchDog(active={len(self._tasks)})"
