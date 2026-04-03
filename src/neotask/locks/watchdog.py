"""
@FileName: watchdog.py
@Description: 看门狗机制 - 自动续期锁。
@Author: HiPeng
@Time: 2026/4/1 19:08
"""
import asyncio
from typing import Optional, Callable, Dict

from neotask.locks.base import TaskLock


class WatchDog:
    """看门狗，负责自动续期锁。

    使用示例：
        >>> lock = TaskLock()
        >>> watchdog = WatchDog(lock)
        >>>   watchdog.start("my_key", ttl=30)
        >>> try:
        ...     # 长时间执行的任务
        ...     asyncio.sleep(60)
        ... finally:
        ...     watchdog.stop("my_key")
    """

    def __init__(self, lock: TaskLock, interval_ratio: float = 0.3):
        """初始化看门狗。

        Args:
            lock: 分布式锁实例
            interval_ratio: 续期间隔与 TTL 的比例，默认 0.3（TTL的30%）
        """
        self._lock = lock
        self._interval_ratio = interval_ratio
        self._tasks: Dict[str, asyncio.Task] = {}
        self._stop_events: Dict[str, asyncio.Event] = {}

    async def start(self, key: str, ttl: int = 30, on_extend_fail: Optional[Callable] = None) -> bool:
        """启动看门狗。

        Args:
            key: 锁的键名
            ttl: 锁的生存时间（秒）
            on_extend_fail: 续期失败时的回调函数

        Returns:
            是否成功启动
        """
        if key in self._tasks:
            return False

        # 检查锁是否被当前进程持有
        owner = await self._lock.get_owner(key)
        if not owner:
            return False

        stop_event = asyncio.Event()
        self._stop_events[key] = stop_event

        task = asyncio.create_task(
            self._watchdog_loop(key, ttl, stop_event, on_extend_fail)
        )
        self._tasks[key] = task

        return True

    async def stop(self, key: str) -> bool:
        """停止看门狗。"""
        if key not in self._tasks:
            return False

        # 发送停止信号
        if key in self._stop_events:
            self._stop_events[key].set()

        # 等待任务结束
        task = self._tasks.pop(key)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        if key in self._stop_events:
            del self._stop_events[key]

        return True

    async def _watchdog_loop(self, key: str, ttl: int, stop_event: asyncio.Event, on_extend_fail: Optional[Callable]):
        """看门狗循环。"""
        interval = ttl * self._interval_ratio

        while not stop_event.is_set():
            try:
                await asyncio.sleep(interval)

                # 续期锁
                success = await self._lock.extend(key, ttl)

                if not success:
                    # 续期失败，锁可能已被释放
                    if on_extend_fail:
                        await on_extend_fail(key)
                    break

            except asyncio.CancelledError:
                break
            except Exception as e:
                if on_extend_fail:
                    await on_extend_fail(key, str(e))
                break

    async def stop_all(self):
        """停止所有看门狗。"""
        for key in list(self._tasks.keys()):
            await self.stop(key)

    def is_running(self, key: str) -> bool:
        """检查看门狗是否在运行。"""
        return key in self._tasks
