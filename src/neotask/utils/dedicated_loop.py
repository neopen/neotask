"""
@FileName: dedicated_loop.py
@Description: 专用事件循环运行器 - 为 loop 绑定的异步客户端（Redis / SQLite）提供统一的
             后台事件循环，避免 TaskPool 内部 loop 与调用方 loop 共用连接对象时报跨 loop 错误。
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Awaitable, Callable, Optional

# 协程工厂：返回需要等待的协程
CoroutineFactory = Callable[[], Awaitable[Any]]


class DedicatedLoopRunner:
    """进程内共享的专用事件循环（守护线程）。

    只在首次执行数据库/缓存命令时惰性启动；进程退出时随守护线程自动结束。
    若线程异常退出，下次调用会重建一个新的 loop。
    """

    def __init__(self, thread_name: str = "neotask-io-loop") -> None:
        self._thread_name = thread_name
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def get(self) -> asyncio.AbstractEventLoop:
        """返回可用的专用事件循环，必要时创建并启动后台线程。"""
        with self._lock:
            if (
                    self._loop is not None
                    and not self._loop.is_closed()
                    and self._thread is not None
                    and self._thread.is_alive()
            ):
                return self._loop

            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=self._run,
                args=(loop,),
                name=self._thread_name,
                daemon=True,
            )
            self._loop = loop
            self._thread = thread
            thread.start()
            return loop

    def _run(self, loop: asyncio.AbstractEventLoop) -> None:
        """后台线程入口：运行 loop 直到进程退出。"""
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        except BaseException:  # pragma: no cover - 异常退出时重建 loop
            pass
        finally:  # pragma: no cover - 仅在线程退出时执行
            try:
                loop.close()
            except Exception:
                pass


# Redis / SQLite 客户端共用一个专用 loop，避免为每个连接创建线程
_SHARED_RUNNER = DedicatedLoopRunner()


def get_shared_loop() -> asyncio.AbstractEventLoop:
    """返回进程内共享的专用事件循环。"""
    return _SHARED_RUNNER.get()


async def run_on_loop(
        factory: CoroutineFactory,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
) -> Any:
    """在指定（默认共享）事件循环上执行 ``factory()`` 返回的协程并等待结果。

    调用方 loop 与目标 loop 相同时直接执行，避免重复投递造成死锁。
    """
    target = loop if loop is not None else get_shared_loop()

    try:
        running = asyncio.get_running_loop()
    except RuntimeError:  # pragma: no cover - 不在协程中调用
        running = None

    if running is target:
        return await factory()

    future = asyncio.run_coroutine_threadsafe(factory(), target)
    return await asyncio.wrap_future(future)
