"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: dedicated_loop.py
@Description: 专用事件循环运行器 - 为 loop 绑定的异步客户端（Redis / SQLite）提供统一的
             后台事件循环，避免 TaskPool 内部 loop 与调用方 loop 共用连接对象时报跨 loop 错误。
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from typing import Any, Awaitable, Callable, Dict, Optional, Set

# 协程工厂：返回需要等待的协程
CoroutineFactory = Callable[[], Awaitable[Any]]

# 后台线程启动后等待 loop 真正进入 run_forever 的上限（秒）
_STARTUP_TIMEOUT = 10.0


class DedicatedLoopError(RuntimeError):
    """专用 I/O 循环意外停止，命令无法完成。

    ``started`` 表示协程是否已经开始执行：为 ``False`` 时可以安全地在新 loop
    上重试（命令尚未产生副作用）；为 ``True`` 时命令可能已部分执行，只能把
    错误交给调用方，不能盲目重试。
    """

    def __init__(
            self,
            message: str = "dedicated I/O loop stopped unexpectedly",
            *,
            started: bool = False,
    ) -> None:
        super().__init__(message)
        self.started = started


class DedicatedLoopRunner:
    """进程内共享的专用事件循环（守护线程）。

    只在首次执行数据库/缓存命令时惰性启动；进程退出时随守护线程自动结束。
    Windows Proactor 下关闭仍有重叠 I/O 的 socket 可能让 ``run_forever()``
    直接退出（线程本身仍存活），因此这里同时跟踪 loop 是否 *正在运行*，
    并在退出时立刻把在途 Future 置为失败，避免调用方永久等待。
    """

    def __init__(self, thread_name: str = "neotask-io-loop") -> None:
        self._thread_name = thread_name
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._alive = False
        self._lock = threading.Lock()
        self._start_lock = threading.Lock()
        self._pending: Dict[asyncio.AbstractEventLoop, Set[concurrent.futures.Future]] = {}

    def current_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        """返回当前可用的 loop；没有或已停止时返回 ``None``。"""
        with self._lock:
            return self._loop if self._alive else None

    def get(self) -> asyncio.AbstractEventLoop:
        """返回正在运行的专用事件循环，必要时创建（或重建）后台线程。"""
        for _ in range(2):
            loop = self._usable_loop()
            if loop is not None:
                return loop

            loop = self._start_loop()
            if loop.is_running():
                return loop

        raise DedicatedLoopError("dedicated I/O loop failed to start")

    def submit(self, factory: CoroutineFactory) -> concurrent.futures.Future:
        """把 ``factory()`` 返回的协程投递到专用 loop，返回对应的 Future。

        loop 恰好在投递前退出时换新 loop 重试一次；投递成功后若 loop 死亡，
        由 :meth:`_retire` 用 :class:`DedicatedLoopError` 结束 Future，而不是挂起。
        """
        for _ in range(2):
            loop = self.get()
            coro = factory()

            with self._lock:
                if (
                        loop is self._loop
                        and self._alive
                        and not loop.is_closed()
                        and loop.is_running()
                ):
                    future = asyncio.run_coroutine_threadsafe(coro, loop)
                    self._pending.setdefault(loop, set()).add(future)
                else:
                    future = None

            if future is not None:
                future.add_done_callback(
                    lambda done, owner=loop: self._forget(owner, done)
                )
                return future

            coro.close()
            # loop 在 get() 与提交之间退出，换一个新 loop 重试

        raise DedicatedLoopError("dedicated I/O loop stopped before command was submitted")

    def _usable_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        with self._lock:
            loop, thread = self._loop, self._thread
            if (
                    loop is not None
                    and self._alive
                    and thread is not None
                    and thread.is_alive()
                    and not loop.is_closed()
                    and loop.is_running()
            ):
                return loop
            return None

    def _start_loop(self) -> asyncio.AbstractEventLoop:
        with self._start_lock:
            loop = self._usable_loop()
            if loop is not None:
                return loop

            old = self._loop
            if old is not None:
                self._retire(old)

            loop = asyncio.new_event_loop()
            ready = threading.Event()
            thread = threading.Thread(
                target=self._run,
                args=(loop, ready),
                name=self._thread_name,
                daemon=True,
            )

            with self._lock:
                self._loop = loop
                self._thread = thread
                self._alive = True
                self._pending[loop] = set()
                thread.start()

            # 等 loop 真正跑起来再返回，杜绝把协程投递到「线程在、loop 未运行」的窗口
            if not ready.wait(_STARTUP_TIMEOUT):
                self._retire(loop)
            return loop

    def _run(self, loop: asyncio.AbstractEventLoop, ready: threading.Event) -> None:
        """后台线程入口：运行 loop 直到进程退出。"""
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)
        try:
            loop.run_forever()
        except BaseException:  # pragma: no cover - Windows Proactor 异常退出
            pass
        finally:
            # 先标记死亡并结束在途 Future，再尝试关闭 loop：
            # 即便 close() 卡住，后续调用也不会再投递到这个 loop。
            self._retire(loop)
            ready.set()
            try:  # pragma: no cover - 仅在线程退出时执行
                loop.close()
            except Exception:
                pass

    def _retire(self, loop: asyncio.AbstractEventLoop) -> None:
        """标记 loop 不可用，并用 :class:`DedicatedLoopError` 结束其所有在途 Future。"""
        with self._lock:
            if self._loop is loop:
                self._alive = False
            pending = self._pending.pop(loop, set())

        for future in pending:
            if future.done():
                continue
            try:
                future.set_exception(
                    DedicatedLoopError("dedicated I/O loop stopped before command completed")
                )
            except Exception:  # pragma: no cover - Future 恰好已完成/已取消
                pass

    def _forget(
            self,
            loop: asyncio.AbstractEventLoop,
            future: concurrent.futures.Future,
    ) -> None:
        with self._lock:
            pending = self._pending.get(loop)
            if pending is not None:
                pending.discard(future)


# Redis / SQLite 客户端共用一个专用 loop，避免为每个连接创建线程
_SHARED_RUNNER = DedicatedLoopRunner()


def get_shared_loop() -> asyncio.AbstractEventLoop:
    """返回进程内共享的专用事件循环。"""
    return _SHARED_RUNNER.get()


async def _run_shared(factory: CoroutineFactory) -> Any:
    """在共享 loop 上执行协程；loop 死亡时以 :class:`DedicatedLoopError` 快速失败。"""
    started = False

    async def _guarded() -> Any:
        nonlocal started
        started = True
        return await factory()

    future = _SHARED_RUNNER.submit(_guarded)
    try:
        return await asyncio.wrap_future(future)
    except DedicatedLoopError as exc:
        raise DedicatedLoopError(str(exc), started=started) from None


async def run_on_loop(
        factory: CoroutineFactory,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
) -> Any:
    """在指定（默认共享）事件循环上执行 ``factory()`` 返回的协程并等待结果。

    调用方 loop 与目标 loop 相同时直接执行，避免重复投递造成死锁。
    共享 loop 意外退出时抛出 :class:`DedicatedLoopError`（不会一直等待）；
    调用方可依据 ``exc.started`` 判断是否可以安全重试。
    """
    target = loop if loop is not None else get_shared_loop()

    try:
        running = asyncio.get_running_loop()
    except RuntimeError:  # pragma: no cover - 不在协程中调用
        running = None

    if running is target:
        return await factory()

    if loop is None or target is _SHARED_RUNNER.current_loop():
        return await _run_shared(factory)

    # 显式传入的非共享 loop（兼容旧用法）：无法自动重建，但至少不能挂起
    if target.is_closed() or not target.is_running():
        raise DedicatedLoopError("target event loop is not running")

    future = asyncio.run_coroutine_threadsafe(factory(), target)
    return await asyncio.wrap_future(future)
