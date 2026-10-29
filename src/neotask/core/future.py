"""
@FileName: future.py
@Description: 任务 Task Future for async wait operations.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/1 17:41
"""

import asyncio
import threading
from typing import Optional, Any, Dict, List, Tuple

from neotask.common.exceptions import TimeoutError
from neotask.common.logger import debug

# 等待者登记：(调用方事件循环, 该循环上的 Future)
_Waiter = Tuple[asyncio.AbstractEventLoop, "asyncio.Future[Tuple[Any, Optional[str]]]"]


class TaskFuture:
    """Future object for waiting task completion.

    Task 的完成通知可能来自 TaskPool 的内部事件循环，而等待者可能运行在另一个
    事件循环中（例如 ``asyncio.run`` 或测试的 loop）。因此这里不使用
    ``asyncio.Event``，而是为每个等待循环创建一个 Future，并在完成时通过
    ``loop.call_soon_threadsafe`` 唤醒对应循环，避免跨循环丢失唤醒。
    """

    def __init__(self, task_id: str):
        self.task_id = task_id
        self._lock = threading.Lock()
        self._waiters: List[_Waiter] = []
        self._result: Optional[Any] = None
        self._error: Optional[str] = None
        self._completed = False

    def set_result(self, result: Any) -> None:
        """Set task result and notify waiters."""
        debug(f"[Future] Setting result for task {self.task_id}")
        self._complete(result, None)

    def set_error(self, error: str) -> None:
        """Set task error and notify waiters."""
        debug(f"[Future] Setting error for task {self.task_id}: {error}")
        self._complete(None, error)

    def _complete(self, result: Any, error: Optional[str]) -> None:
        """完成 Future 并唤醒所有事件循环上的等待者（幂等）。"""
        with self._lock:
            if self._completed:
                return
            self._result = result
            self._error = error
            self._completed = True
            waiters, self._waiters = self._waiters, []

        for loop, waiter in waiters:
            self._wake_waiter(loop, waiter, result, error)

    @staticmethod
    def _set_waiter_result(
            waiter: "asyncio.Future[Tuple[Any, Optional[str]]]",
            result: Any,
            error: Optional[str]
    ) -> None:
        """在等待者所属循环中执行，安全地写入结果。"""
        if waiter.done():
            return
        waiter.set_result((result, error))

    def _wake_waiter(
            self,
            loop: asyncio.AbstractEventLoop,
            waiter: "asyncio.Future[Tuple[Any, Optional[str]]]",
            result: Any,
            error: Optional[str]
    ) -> None:
        """跨线程安全地唤醒指定循环上的等待者。"""
        try:
            if loop.is_closed():
                return
            loop.call_soon_threadsafe(self._set_waiter_result, waiter, result, error)
        except RuntimeError:
            # 循环在唤醒过程中被关闭，忽略即可
            pass

    async def wait(self, timeout: float = 300) -> Any:
        """Wait for task completion from any event loop."""
        loop = asyncio.get_running_loop()

        with self._lock:
            if self._completed:
                waiter = None
                result, error = self._result, self._error
            else:
                waiter = loop.create_future()
                self._waiters.append((loop, waiter))
                result = error = None

        if waiter is None:
            if error:
                raise Exception(error)
            return result

        try:
            result, error = await asyncio.wait_for(waiter, timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(self.task_id, timeout)
        finally:
            with self._lock:
                self._waiters = [
                    (lp, fut) for lp, fut in self._waiters if fut is not waiter
                ]

        if error:
            raise Exception(error)
        return result

    @property
    def is_completed(self) -> bool:
        """Check if task is completed."""
        return self._completed


class FutureManager:
    """Manager for task futures.

    管理器可能同时被 TaskPool 内部循环和调用方循环访问，因此使用
    ``threading.Lock`` 保护字典；所有临界区都不包含 await。
    """

    def __init__(self):
        self._futures: Dict[str, TaskFuture] = {}
        self._lock = threading.Lock()

    async def create(self, task_id: str) -> TaskFuture:
        """Create a new future."""
        with self._lock:
            future = TaskFuture(task_id)
            self._futures[task_id] = future
            return future

    async def get(self, task_id: str) -> TaskFuture:
        """Get existing future or create new."""
        with self._lock:
            if task_id not in self._futures:
                debug(f"[FutureManager] Creating new future for task {task_id}")
                self._futures[task_id] = TaskFuture(task_id)
            return self._futures[task_id]

    async def complete(self, task_id: str, result: Any = None, error: str = None) -> None:
        """Complete a future."""
        with self._lock:
            future = self._futures.get(task_id)

        if future:
            debug(f"[FutureManager] Completing future for task {task_id}, error={error}")
            if error:
                future.set_error(error)
            else:
                future.set_result(result)
        else:
            debug(f"[FutureManager] No future found for task {task_id}")

    async def wait(self, task_id: str, timeout: float = 300) -> Any:
        """Wait for a future."""
        future = await self.get(task_id)
        return await future.wait(timeout)

    async def remove(self, task_id: str) -> None:
        """Remove a future."""
        with self._lock:
            self._futures.pop(task_id, None)

    async def get_and_remove(self, task_id: str) -> Optional[TaskFuture]:
        """Get future and remove it from manager."""
        with self._lock:
            return self._futures.pop(task_id, None)

    async def cancel_all(self) -> None:
        """Cancel all pending futures."""
        with self._lock:
            futures = list(self._futures.values())
            self._futures.clear()

        for future in futures:
            if not future.is_completed:
                future.set_error("Scheduler shutting down")
