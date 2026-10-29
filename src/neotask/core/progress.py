"""
@FileName: progress.py
@Description: 任务进度上报上下文 - 让 executor 中的业务代码把进度回传给 Worker。
             Task progress reporting context so user task code can publish progress
             updates to the worker that is executing it.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/28
"""

import asyncio
import contextvars
from typing import Awaitable, Callable, Optional

# 进度回调签名：async def callback(progress: float, message: str) -> None
ProgressCallback = Callable[[float, str], Awaitable[None]]

# 当前任务绑定的进度上报器；由 WorkerPool 在执行任务前设置。
_current_progress_reporter: "contextvars.ContextVar[Optional[ProgressReporter]]" = (
    contextvars.ContextVar("neotask_progress_reporter", default=None)
)


class ProgressReporter:
    """绑定到单个任务与事件循环的进度上报器

    业务代码通常不需要直接创建它，只需调用 :func:`report_progress`（异步任务）
    或 :func:`report_progress_sync`（线程中的同步任务）。自定义 executor 也可以
    通过 ``execute_with_progress(task_data, progress_callback)`` 拿到等价的回调。

    上报在失败时是「尽力而为」的：当任务已被删除、事件循环已关闭或存储不可写时，
    不会让业务任务因此失败。
    """

    def __init__(
            self,
            callback: ProgressCallback,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        self._callback = callback
        self._loop = loop or asyncio.get_running_loop()

    async def report(self, progress: float, message: str = "") -> None:
        """异步上报进度"""
        await self._callback(progress, message)

    async def __call__(self, progress: float, message: str = "") -> None:
        """支持直接 ``await reporter(progress, message)``"""
        await self.report(progress, message)

    def report_sync(self, progress: float, message: str = "", timeout: float = 5.0) -> bool:
        """同步上报进度（供线程执行器中的同步任务调用）

        - 在 Worker 的事件循环线程内调用：用 ``create_task`` 调度，避免阻塞死锁；
        - 在线程池线程内调用：通过 ``run_coroutine_threadsafe`` 提交并等待完成，
          保证进度事件在任务继续执行前已按顺序入队。

        Returns:
            是否成功提交（事件循环已关闭或跨循环调用时返回 False）
        """
        if self._loop.is_closed():
            return False

        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        coro = self._callback(progress, message)

        if running_loop is self._loop:
            self._loop.create_task(coro)
            return True

        if running_loop is not None:
            # 不同事件循环之间无法安全同步等待，放弃本次上报。
            coro.close()
            return False

        try:
            future = asyncio.run_coroutine_threadsafe(coro, self._loop)
            future.result(timeout=timeout)
            return True
        except Exception:
            return False


def set_progress_reporter(reporter: ProgressReporter) -> contextvars.Token:
    """绑定当前任务的进度上报器（内部 API，由 WorkerPool 调用）"""
    return _current_progress_reporter.set(reporter)


def reset_progress_reporter(token: contextvars.Token) -> None:
    """恢复绑定前的进度上报器（内部 API，由 WorkerPool 调用）"""
    _current_progress_reporter.reset(token)


def get_progress_reporter() -> Optional[ProgressReporter]:
    """获取当前任务绑定的进度上报器，没有则返回 None"""
    return _current_progress_reporter.get()


async def report_progress(progress: float, message: str = "") -> bool:
    """在异步任务中上报执行进度

    Args:
        progress: 进度值，范围 0.0 - 1.0（超出范围会被 Worker 收敛到边界）
        message: 可选的进度描述

    Returns:
        是否成功上报。任务不在 Worker 执行上下文中时返回 False。

    Example:
        >>> from neotask import report_progress
        >>> async def my_task(data):
        ...     await report_progress(0.5, "loading")
        ...     return {"ok": True}
    """
    reporter = _current_progress_reporter.get()
    if reporter is None:
        return False
    await reporter.report(progress, message)
    return True


def report_progress_sync(progress: float, message: str = "") -> bool:
    """在线程中执行的同步任务里上报进度

    异步任务请使用 :func:`report_progress`。进程执行器（``ProcessExecutor``）
    运行在独立进程中，无法共享该上下文，因此不支持进度上报。

    Returns:
        是否成功提交进度上报
    """
    reporter = _current_progress_reporter.get()
    if reporter is None:
        return False
    return reporter.report_sync(progress, message)
