"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: checkpoint.py
@Description: 任务 checkpoint 上报上下文 - 让 executor 中的业务代码读写当前
             任务的中间状态。Task checkpoint context so user task code can
             save/restore progress across retries and restarts.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio
import contextvars
from concurrent.futures import Future
from typing import Any, Callable, Coroutine, Dict, Optional

#: 恢复执行时注入到 ``task.data`` 的保留键。业务数据若使用同名键会被覆盖。
CHECKPOINT_DATA_KEY = "__checkpoint__"

SaveCallback = Callable[[Dict[str, Any]], Coroutine[Any, Any, bool]]
LoadCallback = Callable[[], Coroutine[Any, Any, Optional[Dict[str, Any]]]]

_current_checkpoint_context: "contextvars.ContextVar[Optional[CheckpointContext]]" = (
    contextvars.ContextVar("neotask_checkpoint_context", default=None)
)


class CheckpointContext:
    """绑定到单次执行的 checkpoint 读写器

    Worker 在执行任务前绑定当前 ``task_id`` 对应的回调；业务代码只需调用
    :func:`save_checkpoint` / :func:`load_checkpoint`。显式形态见
    ``pool.save_checkpoint(task_id, state)`` / ``pool.load_checkpoint(task_id)``。
    """

    def __init__(
        self,
        save: SaveCallback,
        load: LoadCallback,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self._save = save
        self._load = load
        self._loop = loop

    async def save(self, state: Dict[str, Any]) -> bool:
        """异步写入 checkpoint"""
        return await self._save(state)

    async def load(self) -> Optional[Dict[str, Any]]:
        """异步读取最新 checkpoint"""
        return await self._load()

    def _running_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return None

    def save_sync(self, state: Dict[str, Any], timeout: float = 5.0) -> bool:
        """同步写入（供线程执行器中的同步任务调用）

        - 在 Worker 事件循环线程内调用：用 ``create_task`` 调度后立即返回，
          落库为尽力而为；
        - 在线程池线程内调用：提交到 Worker loop 并等待写入完成。
        """
        loop = self._loop
        if loop is None or loop.is_closed():
            return False

        coro = self._save(state)
        running_loop = self._running_loop()

        if running_loop is loop:
            loop.create_task(coro)
            return True

        if running_loop is not None:
            coro.close()
            return False

        try:
            future: Future = asyncio.run_coroutine_threadsafe(coro, loop)
            return bool(future.result(timeout=timeout))
        except Exception:
            return False

    def load_sync(self, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """同步读取最新 checkpoint

        仅在无运行中循环的线程里可用；在事件循环线程内请 ``await load()``。
        """
        loop = self._loop
        if loop is None or loop.is_closed():
            return None

        running_loop = self._running_loop()
        if running_loop is not None:
            return None

        try:
            future: Future = asyncio.run_coroutine_threadsafe(self._load(), loop)
            checkpoint: Optional[Dict[str, Any]] = future.result(timeout=timeout)
            return checkpoint
        except Exception:
            return None


def set_checkpoint_context(
    checkpoint_context: CheckpointContext,
) -> contextvars.Token:
    """绑定当前任务的 checkpoint 上下文（内部 API，由 WorkerPool 调用）"""
    return _current_checkpoint_context.set(checkpoint_context)


def reset_checkpoint_context(token: contextvars.Token) -> None:
    """恢复绑定前的 checkpoint 上下文（内部 API，由 WorkerPool 调用）"""
    _current_checkpoint_context.reset(token)


def get_checkpoint_context() -> Optional[CheckpointContext]:
    """获取当前任务绑定的 checkpoint 上下文，没有则返回 None"""
    return _current_checkpoint_context.get()


async def save_checkpoint(state: Dict[str, Any]) -> bool:
    """在异步执行器中保存当前任务的中间状态

    Args:
        state: 可 JSON 序列化的状态字典

    Returns:
        是否成功写入。任务不在 Worker 执行上下文中时返回 ``False``，
        与 ``report_progress`` 的约定一致（不抛异常）。

    Note:
        进程执行器（``ProcessExecutor``）拿不到该上下文，请在父进程使用
        ``pool.save_checkpoint(task_id, state)``。
    """
    checkpoint_context = _current_checkpoint_context.get()
    if checkpoint_context is None:
        return False
    return await checkpoint_context.save(state)


async def load_checkpoint() -> Optional[Dict[str, Any]]:
    """在异步执行器中读取当前任务最近的 checkpoint

    Returns:
        状态字典；没有上下文或没有 checkpoint 时返回 ``None``
    """
    checkpoint_context = _current_checkpoint_context.get()
    if checkpoint_context is None:
        return None
    return await checkpoint_context.load()


def save_checkpoint_sync(
    state: Dict[str, Any],
    timeout: float = 5.0,
) -> bool:
    """在线程执行器的同步任务中保存 checkpoint

    异步任务请 ``await save_checkpoint(state)``；进程执行器请使用
    ``pool.save_checkpoint(task_id, state)``。
    """
    checkpoint_context = _current_checkpoint_context.get()
    if checkpoint_context is None:
        return False
    return checkpoint_context.save_sync(state, timeout=timeout)


def load_checkpoint_sync(timeout: float = 5.0) -> Optional[Dict[str, Any]]:
    """在线程执行器的同步任务中读取 checkpoint"""
    checkpoint_context = _current_checkpoint_context.get()
    if checkpoint_context is None:
        return None
    return checkpoint_context.load_sync(timeout=timeout)
