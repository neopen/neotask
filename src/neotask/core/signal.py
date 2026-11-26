"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: signal.py
@Description: 人工介入等待上下文 - 让 executor 中的业务代码等待外部信号。
              Human-in-the-loop wait context so user task code can suspend until
              an external signal arrives.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/25
"""

import asyncio
import contextvars
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional

if TYPE_CHECKING:
    from neotask.models.task import Task

#: 等待与信号数据在 ``task.data`` 中的保留键。业务数据不得占用同名键。
SIGNAL_DATA_KEY = "__signal__"

SuspendCallback = Callable[[str, Optional[float]], Awaitable[bool]]
ConsumeCallback = Callable[[str], Awaitable[bool]]
ReloadCallback = Callable[[], Awaitable[Optional["Task"]]]

_current_signal_waiter: "contextvars.ContextVar[Optional[SignalWaiter]]" = (
    contextvars.ContextVar("neotask_signal_waiter", default=None)
)


class TaskSuspended(Exception):
    """内部信号：Handler 已持久化为 WAITING，Worker 捕获后释放槽位

    该异常只会在执行器内部传播，不会暴露给任务调用方；任务 future 保持挂起，
    待 signal 唤醒后重新执行。
    """

    def __init__(self, signal: str) -> None:
        super().__init__(f"task suspended waiting for signal '{signal}'")
        self.signal = signal


class SignalWaiter:
    """绑定到单次执行的信号等待器

    Worker 在执行任务前把当前任务与生命周期回调绑定进来；业务代码只需调用
    :func:`wait_for_signal`。任务数据中的 ``__signal__`` 记录等待键、序号与
    已到达的 payload，消费后清除，避免重放时二次命中。

    等待语义是「可重入重放」：挂起后任务重新入队，Handler 从头执行，命中已到达
    的信号时直接返回 payload。与自动重试一致，Handler 应保证重复执行无副作用。
    """

    def __init__(
        self,
        task: "Task",
        suspend: SuspendCallback,
        consume: ConsumeCallback,
        default_timeout: Optional[float] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        reload: Optional[ReloadCallback] = None,
    ) -> None:
        self._task = task
        self._suspend = suspend
        self._consume = consume
        self._default_timeout = default_timeout
        self._loop = loop
        self._reload = reload

    def _effective_timeout(self, timeout: Optional[float]) -> Optional[float]:
        value = self._default_timeout if timeout is None else timeout
        if value is None:
            return None
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"invalid wait timeout: {value!r}") from None
        return seconds if seconds > 0 else None

    async def _reload_task(self) -> None:
        """挂起前回查存储，避免跨实例缓存的旧数据覆盖已到达信号

        重放执行时，唤醒信号可能由其它实例写入存储，本实例缓存里的
        ``__signal__`` 仍是旧副本；此时直接挂起会用旧数据覆盖 arrived payload。
        """
        if self._reload is None:
            return
        fresh = await self._reload()
        if fresh is not None:
            self._task = fresh

    async def wait(
        self, signal: str, timeout: Optional[float] = None
    ) -> Any:
        """等待信号；已到达则立即返回 payload，否则挂起并发起重放"""
        key = _validate_signal(signal)
        entry = _signal_entry(self._task.data)

        if entry and _is_arrived(entry, key):
            payload = entry.get("payload")
            await self._consume(key)
            return payload

        await self._reload_task()
        entry = _signal_entry(self._task.data)
        if entry and _is_arrived(entry, key):
            payload = entry.get("payload")
            await self._consume(key)
            return payload

        ok = await self._suspend(key, self._effective_timeout(timeout))
        if not ok:
            raise RuntimeError(
                f"Task {self._task.task_id} is not in a suspendable state "
                f"(status={self._task.status.value})"
            )
        raise TaskSuspended(key)

    def wait_sync(
        self,
        signal: str,
        timeout: Optional[float] = None,
        call_timeout: float = 5.0,
    ) -> Any:
        """同步等待版本（供线程执行器中的同步任务调用）

        在 Worker 事件循环线程内调用会死锁，请改用 ``await wait_for_signal()``。
        存储操作提交到 Worker loop 执行，成功后抛出 :class:`TaskSuspended`，
        由 Worker 释放槽位。
        """
        loop = self._loop
        if loop is None or loop.is_closed():
            raise RuntimeError("wait_for_signal_sync() requires a running worker loop")
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is not None:
            raise RuntimeError(
                "wait_for_signal_sync() cannot be called from an event loop; "
                "use 'await wait_for_signal()' instead"
            )

        future: Future = asyncio.run_coroutine_threadsafe(
            self.wait(signal, timeout), loop
        )
        return future.result(timeout=call_timeout)


def _signal_entry(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    entry = data.get(SIGNAL_DATA_KEY)
    return entry if isinstance(entry, dict) else None


def _is_arrived(entry: Optional[Dict[str, Any]], signal: str) -> bool:
    return bool(
        entry
        and entry.get("signal") == signal
        and entry.get("arrived")
        and not entry.get("consumed")
    )


def _validate_signal(signal: str) -> str:
    if not isinstance(signal, str) or not signal.strip():
        raise ValueError("signal must be a non-empty string")
    return signal.strip()


def set_signal_waiter(waiter: SignalWaiter) -> contextvars.Token:
    """绑定当前任务的信号等待器（内部 API，由 WorkerPool 调用）"""
    return _current_signal_waiter.set(waiter)


def reset_signal_waiter(token: contextvars.Token) -> None:
    """恢复绑定前的信号等待器（内部 API，由 WorkerPool 调用）"""
    _current_signal_waiter.reset(token)


def get_signal_waiter() -> Optional[SignalWaiter]:
    """获取当前任务绑定的信号等待器，没有则返回 None"""
    return _current_signal_waiter.get()


async def wait_for_signal(
    signal: str, timeout: Optional[float] = None
) -> Any:
    """在任务执行体内等待外部信号

    首次执行：保存等待键与可选超时，任务转为 ``waiting``，Worker 释放并发
    槽位并停止本次执行；``pool.signal(task_id, payload)`` 到达后任务以
    ``pending`` 重新入队并重新执行 Handler，本次调用直接返回 payload。

    超时只约束「等待时长」，不占执行超时预算；到期由后台扫描把任务置为
    ``failed``，``failure_reason="waiting_timeout"``。

    Args:
        signal: 业务等待键，如 ``"approval"``；同一任务多次等待靠它区分
        timeout: 等待超时（秒）；``None`` 时使用配置的 ``waiting_timeout``，
            两者都为空则不超时

    Returns:
        ``pool.signal(task_id, payload)`` 传入的 payload

    Raises:
        RuntimeError: 不在 Worker 执行上下文中（如进程执行器）调用
    """
    waiter = _current_signal_waiter.get()
    if waiter is None:
        raise RuntimeError(
            "wait_for_signal() must be called inside a NeoTask handler executed "
            "by TaskPool/TaskScheduler (process executor is not supported)"
        )
    return await waiter.wait(signal, timeout)


def wait_for_signal_sync(
    signal: str,
    timeout: Optional[float] = None,
    call_timeout: float = 5.0,
) -> Any:
    """在同步任务中等待外部信号（线程执行器）

    异步任务请 ``await wait_for_signal()``；进程执行器不支持等待。
    """
    waiter = _current_signal_waiter.get()
    if waiter is None:
        raise RuntimeError(
            "wait_for_signal_sync() must be called inside a NeoTask handler executed "
            "by TaskPool/TaskScheduler (process executor is not supported)"
        )
    return waiter.wait_sync(signal, timeout, call_timeout=call_timeout)
