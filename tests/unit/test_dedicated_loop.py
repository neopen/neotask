"""
@FileName: test_dedicated_loop.py
@Description: 专用 I/O 循环韧性回归测试 - loop 意外停止后必须重建/快速失败，
             不得让后续命令永久挂起；Redis 客户端关闭前必须排空在途命令。
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/10
"""

import asyncio
import time
from typing import Callable

import pytest

from neotask.utils import dedicated_loop
from neotask.utils.dedicated_loop import (
    DedicatedLoopError,
    DedicatedLoopRunner,
    get_shared_loop,
    run_on_loop,
)
from neotask.utils.redis_compat import create_client


def _wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not met within timeout")


def _stop(loop: asyncio.AbstractEventLoop) -> None:
    loop.call_soon_threadsafe(loop.stop)


def test_loop_is_rebuilt_after_stop():
    """run_forever 退出（线程仍存活）后，get() 必须返回新的可用 loop。"""
    runner = DedicatedLoopRunner("test-io-loop-rebuild")
    loop1 = runner.get()
    assert runner.submit(lambda: asyncio.sleep(0, result="first")).result(2) == "first"

    _stop(loop1)
    _wait_until(lambda: not loop1.is_running())

    loop2 = runner.get()
    try:
        assert loop2 is not loop1
        assert runner.submit(lambda: asyncio.sleep(0, result="second")).result(2) == "second"
    finally:
        _stop(loop2)


def test_pending_future_fails_instead_of_hanging():
    """loop 停止时，已投递但未完成的 Future 必须立刻失败而不是永远等待。"""
    runner = DedicatedLoopRunner("test-io-loop-pending")
    loop = runner.get()

    async def _hang():
        await asyncio.Future()

    future = runner.submit(_hang)
    _stop(loop)

    with pytest.raises(DedicatedLoopError):
        future.result(timeout=3)


@pytest.mark.asyncio
async def test_run_on_loop_recovers_after_loop_stop(monkeypatch):
    """共享 loop 死亡后，下一条命令应自动在新 loop 上执行。"""
    runner = DedicatedLoopRunner("test-io-loop-recover")
    monkeypatch.setattr(dedicated_loop, "_SHARED_RUNNER", runner)

    assert await run_on_loop(lambda: asyncio.sleep(0, result=1)) == 1

    loop = runner.get()
    _stop(loop)
    _wait_until(lambda: runner.current_loop() is None)

    assert await run_on_loop(lambda: asyncio.sleep(0, result=2)) == 2
    _stop(runner.get())


@pytest.mark.asyncio
async def test_run_on_loop_rejects_dead_explicit_loop():
    """显式传入已停止的 loop 时应立即报错，而不是无限等待。"""
    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(DedicatedLoopError):
            await run_on_loop(lambda: asyncio.sleep(0), loop=loop)
    finally:
        loop.close()


@pytest.mark.asyncio
async def test_redis_close_waits_for_inflight_commands():
    """close() 必须等待在途命令结束后再关闭连接池。"""
    events = []

    class FakeClient:
        async def ping(self):
            events.append("start")
            await asyncio.sleep(0.2)
            events.append("end")
            return "PONG"

        async def aclose(self):
            events.append("close")

    client = create_client("redis://127.0.0.1:6379/0", decode_responses=True)
    client._client = FakeClient()
    client._bound_loop = get_shared_loop()

    task = asyncio.create_task(client.ping())
    await asyncio.sleep(0.05)

    await client.close()

    assert await task == "PONG"
    assert events == ["start", "end", "close"]
