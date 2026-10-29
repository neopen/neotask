"""
@FileName: test_event_loop.py
@Description: 事件循环相关 fixtures
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import asyncio
import sys
import pytest


# Windows 上解决事件循环关闭问题
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(scope="session")
def event_loop_policy():
    """设置事件循环策略"""
    if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.get_event_loop_policy()


@pytest.fixture(scope="function")
def event_loop():
    """为每个测试函数创建独立的事件循环"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    # 清理待处理的任务：先取消，再限时等待，避免个别不响应取消的协程把
    # teardown 拖成永久挂起（Windows 上表现为进程结束不了）。
    pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        try:
            loop.run_until_complete(
                asyncio.wait(pending, timeout=5)
            )
        except Exception:
            pass
    loop.close()


@pytest.fixture(scope="session")
def event_loop2():
    """创建事件循环 - 修复 Windows 关闭问题"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    # 关闭前处理所有 pending 任务（限时等待，避免 teardown 挂起）
    if not loop.is_closed():
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            try:
                loop.run_until_complete(asyncio.wait(pending, timeout=5))
            except Exception:
                pass
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()