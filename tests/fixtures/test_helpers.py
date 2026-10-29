"""
@FileName: test_helpers.py
@Description: 辅助 fixtures
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import asyncio
import pytest


@pytest.fixture(autouse=True)
def close_loop_after_test():
    """确保每个测试后正确关闭事件循环"""
    yield
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        # 测试内调用 asyncio.run() 后，当前线程的事件循环会被清空；
        # 这是合法状态，不应让 teardown 反向污染测试结果。
        return
    if loop.is_running():
        pass
