"""
@FileName: helpers.py
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
    loop = asyncio.get_event_loop()
    if loop.is_running():
        pass