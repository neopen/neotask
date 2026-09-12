"""
@FileName: event.py
@Description: 事件总线相关 fixtures
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import pytest

from neotask.event.bus import EventBus


@pytest.fixture
async def event_bus():
    """事件总线（异步 fixture）"""
    bus = EventBus()
    await bus.start()
    yield bus
    await bus.stop()