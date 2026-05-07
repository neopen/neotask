"""
@FileName: executor.py
@Description: 执行器相关 fixtures
@Author: HiPeng
@Time: 2026/4/29
"""

import asyncio
from typing import Dict, Any
import pytest


async def echo_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """回显执行器 - 返回输入数据"""
    await asyncio.sleep(0.01)
    return {"result": "echo", "data": data}


async def slow_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """慢速执行器 - 模拟耗时任务"""
    await asyncio.sleep(0.3)
    return {"result": "slow_done", "data": data}


@pytest.fixture
def mock_executor():
    """模拟任务执行器"""

    async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"result": "success", "data": data}

    return executor


def create_test_executor():
    """创建测试执行器"""

    async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"result": "processed", "data": data}

    return executor