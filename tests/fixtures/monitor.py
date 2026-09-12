"""
@FileName: monitor.py
@Description: 监控相关 fixtures
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import pytest

from neotask.monitor.metrics import MetricsCollector


@pytest.fixture
def metrics_collector():
    """指标收集器"""
    return MetricsCollector(window_size=100)