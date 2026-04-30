"""
@FileName: conftest.py
@Description: pytest 配置 - 导入所有 fixtures
@Author: HiPeng
@Time: 2026/4/29
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 添加 tests 目录到路径
tests_dir = Path(__file__).parent
sys.path.insert(0, str(tests_dir))

# 使用相对导入
from fixtures.event_loop import *
from fixtures.redis import *
from fixtures.storage import *
from fixtures.queue import *
from fixtures.task import *
from fixtures.executor import *
from fixtures.event import *
from fixtures.monitor import *
from fixtures.distributed import *
from fixtures.helpers import *

# 可选：添加 pytest 配置钩子
def pytest_configure(config):
    """pytest 配置钩子"""
    # 注册自定义标记
    config.addinivalue_line("markers", "distributed: 分布式功能测试")
    config.addinivalue_line("markers", "lock: 分布式锁测试")
    config.addinivalue_line("markers", "heartbeat: 节点心跳测试")
    config.addinivalue_line("markers", "sharding: 任务分片测试")