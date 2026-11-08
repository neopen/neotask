"""
@FileName: constants.py
@Description: 常量定义
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/1 19:03
"""

from __future__ import annotations

#: 所有 Redis 键的统一根前缀。
#: 任务数据、状态索引、队列、锁、节点注册表、周期任务、死信队列等
#: 均由该前缀派生，便于与业务方共用同一个 Redis 实例时隔离命名空间。
REDIS_KEY_PREFIX = "neotask:"

#: 进度落库的最小时间间隔（秒），避免高频上报放大 SQLite 写入
DEFAULT_PROGRESS_FLUSH_INTERVAL = 0.2

#: 进度落库的最小增量阈值（0-1），与时间间隔取“或”关系
DEFAULT_PROGRESS_FLUSH_DELTA = 0.05

#: 写回缓存的批量落库间隔（秒），多个任务的进度合并到一个事务提交
DEFAULT_PROGRESS_FLUSH_BATCH_INTERVAL = 1.0

#: 生命周期缓存默认上限；超出后优先淘汰已结束任务
DEFAULT_LIFECYCLE_CACHE_MAX_SIZE = 1000

#: RUNNING 状态延迟落库窗口（秒）。0 表示立即落库（默认）；
#: >0 时短任务在窗口内完成只需写一次终态，减少每任务的跨 loop 写入次数。
DEFAULT_RUNNING_PERSIST_DELAY = 0.0


def redis_key(name: str) -> str:
    """为 Redis 键拼接统一根前缀。

    已带有根前缀的名称会原样返回，避免重复叠加
    （例如周期任务存储的 ``key_prefix`` 本身就以 ``neotask:`` 开头）。

    Args:
        name: 不含根前缀的键名，如 ``task:TSK001`` 或 ``queue:priority``

    Returns:
        完整键名，如 ``neotask:task:TSK001``
    """
    if name.startswith(REDIS_KEY_PREFIX):
        return name
    return f"{REDIS_KEY_PREFIX}{name}"
