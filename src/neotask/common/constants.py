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
