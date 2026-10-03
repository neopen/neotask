"""
@FileName: redis_compat.py
@Description: Redis 建连兼容助手 - 统一 RESP 协议选择，兼容旧版 Redis 服务端
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/03 00:00
"""

from __future__ import annotations

import os
from typing import Any, Optional

# 默认使用 RESP2（protocol=2）。
# redis-py 高版本可能默认以 RESP3 握手（连接时发送 HELLO 命令），而 Redis < 6.0
# 不支持 HELLO，会直接报 "unknown command 'HELLO'"，导致整个 Redis 后端开箱不可用
# （见 FINDINGS 2.4）。RESP2 对新老服务端都兼容。
DEFAULT_PROTOCOL = 2

# 允许通过环境变量覆盖协议版本（如确需 RESP3：NEOTASK_REDIS_PROTOCOL=3）
_PROTOCOL_ENV = "NEOTASK_REDIS_PROTOCOL"


def resolve_protocol(protocol: Optional[int] = None) -> int:
    """解析应使用的 RESP 协议版本。

    优先级：显式入参 > 环境变量 ``NEOTASK_REDIS_PROTOCOL`` > 默认 RESP2。
    """
    if protocol is not None:
        return protocol
    raw = os.getenv(_PROTOCOL_ENV)
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return DEFAULT_PROTOCOL


def from_url(redis_url: str, *, protocol: Optional[int] = None, **kwargs: Any):
    """基于 URL 创建 ``redis.asyncio.ConnectionPool``，默认使用 RESP2。

    - 显式传入 ``protocol`` 时优先采用；否则读取环境变量，最终回退 RESP2。
    - 旧版 redis-py（<5.0）不支持 ``protocol`` 关键字参数，捕获 ``TypeError``
      后自动降级为不传该参数，避免在低版本依赖上直接报错。
    """
    from redis.asyncio import ConnectionPool

    proto = resolve_protocol(protocol)
    try:
        return ConnectionPool.from_url(redis_url, protocol=proto, **kwargs)
    except TypeError:
        # redis-py < 5.0 不支持 protocol 关键字参数
        return ConnectionPool.from_url(redis_url, **kwargs)
