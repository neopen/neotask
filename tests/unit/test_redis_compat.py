"""
@FileName: test_redis_compat.py
@Description: redis_compat 跨事件循环客户端回归测试 - 防止连接池重新绑定到不同 loop。
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

import asyncio

import pytest

from neotask.utils.redis_compat import DEFAULT_PROTOCOL, create_client, resolve_protocol
from tests.conftest import skip_if_no_redis

REDIS_URL = "redis://127.0.0.1:6379/12"
KEY = "neotask:test:loop-bound-client"
SET_KEY = f"{KEY}:set"


class TestResolveProtocol:
    """RESP 协议版本解析（无需 Redis 服务）"""

    def test_default_is_resp2(self, monkeypatch):
        monkeypatch.delenv("NEOTASK_REDIS_PROTOCOL", raising=False)
        assert resolve_protocol() == DEFAULT_PROTOCOL == 2

    def test_explicit_argument_wins(self, monkeypatch):
        monkeypatch.setenv("NEOTASK_REDIS_PROTOCOL", "3")
        assert resolve_protocol(2) == 2
        assert resolve_protocol() == 3


def _use_client_in_standalone_loop(client) -> None:
    """在一个全新的（非测试）事件循环中首次使用客户端。"""

    async def _run() -> None:
        await client.set(KEY, "from-loop-a")

        pipe = client.pipeline()
        pipe.sadd(SET_KEY, "a")
        await pipe.execute()

        script = client.register_script("return redis.call('GET', KEYS[1])")
        assert await script(keys=[KEY]) == "from-loop-a"

    asyncio.run(_run())


@pytest.mark.asyncio
@skip_if_no_redis
async def test_client_reusable_across_event_loops():
    """同一客户端先在独立 loop 中建连，再回到测试 loop 使用，不应报跨 loop 错误。"""
    client = create_client(REDIS_URL, decode_responses=True)
    try:
        # 模拟 TaskPool 内部守护 loop：连接池在另一个 loop 中首次使用
        await asyncio.get_running_loop().run_in_executor(
            None, _use_client_in_standalone_loop, client
        )

        # 切回当前测试 loop，继续使用同一个客户端
        assert await client.get(KEY) == "from-loop-a"
        assert await client.smembers(SET_KEY) == {"a"}

        # 关闭后再次使用应自动重建连接（与 redis-py 语义一致）
        await client.close()
        assert await client.get(KEY) == "from-loop-a"
    finally:
        await client.delete(KEY, SET_KEY)
        await client.close()
