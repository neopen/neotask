"""
@FileName: test_redis.py
@Description: Redis 相关 fixtures
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import os
import functools
import pytest
import redis.asyncio as redis

# Redis 配置
# 使用 127.0.0.1 而非 localhost：Windows 上 localhost 可能先解析到 IPv6 ::1，
# 而本地 Redis 只监听 IPv4，连接会先超时再回退。
REDIS_URL = "redis://127.0.0.1:6379/11"
TEST_REDIS_URL = "redis://127.0.0.1:6379/12"


def skip_if_no_redis(func):
    """装饰器：如果 Redis 不可用则跳过测试"""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            client = redis.from_url(TEST_REDIS_URL, decode_responses=True,
                                    socket_connect_timeout=2, socket_timeout=2)
            await client.ping()
            await client.close()
        except Exception:
            pytest.skip("Redis not available")
        return await func(*args, **kwargs)

    # 如果是同步函数，也用同步方式处理
    if not hasattr(func, '__wrapped__'):
        wrapper_sync = functools.wraps(func)(lambda *a, **kw: func(*a, **kw))
        try:
            client = redis.from_url(TEST_REDIS_URL, decode_responses=True,
                                    socket_connect_timeout=2, socket_timeout=2)
            import asyncio as _asyncio
            loop = _asyncio.get_event_loop()
            loop.run_until_complete(client.ping())
            loop.run_until_complete(client.close())
        except Exception:
            pass

    return wrapper


@pytest.fixture(scope="function")
async def redis_client():
    """Redis 客户端 fixture（每个测试独立事件循环，避免跨 loop 复用连接）"""
    client = redis.from_url(TEST_REDIS_URL, decode_responses=True, max_connections=10,
                           socket_connect_timeout=5, socket_timeout=5)
    try:
        await client.ping()
        yield client
    finally:
        await client.close()


@pytest.fixture(autouse=False)  # 改为非自动使用
async def clean_redis(redis_client, request):
    """手动清理 Redis - 只在需要时使用"""
    yield
    # 使用 try/except 避免事件循环关闭错误
    try:
        await redis_client.flushdb()
    except (RuntimeError, ConnectionError) as e:
        # 事件循环已关闭或连接错误，忽略
        pass


@pytest.fixture(scope="function")
def distributed_redis_url():
    """分布式测试 Redis URL"""
    return os.environ.get("TEST_REDIS_URL", "redis://127.0.0.1:6379/12")