"""
@FileName: redis.py
@Description: Redis 锁实现（分布式环境）
@Author: HiPeng
@Time: 2026/4/1 19:08
"""

import uuid
from typing import Optional

import redis.asyncio as redis

from neotask.locks.base import TaskLock


class RedisLock(TaskLock):
    """Redis 分布式锁实现。

    特点：
    - 支持多节点部署
    - 基于 Redis SET NX EX 命令
    - 支持锁续期（看门狗）

    使用示例：
        >>> lock = RedisLock("redis://localhost:6379")
        >>> async with lock.lock("my_key", ttl=30, auto_extend=True):
        ...     # 长时间执行的任务
        ...     pass
    """

    LOCK_PREFIX = "neotask:lock:"

    def __init__(self, redis_url: str, key_prefix: str = None):
        """初始化 Redis 锁。

        Args:
            redis_url: Redis 连接 URL
            key_prefix: 键前缀
        """
        self._redis_url = redis_url
        self._key_prefix = key_prefix or self.LOCK_PREFIX
        self._client: Optional[redis.Redis] = None

    async def _get_client(self) -> redis.Redis:
        """获取 Redis 客户端（懒加载）。"""
        if self._client is None:
            self._client = await redis.from_url(
                self._redis_url,
                decode_responses=True,
                max_connections=10,
            )
        return self._client

    def _get_lock_key(self, key: str) -> str:
        """获取完整的锁键名。"""
        return f"{self._key_prefix}{key}"

    async def acquire(self, key: str, ttl: int = 30) -> bool:
        """获取 Redis 锁。

        使用 SET NX EX 命令实现原子操作。
        """
        client = await self._get_client()
        lock_key = self._get_lock_key(key)
        owner = str(uuid.uuid4())

        # SET key value NX EX seconds
        result = await client.set(lock_key, owner, nx=True, ex=ttl)
        return result is True

    async def release(self, key: str) -> bool:
        """释放 Redis 锁。

        使用 Lua 脚本确保只有持有者才能释放锁。
        """
        client = await self._get_client()
        lock_key = self._get_lock_key(key)

        # Lua 脚本：只有 value 匹配时才删除
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """

        # 获取当前持有者
        owner = await client.get(lock_key)
        if not owner:
            return False

        result = await client.eval(lua_script, 1, lock_key, owner)
        return result == 1

    async def extend(self, key: str, ttl: int = 30) -> bool:
        """延长 Redis 锁。

        使用 Lua 脚本确保只有持有者才能延长。
        """
        client = await self._get_client()
        lock_key = self._get_lock_key(key)

        # Lua 脚本：只有 value 匹配时才延长
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """

        # 获取当前持有者
        owner = await client.get(lock_key)
        if not owner:
            return False

        result = await client.eval(lua_script, 1, lock_key, owner, ttl)
        return result == 1

    async def is_locked(self, key: str) -> bool:
        """检查锁是否被持有。"""
        client = await self._get_client()
        lock_key = self._get_lock_key(key)
        value = await client.get(lock_key)
        return value is not None

    async def get_owner(self, key: str) -> Optional[str]:
        """获取锁的持有者。"""
        client = await self._get_client()
        lock_key = self._get_lock_key(key)
        return await client.get(lock_key)

    async def close(self):
        """关闭 Redis 连接。"""
        if self._client:
            await self._client.close()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
