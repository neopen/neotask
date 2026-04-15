"""
@FileName: redis.py
@Description: Redis分布式锁实现 - 添加扫描和清理功能
@Author: HiPeng
@Time: 2026/4/15
"""

import time
import uuid
from typing import Optional, List, Dict, Any

import redis.asyncio as redis
from redis.asyncio import ConnectionPool

from neotask.lock.base import TaskLock


class RedisLock(TaskLock):
    """Redis分布式锁 - 支持扫描和清理"""

    # Lua 脚本
    LUA_ACQUIRE = """
    local key = KEYS[1]
    local owner = ARGV[1]
    local ttl = tonumber(ARGV[2])
    
    if redis.call('SET', key, owner, 'NX', 'EX', ttl) then
        return 1
    else
        return 0
    end
    """

    LUA_RELEASE = """
    local key = KEYS[1]
    local owner = ARGV[1]
    
    local current = redis.call('GET', key)
    if current == owner then
        return redis.call('DEL', key)
    else
        return 0
    end
    """

    LUA_EXTEND = """
    local key = KEYS[1]
    local owner = ARGV[1]
    local ttl = tonumber(ARGV[2])
    
    local current = redis.call('GET', key)
    if current == owner then
        return redis.call('EXPIRE', key, ttl)
    else
        return 0
    end
    """

    LUA_GET_INFO = """
    local key = KEYS[1]
    local owner = redis.call('GET', key)
    local ttl = redis.call('TTL', key)
    
    return {owner, ttl}
    """

    def __init__(self, redis_url: str, key_prefix: str = "lock:"):
        """初始化Redis锁

        Args:
            redis_url: Redis连接URL
            key_prefix: 键前缀
        """
        self._redis_url = redis_url
        self._key_prefix = key_prefix
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
        self._owner: Optional[str] = None

    async def _get_client(self) -> redis.Redis:
        """获取Redis客户端"""
        if self._client is None:
            self._pool = ConnectionPool.from_url(
                self._redis_url,
                decode_responses=True
            )
            self._client = redis.Redis(connection_pool=self._pool)
        return self._client

    def _get_key(self, key: str) -> str:
        """获取完整的键名"""
        return f"{self._key_prefix}{key}"

    async def acquire(self, key: str, ttl: int = 30) -> bool:
        """获取锁"""
        client = await self._get_client()
        self._owner = str(uuid.uuid4())

        result = await client.set(
            self._get_key(key),
            self._owner,
            nx=True,
            ex=ttl
        )
        return result is not None

    async def release(self, key: str) -> bool:
        """释放锁"""
        client = await self._get_client()
        script = client.register_script(self.LUA_RELEASE)
        result = await script(
            keys=[self._get_key(key)],
            args=[self._owner]
        )
        return result == 1

    async def extend(self, key: str, ttl: int = 30) -> bool:
        """延长锁的生存时间"""
        client = await self._get_client()
        script = client.register_script(self.LUA_EXTEND)
        result = await script(
            keys=[self._get_key(key)],
            args=[self._owner, ttl]
        )
        return result == 1

    async def is_locked(self, key: str) -> bool:
        """检查锁是否被持有"""
        client = await self._get_client()
        value = await client.get(self._get_key(key))
        return value is not None

    async def get_owner(self, key: str) -> Optional[str]:
        """获取锁的持有者"""
        client = await self._get_client()
        return await client.get(self._get_key(key))

    # ========== 新增：扫描和清理方法 ==========

    async def scan_locks(self, pattern: str = "lock:*", count: int = 100) -> List[str]:
        """扫描所有锁键

        Args:
            pattern: 键名匹配模式
            count: 每次扫描数量

        Returns:
            锁键列表
        """
        client = await self._get_client()
        keys = []
        cursor = 0

        while True:
            cursor, batch = await client.scan(
                cursor=cursor,
                match=f"{self._key_prefix}{pattern.replace('lock:', '')}",
                count=count
            )
            keys.extend(batch)
            if cursor == 0:
                break

        return keys

    async def get_lock_info(self, key: str) -> Optional[Dict[str, Any]]:
        """获取锁的详细信息

        Args:
            key: 锁键名

        Returns:
            锁信息字典
        """
        client = await self._get_client()
        full_key = self._get_key(key)

        # 获取锁信息和 TTL
        script = client.register_script(self.LUA_GET_INFO)
        result = await script(keys=[full_key])

        if not result or not result[0]:
            return None

        owner = result[0]
        ttl = result[1] if len(result) > 1 else -1

        # 获取创建时间（使用 Redis 的 OBJECT IDLETIME 或存储额外信息）
        # 这里简化处理，使用当前时间减去 TTL 估算
        now = time.time()
        created_at = now - (30 - ttl) if ttl > 0 else now

        return {
            "key": key,
            "full_key": full_key,
            "owner": owner,
            "is_locked": True,
            "created_at": created_at,
            "expire_at": now + ttl if ttl > 0 else None,
            "ttl_remaining": ttl if ttl > 0 else 0,
            "is_stale": ttl <= 0
        }

    async def cleanup_stale_locks(self, ttl_threshold: Optional[int] = None) -> int:
        """清理僵尸锁

        Args:
            ttl_threshold: TTL 阈值（秒），超过此时间的锁被视为僵尸

        Returns:
            清理的锁数量
        """
        client = await self._get_client()
        keys = await self.scan_locks(count=1000)

        cleaned = 0
        now = time.time()

        for full_key in keys:
            # 获取 TTL
            ttl = await client.ttl(full_key)

            should_clean = False

            if ttl_threshold is not None:
                # 基于创建时间判断（使用 OBJECT IDLETIME）
                idle_time = await client.object("idletime", full_key)
                if idle_time is not None and idle_time > ttl_threshold:
                    should_clean = True
            else:
                # TTL <= 0 表示已过期
                if ttl <= 0:
                    should_clean = True

            if should_clean:
                await client.delete(full_key)
                cleaned += 1

        return cleaned

    async def cleanup_by_owner(self, owner: str) -> int:
        """清理指定持有者的所有锁

        Args:
            owner: 持有者标识

        Returns:
            清理的锁数量
        """
        client = await self._get_client()
        keys = await self.scan_locks(count=1000)

        cleaned = 0
        for full_key in keys:
            current_owner = await client.get(full_key)
            if current_owner == owner:
                await client.delete(full_key)
                cleaned += 1

        return cleaned

    async def close(self) -> None:
        """关闭Redis连接"""
        if self._client:
            await self._client.close()
        if self._pool:
            await self._pool.disconnect()

    def __repr__(self) -> str:
        return f"RedisLock(url={self._redis_url})"
