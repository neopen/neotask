"""
@FileName: redis.py
@Description: Redis storage implementation with connection pooling.
@Author: HiPeng
@Time: 2026/3/27 23:55
"""

from typing import List, Optional, Any

import redis.asyncio as redis
from redis.asyncio import ConnectionPool

from neotask.models.task import Task, TaskStatus
from neotask.storage.base import TaskRepository, QueueRepository


class RedisTaskRepository(TaskRepository):
    """Redis-based task repository."""

    def __init__(self, redis_url: str, max_connections: int = 10):
        self.redis_url = redis_url
        self.max_connections = max_connections
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[redis.Redis] = None

    async def _get_client(self) -> redis.Redis:
        """Get Redis client with connection pooling."""
        if self._client is None:
            self._pool = ConnectionPool.from_url(
                self.redis_url,
                max_connections=self.max_connections,
                decode_responses=True
            )
            self._client = redis.Redis(connection_pool=self._pool)
        return self._client

    async def save(self, task: Task) -> None:
        client = await self._get_client()
        key = f"task:{task.task_id}"
        await client.hset(key, mapping=task.to_dict())
        await client.sadd(f"status:{task.status.value}", task.task_id)

    async def get(self, task_id: str) -> Optional[Task]:
        client = await self._get_client()
        key = f"task:{task_id}"
        data = await client.hgetall(key)
        if not data:
            return None
        return Task.from_dict(data)

    async def delete(self, task_id: str) -> bool:
        client = await self._get_client()
        task = await self.get(task_id)
        if task:
            await client.srem(f"status:{task.status.value}", task_id)
        key = f"task:{task_id}"
        result = await client.delete(key)
        return result > 0

    async def list_by_status(self, status: TaskStatus, limit: int = 100, offset: int = 0) -> List[Task]:
        client = await self._get_client()
        task_ids = await client.smembers(f"status:{status.value}")
        tasks = []
        for task_id in list(task_ids)[offset:offset + limit]:
            task = await self.get(task_id)
            if task:
                tasks.append(task)
        return tasks

    async def update_status(self, task_id: str, status: TaskStatus) -> None:
        client = await self._get_client()
        old_task = await self.get(task_id)
        if old_task:
            await client.srem(f"status:{old_task.status.value}", task_id)

        key = f"task:{task_id}"
        await client.hset(key, "status", status.value)
        await client.sadd(f"status:{status.value}", task_id)

    async def exists(self, task_id: str) -> bool:
        client = await self._get_client()
        key = f"task:{task_id}"
        return await client.exists(key) > 0

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.close()
            self._client = None
        if self._pool:
            await self._pool.disconnect()
            self._pool = None


class RedisQueueRepository(QueueRepository):
    """Redis-based priority queue repository."""

    def __init__(self, redis_url: str, max_connections: int = 10):
        self.redis_url = redis_url
        self.max_connections = max_connections
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
        self._queue_key = "queue:priority"
        self._pop_script: Optional[Any] = None

    async def _get_client(self) -> redis.Redis:
        """Get Redis client with connection pooling."""
        if self._client is None:
            self._pool = ConnectionPool.from_url(
                self.redis_url,
                max_connections=self.max_connections,
                decode_responses=True
            )
            self._client = redis.Redis(connection_pool=self._pool)
        return self._client

    async def _get_pop_script(self):
        """Get or create Lua pop script."""
        if self._pop_script is None:
            client = await self._get_client()
            lua_script = """
            local queue_key = KEYS[1]
            local count = tonumber(ARGV[1])
            local task_ids = redis.call('ZRANGE', queue_key, 0, count-1)
            if #task_ids > 0 then
                redis.call('ZREM', queue_key, unpack(task_ids))
            end
            return task_ids
            """
            self._pop_script = client.register_script(lua_script)
        return self._pop_script

    async def push(self, task_id: str, priority: int) -> None:
        client = await self._get_client()
        await client.zadd(self._queue_key, {task_id: priority})

    async def pop(self, count: int = 1) -> List[str]:
        script = await self._get_pop_script()
        return await script(keys=[self._queue_key], args=[count])

    async def remove(self, task_id: str) -> bool:
        client = await self._get_client()
        removed = await client.zrem(self._queue_key, task_id)
        return removed > 0

    async def size(self) -> int:
        client = await self._get_client()
        return await client.zcard(self._queue_key)

    async def peek(self, count: int = 1) -> List[str]:
        client = await self._get_client()
        return await client.zrange(self._queue_key, 0, count - 1)

    async def clear(self) -> None:
        client = await self._get_client()
        await client.delete(self._queue_key)

    async def contains(self, task_id: str) -> bool:
        """检查任务是否在队列中"""
        client = await self._get_client()
        score = await client.zscore(self._queue_key, task_id)
        return score is not None

    async def pause(self) -> None:
        """暂停队列"""
        client = await self._get_client()
        await client.set(f"{self._queue_key}:paused", "1", ex=3600)

    async def resume(self) -> None:
        """恢复队列"""
        client = await self._get_client()
        await client.delete(f"{self._queue_key}:paused")

    async def is_paused(self) -> bool:
        """检查是否暂停"""
        client = await self._get_client()
        return await client.exists(f"{self._queue_key}:paused") > 0

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.close()
            self._client = None
        if self._pool:
            await self._pool.disconnect()
            self._pool = None