"""
@FileName: redis.py
@Description: 
@Author: HiPeng
@Time: 2026/3/27 23:55
"""
"""Redis storage implementation."""

from typing import List, Optional

import redis.asyncio as redis

from neotask.models.task import Task, TaskStatus
from neotask.storage.base import TaskRepository, QueueRepository


class RedisTaskRepository(TaskRepository):
    """Redis-based task repository."""

    def __init__(self, redis_url: str):
        self._client = redis.from_url(redis_url, decode_responses=True)

    async def save(self, task: Task) -> None:
        key = f"task:{task.task_id}"
        await self._client.hset(key, mapping=task.to_dict())

    async def get(self, task_id: str) -> Optional[Task]:
        key = f"task:{task_id}"
        data = await self._client.hgetall(key)
        if not data:
            return None
        return Task.from_dict(data)

    async def delete(self, task_id: str) -> None:
        key = f"task:{task_id}"
        await self._client.delete(key)

    async def list_by_status(self, status: TaskStatus, limit: int = 100) -> List[Task]:
        pattern = "task:*"
        tasks = []
        cursor = 0

        while len(tasks) < limit:
            cursor, keys = await self._client.scan(cursor, match=pattern, count=limit)
            for key in keys:
                data = await self._client.hgetall(key)
                if data and data.get("status") == status.value:
                    tasks.append(Task.from_dict(data))
            if cursor == 0:
                break

        return tasks[:limit]

    async def update_status(self, task_id: str, status: TaskStatus) -> None:
        key = f"task:{task_id}"
        await self._client.hset(key, "status", status.value)

    async def close(self) -> None:
        """Close Redis connection."""
        await self._client.close()


class RedisQueueRepository(QueueRepository):
    """Redis-based priority queue repository."""

    def __init__(self, redis_url: str):
        self._client = redis.from_url(redis_url, decode_responses=True)
        self._queue_key = "queue:priority"

    async def push(self, task_id: str, priority: int) -> None:
        await self._client.zadd(self._queue_key, {task_id: priority})

    async def pop(self, count: int = 1) -> List[str]:
        # Atomic pop using Lua script
        lua_script = """
        local queue_key = KEYS[1]
        local count = tonumber(ARGV[1])
        local task_ids = redis.call('ZRANGE', queue_key, 0, count-1)
        if #task_ids > 0 then
            redis.call('ZREM', queue_key, unpack(task_ids))
        end
        return task_ids
        """
        script = self._client.register_script(lua_script)
        return await script(keys=[self._queue_key], args=[count])

    async def remove(self, task_id: str) -> bool:
        removed = await self._client.zrem(self._queue_key, task_id)
        return removed > 0

    async def size(self) -> int:
        return await self._client.zcard(self._queue_key)

    async def peek(self, count: int = 1) -> List[str]:
        """Peek at top tasks without removing."""
        return await self._client.zrange(self._queue_key, 0, count - 1)

    async def close(self) -> None:
        """Close Redis connection."""
        await self._client.close()
