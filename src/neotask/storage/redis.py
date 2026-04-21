"""
@FileName: redis.py
@Description: Redis storage implementation using sync client in thread pool.
@Author: HiPeng
@Time: 2026/3/27 23:55
"""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import redis

from neotask.models.task import Task, TaskStatus
from neotask.storage.base import TaskRepository, QueueRepository


class RedisTaskRepository(TaskRepository):
    """Redis-based task repository using sync client."""

    def __init__(self, redis_url: str, max_connections: int = 10):
        self.redis_url = redis_url
        self.max_connections = max_connections
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._client: Optional[redis.Redis] = None

    def _get_client(self) -> redis.Redis:
        """Get sync Redis client."""
        if self._client is None:
            self._client = redis.Redis.from_url(
                self.redis_url,
                decode_responses=True,
                max_connections=self.max_connections
            )
        return self._client

    async def _run_sync(self, func, *args, **kwargs):
        """Run sync function in thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: func(*args, **kwargs)
        )

    async def save(self, task: Task) -> None:
        client = self._get_client()
        key = f"task:{task.task_id}"

        def _save():
            # Store entire task as JSON
            client.set(key, json.dumps(task.to_dict()))
            # Also add to status index
            client.sadd(f"status:{task.status.value}", task.task_id)

        await self._run_sync(_save)

    async def get(self, task_id: str) -> Optional[Task]:
        client = self._get_client()
        key = f"task:{task_id}"

        def _get():
            data = client.get(key)
            if not data:
                return None
            return json.loads(data)

        data = await self._run_sync(_get)
        if not data:
            return None
        return Task.from_dict(data)

    async def delete(self, task_id: str) -> None:
        client = self._get_client()

        def _delete():
            # Get task first to know its status
            task_data = client.get(f"task:{task_id}")
            if task_data:
                task_dict = json.loads(task_data)
                status = task_dict.get("status")
                if status:
                    client.srem(f"status:{status}", task_id)
            client.delete(f"task:{task_id}")

        await self._run_sync(_delete)

    async def list_by_status(self, status: TaskStatus, limit: int = 100, offset: int = 0) -> List[Task]:
        client = self._get_client()

        def _list():
            task_ids = client.smembers(f"status:{status.value}")
            tasks = []
            for task_id in list(task_ids)[offset:offset + limit]:
                data = client.get(f"task:{task_id}")
                if data:
                    tasks.append(json.loads(data))
            return tasks

        tasks_data = await self._run_sync(_list)
        return [Task.from_dict(data) for data in tasks_data]

    async def update_status(self, task_id: str, status: TaskStatus, **kwargs) -> bool:
        client = self._get_client()

        def _update():
            # Get old status to remove from index
            old_data = client.get(f"task:{task_id}")
            if old_data:
                old_dict = json.loads(old_data)
                old_status = old_dict.get("status")
                if old_status:
                    client.srem(f"status:{old_status}", task_id)

            # Get current task, update, save
            data = client.get(f"task:{task_id}")
            if not data:
                return False

            task_dict = json.loads(data)
            task_dict["status"] = status.value

            for key_name, value in kwargs.items():
                if value is not None:
                    task_dict[key_name] = value

            client.set(f"task:{task_id}", json.dumps(task_dict))
            client.sadd(f"status:{status.value}", task_id)
            return True

        return await self._run_sync(_update)

    async def exists(self, task_id: str) -> bool:
        client = self._get_client()

        def _exists():
            return client.exists(f"task:{task_id}") > 0

        return await self._run_sync(_exists)

    async def close(self) -> None:
        """Close Redis connection."""

        def _close():
            if self._client:
                self._client.close()
            self._executor.shutdown(wait=True)

        await self._run_sync(_close)


class RedisQueueRepository(QueueRepository):
    """Redis-based priority queue repository using sync client."""

    def __init__(self, redis_url: str, max_connections: int = 10):
        self.redis_url = redis_url
        self.max_connections = max_connections
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._client: Optional[redis.Redis] = None
        self._queue_key = "queue:priority"

    def _get_client(self) -> redis.Redis:
        """Get sync Redis client."""
        if self._client is None:
            self._client = redis.Redis.from_url(
                self.redis_url,
                decode_responses=True,
                max_connections=self.max_connections
            )
        return self._client

    async def _run_sync(self, func, *args, **kwargs):
        """Run sync function in thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: func(*args, **kwargs)
        )

    async def push(self, task_id: str, priority: int) -> None:
        client = self._get_client()

        def _push():
            client.zadd(self._queue_key, {task_id: priority})

        await self._run_sync(_push)

    async def pop(self, count: int = 1) -> List[str]:
        client = self._get_client()

        def _pop():
            # Get and remove tasks atomically using pipeline
            pipe = client.pipeline()
            pipe.zrange(self._queue_key, 0, count - 1)
            pipe.zremrangebyrank(self._queue_key, 0, count - 1)
            results = pipe.execute()
            return results[0] if results else []

        return await self._run_sync(_pop)

    async def remove(self, task_id: str) -> bool:
        client = self._get_client()

        def _remove():
            return client.zrem(self._queue_key, task_id) > 0

        return await self._run_sync(_remove)

    async def size(self) -> int:
        client = self._get_client()

        def _size():
            return client.zcard(self._queue_key)

        return await self._run_sync(_size)

    async def peek(self, count: int = 1) -> List[str]:
        client = self._get_client()

        def _peek():
            return client.zrange(self._queue_key, 0, count - 1)

        return await self._run_sync(_peek)

    async def clear(self) -> None:
        client = self._get_client()

        def _clear():
            client.delete(self._queue_key)

        await self._run_sync(_clear)

    async def close(self) -> None:
        """Close Redis connection."""

        def _close():
            if self._client:
                self._client.close()
            self._executor.shutdown(wait=True)

        await self._run_sync(_close)
