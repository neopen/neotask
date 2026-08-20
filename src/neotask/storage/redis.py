"""
@FileName: redis.py
@Description: Redis storage implementation with connection pooling.
@Author: HiPeng
@Time: 2026/3/27 23:55
"""

import time
import json
from typing import List, Optional, Any, Tuple

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
        """保存任务 - 使用 JSON 序列化存储"""
        client = await self._get_client()
        key = f"task:{task.task_id}"

        # 使用 JSON 字符串存储整个任务
        task_dict = task.to_dict()
        await client.set(key, json.dumps(task_dict, default=str))

        # 添加到状态索引
        await client.sadd(f"status:{task.status.value}", task.task_id)

    async def get(self, task_id: str) -> Optional[Task]:
        """获取任务"""
        client = await self._get_client()
        key = f"task:{task_id}"
        data = await client.get(key)

        if not data:
            return None

        task_dict = json.loads(data)
        return Task.from_dict(task_dict)

    async def delete(self, task_id: str) -> bool:
        """删除任务"""
        client = await self._get_client()
        task = await self.get(task_id)
        if task:
            await client.srem(f"status:{task.status.value}", task_id)
        key = f"task:{task_id}"
        result = await client.delete(key)
        return result > 0

    async def delete_batch(self, task_ids: List[str]) -> int:
        """批量删除任务

        Args:
            task_ids: 任务ID列表

        Returns:
            成功删除的数量
        """
        if not task_ids:
            return 0

        client = await self._get_client()
        pipe = client.pipeline()

        deleted_count = 0

        for task_id in task_ids:
            key = f"task:{task_id}"
            data = await client.get(key)

            if data:
                task_dict = json.loads(data)
                status = task_dict.get("status")
                if status:
                    # 从状态索引中移除
                    pipe.srem(f"status:{status}", task_id)
                # 删除任务
                pipe.delete(key)
                deleted_count += 1
            else:
                # 任务不存在，直接尝试删除键
                pipe.delete(key)

        await pipe.execute()
        return deleted_count

    async def list_by_status(self, status: TaskStatus, limit: int = 100, offset: int = 0) -> List[Task]:
        """按状态列出任务"""
        client = await self._get_client()
        task_ids = await client.smembers(f"status:{status.value}")

        tasks = []
        task_id_list = list(task_ids)

        for task_id in task_id_list[offset:offset + limit]:
            task = await self.get(task_id)
            if task:
                tasks.append(task)

        return tasks

    async def update_status(self, task_id: str, status: TaskStatus, **kwargs) -> bool:
        """更新任务状态"""
        client = await self._get_client()
        key = f"task:{task_id}"

        # 获取现有任务
        data = await client.get(key)
        if not data:
            return False

        task_dict = json.loads(data)
        old_status = task_dict.get("status")
        task_dict["status"] = status.value

        # 更新其他字段
        for key_name, value in kwargs.items():
            if key_name in task_dict:
                task_dict[key_name] = value

        # 保存更新
        await client.set(key, json.dumps(task_dict, default=str))

        # 更新状态索引
        if old_status:
            await client.srem(f"status:{old_status}", task_id)
        await client.sadd(f"status:{status.value}", task_id)

        return True

    async def update_status_batch(
        self,
        updates: List[Tuple[str, TaskStatus, dict]]
    ) -> int:
        """批量更新任务状态（使用 pipeline）

        Args:
            updates: 更新列表，每个元素为 (task_id, status, kwargs)

        Returns:
            成功更新的数量
        """
        if not updates:
            return 0

        client = await self._get_client()
        pipe = client.pipeline()
        success_count = 0

        for task_id, status, kwargs in updates:
            key = f"task:{task_id}"

            # 获取现有任务
            data = await client.get(key)
            if data:
                task_dict = json.loads(data)
                old_status = task_dict.get("status")
                task_dict["status"] = status.value if hasattr(status, 'value') else status

                for key_name, value in kwargs.items():
                    if key_name in task_dict:
                        task_dict[key_name] = value

                pipe.set(key, json.dumps(task_dict, default=str))

                # 更新状态索引
                if old_status:
                    pipe.srem(f"status:{old_status}", task_id)
                pipe.sadd(f"status:{status.value if hasattr(status, 'value') else status}", task_id)

                success_count += 1

        await pipe.execute()
        return success_count

    async def exists(self, task_id: str) -> bool:
        """检查任务是否存在"""
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
        self._delayed_key = "queue:delayed"
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

    async def push(self, task_id: str, priority: int, delay: float = 0) -> None:
        """推入任务到队列"""
        client = await self._get_client()

        if delay > 0:
            # 延迟任务使用单独的延迟队列
            execute_at = time.time() + delay
            await client.zadd(self._delayed_key, {task_id: execute_at})
        else:
            await client.zadd(self._queue_key, {task_id: priority})

    async def pop(self, count: int = 1) -> List[str]:
        """弹出任务（先处理延迟队列，再弹出优先级队列）"""
        client = await self._get_client()

        # 1. 处理到期的延迟任务
        now = time.time()
        delayed_tasks = await client.zrangebyscore(
            self._delayed_key, 0, now, start=0, num=count
        )

        if delayed_tasks:
            # 移除延迟队列中的任务
            await client.zrem(self._delayed_key, *delayed_tasks)
            # 加入优先级队列（默认中等优先级）
            for task_id in delayed_tasks:
                await client.zadd(self._queue_key, {task_id: 5})

        # 2. 从优先级队列弹出
        script = await self._get_pop_script()
        pop_result = await script(keys=[self._queue_key], args=[count])

        return pop_result

    async def pop_delayed(self, count: int = 1) -> List[str]:
        """仅弹出到期的延迟任务（不自动加入优先级队列）"""
        client = await self._get_client()
        now = time.time()

        task_ids = await client.zrangebyscore(
            self._delayed_key, 0, now, start=0, num=count
        )

        if task_ids:
            await client.zrem(self._delayed_key, *task_ids)

        return task_ids

    async def remove(self, task_id: str) -> bool:
        """移除任务"""
        client = await self._get_client()

        removed = await client.zrem(self._queue_key, task_id)
        if not removed:
            removed = await client.zrem(self._delayed_key, task_id)

        return removed > 0

    async def size(self) -> int:
        """获取队列大小"""
        client = await self._get_client()
        priority_size = await client.zcard(self._queue_key)
        delayed_size = await client.zcard(self._delayed_key)
        return priority_size + delayed_size

    async def peek(self, count: int = 1) -> List[str]:
        """查看队首任务"""
        client = await self._get_client()
        return await client.zrange(self._queue_key, 0, count - 1)

    async def clear(self) -> None:
        """清空队列"""
        client = await self._get_client()
        await client.delete(self._queue_key)
        await client.delete(self._delayed_key)

    async def contains(self, task_id: str) -> bool:
        """检查任务是否在队列中"""
        client = await self._get_client()

        score = await client.zscore(self._queue_key, task_id)
        if score is not None:
            return True

        score = await client.zscore(self._delayed_key, task_id)
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

    async def disable(self) -> None:
        """禁用队列"""
        client = await self._get_client()
        await client.set(f"{self._queue_key}:disabled", "1", ex=3600)

    async def enable(self) -> None:
        """启用队列"""
        client = await self._get_client()
        await client.delete(f"{self._queue_key}:disabled")

    async def is_disabled(self) -> bool:
        """检查是否禁用"""
        client = await self._get_client()
        return await client.exists(f"{self._queue_key}:disabled") > 0

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.close()
            self._client = None
        if self._pool:
            await self._pool.disconnect()
            self._pool = None