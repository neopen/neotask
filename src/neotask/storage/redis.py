"""
@FileName: test_redis.py
@Description: Redis storage implementation with connection pooling.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/3/27 23:55
"""

import time
import json
from typing import TYPE_CHECKING, List, Optional, Any, Tuple, Dict

from neotask.utils import redis_compat

if TYPE_CHECKING:
    import redis.asyncio as redis
    from redis.asyncio import ConnectionPool

try:
    import redis.asyncio as redis
    from redis.asyncio import ConnectionPool

    HAS_REDIS = True
except ImportError:  # pragma: no cover
    redis = None  # type: ignore[assignment]
    ConnectionPool = None  # type: ignore[assignment]
    HAS_REDIS = False

from neotask.common.constants import redis_key
from neotask.common.exceptions import TaskAlreadyExistsError
from neotask.models.task import Task, TaskPriority, TaskStatus
from neotask.storage.base import TaskRepository, QueueRepository

#: 队列 zset 键：任务仓储的「创建即入队」原子脚本需要直接写入这两个键
_QUEUE_PRIORITY_KEY = redis_key("queue:priority")
_QUEUE_DELAYED_KEY = redis_key("queue:delayed")

#: 终态任务不再接受进度更新（与 SQLite 后端保持一致）
_TERMINAL_STATUS_VALUES = {
    TaskStatus.SUCCESS.value,
    TaskStatus.FAILED.value,
    TaskStatus.CANCELLED.value,
}

#: 新任务写入脚本：任务键不存在时写 JSON 并加入状态索引（原子，1 次往返）
_SAVE_IF_ABSENT_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SADD', KEYS[2], ARGV[2])
return 1
"""

#: 提交脚本：查重 → 容量校验 → 写任务 → 状态索引 → 入队（整条提交 1 次往返）
#: 返回 -1 已存在 / 0 队列已满（任务仍写入，交由调用方标记失败）/ 1 成功
_SUBMIT_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return -1
end
local size = redis.call('ZCARD', KEYS[3]) + redis.call('ZCARD', KEYS[4])
local limit = tonumber(ARGV[3])
if size >= limit then
    redis.call('SET', KEYS[1], ARGV[1])
    redis.call('SADD', KEYS[2], ARGV[2])
    return 0
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SADD', KEYS[2], ARGV[2])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[2])
return 1
"""


class RedisTaskRepository(TaskRepository):
    """Redis-based task repository."""

    def __init__(self, redis_url: str, max_connections: int = 10):
        self.redis_url = redis_url
        self.max_connections = max_connections
        # 使用 loop 安全客户端：命令统一在专用事件循环执行，
        # 避免 TaskPool 内部 loop 与调用方 loop 共用连接池时跨 loop 报错
        self._client: Optional[Any] = None
        self._save_if_absent_script: Optional[Any] = None
        self._submit_script: Optional[Any] = None


    async def _get_client(self) -> Any:
        """Get Redis client with connection pooling."""
        if not HAS_REDIS:
            raise RuntimeError("redis not installed. Run: pip install neotask[redis]")
        if self._client is None:
            self._client = redis_compat.create_client(
                self.redis_url,
                max_connections=self.max_connections,
                decode_responses=True
            )
        return self._client

    @staticmethod
    def _task_key(task_id: str) -> str:
        """任务数据键（统一根前缀 + task:<task_id>）"""
        return redis_key(f"task:{task_id}")

    @staticmethod
    def _status_key(status: str) -> str:
        """状态索引键（统一根前缀 + status:<status>）"""
        return redis_key(f"status:{status}")

    async def save(self, task: Task) -> None:
        """保存任务 - 使用 JSON 序列化存储"""
        client = await self._get_client()
        key = self._task_key(task.task_id)

        # 同步状态索引：先移除旧状态的成员关系，否则任务会同时残留在多个 status:* 集合中
        old_data = await client.get(key)
        if old_data:
            try:
                old_status = json.loads(old_data).get("status")
            except (ValueError, TypeError):
                old_status = None
            if old_status and old_status != task.status.value:
                await client.srem(self._status_key(old_status), task.task_id)

        # 使用 JSON 字符串存储整个任务
        task_dict = task.to_dict()
        await client.set(key, json.dumps(task_dict, default=str))

        # 添加到状态索引
        await client.sadd(self._status_key(task.status.value), task.task_id)

    async def save_if_absent(self, task: Task) -> bool:
        """单条 Lua 完成「查重 + SET + SADD」，把新任务提交压到 1 次往返"""
        client = await self._get_client()
        script = self._save_if_absent_script
        if script is None:
            script = client.register_script(_SAVE_IF_ABSENT_LUA)
            self._save_if_absent_script = script
        created = await script(
            keys=[
                self._task_key(task.task_id),
                self._status_key(task.status.value),
            ],
            args=[
                json.dumps(task.to_dict(), default=str),
                task.task_id,
            ],
        )
        return int(created) == 1

    async def save_if_absent_and_enqueue(
            self, task: Task, max_size: int
    ) -> Optional[bool]:
        """单条 Lua 完成「查重 → 容量校验 → 写任务 → 状态索引 → 入队」

        Returns:
            True: 创建并入队成功
            False: 队列已满（任务仍已写入存储，调用方应标记失败）
            None: 不会返回（该后端支持合并）

        Raises:
            TaskAlreadyExistsError: ``task_id`` 已存在
        """
        client = await self._get_client()
        script = self._submit_script
        if script is None:
            script = client.register_script(_SUBMIT_LUA)
            self._submit_script = script

        outcome = await script(
            keys=[
                self._task_key(task.task_id),
                self._status_key(task.status.value),
                _QUEUE_PRIORITY_KEY,
                _QUEUE_DELAYED_KEY,
            ],
            args=[
                json.dumps(task.to_dict(), default=str),
                task.task_id,
                max_size,
                task.priority.value,
            ],
        )
        outcome = int(outcome)
        if outcome < 0:
            raise TaskAlreadyExistsError(task.task_id)
        return outcome == 1

    async def save_batch(self, tasks: List[Task]) -> List[str]:
        """批量保存任务：一次 ``MGET`` 查重 + 一个 pipeline 写入。

        返回实际新增的 task_id（已存在的会被跳过，不覆盖已有任务）。
        """
        if not tasks:
            return []

        client = await self._get_client()
        keys = [self._task_key(task.task_id) for task in tasks]
        existing_values = await client.mget(keys)

        pipe = client.pipeline()
        saved: List[str] = []
        status_ids: Dict[str, List[str]] = {}
        for task, old_value in zip(tasks, existing_values):
            if old_value:
                continue
            saved.append(task.task_id)
            pipe.set(
                self._task_key(task.task_id),
                json.dumps(task.to_dict(), default=str),
            )
            status_ids.setdefault(task.status.value, []).append(task.task_id)

        for status, task_ids in status_ids.items():
            pipe.sadd(self._status_key(status), *task_ids)

        await pipe.execute()
        return saved

    async def get(self, task_id: str) -> Optional[Task]:
        """获取任务"""
        client = await self._get_client()
        key = self._task_key(task_id)
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
            await client.srem(self._status_key(task.status.value), task_id)
        key = self._task_key(task_id)
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
            key = self._task_key(task_id)
            data = await client.get(key)

            if data:
                task_dict = json.loads(data)
                status = task_dict.get("status")
                if status:
                    # 从状态索引中移除
                    pipe.srem(self._status_key(status), task_id)
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
        task_ids = await client.smembers(self._status_key(status.value))

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
        key = self._task_key(task_id)

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
            await client.srem(self._status_key(old_status), task_id)
        await client.sadd(self._status_key(status.value), task_id)

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
            key = self._task_key(task_id)

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
                    pipe.srem(self._status_key(old_status), task_id)
                pipe.sadd(
                    self._status_key(status.value if hasattr(status, "value") else status),
                    task_id,
                )

                success_count += 1

        await pipe.execute()
        return success_count

    async def update_progress(self, task_id: str, progress: float, message: str = "") -> bool:
        """单条更新进度：GET + SET，避免走基类 get+save 的重复读取"""
        client = await self._get_client()
        key = self._task_key(task_id)

        data = await client.get(key)
        if not data:
            return False
        try:
            task_dict = json.loads(data)
        except (ValueError, TypeError):
            return False

        if task_dict.get("status") in _TERMINAL_STATUS_VALUES:
            return False

        task_dict["progress"] = progress
        task_dict["progress_message"] = message
        await client.set(key, json.dumps(task_dict, default=str))
        return True

    async def update_progress_batch(
            self,
            updates: Dict[str, Tuple[float, str]],
    ) -> int:
        """批量更新进度：一次 pipeline 读 + 一次 pipeline 写

        写回缓存每秒会把多个任务的进度合并到这一批里，逐条 GET/SET 会产生
        O(任务数) 次往返；用 pipeline 后整批只占 2 次往返，多进程高并发时
        不会把 Redis 连接压满。
        """
        if not updates:
            return 0

        client = await self._get_client()
        task_ids = list(updates)

        # 1. 批量读取现有任务 JSON
        read_pipe = client.pipeline()
        for task_id in task_ids:
            read_pipe.get(self._task_key(task_id))
        raws = await read_pipe.execute()

        # 2. 合并进度后批量写回
        write_pipe = client.pipeline()
        updated = 0
        for task_id, raw in zip(task_ids, raws):
            if not raw:
                continue
            try:
                task_dict = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if task_dict.get("status") in _TERMINAL_STATUS_VALUES:
                continue

            progress, message = updates[task_id]
            task_dict["progress"] = progress
            task_dict["progress_message"] = message
            write_pipe.set(
                self._task_key(task_id),
                json.dumps(task_dict, default=str),
            )
            updated += 1

        if updated:
            await write_pipe.execute()
        return updated

    async def exists(self, task_id: str) -> bool:
        """检查任务是否存在"""
        client = await self._get_client()
        key = self._task_key(task_id)
        return await client.exists(key) > 0

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.close()
            self._client = None


class RedisQueueRepository(QueueRepository):
    """Redis-based priority queue repository."""

    def __init__(self, redis_url: str, max_connections: int = 10):
        self.redis_url = redis_url
        self.max_connections = max_connections
        # 同 RedisTaskRepository：使用 loop 安全客户端
        self._client: Optional[Any] = None
        self._queue_key = _QUEUE_PRIORITY_KEY
        self._delayed_key = _QUEUE_DELAYED_KEY

        self._pop_script: Optional[Any] = None

    async def _get_client(self) -> Any:
        """Get Redis client with connection pooling."""
        if not HAS_REDIS:
            raise RuntimeError("redis not installed. Run: pip install neotask[redis]")
        if self._client is None:
            self._client = redis_compat.create_client(
                self.redis_url,
                max_connections=self.max_connections,
                decode_responses=True
            )
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

    async def push_batch(self, items: List[Tuple[str, int]]) -> int:
        """批量入队：一个 pipeline 完成全部 ZADD"""
        if not items:
            return 0

        client = await self._get_client()
        pipe = client.pipeline()
        for task_id, priority in items:
            pipe.zadd(self._queue_key, {task_id: priority})
        await pipe.execute()
        return len(items)

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
            # queue:delayed 只存 task_id 到到期时间的映射 不带优先级字段
            # 迁移回优先级队列时无法还原 统一按 NORMAL 入队
            for task_id in delayed_tasks:
                await client.zadd(self._queue_key, {task_id: TaskPriority.NORMAL.value})

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
        """获取队列大小（两个 ZCARD 合并为一个 pipeline 往返）"""
        client = await self._get_client()
        pipe = client.pipeline(transaction=False)
        pipe.zcard(self._queue_key)
        pipe.zcard(self._delayed_key)
        priority_size, delayed_size = await pipe.execute()
        return int(priority_size) + int(delayed_size)

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
