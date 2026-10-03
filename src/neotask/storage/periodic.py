"""
@FileName: periodic.py
@Description: 周期任务持久化存储 - 为 PeriodicTaskManager 提供跨重启的任务定义/执行记录存储。
             Periodic task persistence store (task definitions + execution records)
             backing memory / sqlite / redis backends via the Repository pattern.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/3
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from neotask.common.logger import debug

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


class PeriodicStore(ABC):
    """周期任务存储抽象接口

    以「可序列化的 dict 载荷」为存取单位，把持久化细节（表结构 / key 布局）
    与 ``PeriodicTaskManager`` 解耦。任务定义与执行记录分属两个逻辑集合。
    """

    @abstractmethod
    async def save_task(self, task_id: str, payload: Dict[str, Any]) -> None:
        """保存（新增或覆盖）单个周期任务定义"""
        raise NotImplementedError

    @abstractmethod
    async def load_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """按 ID 读取单个周期任务定义，不存在返回 None"""
        raise NotImplementedError

    @abstractmethod
    async def delete_task(self, task_id: str) -> None:
        """删除周期任务定义及其执行记录"""
        raise NotImplementedError

    @abstractmethod
    async def load_all_tasks(self) -> List[Dict[str, Any]]:
        """读取全部周期任务定义（供启动时恢复）"""
        raise NotImplementedError

    @abstractmethod
    async def save_execution(self, execution_id: str, payload: Dict[str, Any]) -> None:
        """保存单条执行记录"""
        raise NotImplementedError

    @abstractmethod
    async def load_executions(self, task_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """读取某周期任务最近的执行记录（按时间倒序，最多 limit 条）"""
        raise NotImplementedError

    async def close(self) -> None:
        """释放底层连接（默认无操作）"""
        return None


class MemoryPeriodicStore(PeriodicStore):
    """内存周期任务存储

    仅在同一进程内有效，用于 ``enable_persistence=True`` 且
    ``storage_type='memory'`` 时的行为一致性；无法跨重启（内存本就不持久）。
    """

    def __init__(self) -> None:
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._executions: Dict[str, List[Dict[str, Any]]] = {}

    async def save_task(self, task_id: str, payload: Dict[str, Any]) -> None:
        self._tasks[task_id] = json.loads(json.dumps(payload))

    async def load_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        data = self._tasks.get(task_id)
        return json.loads(json.dumps(data)) if data else None

    async def delete_task(self, task_id: str) -> None:
        self._tasks.pop(task_id, None)
        self._executions.pop(task_id, None)

    async def load_all_tasks(self) -> List[Dict[str, Any]]:
        return [json.loads(json.dumps(v)) for v in self._tasks.values()]

    async def save_execution(self, execution_id: str, payload: Dict[str, Any]) -> None:
        task_id = payload.get("task_id", "")
        self._executions.setdefault(task_id, []).append(json.loads(json.dumps(payload)))

    async def load_executions(self, task_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        records = self._executions.get(task_id, [])
        # 倒序返回最近的记录
        recent = records[-limit:][::-1] if limit > 0 else records[::-1]
        return [json.loads(json.dumps(r)) for r in recent]


class SQLitePeriodicStore(PeriodicStore):
    """基于 aiosqlite 的周期任务存储

    与 :class:`SQLiteTaskRepository` 一致：按需惰性建连，可与任务表共用同一
    数据库文件（各持独立连接）。
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = db_path or "neotask.db"
        self._conn = None

    async def _ensure_init(self) -> None:
        if self._conn is None:
            import aiosqlite

            self._conn = await aiosqlite.connect(self.db_path)
            await self._init_db()

    async def _init_db(self) -> None:
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS periodic_tasks (
                task_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT
            )
        """)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS periodic_executions (
                execution_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_periodic_exec_task
            ON periodic_executions(task_id)
        """)
        await self._conn.commit()

    async def save_task(self, task_id: str, payload: Dict[str, Any]) -> None:
        await self._ensure_init()
        await self._conn.execute(
            "INSERT OR REPLACE INTO periodic_tasks (task_id, payload, updated_at) VALUES (?, ?, ?)",
            (task_id, json.dumps(payload), payload.get("updated_at")),
        )
        await self._conn.commit()

    async def load_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT payload FROM periodic_tasks WHERE task_id = ?",
            (task_id,),
        )
        row = await cursor.fetchone()
        return json.loads(row[0]) if row else None

    async def delete_task(self, task_id: str) -> None:
        await self._ensure_init()
        await self._conn.execute("DELETE FROM periodic_tasks WHERE task_id = ?", (task_id,))
        await self._conn.execute("DELETE FROM periodic_executions WHERE task_id = ?", (task_id,))
        await self._conn.commit()

    async def load_all_tasks(self) -> List[Dict[str, Any]]:
        await self._ensure_init()
        cursor = await self._conn.execute("SELECT payload FROM periodic_tasks")
        rows = await cursor.fetchall()
        return [json.loads(r[0]) for r in rows]

    async def save_execution(self, execution_id: str, payload: Dict[str, Any]) -> None:
        await self._ensure_init()
        await self._conn.execute(
            "INSERT OR REPLACE INTO periodic_executions (execution_id, task_id, created_at, payload) "
            "VALUES (?, ?, ?, ?)",
            (
                execution_id,
                payload.get("task_id", ""),
                payload.get("scheduled_time") or payload.get("start_time") or "",
                json.dumps(payload),
            ),
        )
        await self._conn.commit()

    async def load_executions(self, task_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT payload FROM periodic_executions WHERE task_id = ? "
            "ORDER BY created_at DESC LIMIT ?",
            (task_id, limit),
        )
        rows = await cursor.fetchall()
        return [json.loads(r[0]) for r in rows]

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None


class RedisPeriodicStore(PeriodicStore):
    """基于 Redis 的周期任务存储

    key 布局：
    - ``periodic_task:<id>``  -> 任务定义 JSON
    - ``periodic_tasks``      -> 任务 ID 集合（供 load_all）
    - ``periodic_exec:<id>``  -> 该任务执行记录列表（LPUSH，天然时间倒序）
    """

    def __init__(self, redis_url: str, key_prefix: str = "neotask:", max_connections: int = 10) -> None:
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.max_connections = max_connections
        self._pool: Optional["ConnectionPool"] = None
        self._client: Optional["redis.Redis"] = None

    async def _get_client(self) -> "redis.Redis":
        if not HAS_REDIS:
            raise RuntimeError("redis not installed. Run: pip install neotask[redis]")
        if self._client is None:
            from neotask.utils import redis_compat

            self._pool = redis_compat.from_url(
                self.redis_url,
                max_connections=self.max_connections,
                decode_responses=True,
            )
            self._client = redis.Redis(connection_pool=self._pool)
        return self._client

    def _task_key(self, task_id: str) -> str:
        return f"{self.key_prefix}periodic_task:{task_id}"

    def _exec_key(self, task_id: str) -> str:
        return f"{self.key_prefix}periodic_exec:{task_id}"

    @property
    def _index_key(self) -> str:
        return f"{self.key_prefix}periodic_tasks"

    async def save_task(self, task_id: str, payload: Dict[str, Any]) -> None:
        client = await self._get_client()
        await client.set(self._task_key(task_id), json.dumps(payload))
        await client.sadd(self._index_key, task_id)

    async def load_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        client = await self._get_client()
        raw = await client.get(self._task_key(task_id))
        return json.loads(raw) if raw else None

    async def delete_task(self, task_id: str) -> None:
        client = await self._get_client()
        await client.delete(self._task_key(task_id))
        await client.delete(self._exec_key(task_id))
        await client.srem(self._index_key, task_id)

    async def load_all_tasks(self) -> List[Dict[str, Any]]:
        client = await self._get_client()
        ids = await client.smembers(self._index_key)
        tasks: List[Dict[str, Any]] = []
        for task_id in ids:
            raw = await client.get(self._task_key(task_id))
            if raw:
                tasks.append(json.loads(raw))
            else:
                # 索引残留（key 已过期/被外部删除），顺手清理
                await client.srem(self._index_key, task_id)
        return tasks

    async def save_execution(self, execution_id: str, payload: Dict[str, Any]) -> None:
        client = await self._get_client()
        task_id = payload.get("task_id", "")
        # 头插保留最近记录；裁剪到 1000 条防止无限增长
        await client.lpush(self._exec_key(task_id), json.dumps(payload))
        await client.ltrim(self._exec_key(task_id), 0, 999)

    async def load_executions(self, task_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        client = await self._get_client()
        raws = await client.lrange(self._exec_key(task_id), 0, (limit - 1) if limit > 0 else -1)
        return [json.loads(r) for r in raws]

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
        if self._pool is not None:
            await self._pool.disconnect()
            self._pool = None


def create_periodic_store(
        storage_type: str,
        sqlite_path: str = "neotask.db",
        redis_url: Optional[str] = None,
) -> PeriodicStore:
    """按存储类型创建周期任务存储

    Args:
        storage_type: "memory" | "sqlite" | "redis"
        sqlite_path: SQLite 数据库路径
        redis_url: Redis 连接 URL

    Returns:
        PeriodicStore 实例

    Raises:
        ValueError: 未知存储类型或缺少必要参数
    """
    if storage_type == "memory":
        debug("Periodic persistence using in-memory store (not durable across restart)")
        return MemoryPeriodicStore()
    if storage_type == "sqlite":
        return SQLitePeriodicStore(sqlite_path)
    if storage_type == "redis":
        if not redis_url:
            raise ValueError("Redis URL is required for redis periodic store")
        return RedisPeriodicStore(redis_url)
    raise ValueError(f"Unknown storage type for periodic store: {storage_type}")
