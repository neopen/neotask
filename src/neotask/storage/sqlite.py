"""
@FileName: sqlite.py
@Description: SQLite storage implementation.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/3/27 23:55
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from neotask.models.task import Task, TaskPriority, TaskStatus
from neotask.storage.base import TaskRepository, QueueRepository
from neotask.utils import sqlite_compat

#: DELETE ... RETURNING 自 SQLite 3.35 起支持；多进程下用它把「取任务 + 出队」
#: 合并成一条原子语句，避免两个进程 SELECT 到同一批任务后重复执行。
_HAS_DELETE_RETURNING = sqlite3.sqlite_version_info >= (3, 35, 0)

#: 批量写入的分块大小（避免 IN 子句参数个数超出 SQLite 上限）
_BATCH_CHUNK = 500

#: tasks 表允许按字段更新的列
_TASK_COLUMNS = {
    "data", "status", "priority", "node_id", "retry_count", "ttl",
    "created_at", "started_at", "completed_at", "result", "error",
    "progress", "progress_message",
}


def _serialize_field(name: str, value: Any) -> Any:
    """把任务字段转换成 SQLite 可绑定的值"""
    if name == "status":
        return value.value if isinstance(value, TaskStatus) else value
    if name == "priority":
        return value.value if isinstance(value, TaskPriority) else value
    if name in ("created_at", "started_at", "completed_at"):
        if isinstance(value, datetime):
            return value.isoformat()
        return value
    if name == "data":
        return json.dumps(value) if value is not None else "{}"
    if name == "result":
        return json.dumps(value) if value is not None else None
    return value



class SQLiteTaskRepository(TaskRepository):
    """SQLite-based task repository."""

    #: 任务数据落盘，重启后可恢复
    is_persistent = True

    def __init__(
            self,
            db_path: Optional[str] = None,
            *,
            pragmas: Optional[Iterable[str]] = None,
    ):
        self.db_path = Path(db_path or "/data/tasks.db")
        self._pragmas = pragmas

        # 自动创建所有缺失的父目录（如 data 文件夹）
        # parents=True 表示如果父目录不存在一并创建
        # exist_ok=True 表示如果文件夹已存在则不报错
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = None
        self._initialized = False

    async def _ensure_init(self) -> None:
        """Ensure database is initialized.

        多个协程（如池启动阶段的队列恢复与首个 submit）可能并发进入；不能仅凭
        ``_conn`` 非空就跳过建表，否则后到者会在建表完成前执行查询。
        """
        if self._initialized:
            return
        if self._conn is None:
            # loop 安全连接：所有 SQL 操作统一在专用事件循环执行；
            # 同路径的任务/队列仓储共用一条连接，避免互抢写锁
            self._conn = sqlite_compat.acquire_shared_connection(
                self.db_path,
                pragmas=self._pragmas,
            )
        conn = self._conn
        await self._init_db(conn)
        if self._conn is conn:
            self._initialized = True


    async def _init_db(self, conn) -> None:
        """Initialize database tables asynchronously."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL,
                node_id TEXT NOT NULL,
                retry_count INTEGER NOT NULL,
                ttl INTEGER DEFAULT 3600,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                result TEXT,
                error TEXT,
                progress REAL DEFAULT 0.0,
                progress_message TEXT DEFAULT ''
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_status 
            ON tasks(status)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_priority 
            ON tasks(priority)
        """)
        # task_id 为主键，无需额外索引；created_at 用于按时间范围统计/清理
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_created_at
            ON tasks(created_at)
        """)
        await conn.commit()

    @staticmethod
    def _task_values(task: Task) -> tuple:
        """任务行的绑定参数（与 INSERT 列顺序一致）"""
        return (
            task.task_id,
            json.dumps(task.data),
            task.status.value,
            task.priority.value,
            task.node_id,
            task.retry_count,
            getattr(task, 'ttl', 3600),
            task.created_at.isoformat(),
            task.started_at.isoformat() if task.started_at else None,
            task.completed_at.isoformat() if task.completed_at else None,
            json.dumps(task.result) if task.result is not None else None,
            task.error,
            getattr(task, 'progress', 0.0),
            getattr(task, 'progress_message', '')
        )

    async def save(self, task: Task) -> None:
        await self._ensure_init()
        await self._conn.execute("""
            INSERT OR REPLACE INTO tasks 
            (task_id, data, status, priority, node_id, retry_count, ttl,
             created_at, started_at, completed_at, result, error, progress, progress_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, self._task_values(task))
        await self._conn.commit()

    async def save_if_absent(self, task: Task) -> bool:
        """插入新任务；主键已存在则跳过（单条语句完成查重 + 写入）"""
        await self._ensure_init()
        cursor = await self._conn.execute("""
            INSERT OR IGNORE INTO tasks 
            (task_id, data, status, priority, node_id, retry_count, ttl,
             created_at, started_at, completed_at, result, error, progress, progress_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, self._task_values(task))
        inserted = cursor.rowcount > 0
        await cursor.close()
        await self._conn.commit()
        return inserted

    async def save_batch(self, tasks: List[Task]) -> List[str]:
        """批量保存任务：一次查重 + 一次 ``executemany``，整批单事务提交。

        返回实际新增的 task_id（已存在的会被跳过，不覆盖已有任务）。
        """
        if not tasks:
            return []

        await self._ensure_init()

        # 先按块查重，避免 IN 子句参数个数超过 SQLite 上限
        existing: set = set()
        task_ids = [task.task_id for task in tasks]
        for start in range(0, len(task_ids), _BATCH_CHUNK):
            chunk = task_ids[start:start + _BATCH_CHUNK]
            placeholders = ",".join("?" * len(chunk))
            cursor = await self._conn.execute(
                f"SELECT task_id FROM tasks WHERE task_id IN ({placeholders})",
                chunk
            )
            try:
                rows = await cursor.fetchall()
            finally:
                await cursor.close()
            existing.update(row[0] for row in rows)

        new_tasks = [task for task in tasks if task.task_id not in existing]
        if not new_tasks:
            return []

        params = [
            (
                task.task_id,
                json.dumps(task.data),
                task.status.value,
                task.priority.value,
                task.node_id,
                task.retry_count,
                getattr(task, 'ttl', 3600),
                task.created_at.isoformat(),
                task.started_at.isoformat() if task.started_at else None,
                task.completed_at.isoformat() if task.completed_at else None,
                json.dumps(task.result) if task.result is not None else None,
                task.error,
                getattr(task, 'progress', 0.0),
                getattr(task, 'progress_message', ''),
            )
            for task in new_tasks
        ]
        # 单事务写入：任意一条失败（含磁盘/约束错误）整批回滚
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            await self._conn.executemany("""
                INSERT OR IGNORE INTO tasks
                (task_id, data, status, priority, node_id, retry_count, ttl,
                 created_at, started_at, completed_at, result, error, progress, progress_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, params)
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise
        return [task.task_id for task in new_tasks]

    async def save_fields(self, task: Task, fields: Iterable[str]) -> bool:
        """只更新指定字段，避免高频状态变更重写整行（含 data/result JSON）"""
        await self._ensure_init()

        columns = [name for name in fields if name in _TASK_COLUMNS]
        if not columns:
            return False

        params = [
            _serialize_field(name, getattr(task, name, None))
            for name in columns
        ]
        params.append(task.task_id)

        cursor = await self._conn.execute(
            f"UPDATE tasks SET {', '.join(f'{name} = ?' for name in columns)} "
            "WHERE task_id = ?",
            params
        )
        await self._conn.commit()
        return cursor.rowcount > 0

    async def update_progress(
            self,
            task_id: str,
            progress: float,
            message: str = ""
    ) -> bool:
        """单条 UPDATE 更新进度，仅对未结束任务生效"""
        await self._ensure_init()
        cursor = await self._conn.execute(
            "UPDATE tasks SET progress = ?, progress_message = ? "
            "WHERE task_id = ? AND status NOT IN (?, ?, ?)",
            (
                progress,
                message,
                task_id,
                TaskStatus.SUCCESS.value,
                TaskStatus.FAILED.value,
                TaskStatus.CANCELLED.value,
            )
        )
        await self._conn.commit()
        return cursor.rowcount > 0

    async def update_progress_batch(
            self,
            updates: Dict[str, Tuple[float, str]],
    ) -> int:
        """单事务批量更新进度，减少提交次数"""
        if not updates:
            return 0
        await self._ensure_init()

        params = [
            (
                progress,
                message,
                task_id,
                TaskStatus.SUCCESS.value,
                TaskStatus.FAILED.value,
                TaskStatus.CANCELLED.value,
            )
            for task_id, (progress, message) in updates.items()
        ]
        cursor = await self._conn.executemany(
            "UPDATE tasks SET progress = ?, progress_message = ? "
            "WHERE task_id = ? AND status NOT IN (?, ?, ?)",
            params
        )
        await self._conn.commit()
        return len(params) if cursor.rowcount < 0 else cursor.rowcount

    async def get(self, task_id: str) -> Optional[Task]:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?",
            (task_id,)
        )
        try:
            row = await cursor.fetchone()
            columns = [desc[0] for desc in cursor.description]
        finally:
            # 必须关闭游标：未耗尽的 SELECT 会持有读事务，随后的写入会被
            # SQLite 当作「读转写升级」，在 WAL 下直接返回 database is locked
            # 且不经过 busy_timeout 等待。
            await cursor.close()

        if not row:
            return None

        data = dict(zip(columns, row))

        return Task.from_dict({
            "task_id": data["task_id"],
            "data": data["data"],
            "status": data["status"],
            "priority": data["priority"],
            "node_id": data["node_id"],
            "retry_count": data["retry_count"],
            "ttl": data.get("ttl", 3600),
            "created_at": data["created_at"],
            "started_at": data["started_at"],
            "completed_at": data["completed_at"],
            "result": data["result"],
            "error": data["error"],
            "progress": data.get("progress", 0.0),
            "progress_message": data.get("progress_message", "")
        })

    async def delete(self, task_id: str) -> None:
        await self._ensure_init()
        await self._conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
        await self._conn.commit()

    async def delete_batch(self, task_ids: List[str]) -> int:
        """批量删除任务"""
        if not task_ids:
            return 0

        await self._ensure_init()
        placeholders = ",".join("?" * len(task_ids))
        cursor = await self._conn.execute(
            f"DELETE FROM tasks WHERE task_id IN ({placeholders})",
            task_ids
        )
        await self._conn.commit()
        return cursor.rowcount

    async def list_by_status(self, status: TaskStatus, limit: int = 100, offset: int = 0) -> List[Task]:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT * FROM tasks WHERE status = ? LIMIT ? OFFSET ?",
            (status.value, limit, offset)
        )
        try:
            rows = await cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        finally:
            await cursor.close()

        tasks = []
        for row in rows:
            data = dict(zip(columns, row))
            tasks.append(Task.from_dict({
                "task_id": data["task_id"],
                "data": data["data"],
                "status": data["status"],
                "priority": data["priority"],
                "node_id": data["node_id"],
                "retry_count": data["retry_count"],
                "ttl": data.get("ttl", 3600),
                "created_at": data["created_at"],
                "started_at": data["started_at"],
                "completed_at": data["completed_at"],
                "result": data["result"],
                "error": data["error"],
                "progress": data.get("progress", 0.0),
                "progress_message": data.get("progress_message", "")
            }))

        return tasks

    async def update_status(self, task_id: str, status: TaskStatus, **kwargs) -> bool:
        await self._ensure_init()

        # 构建单条 UPDATE，不做额外 EXISTS 查询
        updates = ["status = ?"]
        params: List[Any] = [status.value]

        for key, value in kwargs.items():
            if key in _TASK_COLUMNS:
                updates.append(f"{key} = ?")
                params.append(_serialize_field(key, value))

        params.append(task_id)

        cursor = await self._conn.execute(
            f"UPDATE tasks SET {', '.join(updates)} WHERE task_id = ?",
            params
        )
        await self._conn.commit()
        return cursor.rowcount > 0

    async def update_status_batch(
            self,
            updates: List[Tuple[str, TaskStatus, dict]]
    ) -> int:
        """批量更新任务状态

        Args:
            updates: 更新列表，每个元素为 (task_id, status, kwargs)

        Returns:
            成功更新的数量
        """
        if not updates:
            return 0

        await self._ensure_init()
        success_count = 0

        for task_id, status, kwargs in updates:
            try:
                result = await self.update_status(task_id, status, **kwargs)
                if result:
                    success_count += 1
            except Exception:
                continue

        return success_count

    async def exists(self, task_id: str) -> bool:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT 1 FROM tasks WHERE task_id = ? LIMIT 1",
            (task_id,)
        )
        try:
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        return row is not None

    async def close(self) -> None:
        """Close database connection."""
        conn = self._conn
        self._conn = None
        self._initialized = False
        if conn is not None:
            await sqlite_compat.release_shared_connection(conn)

    async def list(self) -> List[Task]:
        """List all tasks."""
        await self._ensure_init()
        cursor = await self._conn.execute("SELECT * FROM tasks")
        try:
            rows = await cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        finally:
            await cursor.close()

        tasks = []
        for row in rows:
            data = dict(zip(columns, row))
            tasks.append(Task.from_dict({
                "task_id": data["task_id"],
                "data": data["data"],
                "status": data["status"],
                "priority": data["priority"],
                "node_id": data["node_id"],
                "retry_count": data["retry_count"],
                "ttl": data.get("ttl", 3600),
                "created_at": data["created_at"],
                "started_at": data["started_at"],
                "completed_at": data["completed_at"],
                "result": data["result"],
                "error": data["error"],
                "progress": data.get("progress", 0.0),
                "progress_message": data.get("progress_message", "")
            }))

        return tasks


class SQLiteQueueRepository(QueueRepository):
    """SQLite-based priority queue repository."""

    #: 队列落盘，重启后未执行的任务仍在队列中
    is_persistent = True

    def __init__(
            self,
            db_path: Optional[str] = None,
            *,
            pragmas: Optional[Iterable[str]] = None,
    ):
        self.db_path = Path(db_path or "tasks.db")
        self._pragmas = pragmas
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = None
        self._initialized = False

    async def _ensure_init(self) -> None:
        """Ensure database is initialized."""
        if self._initialized:
            return
        if self._conn is None:
            # 与任务仓储共用同一条连接，避免两连接互抢写锁
            self._conn = sqlite_compat.acquire_shared_connection(
                self.db_path,
                pragmas=self._pragmas,
            )
        conn = self._conn
        await self._init_db(conn)
        if self._conn is conn:
            self._initialized = True


    async def _init_db(self, conn) -> None:
        """Initialize database tables."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                task_id TEXT PRIMARY KEY,
                priority INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        await conn.commit()

    async def push(self, task_id: str, priority: int) -> None:
        await self._ensure_init()
        from datetime import datetime
        await self._conn.execute(
            "INSERT OR REPLACE INTO queue (task_id, priority, created_at) VALUES (?, ?, ?)",
            (task_id, priority, datetime.now().isoformat())
        )
        await self._conn.commit()

    async def push_batch(self, items: List[Tuple[str, int]]) -> int:
        """批量入队：一次 ``executemany`` + 单事务提交"""
        if not items:
            return 0

        await self._ensure_init()
        from datetime import datetime
        now = datetime.now().isoformat()
        params = [(task_id, priority, now) for task_id, priority in items]

        # 单事务写入：任意一条失败整批回滚，避免队列里出现半批任务
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            await self._conn.executemany(
                "INSERT OR REPLACE INTO queue (task_id, priority, created_at) VALUES (?, ?, ?)",
                params
            )
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise
        return len(items)

    async def pop(self, count: int = 1) -> List[str]:
        await self._ensure_init()

        if _HAS_DELETE_RETURNING:
            # 单条语句完成「按优先级取任务 + 删除」，跨进程原子，不会重复认领
            cursor = await self._conn.execute(
                "DELETE FROM queue WHERE task_id IN ("
                "SELECT task_id FROM queue "
                "ORDER BY priority ASC, created_at ASC LIMIT ?"
                ") RETURNING task_id",
                (count,)
            )
            try:
                rows = await cursor.fetchall()
            finally:
                await cursor.close()
            await self._conn.commit()
            return [row[0] for row in rows]

        # 旧版 SQLite：无 RETURNING，SELECT + DELETE 非原子（仅单进程安全）
        cursor = await self._conn.execute(
            "SELECT task_id FROM queue ORDER BY priority ASC, created_at ASC LIMIT ?",
            (count,)
        )
        try:
            rows = await cursor.fetchall()
        finally:
            await cursor.close()
        task_ids = [row[0] for row in rows]

        if task_ids:
            placeholders = ",".join("?" * len(task_ids))
            await self._conn.execute(
                f"DELETE FROM queue WHERE task_id IN ({placeholders})",
                task_ids
            )
            await self._conn.commit()

        return task_ids

    async def remove(self, task_id: str) -> bool:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "DELETE FROM queue WHERE task_id = ?",
            (task_id,)
        )
        await self._conn.commit()
        return cursor.rowcount > 0

    async def size(self) -> int:
        await self._ensure_init()
        cursor = await self._conn.execute("SELECT COUNT(*) FROM queue")
        try:
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        return row[0] if row else 0

    async def peek(self, count: int = 1) -> List[str]:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT task_id FROM queue ORDER BY priority ASC, created_at ASC LIMIT ?",
            (count,)
        )
        try:
            rows = await cursor.fetchall()
        finally:
            await cursor.close()
        return [row[0] for row in rows]

    async def clear(self) -> None:
        await self._ensure_init()
        await self._conn.execute("DELETE FROM queue")
        await self._conn.commit()

    async def close(self) -> None:
        """Close database connection."""
        conn = self._conn
        self._conn = None
        self._initialized = False
        if conn is not None:
            await sqlite_compat.release_shared_connection(conn)
