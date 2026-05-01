"""
@FileName: sqlite.py
@Description: SQLite storage implementation.
@Author: HiPeng
@Time: 2026/3/27 23:55
"""

import json
from typing import List, Optional

import aiosqlite

from neotask.models.task import Task, TaskStatus, TaskPriority
from neotask.storage.base import TaskRepository, QueueRepository


class SQLiteTaskRepository(TaskRepository):
    """SQLite-based task repository."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or "data/tasks.db"
        self._conn = None

    async def _ensure_init(self) -> None:
        """Ensure database is initialized."""
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path)
            await self._init_db()

    async def _init_db(self) -> None:
        """Initialize database tables asynchronously."""
        await self._conn.execute("""
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
                error TEXT
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_status 
            ON tasks(status)
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_priority 
            ON tasks(priority)
        """)
        await self._conn.commit()

    async def save(self, task: Task) -> None:
        await self._ensure_init()
        await self._conn.execute("""
            INSERT OR REPLACE INTO tasks 
            (task_id, data, status, priority, node_id, retry_count, ttl,
             created_at, started_at, completed_at, result, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
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
            json.dumps(task.result) if task.result else None,
            task.error
        ))
        await self._conn.commit()

    async def get(self, task_id: str) -> Optional[Task]:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?",
            (task_id,)
        )
        row = await cursor.fetchone()

        if not row:
            return None

        columns = [desc[0] for desc in cursor.description]
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
        })

    async def delete(self, task_id: str) -> None:
        await self._ensure_init()
        await self._conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
        await self._conn.commit()

    async def list_by_status(self, status: TaskStatus, limit: int = 100, offset: int = 0) -> List[Task]:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT * FROM tasks WHERE status = ? LIMIT ? OFFSET ?",
            (status.value, limit, offset)
        )
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

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
            }))

        return tasks

    async def update_status(self, task_id: str, status: TaskStatus, **kwargs) -> bool:
        await self._ensure_init()
        if not await self.exists(task_id):
            return False

        await self._conn.execute(
            "UPDATE tasks SET status = ? WHERE task_id = ?",
            (status.value, task_id)
        )
        await self._conn.commit()
        return True

    async def exists(self, task_id: str) -> bool:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT 1 FROM tasks WHERE task_id = ? LIMIT 1",
            (task_id,)
        )
        row = await cursor.fetchone()
        return row is not None

    async def close(self) -> None:
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def list(self) -> List[Task]:
        """List all tasks."""
        await self._ensure_init()
        cursor = await self._conn.execute("SELECT * FROM tasks")
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

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
            }))

        return tasks


class SQLiteQueueRepository(QueueRepository):
    """SQLite-based priority queue repository."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or "tasks.db"
        self._conn = None

    async def _ensure_init(self) -> None:
        """Ensure database is initialized."""
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path)
            await self._init_db()

    async def _init_db(self) -> None:
        """Initialize database tables."""
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                task_id TEXT PRIMARY KEY,
                priority INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        await self._conn.commit()

    async def push(self, task_id: str, priority: int) -> None:
        await self._ensure_init()
        from datetime import datetime
        await self._conn.execute(
            "INSERT OR REPLACE INTO queue (task_id, priority, created_at) VALUES (?, ?, ?)",
            (task_id, priority, datetime.now().isoformat())
        )
        await self._conn.commit()

    async def pop(self, count: int = 1) -> List[str]:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT task_id FROM queue ORDER BY priority ASC, created_at ASC LIMIT ?",
            (count,)
        )
        rows = await cursor.fetchall()
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
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def peek(self, count: int = 1) -> List[str]:
        await self._ensure_init()
        cursor = await self._conn.execute(
            "SELECT task_id FROM queue ORDER BY priority ASC, created_at ASC LIMIT ?",
            (count,)
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def clear(self) -> None:
        await self._ensure_init()
        await self._conn.execute("DELETE FROM queue")
        await self._conn.commit()

    async def close(self) -> None:
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None
