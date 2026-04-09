"""
@FileName: sqlite.py
@Description: SQLite storage implementation.
@Author: HiPeng
@Time: 2026/3/27 23:55
"""

import json
import sqlite3
from typing import List, Optional
import aiosqlite
from neotask.models.task import Task, TaskStatus
from neotask.storage.base import TaskRepository, QueueRepository


class SQLiteTaskRepository(TaskRepository):
    """SQLite-based task repository."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or "tasks.db"
        self._initialized = False
        self._init_db()

    async def _ensure_init(self) -> None:
        """Ensure database is initialized."""
        if not self._initialized:
            await self._init_db()
            self._initialized = True

    async def _init_db(self) -> None:
        """Initialize database tables asynchronously."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    node_id TEXT NOT NULL,
                    retry_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    result TEXT,
                    error TEXT
                )
            """)
            # Create index for faster queries
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_status 
                ON tasks(status)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_priority 
                ON tasks(priority)
            """)
            await db.commit()

    async def save(self, task: Task) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO tasks 
                (task_id, data, status, priority, node_id, retry_count, 
                 created_at, started_at, completed_at, result, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task.task_id,
                json.dumps(task.data),
                task.status.value,
                task.priority.value,
                task.node_id,
                task.retry_count,
                task.created_at.isoformat(),
                task.started_at.isoformat() if task.started_at else None,
                task.completed_at.isoformat() if task.completed_at else None,
                json.dumps(task.result) if task.result else None,
                task.error
            ))
            await db.commit()

    async def get(self, task_id: str) -> Optional[Task]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
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
                "created_at": data["created_at"],
                "started_at": data["started_at"],
                "completed_at": data["completed_at"],
                "result": data["result"],
                "error": data["error"],
            })

    async def delete(self, task_id: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
            await db.commit()

    async def list_by_status(self, status: TaskStatus, limit: int = 100, offset: int = 0) -> List[Task]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT * FROM tasks WHERE status = ? LIMIT ?",
                (status.value, limit)
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
                    "created_at": data["created_at"],
                    "started_at": data["started_at"],
                    "completed_at": data["completed_at"],
                    "result": data["result"],
                    "error": data["error"],
                }))

            return tasks

    async def update_status(self, task_id: str, status: TaskStatus) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE tasks SET status = ? WHERE task_id = ?",
                (status.value, task_id)
            )
            await db.commit()

    async def exists(self, task_id: str) -> bool:
        """Check if task exists."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM tasks WHERE task_id = ? LIMIT 1",
                (task_id,)
            )
            row = await cursor.fetchone()
            return row is not None


class SQLiteQueueRepository(QueueRepository):
    """SQLite-based priority queue repository."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or "tasks.db"
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                task_id TEXT PRIMARY KEY,
                priority INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

        conn.commit()
        conn.close()

    async def push(self, task_id: str, priority: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO queue (task_id, priority, created_at) VALUES (?, ?, datetime('now'))",
                (task_id, priority)
            )
            await db.commit()

    async def pop(self, count: int = 1) -> List[str]:
        async with aiosqlite.connect(self.db_path) as db:
            # Get highest priority tasks (lowest priority number)
            cursor = await db.execute(
                "SELECT task_id FROM queue ORDER BY priority ASC, created_at ASC LIMIT ?",
                (count,)
            )
            rows = await cursor.fetchall()
            task_ids = [row[0] for row in rows]

            # Delete popped tasks
            if task_ids:
                placeholders = ",".join("?" * len(task_ids))
                await db.execute(
                    f"DELETE FROM queue WHERE task_id IN ({placeholders})",
                    task_ids
                )
                await db.commit()

            return task_ids

    async def remove(self, task_id: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "DELETE FROM queue WHERE task_id = ?",
                (task_id,)
            )
            await db.commit()
            return cursor.rowcount > 0

    async def size(self) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM queue")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def peek(self, count: int = 1) -> List[str]:
        """Peek at top tasks without removing."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT task_id FROM queue ORDER BY priority ASC, created_at ASC LIMIT ?",
                (count,)
            )
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def clear(self) -> None:
        """Clear all tasks from queue."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM queue")
            await db.commit()