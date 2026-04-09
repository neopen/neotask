"""
@FileName: memory.py
@Description: 内存存储实现。In-memory storage implementation.
@Author: HiPeng
@Time: 2026/3/27 23:54
"""
import asyncio
from typing import Dict, List, Optional
from neotask.models.task import Task, TaskStatus
from neotask.storage.base import TaskRepository, QueueRepository
import heapq


class MemoryTaskRepository(TaskRepository):
    """In-memory task repository."""

    def __init__(self):
        self._tasks: Dict[str, Task] = {}
        self._lock = asyncio.Lock()

    async def save(self, task: Task) -> None:
        async with self._lock:
            self._tasks[task.task_id] = task

    async def get(self, task_id: str) -> Optional[Task]:
        async with self._lock:
            return self._tasks.get(task_id)

    async def delete(self, task_id: str) -> None:
        async with self._lock:
            self._tasks.pop(task_id, None)

    async def list_by_status(self, status: TaskStatus, limit: int = 100, offset: int = 0) -> List[Task]:
        async with self._lock:
            tasks = [t for t in self._tasks.values() if t.status == status]
            return tasks[:limit]

    async def update_status(self, task_id: str, status: TaskStatus) -> None:
        async with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].status = status

    async def exists(self, task_id: str) -> bool:
        """Check if task exists."""
        async with self._lock:
            return task_id in self._tasks


class MemoryQueueRepository(QueueRepository):
    """In-memory priority queue repository."""

    def __init__(self):
        self._queue: List[tuple] = []
        self._lock = asyncio.Lock()

    async def push(self, task_id: str, priority: int) -> None:
        async with self._lock:
            heapq.heappush(self._queue, (priority, task_id))

    async def pop(self, count: int = 1) -> List[str]:
        async with self._lock:
            result = []
            for _ in range(min(count, len(self._queue))):
                _, task_id = heapq.heappop(self._queue)
                result.append(task_id)
            return result

    async def remove(self, task_id: str) -> bool:
        async with self._lock:
            original_len = len(self._queue)
            self._queue = [(p, tid) for p, tid in self._queue if tid != task_id]
            heapq.heapify(self._queue)
            return len(self._queue) < original_len

    async def size(self) -> int:
        async with self._lock:
            return len(self._queue)

    async def peek(self, count: int = 1) -> List[str]:
        """Peek at top tasks without removing."""
        async with self._lock:
            sorted_queue = sorted(self._queue)
            return [tid for _, tid in sorted_queue[:count]]

    async def clear(self) -> None:
        """Clear all tasks from queue."""
        async with self._lock:
            self._queue.clear()