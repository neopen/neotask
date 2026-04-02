"""
@FileName: queue.py
@Description: 队列管理器。队列管理、优先级、预取。Priority queue management.
@Author: HiPeng
@Time: 2026/4/1 18:15
"""

import asyncio
from typing import List

from neotask.models.task import TaskPriority
from neotask.storage.base import QueueRepository


class PriorityQueue:
    """Priority queue wrapper with async operations."""

    def __init__(self, repository: QueueRepository, max_size: int = 1000):
        self._repository = repository
        self._max_size = max_size
        self._lock = asyncio.Lock()

    async def push(self, task_id: str, priority: TaskPriority) -> bool:
        """Push task to queue."""
        if await self._repository.size() >= self._max_size:
            return False

        await self._repository.push(task_id, priority.value)
        return True

    async def pop(self, count: int = 1) -> List[str]:
        """Pop highest priority tasks."""
        return await self._repository.pop(count)

    async def remove(self, task_id: str) -> bool:
        """Remove task from queue."""
        return await self._repository.remove(task_id)

    async def size(self) -> int:
        """Get queue size."""
        return await self._repository.size()

    async def is_empty(self) -> bool:
        """Check if queue is empty."""
        return await self.size() == 0
