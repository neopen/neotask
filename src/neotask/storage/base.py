"""
@FileName: base.py
@Description: 数据存储。Storage abstraction using Repository pattern.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/3/27 23:54
"""

from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Optional, Tuple
from neotask.models.task import Task, TaskStatus


class TaskRepository(ABC):
    """Repository interface for task storage."""

    @abstractmethod
    async def save(self, task: Task) -> None:
        """Save task to storage."""
        pass

    async def save_fields(self, task: Task, fields: Iterable[str]) -> bool:
        """只持久化指定字段。

        默认实现退化为整条记录重写，保证所有后端行为一致；SQLite 等后端可以
        覆盖为单条 ``UPDATE``，避免高频状态变更时反复重写整行。

        Args:
            task: 已更新完字段的任务对象
            fields: 需要落库的字段名

        Returns:
            是否成功写入
        """
        await self.save(task)
        return True

    @abstractmethod
    async def get(self, task_id: str) -> Optional[Task]:
        """Get task by ID."""
        pass

    @abstractmethod
    async def delete(self, task_id: str) -> None:
        """Delete task from storage."""
        pass

    @abstractmethod
    async def list_by_status(self, status: TaskStatus, limit: int = 100, offset: int = 0) -> List[Task]:
        """List tasks by status."""
        pass

    @abstractmethod
    async def update_status(self, task_id: str, status: TaskStatus) -> None:
        """Update task status."""
        pass

    async def update_progress(self, task_id: str, progress: float, message: str = "") -> bool:
        """Update task progress.

        默认实现基于 get + save，适用于所有后端；需要高频更新的实现可以覆盖为
        单条 UPDATE/SET，避免每次重写整条任务记录。

        Returns:
            True if the task exists and was updated, False otherwise.
        """
        task = await self.get(task_id)
        if not task:
            return False
        task.update_progress(progress, message)
        await self.save(task)
        return True

    async def update_progress_batch(
            self,
            updates: Dict[str, Tuple[float, str]],
    ) -> int:
        """批量落库进度。

        默认逐条调用 :meth:`update_progress`；SQLite 可覆盖为单事务批量更新，
        把同一批任务的多次进度写入合并成一次提交。

        Args:
            updates: ``{task_id: (progress, message)}``

        Returns:
            成功更新的数量
        """
        updated = 0
        for task_id, (progress, message) in updates.items():
            if await self.update_progress(task_id, progress, message):
                updated += 1
        return updated

    @abstractmethod
    async def update_status_batch(
            self,
            updates: List[tuple]  # [(task_id, status, **kwargs), ...]
    ) -> int:
        """批量更新任务状态

        Args:
            updates: 更新列表，每个元素为 (task_id, status, kwargs)

        Returns:
            成功更新的数量
        """
        pass

    @abstractmethod
    async def delete_batch(self, task_ids: List[str]) -> int:
        """批量删除任务

        Args:
            task_ids: 任务ID列表

        Returns:
            成功删除的数量
        """
        pass

    @abstractmethod
    async def exists(self, task_id: str) -> bool:
        """Check if task exists."""
        pass


class QueueRepository(ABC):
    """Repository interface for task queue."""

    @abstractmethod
    async def push(self, task_id: str, priority: int) -> None:
        """Push task to queue with priority."""
        pass

    @abstractmethod
    async def pop(self, count: int = 1) -> List[str]:
        """Pop highest priority tasks from queue."""
        pass

    @abstractmethod
    async def remove(self, task_id: str) -> bool:
        """Remove task from queue."""
        pass

    @abstractmethod
    async def size(self) -> int:
        """Get queue size."""
        pass

    @abstractmethod
    async def peek(self, count: int = 1) -> List[str]:
        """Peek at top tasks without removing."""
        pass

    @abstractmethod
    async def clear(self) -> None:
        """Clear all tasks from queue."""
        pass