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

    async def save_batch(self, tasks: List[Task]) -> List[str]:
        """批量保存任务，返回**实际新增**的 task_id 列表。

        默认实现退化为逐条 ``save``；支持事务/pipeline 的后端应覆盖为一次
        往返写入（SQLite 单事务 ``executemany``、Redis ``pipeline``），这是
        提交侧批量接口 ``submit_many`` 的性能基础。

        已存在的 task_id 会被跳过（不覆盖已有任务），因此返回值可能小于
        ``tasks`` 的长度。
        """
        saved: List[str] = []
        for task in tasks:
            await self.save(task)
            saved.append(task.task_id)
        return saved

    async def save_if_absent(self, task: Task) -> bool:
        """保存新任务；若 ``task_id`` 已存在则跳过并返回 False。

        Returns:
            是否实际写入（True = 新建，False = 已存在）

        默认实现先查存在性再写入（两次往返）；支持原子写入的后端应覆盖：
        SQLite 用 ``INSERT OR IGNORE`` + ``rowcount``，Redis 用单条 Lua。
        """
        if await self.exists(task.task_id):
            return False
        await self.save(task)
        return True

    async def save_if_absent_and_enqueue(
            self, task: Task, max_size: int
    ) -> Optional[bool]:
        """（可选能力）原子创建任务并入队。

        Returns:
            None: 后端不支持合并，调用方应分两步（``save_if_absent`` + 队列 push）
            True/False: 已由后端完成入队 / 队列已满

        支持合并的后端（Redis）用单条 Lua 把「查重 → 容量校验 → 写任务 →
        状态索引 → 入队」压成 1 次往返，避免提交路径上的多次跨 loop 通信。
        """
        return None

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

    async def close(self) -> None:
        """Release underlying resources; no-op for memory implementations."""
        return None


class QueueRepository(ABC):
    """Repository interface for task queue."""

    @abstractmethod
    async def push(self, task_id: str, priority: int) -> None:
        """Push task to queue with priority."""
        pass

    async def push_batch(self, items: List[Tuple[str, int]]) -> int:
        """批量入队，返回成功入队的数量。

        默认实现退化为逐条 ``push``；支持批量写入的后端应覆盖为一次往返
        （SQLite ``executemany``、Redis ``pipeline``）。

        Args:
            items: ``[(task_id, priority), ...]``
        """
        for task_id, priority in items:
            await self.push(task_id, priority)
        return len(items)

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

    async def close(self) -> None:
        """Release underlying resources; no-op for memory implementations."""
        return None