"""
@FileName: memory.py
@Description: 内存存储实现
@Author: HiPeng
@Time: 2026/3/27 23:54
"""

import asyncio
import copy
import heapq
from typing import Optional, Dict, List, Tuple

from neotask.common.logger import debug, warning
from neotask.models.task import Task, TaskStatus
from neotask.storage.base import TaskRepository, QueueRepository


class MemoryTaskRepository(TaskRepository):
    """内存任务存储

    使用字典存储任务，支持深拷贝避免外部修改。
    设计模式：Repository Pattern - 任务数据访问
    """

    def __init__(self):
        self._tasks: Dict[str, Task] = {}
        self._lock = asyncio.Lock()

    async def save(self, task: Task) -> None:
        """保存任务（深拷贝避免外部修改）

        Args:
            task: 要保存的任务对象
        """
        async with self._lock:
            self._tasks[task.task_id] = copy.deepcopy(task)
            debug(f"Task {task.task_id} saved to memory repository")

    async def get(self, task_id: str) -> Optional[Task]:
        """获取任务（返回深拷贝）

        Args:
            task_id: 任务ID

        Returns:
            任务对象的深拷贝，不存在则返回 None
        """
        async with self._lock:
            task = self._tasks.get(task_id)
            if task:
                debug(f"Task {task_id} retrieved from memory repository")
                return copy.deepcopy(task)
            debug(f"Task {task_id} not found in memory repository")
            return None

    async def delete(self, task_id: str) -> bool:
        """删除任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功删除
        """
        async with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                debug(f"Task {task_id} deleted from memory repository")
                return True
            warning(f"Task {task_id} not found for deletion")
            return False

    async def exists(self, task_id: str) -> bool:
        """检查任务是否存在

        Args:
            task_id: 任务ID

        Returns:
            是否存在
        """
        async with self._lock:
            return task_id in self._tasks

    async def list_by_status(
            self,
            status: TaskStatus,
            limit: int = 100,
            offset: int = 0
    ) -> List[Task]:
        """按状态列出任务

        Args:
            status: 任务状态
            limit: 返回数量限制
            offset: 偏移量

        Returns:
            任务列表
        """
        async with self._lock:
            tasks = [
                copy.deepcopy(task) for task in self._tasks.values()
                if task.status == status
            ]
            result = tasks[offset:offset + limit]
            debug(f"Listed {len(result)} tasks with status {status.value}")
            return result

    async def update_status(
            self,
            task_id: str,
            status: TaskStatus,
            **kwargs
    ) -> bool:
        """更新任务状态

        Args:
            task_id: 任务ID
            status: 新状态
            **kwargs: 其他要更新的字段

        Returns:
            是否成功更新
        """
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                warning(f"Task {task_id} not found for status update")
                return False
            task.status = status
            for key, value in kwargs.items():
                if hasattr(task, key):
                    setattr(task, key, value)
            debug(f"Task {task_id} status updated to {status.value}")
            return True


class MemoryQueueRepository(QueueRepository):
    """内存队列存储

    使用堆实现的优先级队列，支持按优先级排序。
    设计模式：Repository Pattern - 队列数据访问
    """

    def __init__(self):
        self._heap: List[Tuple[int, int, str]] = []  # (priority, sequence, task_id)
        self._lock = asyncio.Lock()
        self._sequence = 0
        self._paused = False
        self._disabled = False

    async def push(self, task_id: str, priority: int) -> None:
        """入队

        使用优先级作为第一排序键，序列号作为第二排序键确保 FIFO。

        Args:
            task_id: 任务ID
            priority: 优先级（数字越小优先级越高）
        """
        async with self._lock:
            self._sequence += 1
            heapq.heappush(self._heap, (priority, self._sequence, task_id))
            debug(f"Queue push: task_id={task_id}, priority={priority}, seq={self._sequence}")

    async def pop(self, count: int = 1) -> List[str]:
        """出队

        弹出优先级最高的任务（priority 最小的）。

        Args:
            count: 弹出数量

        Returns:
            任务ID列表
        """
        async with self._lock:
            if self._paused:
                debug("Queue pop skipped: queue is paused")
                return []

            if not self._heap:
                return []

            result = []
            for _ in range(min(count, len(self._heap))):
                priority, seq, task_id = heapq.heappop(self._heap)
                result.append(task_id)
                debug(f"Queue pop: task_id={task_id}, priority={priority}, seq={seq}")

            if result:
                debug(f"Queue pop result: {result}")
            return result

    async def remove(self, task_id: str) -> bool:
        """移除任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功移除
        """
        async with self._lock:
            original_len = len(self._heap)
            self._heap = [item for item in self._heap if item[2] != task_id]
            heapq.heapify(self._heap)
            removed = len(self._heap) < original_len
            if removed:
                debug(f"Queue remove: task_id={task_id} removed")
            else:
                warning(f"Queue remove: task_id={task_id} not found")
            return removed

    async def size(self) -> int:
        """获取队列大小

        Returns:
            队列中的任务数量
        """
        async with self._lock:
            return len(self._heap)

    async def peek(self, count: int = 1) -> List[str]:
        """查看队首任务

        Args:
            count: 查看数量

        Returns:
            任务ID列表
        """
        async with self._lock:
            if not self._heap:
                return []
            sorted_heap = sorted(self._heap)
            result = [item[2] for item in sorted_heap[:count]]
            debug(f"Queue peek: {result}")
            return result

    async def clear(self) -> None:
        """清空队列"""
        async with self._lock:
            self._heap.clear()
            self._sequence = 0
            debug("Queue cleared")

    async def pause(self) -> None:
        """暂停队列（暂停出队）"""
        self._paused = True
        debug("Queue paused")

    async def resume(self) -> None:
        """恢复队列（恢复出队）"""
        self._paused = False
        debug("Queue resumed")

    async def is_paused(self) -> bool:
        """检查队列是否暂停

        Returns:
            是否暂停
        """
        return self._paused

    async def disable(self) -> None:
        """禁用队列（禁用入队）"""
        self._disabled = True
        debug("Queue disabled")

    async def enable(self) -> None:
        """启用队列（启用入队）"""
        self._disabled = False
        debug("Queue enabled")

    async def is_disabled(self) -> bool:
        """检查队列是否禁用

        Returns:
            是否禁用
        """
        return self._disabled
