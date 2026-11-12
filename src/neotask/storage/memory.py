"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: memory.py
@Description: 内存存储实现
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/3/27 23:54
"""

import copy
import heapq
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional, Dict, List, Tuple

from neotask.common.logger import debug, warning
from neotask.models.task import Task, TaskStatus
from neotask.storage.base import (
    CheckpointRecord,
    CheckpointRepository,
    TaskRepository,
    QueueRepository,
    new_cost_summary,
    record_task_cost,
)


class MemoryTaskRepository(TaskRepository):
    """内存任务存储

    使用字典存储任务，支持深拷贝避免外部修改。
    设计模式：Repository Pattern - 任务数据访问
    """

    def __init__(self):
        self._tasks: Dict[str, Task] = {}
        # 仓储可能被 TaskPool 内部循环与调用方循环同时访问，使用线程锁
        self._lock = threading.Lock()

    async def save(self, task: Task) -> None:
        """保存任务（深拷贝避免外部修改）

        Args:
            task: 要保存的任务对象
        """
        with self._lock:
            self._tasks[task.task_id] = copy.deepcopy(task)
            debug(f"Task {task.task_id} saved to memory repository")

    async def save_batch(self, tasks: List[Task]) -> List[str]:
        """批量保存任务（跳过已存在的 task_id）

        先完成整批深拷贝，再一次性写入字典；任一任务拷贝失败都不会留下
        半批数据。
        """
        if not tasks:
            return []

        # 深拷贝可能因不可拷贝字段失败，放在加锁与写入之前完成
        snapshots = [(task.task_id, copy.deepcopy(task)) for task in tasks]

        saved: List[str] = []
        with self._lock:
            for task_id, snapshot in snapshots:
                if task_id in self._tasks:
                    continue
                self._tasks[task_id] = snapshot
                saved.append(task_id)
        return saved

    async def save_if_absent(self, task: Task) -> bool:
        """保存新任务（已存在则跳过），一次加锁完成查重与写入"""
        with self._lock:
            if task.task_id in self._tasks:
                return False
            self._tasks[task.task_id] = copy.deepcopy(task)
        return True

    async def get(self, task_id: str) -> Optional[Task]:
        """获取任务（返回深拷贝）

        Args:
            task_id: 任务ID

        Returns:
            任务对象的深拷贝，不存在则返回 None
        """
        with self._lock:
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
        with self._lock:
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
        with self._lock:
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
        with self._lock:
            tasks = [
                copy.deepcopy(task) for task in self._tasks.values()
                if task.status == status
            ]
            result = tasks[offset:offset + limit]
            debug(f"Listed {len(result)} tasks with status {status.value}")
            return result

    async def count_by_status(self, status: TaskStatus) -> int:
        """统计指定状态的任务数量（不拷贝任务对象）"""
        with self._lock:
            return sum(1 for task in self._tasks.values() if task.status == status)

    async def list_by_parent(
            self, parent_task_id: str, limit: int = 100, offset: int = 0
    ) -> List[Task]:
        """按父任务 ID 列出子任务（深拷贝，按创建时间升序）"""
        with self._lock:
            children = sorted(
                (
                    copy.deepcopy(task)
                    for task in self._tasks.values()
                    if task.parent_task_id == parent_task_id
                ),
                key=lambda task: task.created_at,
            )
        return children[offset:offset + limit]

    async def cost_summary(self, task_ids=None) -> Dict:
        """内存后端直接遍历字典聚合，无需反序列化"""
        summary = new_cost_summary()
        with self._lock:
            if task_ids is None:
                tasks = list(self._tasks.values())
            else:
                tasks = [
                    self._tasks[task_id] for task_id in task_ids
                    if task_id in self._tasks
                ]
            for task in tasks:
                record_task_cost(summary, task)
        return summary

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
        with self._lock:
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

    async def update_status_batch(
            self,
            updates: List[tuple]
    ) -> int:
        """批量更新任务状态"""
        with self._lock:
            success_count = 0
            for update in updates:
                task_id = update[0]
                status = update[1]
                kwargs = update[2] if len(update) > 2 else {}

                task = self._tasks.get(task_id)
                if task:
                    task.status = status
                    for key, value in kwargs.items():
                        if hasattr(task, key):
                            setattr(task, key, value)
                    success_count += 1
            return success_count

    async def delete_batch(self, task_ids: List[str]) -> int:
        """批量删除任务"""
        with self._lock:
            deleted = 0
            for task_id in task_ids:
                if task_id in self._tasks:
                    del self._tasks[task_id]
                    deleted += 1
            return deleted


class MemoryCheckpointRepository(CheckpointRepository):
    """内存 checkpoint 存储

    每个任务维护按版本升序的列表。单 loop 顺序执行下「写 checkpoint + 补写
    任务标记」两次赋值之间没有可失败点，因此无需额外事务机制。
    """

    def __init__(self):
        self._records: Dict[str, List[CheckpointRecord]] = {}
        self._lock = threading.Lock()

    async def save(self, task_id: str, version: int, state: Dict) -> None:
        record = CheckpointRecord(
            task_id=task_id,
            version=int(version),
            state=copy.deepcopy(state),
            created_at=datetime.now(timezone.utc),
        )
        with self._lock:
            records = self._records.setdefault(task_id, [])
            for index, existing in enumerate(records):
                if existing.version == record.version:
                    records[index] = record
                    break
            else:
                records.append(record)
                records.sort(key=lambda item: item.version)
        debug(f"Checkpoint v{version} saved for task {task_id}")

    async def load_latest(self, task_id: str) -> Optional[CheckpointRecord]:
        with self._lock:
            records = self._records.get(task_id)
            if not records:
                return None
            return copy.deepcopy(records[-1])

    async def load(self, task_id: str, version: int) -> Optional[CheckpointRecord]:
        with self._lock:
            for record in self._records.get(task_id, []):
                if record.version == version:
                    return copy.deepcopy(record)
        return None

    async def prune(self, task_id: str, keep: int) -> int:
        keep = max(0, int(keep))
        with self._lock:
            records = self._records.get(task_id)
            if not records or len(records) <= keep:
                return 0
            removed = len(records) - keep
            if keep == 0:
                self._records.pop(task_id, None)
            else:
                self._records[task_id] = records[removed:]
            return removed

    async def delete(self, task_id: str) -> None:
        with self._lock:
            self._records.pop(task_id, None)


class MemoryQueueRepository(QueueRepository):
    """内存队列存储

    使用堆实现的优先级队列，支持按优先级排序。
    设计模式：Repository Pattern - 队列数据访问
    """

    #: 队列只存在内存中，进程重启后未执行的任务需要由任务表重建
    is_persistent = False

    def __init__(self):
        self._heap: List[Tuple[int, int, str]] = []  # (priority, sequence, task_id)
        # 队列可能被任务池内部循环与调用方循环同时访问，使用线程锁
        self._lock = threading.Lock()
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
        with self._lock:
            self._sequence += 1
            heapq.heappush(self._heap, (priority, self._sequence, task_id))
            debug(f"Queue push: task_id={task_id}, priority={priority}, seq={self._sequence}")

    async def push_batch(self, items: List[Tuple[str, int]]) -> int:
        """批量入队

        先在临时堆上完成全部插入，成功后再整体替换；中途异常不会影响
        已存在的队列内容。
        """
        if not items:
            return 0

        with self._lock:
            sequence = self._sequence
            new_heap = list(self._heap)
            for task_id, priority in items:
                sequence += 1
                new_heap.append((priority, sequence, task_id))
            heapq.heapify(new_heap)
            self._heap = new_heap
            self._sequence = sequence
        return len(items)

    async def pop(self, count: int = 1) -> List[str]:
        """出队

        弹出优先级最高的任务（priority 最小的）。

        Args:
            count: 弹出数量

        Returns:
            任务ID列表
        """
        with self._lock:
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
        with self._lock:
            original_len = len(self._heap)
            self._heap = [item for item in self._heap if item[2] != task_id]
            heapq.heapify(self._heap)
            removed = len(self._heap) < original_len
            if removed:
                debug(f"Queue remove: task_id={task_id} removed")
            else:
                # QueueScheduler.remove 会先试优先级队列、再试延迟队列，
                # 因此这里“未命中”属于正常流程，不应作为告警输出。
                debug(f"Queue remove: task_id={task_id} not found")
            return removed

    async def size(self) -> int:
        """获取队列大小

        Returns:
            队列中的任务数量
        """
        with self._lock:
            return len(self._heap)

    async def peek(self, count: int = 1) -> List[str]:
        """查看队首任务

        Args:
            count: 查看数量

        Returns:
            任务ID列表
        """
        with self._lock:
            if not self._heap:
                return []
            sorted_heap = sorted(self._heap)
            result = [item[2] for item in sorted_heap[:count]]
            debug(f"Queue peek: {result}")
            return result

    async def clear(self) -> None:
        """清空队列"""
        with self._lock:
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


class MemoryContextStore:
    """进程内 Blackboard：``key -> (value, expires_at)``

    仅 Memory 后端提供。TTL 采用惰性过期（读取时判定）+ 写入时顺带清扫，
    不启动后台线程，符合「本地轻量上下文」的定位。
    """

    def __init__(self):
        self._data: Dict[str, Tuple[Any, Optional[float]]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _is_expired(expires_at: Optional[float]) -> bool:
        return expires_at is not None and expires_at <= time.time()

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """写入或覆盖一个键；``ttl`` 为秒，None 表示不过期"""
        expires_at = time.time() + float(ttl) if ttl is not None else None
        with self._lock:
            # 顺带清理已过期条目，避免只写不读的场景无限增长
            expired = [
                name for name, (_, deadline) in self._data.items()
                if self._is_expired(deadline)
            ]
            for name in expired:
                self._data.pop(name, None)
            self._data[key] = (copy.deepcopy(value), expires_at)
        return True

    def get(self, key: str) -> Any:
        """读取一个键；不存在或已过期返回 None"""
        with self._lock:
            item = self._data.get(key)
            if item is None:
                return None
            value, expires_at = item
            if self._is_expired(expires_at):
                self._data.pop(key, None)
                return None
            return copy.deepcopy(value)

    def delete(self, key: str) -> bool:
        """删除一个键，返回是否命中"""
        with self._lock:
            return self._data.pop(key, None) is not None
