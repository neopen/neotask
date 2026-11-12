"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: base.py
@Description: 数据存储。Storage abstraction using Repository pattern.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/3/27 23:54
"""

from abc import ABC, abstractmethod
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from neotask.models.task import Task, TaskStatus


def new_cost_summary() -> Dict[str, Any]:
    """成本聚合结果的空模板（``task_count`` 等计数按任务累加）"""
    return {
        "task_count": 0,
        "tasks_with_cost": 0,
        "tokens_in": 0,
        "tokens_out": 0,
        "api_calls": 0,
        "estimated_cost": 0.0,
        "currency": "",
    }


def merge_cost_detail(
        summary: Dict[str, Any],
        cost_tracking: Optional[Dict[str, Any]],
) -> None:
    """只累加明细字段（tokens/api_calls/currency），不动金额与任务计数"""
    cost = cost_tracking or {}
    summary["tokens_in"] += int(cost.get("tokens_in") or 0)
    summary["tokens_out"] += int(cost.get("tokens_out") or 0)
    summary["api_calls"] += int(cost.get("api_calls") or 0)

    currency = str(cost.get("currency") or "")
    if currency:
        if not summary["currency"]:
            summary["currency"] = currency
        elif summary["currency"] != currency:
            summary["currency"] = "MIXED"


def record_cost_values(
        summary: Dict[str, Any],
        cost_tracking: Optional[Dict[str, Any]],
        estimated_cost: float = 0.0,
) -> None:
    """把一条任务的成本明细累加进聚合结果

    多个任务使用不同币种时把 ``currency`` 标记为 ``MIXED``，避免把不同
    币种的金额静默相加。
    """
    cost = cost_tracking or {}
    if not cost and not estimated_cost:
        return

    summary["tasks_with_cost"] += 1
    merge_cost_detail(summary, cost)
    summary["estimated_cost"] += float(estimated_cost or 0.0)


def record_task_cost(summary: Dict[str, Any], task: Task) -> None:
    """累加单个任务的成本（``task_count`` 计入所有任务）"""
    summary["task_count"] += 1
    record_cost_values(summary, task.cost_tracking, task.estimated_cost)


def _is_terminal_expired(
        task: Task,
        max_age_seconds: Optional[int],
        now_timestamp: float,
) -> bool:
    """判断终态任务是否超过保留期。

    ``max_age_seconds`` 为 None/0 时按任务自身 ttl（完成时间 + ttl）；
    大于 0 时统一按该时长计算。
    """
    if not task.status.is_terminal():
        return False

    check_time = task.completed_at or task.created_at
    if check_time is None:
        return False

    if max_age_seconds and max_age_seconds > 0:
        return check_time.timestamp() < now_timestamp - max_age_seconds

    ttl = task.ttl if task.completed_at else task.ttl * 2
    return check_time.timestamp() + ttl < now_timestamp


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

    async def count_by_status(self, status: TaskStatus) -> int:
        """统计指定状态的任务数量。

        默认实现分页拉取任务实体后计数，任何后端都能工作；内置后端均覆盖为
        计数查询，避免为一次统计把全部任务加载成对象。
        """
        total = 0
        offset = 0
        batch = 1000
        while True:
            tasks = await self.list_by_status(status, limit=batch, offset=offset)
            total += len(tasks)
            if len(tasks) < batch:
                return total
            offset += batch

    @abstractmethod
    async def update_status(self, task_id: str, status: TaskStatus) -> None:
        """Update task status."""
        pass

    async def list_by_parent(
            self, parent_task_id: str, limit: int = 100, offset: int = 0
    ) -> List[Task]:
        """按父任务 ID 列出子任务（按创建时间升序）。

        默认实现分页扫描各状态集合；内置后端均覆盖为索引/字典查询，
        避免为一次父子查询读取全部任务。
        """
        matched: List[Task] = []
        for status in TaskStatus:
            status_offset = 0
            while True:
                tasks = await self.list_by_status(
                    status, limit=1000, offset=status_offset
                )
                matched.extend(
                    task for task in tasks
                    if task.parent_task_id == parent_task_id
                )
                if len(tasks) < 1000:
                    break
                status_offset += len(tasks)

        matched.sort(key=lambda task: task.created_at)
        return matched[offset:offset + limit]

    async def cost_summary(
            self, task_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """聚合任务成本。

        ``task_ids=None`` 时聚合全部任务；SQLite 后端下推为 SQL 聚合。
        币种混用时结果中的 ``currency`` 为 ``MIXED``。
        """
        summary = new_cost_summary()
        if task_ids is not None:
            for task_id in task_ids:
                task = await self.get(task_id)
                if task:
                    record_task_cost(summary, task)
            return summary

        for status in TaskStatus:
            offset = 0
            while True:
                tasks = await self.list_by_status(status, limit=1000, offset=offset)
                for task in tasks:
                    record_task_cost(summary, task)
                if len(tasks) < 1000:
                    break
                offset += len(tasks)
        return summary

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

    async def delete_expired_terminal(
            self,
            max_age_seconds: Optional[int] = None,
    ) -> List[str]:
        """删除已过保留期的终态任务，返回被删除的 task_id 列表。

        ``max_age_seconds`` 为 None/0 时按任务自身 ttl（完成时间 + ttl）；
        大于 0 时使用统一保留时长。默认实现按状态分页扫描，所有后端行为
        一致；支持过期索引的后端（Redis）应覆盖为一次范围查询，避免全量
        读取任务实体。
        """
        now = time.time()
        deleted: List[str] = []
        batch = 1000

        for status in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED):
            offset = 0
            while True:
                tasks = await self.list_by_status(status, limit=batch, offset=offset)
                if not tasks:
                    break

                expired = [
                    task for task in tasks
                    if _is_terminal_expired(task, max_age_seconds, now)
                ]
                if expired:
                    expired_ids = [task.task_id for task in expired]
                    await self.delete_batch(expired_ids)
                    deleted.extend(expired_ids)
                    # 删除后同一偏移位置由后续任务填补，保持 offset 继续扫描
                    continue

                offset += len(tasks)
                if len(tasks) < batch:
                    break

        return deleted

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