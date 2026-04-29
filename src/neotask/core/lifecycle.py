"""
@FileName: lifecycle.py
@Description: 任务生命周期管理 - 单一职责原则
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import random
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Union

from neotask.common.exceptions import TaskNotFoundError, TaskAlreadyExistsError
from neotask.common.logger import debug, warning, info
from neotask.core.future import FutureManager
from neotask.event.bus import EventBus, TaskEvent
from neotask.models.task import Task, TaskStatus, TaskPriority, TaskStats
from neotask.storage.base import TaskRepository


class TaskLifecycleManager:
    """任务生命周期管理器

    职责：
    - 任务的CRUD操作
    - 状态转换
    - 结果存储
    - Future管理

    设计模式：Facade Pattern - 封装存储和事件
    """

    def __init__(
            self,
            task_repo: TaskRepository,
            event_bus: Optional[EventBus] = None,
            cache_enabled: bool = True
    ):
        self._task_repo = task_repo
        self._event_bus = event_bus
        self._future_manager = FutureManager()
        self._cache: Dict[str, Task] = {}
        self._cache_enabled = cache_enabled
        self._lock = asyncio.Lock()

    async def create_task(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            node_id: str = "default",
            ttl: int = 3600
    ) -> Task:
        """创建任务"""
        task_id = task_id or self._generate_task_id()

        # 转换优先级为 TaskPriority 枚举
        if isinstance(priority, int):
            priority_enum = TaskPriority(priority)
        else:
            priority_enum = priority

        # 检查是否已存在
        existing = await self._task_repo.get(task_id)
        if existing:
            raise TaskAlreadyExistsError(task_id)

        task = Task(
            task_id=task_id,
            data=data,
            priority=priority_enum,
            node_id=node_id,
            ttl=ttl
        )

        await self._task_repo.save(task)

        if self._cache_enabled:
            async with self._lock:
                self._cache[task_id] = task

        await self._emit_event("task.created", task_id, task.to_dict())

        return task

    async def get_task(self, task_id: str) -> Optional[Task]:
        """获取任务"""
        # 从缓存获取
        if self._cache_enabled:
            async with self._lock:
                if task_id in self._cache:
                    return self._cache[task_id]

        # 从存储获取
        task = await self._task_repo.get(task_id)

        if task and self._cache_enabled:
            async with self._lock:
                self._cache[task_id] = task

        return task

    async def update_status(
            self,
            task_id: str,
            status: TaskStatus,
            **kwargs
    ) -> bool:
        """更新任务状态"""
        task = await self.get_task(task_id)
        if not task:
            return False

        old_status = task.status
        task.status = status

        # 更新额外字段
        for key, value in kwargs.items():
            if hasattr(task, key):
                setattr(task, key, value)

        await self._task_repo.save(task)

        if self._cache_enabled:
            async with self._lock:
                self._cache[task_id] = task

        await self._emit_event(
            "task.status_changed",
            task_id,
            {"old_status": old_status.value, "new_status": status.value}
        )

        return True

    async def start_task(self, task_id: str, node_id: str) -> bool:
        """开始执行任务"""
        task = await self.get_task(task_id)
        if not task or task.status != TaskStatus.PENDING:
            warning(f"Cannot start task {task_id}: status={task.status if task else 'not found'}")
            return False

        task.start(node_id)
        await self._task_repo.save(task)

        if self._cache_enabled:
            async with self._lock:
                self._cache[task_id] = task

        await self._emit_event("task.started", task_id, {"node_id": node_id})

        debug(f"Task {task_id} started on node {node_id}")
        return True

    async def complete_task(self, task_id: str, result: Any) -> bool:
        """完成任务"""
        task = await self.get_task(task_id)

        # 检查任务是否存在
        if not task:
            return False

        # 检查是否已经是终态
        if task.status.is_terminal():
            warning(f"Cannot complete task {task_id}: already in terminal state {task.status.value}")
            return False

        # 只有 RUNNING 状态的任务可以完成
        if task.status != TaskStatus.RUNNING:
            warning(f"Cannot complete task {task_id}: status={task.status.value}, expected RUNNING")
            return False

        task.complete(result)
        await self._task_repo.save(task)

        if self._cache_enabled:
            async with self._lock:
                self._cache[task_id] = task

        # 通知等待者
        await self._future_manager.complete(task_id, result=result)

        # 直接传递 result，不包装
        await self._emit_event("task.completed", task_id, result)

        debug(f"Task {task_id} completed successfully")
        return True

    async def fail_task(self, task_id: str, error: str) -> bool:
        """任务失败

        只有 RUNNING 状态的任务可以标记为失败。
        """
        task = await self.get_task(task_id)

        # 检查任务是否存在
        if not task:
            return False

        # 检查是否已经是终态
        if task.status.is_terminal():
            warning(f"Cannot fail task {task_id}: already in terminal state {task.status.value}")
            return False

        # 【修复】只有 RUNNING 状态的任务可以失败
        if task.status != TaskStatus.RUNNING:
            warning(f"Cannot fail task {task_id}: status={task.status.value}, expected RUNNING")
            return False

        debug(f"[DEBUG] Failing task {task_id}: {error}")

        task.fail(error)
        await self._task_repo.save(task)

        if self._cache_enabled:
            async with self._lock:
                self._cache[task_id] = task

        # 通知等待者 - 确保错误被传递
        await self._future_manager.complete(task_id, error=error)

        await self._emit_event("task.failed", task_id, {"error": error})

        return True


    async def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        task = await self.get_task(task_id)
        if not task:
            return False

        # 只有待处理或运行中的任务可以取消
        if task.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
            return False

        task.cancel()
        await self._task_repo.save(task)

        if self._cache_enabled:
            async with self._lock:
                self._cache[task_id] = task

        # 通知等待者
        await self._future_manager.complete(task_id, error="Task cancelled")

        await self._emit_event("task.cancelled", task_id)

        return True

    async def delete_task(self, task_id: str) -> bool:
        """删除任务"""
        await self._task_repo.delete(task_id)

        if self._cache_enabled:
            async with self._lock:
                self._cache.pop(task_id, None)

        await self._future_manager.remove(task_id)

        await self._emit_event("task.deleted", task_id)

        return True

    async def wait_for_task(
            self,
            task_id: str,
            timeout: float = 300
    ) -> Any:
        """等待任务完成"""
        # 先检查任务是否已完成
        task = await self.get_task(task_id)
        if not task:
            raise TaskNotFoundError(task_id)

        # 如果任务已经是最终状态，直接返回结果或抛出异常
        if task.status == TaskStatus.SUCCESS:
            return task.result
        elif task.status == TaskStatus.FAILED:
            raise Exception(task.error or "Task failed")
        elif task.status == TaskStatus.CANCELLED:
            raise Exception("Task was cancelled")

        # 获取或创建 future
        future = await self._future_manager.get(task_id)

        try:
            # 等待完成
            result = await future.wait(timeout)
            return result
        except Exception as e:
            # 等待失败后，重新检查任务状态（可能任务在等待期间完成了但 future 没收到通知）
            task = await self.get_task(task_id)
            if task:
                if task.status == TaskStatus.SUCCESS:
                    return task.result
                elif task.status == TaskStatus.FAILED:
                    raise Exception(task.error or "Task failed")
                elif task.status == TaskStatus.CANCELLED:
                    raise Exception("Task was cancelled")
            # 如果是超时异常，重新抛出
            from neotask.common.exceptions import TimeoutError
            if isinstance(e, TimeoutError):
                raise
            # 其他异常
            raise

    async def get_task_stats(self) -> TaskStats:
        """获取任务统计

        从存储层聚合统计信息，支持内存、SQLite、Redis 后端。
        """
        stats = TaskStats()

        # 如果缓存启用且有数据，优先使用缓存
        if self._cache_enabled and self._cache:
            async with self._lock:
                for task in self._cache.values():
                    stats.total += 1
                    if task.status == TaskStatus.PENDING:
                        stats.pending += 1
                    elif task.status == TaskStatus.RUNNING:
                        stats.running += 1
                    elif task.status == TaskStatus.SUCCESS:
                        stats.completed += 1
                    elif task.status == TaskStatus.FAILED:
                        stats.failed += 1
                    elif task.status == TaskStatus.CANCELLED:
                        stats.cancelled += 1
        else:
            # 从存储聚合统计
            stats = await self._aggregate_stats_from_storage()

        return stats

    async def _aggregate_stats_from_storage(self) -> TaskStats:
        """从存储层聚合统计信息

        支持所有存储后端（内存、SQLite、Redis）。
        """
        stats = TaskStats()

        # 遍历所有任务状态，从存储获取计数
        for status in TaskStatus:
            # 获取该状态的任务数量
            tasks = await self._task_repo.list_by_status(status, limit=10000)
            count = len(tasks)

            stats.total += count
            if status == TaskStatus.PENDING:
                stats.pending = count
            elif status == TaskStatus.RUNNING:
                stats.running = count
            elif status == TaskStatus.SUCCESS:
                stats.completed = count
            elif status == TaskStatus.FAILED:
                stats.failed = count
            elif status == TaskStatus.CANCELLED:
                stats.cancelled = count

        return stats

    async def cleanup_expired(self) -> int:
        """清理过期任务

        删除超过 TTL 且处于终端状态（SUCCESS/FAILED/CANCELLED）的任务。

        Returns:
            清理的任务数量
        """
        cleaned_count = 0
        now = datetime.now(timezone.utc)

        # 获取所有终端状态的任务
        terminal_statuses = [TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED]

        for status in terminal_statuses:
            tasks = await self._task_repo.list_by_status(status, limit=1000)

            for task in tasks:
                # 检查是否过期
                is_expired = False
                age_seconds = 0

                # 根据任务的 completed_at 或 created_at 判断
                if task.completed_at:
                    # 任务已完成，检查是否超过 TTL
                    age_seconds = (now - task.completed_at).total_seconds()
                    if age_seconds > task.ttl:
                        is_expired = True
                elif task.created_at:
                    # 任务未完成但处于终端状态（异常情况）
                    age_seconds = (now - task.created_at).total_seconds()
                    if age_seconds > task.ttl * 2:  # 给予双倍时间
                        is_expired = True

                if is_expired:
                    # 删除过期任务
                    await self._task_repo.delete(task.task_id)

                    # 从缓存中移除
                    if self._cache_enabled:
                        async with self._lock:
                            self._cache.pop(task.task_id, None)

                    # 清理 future
                    await self._future_manager.remove(task.task_id)

                    cleaned_count += 1
                    debug(f"Cleaned expired task: {task.task_id}, status={task.status.value}, age={age_seconds:.0f}s")

        if cleaned_count > 0:
            info(f"Cleaned {cleaned_count} expired tasks")

        return cleaned_count

    async def cleanup_expired_by_time(self, max_age_seconds: int = 86400) -> int:
        """按时间清理过期任务

        Args:
            max_age_seconds: 最大存活时间（秒），默认 24 小时

        Returns:
            清理的任务数量
        """
        cleaned_count = 0
        now = datetime.now(timezone.utc)
        cutoff_time = now.timestamp() - max_age_seconds

        # 获取所有终端状态的任务
        terminal_statuses = [TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED]

        for status in terminal_statuses:
            tasks = await self._task_repo.list_by_status(status, limit=1000)

            for task in tasks:
                # 检查是否超过最大存活时间
                check_time = task.completed_at or task.created_at
                if check_time:
                    if check_time.timestamp() < cutoff_time:
                        await self._task_repo.delete(task.task_id)

                        if self._cache_enabled:
                            async with self._lock:
                                self._cache.pop(task.task_id, None)

                        await self._future_manager.remove(task.task_id)
                        cleaned_count += 1
                        debug(f"Cleaned old task: {task.task_id}, age={(now - check_time).total_seconds():.0f}s")

        if cleaned_count > 0:
            info(f"Cleaned {cleaned_count} old tasks by time")

        return cleaned_count

    async def list_tasks(
            self,
            status: Optional[TaskStatus] = None,
            limit: int = 100,
            offset: int = 0
    ) -> List[Task]:
        """列出任务"""
        if status:
            return await self._task_repo.list_by_status(status, limit, offset)

        # 获取所有状态的任务
        tasks = []
        for s in TaskStatus:
            tasks.extend(await self._task_repo.list_by_status(s, limit // 5, offset))
        return tasks[:limit]

    async def _emit_event(self, event_type: str, task_id: str, data=None):
        """发送事件"""
        if self._event_bus:
            await self._event_bus.emit(TaskEvent(event_type, task_id, data))

    def _generate_task_id(self) -> str:
        """生成任务ID"""
        return f"TSK{datetime.now().strftime('%y%m%d%H%M%S%f')}{random.randint(100000, 999999)}"
        # return f"TSK{datetime.now().strftime('%y%m%d%H%M%S')}{uuid.uuid4().time}"

    def clear_cache(self):
        """清空缓存"""
        # 同步清空缓存，确保立即生效
        try:
            loop = asyncio.get_running_loop()
            # 如果有运行中的循环，创建任务清空
            loop.create_task(self._clear_cache_async())
        except RuntimeError:
            # 没有运行中的循环，同步清空
            asyncio.run(self._clear_cache_async())

    async def _clear_cache_async(self):
        """异步清空缓存"""
        async with self._lock:
            self._cache.clear()
