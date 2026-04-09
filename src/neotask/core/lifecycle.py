"""
@FileName: lifecycle.py
@Description: 任务生命周期管理 - 单一职责原则
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import random
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from neotask.common.exceptions import TaskNotFoundError, TaskAlreadyExistsError
from neotask.common.logger import debug
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
            event_bus: Optional[EventBus] = None
    ):
        self._task_repo = task_repo
        self._event_bus = event_bus
        self._future_manager = FutureManager()
        self._cache: Dict[str, Task] = {}
        self._cache_enabled = True
        self._lock = asyncio.Lock()

    async def create_task(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: TaskPriority = TaskPriority.NORMAL,
            node_id: str = "default",
            ttl: int = 3600
    ) -> Task:
        """创建任务"""
        task_id = task_id or self._generate_task_id()

        # 检查是否已存在
        existing = await self._task_repo.get(task_id)
        if existing:
            raise TaskAlreadyExistsError(task_id)

        task = Task(
            task_id=task_id,
            data=data,
            priority=priority,
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
            return False

        task.start(node_id)
        await self._task_repo.save(task)

        if self._cache_enabled:
            async with self._lock:
                self._cache[task_id] = task

        # 发送 started 事件 - 确保事件被发送
        await self._emit_event("task.started", task_id, {"node_id": node_id})

        return True

    async def complete_task(self, task_id: str, result: Any) -> bool:
        """完成任务"""
        task = await self.get_task(task_id)
        if not task:
            return False

        task.complete(result)
        await self._task_repo.save(task)

        if self._cache_enabled:
            async with self._lock:
                self._cache[task_id] = task

        # 通知等待者
        await self._future_manager.complete(task_id, result=result)

        await self._emit_event("task.completed", task_id, {"result": result})

        return True

    async def fail_task(self, task_id: str, error: str) -> bool:
        """任务失败"""
        task = await self.get_task(task_id)
        if not task:
            return False

        debug(f"[DEBUG] Failing task {task_id}: {error}")  # 调试用

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
        """获取任务统计"""
        stats = TaskStats()
        # 从存储获取统计
        # TODO: 实现统计查询
        return stats

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

    async def cleanup_expired(self) -> int:
        """清理过期任务"""
        cleaned = 0
        # TODO: 实现过期清理逻辑
        # 删除超过 TTL 且已完成的任务
        return cleaned

    async def _emit_event(self, event_type: str, task_id: str, data=None):
        """发送事件"""
        if self._event_bus:
            await self._event_bus.emit(TaskEvent(event_type, task_id, data))

    def _generate_task_id(self) -> str:
        """生成任务ID"""
        # return f"TSK{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6]}"
        return f"TSK{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}{random.randint(1000, 9999)}"

    def clear_cache(self):
        """清空缓存"""

        async def _clear():
            async with self._lock:
                self._cache.clear()

        # 如果在异步上下文中
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_clear())
        except RuntimeError:
            pass
