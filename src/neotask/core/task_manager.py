"""
@FileName: task_manager.py
@Description: TaskManager - 统一任务管理核心。
    职责：
    - 任务生命周期管理（创建、更新、查询、删除）
    - 存储统一管理（内存/SQLite/Redis）
    - 队列统一管理（优先级队列）
    - 状态统一管理
    - 分布式锁管理
@Author: HiPeng
@Time: 2026/4/3 22:28
"""
import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Union

from neotask.common.exceptions import TaskAlreadyExistsError
from neotask.locks.base import TaskLock
from neotask.locks.factory import LockFactory
from neotask.models.task import Task, TaskStatus, TaskPriority
from neotask.storage.base import TaskRepository, QueueRepository
from neotask.storage.factory import StorageFactory


@dataclass
class TaskStats:
    """任务统计信息。"""
    total: int = 0
    pending: int = 0
    processing: int = 0
    completed: int = 0
    failed: int = 0
    cancelled: int = 0


class TaskManager:
    """统一任务管理器。

    使用示例：
        >>> manager = TaskManager(storage_type="sqlite")
        >>> task_id = manager.create_task({"data": "value"})
        >>> task = manager.get_task(task_id)
        >>> manager.update_task_status(task_id, TaskStatus.RUNNING)
        >>> manager.complete_task(task_id, {"result": "success"})
    """

    def __init__(
            self,
            storage_type: str = "memory",
            sqlite_path: str = "neotask.db",
            redis_url: Optional[str] = None,
            node_id: Optional[str] = None,
            lock_type: str = "memory",
    ):
        """初始化任务管理器。

        Args:
            storage_type: 存储类型 (memory/sqlite/redis)
            sqlite_path: SQLite 数据库路径
            redis_url: Redis 连接 URL
            node_id: 节点标识
            lock_type: 锁类型 (memory/redis)
        """
        self._node_id = node_id or self._generate_node_id()

        # 初始化存储
        self._storage_type = storage_type
        self._task_repo: TaskRepository
        self._queue_repo: QueueRepository
        self._init_storage(storage_type, sqlite_path, redis_url)

        # 初始化锁管理器
        self._lock_type = lock_type
        self._lock_manager: TaskLock
        self._init_lock(lock_type, redis_url)

        # 内存缓存
        self._cache: Dict[str, Task] = {}
        self._cache_enabled = storage_type != "memory"  # 非内存存储时启用缓存

        # 统计信息缓存
        self._stats_cache: Optional[TaskStats] = None
        self._stats_updated_at: Optional[datetime] = None

    def _generate_node_id(self) -> str:
        """生成节点标识。"""
        import socket
        return f"{socket.gethostname()}_{uuid.uuid4().hex[:8]}"

    def _init_storage(self, storage_type: str, sqlite_path: str, redis_url: Optional[str]):
        """初始化存储。"""
        from neotask.models.config import StorageConfig

        if storage_type == "memory":
            config = StorageConfig.memory()
        elif storage_type == "sqlite":
            config = StorageConfig.sqlite(sqlite_path)
        elif storage_type == "redis":
            if not redis_url:
                raise ValueError("Redis URL is required for redis storage")
            config = StorageConfig.redis(redis_url)
        else:
            raise ValueError(f"Unknown storage type: {storage_type}")

        self._task_repo, self._queue_repo = StorageFactory.create(config)

    def _init_lock(self, lock_type: str, redis_url: Optional[str]):
        """初始化锁管理器。"""
        from neotask.models.config import LockConfig

        if lock_type == "memory":
            config = LockConfig.memory()
        elif lock_type == "redis":
            if not redis_url:
                raise ValueError("Redis URL is required for redis lock")
            config = LockConfig.redis(redis_url)
        else:
            raise ValueError(f"Unknown lock type: {lock_type}")

        self._lock_manager = LockFactory.create(config)

    # ========== 任务生命周期管理 ==========

    def create_task(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
    ) -> str:
        """创建任务。

        Args:
            data: 任务数据
            task_id: 任务ID（可选，自动生成）
            priority: 优先级

        Returns:
            task_id

        Raises:
            TaskAlreadyExistsError: 任务ID已存在
        """
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._create_task_async(data, task_id, priority),
            self._get_loop()
        ).result()

    async def create_task_async(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
    ) -> str:
        """异步创建任务。"""
        return await self._create_task_async(data, task_id, priority)

    async def _create_task_async(
            self,
            data: Dict[str, Any],
            task_id: Optional[str],
            priority: Union[int, TaskPriority],
    ) -> str:
        """异步创建任务实现。"""
        task_id = task_id or self._generate_task_id()
        priority_value = priority.value if isinstance(priority, TaskPriority) else priority

        # 检查是否已存在
        existing = await self._task_repo.get(task_id)
        if existing:
            raise TaskAlreadyExistsError(task_id)

        # 创建任务
        task = Task(
            task_id=task_id,
            data=data,
            priority=TaskPriority(priority_value),
            node_id=self._node_id,
        )

        # 保存到存储
        await self._task_repo.save(task)

        # 更新缓存
        if self._cache_enabled:
            self._cache[task_id] = task

        # 清除统计缓存
        self._stats_cache = None

        return task_id

    def get_task(self, task_id: str) -> Optional[Task]:
        """获取任务。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._get_task_async(task_id),
            self._get_loop()
        ).result()

    async def get_task_async(self, task_id: str) -> Optional[Task]:
        """异步获取任务。"""
        return await self._get_task_async(task_id)

    async def _get_task_async(self, task_id: str) -> Optional[Task]:
        """异步获取任务实现。"""
        # 先从缓存获取
        if self._cache_enabled and task_id in self._cache:
            return self._cache[task_id]

        # 从存储获取
        task = await self._task_repo.get(task_id)

        # 更新缓存
        if self._cache_enabled and task:
            self._cache[task_id] = task

        return task

    def update_task_status(self, task_id: str, status: TaskStatus) -> bool:
        """更新任务状态。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._update_task_status_async(task_id, status),
            self._get_loop()
        ).result()

    async def update_task_status_async(self, task_id: str, status: TaskStatus) -> bool:
        """异步更新任务状态。"""
        return await self._update_task_status_async(task_id, status)

    async def _update_task_status_async(self, task_id: str, status: TaskStatus) -> bool:
        """异步更新任务状态实现。"""
        task = await self._get_task_async(task_id)
        if not task:
            return False

        task.status = status
        await self._task_repo.save(task)

        # 更新缓存
        if self._cache_enabled:
            self._cache[task_id] = task

        # 清除统计缓存
        self._stats_cache = None

        return True

    def start_task(self, task_id: str) -> bool:
        """开始执行任务（状态改为 PROCESSING）。"""
        return self.update_task_status(task_id, TaskStatus.RUNNING)

    def complete_task(self, task_id: str, result: Dict[str, Any]) -> bool:
        """完成任务（状态改为 SUCCESS）。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._complete_task_async(task_id, result),
            self._get_loop()
        ).result()

    async def complete_task_async(self, task_id: str, result: Dict[str, Any]) -> bool:
        """异步完成任务。"""
        return await self._complete_task_async(task_id, result)

    async def _complete_task_async(self, task_id: str, result: Dict[str, Any]) -> bool:
        """异步完成任务实现。"""
        task = await self._get_task_async(task_id)
        if not task:
            return False

        task.complete(result)
        await self._task_repo.save(task)

        # 更新缓存
        if self._cache_enabled:
            self._cache[task_id] = task

        # 清除统计缓存
        self._stats_cache = None

        return True

    def fail_task(self, task_id: str, error: str) -> bool:
        """任务失败（状态改为 FAILED）。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._fail_task_async(task_id, error),
            self._get_loop()
        ).result()

    async def fail_task_async(self, task_id: str, error: str) -> bool:
        """异步任务失败。"""
        return await self._fail_task_async(task_id, error)

    async def _fail_task_async(self, task_id: str, error: str) -> bool:
        """异步任务失败实现。"""
        task = await self._get_task_async(task_id)
        if not task:
            return False

        task.fail(error)
        await self._task_repo.save(task)

        # 更新缓存
        if self._cache_enabled:
            self._cache[task_id] = task

        # 清除统计缓存
        self._stats_cache = None

        return True

    def cancel_task(self, task_id: str) -> bool:
        """取消任务（状态改为 CANCELLED）。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._cancel_task_async(task_id),
            self._get_loop()
        ).result()

    async def cancel_task_async(self, task_id: str) -> bool:
        """异步取消任务。"""
        return await self._cancel_task_async(task_id)

    async def _cancel_task_async(self, task_id: str) -> bool:
        """异步取消任务实现。"""
        task = await self._get_task_async(task_id)
        if not task:
            return False

        # 只有 PENDING 和 PROCESSING 状态可以取消
        if task.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
            return False

        task.cancel()
        await self._task_repo.save(task)

        # 更新缓存
        if self._cache_enabled:
            self._cache[task_id] = task

        # 清除统计缓存
        self._stats_cache = None

        return True

    def delete_task(self, task_id: str) -> bool:
        """删除任务。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._delete_task_async(task_id),
            self._get_loop()
        ).result()

    async def delete_task_async(self, task_id: str) -> bool:
        """异步删除任务。"""
        return await self._delete_task_async(task_id)

    async def _delete_task_async(self, task_id: str) -> bool:
        """异步删除任务实现。"""
        await self._task_repo.delete(task_id)

        # 清除缓存
        if self._cache_enabled:
            self._cache.pop(task_id, None)

        # 清除统计缓存
        self._stats_cache = None

        return True

    # ========== 队列管理 ==========

    def enqueue(self, task_id: str, priority: Union[int, TaskPriority] = TaskPriority.NORMAL) -> bool:
        """任务入队。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._enqueue_async(task_id, priority),
            self._get_loop()
        ).result()

    async def enqueue_async(self, task_id: str, priority: Union[int, TaskPriority] = TaskPriority.NORMAL) -> bool:
        """异步任务入队。"""
        return await self._enqueue_async(task_id, priority)

    async def _enqueue_async(self, task_id: str, priority: Union[int, TaskPriority]) -> bool:
        """异步任务入队实现。"""
        priority_value = priority.value if isinstance(priority, TaskPriority) else priority
        await self._queue_repo.push(task_id, priority_value)
        return True

    def dequeue(self, count: int = 1) -> List[str]:
        """任务出队（获取最高优先级任务）。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._dequeue_async(count),
            self._get_loop()
        ).result()

    async def dequeue_async(self, count: int = 1) -> List[str]:
        """异步任务出队。"""
        return await self._dequeue_async(count)

    async def _dequeue_async(self, count: int) -> List[str]:
        """异步任务出队实现。"""
        return await self._queue_repo.pop(count)

    def remove_from_queue(self, task_id: str) -> bool:
        """从队列中移除任务。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._remove_from_queue_async(task_id),
            self._get_loop()
        ).result()

    async def remove_from_queue_async(self, task_id: str) -> bool:
        """异步从队列移除任务。"""
        return await self._remove_from_queue_async(task_id)

    async def _remove_from_queue_async(self, task_id: str) -> bool:
        """异步从队列移除任务实现。"""
        return await self._queue_repo.remove(task_id)

    def get_queue_size(self) -> int:
        """获取队列大小。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._get_queue_size_async(),
            self._get_loop()
        ).result()

    async def get_queue_size_async(self) -> int:
        """异步获取队列大小。"""
        return await self._get_queue_size_async()

    async def _get_queue_size_async(self) -> int:
        """异步获取队列大小实现。"""
        return await self._queue_repo.size()

    # ========== 锁管理 ==========

    def acquire_lock(self, task_id: str, ttl: int = 30) -> bool:
        """获取任务锁。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._acquire_lock_async(task_id, ttl),
            self._get_loop()
        ).result()

    async def acquire_lock_async(self, task_id: str, ttl: int = 30) -> bool:
        """异步获取任务锁。"""
        return await self._acquire_lock_async(task_id, ttl)

    async def _acquire_lock_async(self, task_id: str, ttl: int) -> bool:
        """异步获取任务锁实现。"""
        lock_key = f"task:{task_id}"
        return await self._lock_manager.acquire(lock_key, ttl)

    def release_lock(self, task_id: str) -> bool:
        """释放任务锁。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._release_lock_async(task_id),
            self._get_loop()
        ).result()

    async def release_lock_async(self, task_id: str) -> bool:
        """异步释放任务锁。"""
        return await self._release_lock_async(task_id)

    async def _release_lock_async(self, task_id: str) -> bool:
        """异步释放任务锁实现。"""
        lock_key = f"task:{task_id}"
        return await self._lock_manager.release(lock_key)

    def extend_lock(self, task_id: str, ttl: int = 30) -> bool:
        """延长任务锁。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._extend_lock_async(task_id, ttl),
            self._get_loop()
        ).result()

    async def extend_lock_async(self, task_id: str, ttl: int = 30) -> bool:
        """异步延长任务锁。"""
        return await self._extend_lock_async(task_id, ttl)

    async def _extend_lock_async(self, task_id: str, ttl: int) -> bool:
        """异步延长任务锁实现。"""
        lock_key = f"task:{task_id}"
        return await self._lock_manager.extend(lock_key, ttl)

    # ========== 统计信息 ==========

    def get_stats(self, force_refresh: bool = False) -> TaskStats:
        """获取任务统计信息。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._get_stats_async(force_refresh),
            self._get_loop()
        ).result()

    async def get_stats_async(self, force_refresh: bool = False) -> TaskStats:
        """异步获取任务统计信息。"""
        return await self._get_stats_async(force_refresh)

    async def _get_stats_async(self, force_refresh: bool) -> TaskStats:
        """异步获取任务统计信息实现。"""
        # 检查缓存
        if not force_refresh and self._stats_cache:
            return self._stats_cache

        # 从存储统计
        stats = TaskStats()

        # 获取各种状态的任务数量
        stats.pending = len(await self._task_repo.list_by_status(TaskStatus.PENDING, limit=10000))
        stats.processing = len(await self._task_repo.list_by_status(TaskStatus.RUNNING, limit=10000))
        stats.completed = len(await self._task_repo.list_by_status(TaskStatus.SUCCESS, limit=10000))
        stats.failed = len(await self._task_repo.list_by_status(TaskStatus.FAILED, limit=10000))
        stats.cancelled = len(await self._task_repo.list_by_status(TaskStatus.CANCELLED, limit=10000))
        stats.total = stats.pending + stats.processing + stats.completed + stats.failed + stats.cancelled

        # 更新缓存
        self._stats_cache = stats
        self._stats_updated_at = datetime.now(timezone.utc)

        return stats

    # ========== 批量操作 ==========

    def list_tasks(self, status: Optional[TaskStatus] = None, limit: int = 100) -> List[Task]:
        """列出任务。"""
        import asyncio
        return asyncio.run_coroutine_threadsafe(
            self._list_tasks_async(status, limit),
            self._get_loop()
        ).result()

    async def list_tasks_async(self, status: Optional[TaskStatus] = None, limit: int = 100) -> List[Task]:
        """异步列出任务。"""
        return await self._list_tasks_async(status, limit)

    async def _list_tasks_async(self, status: Optional[TaskStatus], limit: int) -> List[Task]:
        """异步列出任务实现。"""
        if status:
            return await self._task_repo.list_by_status(status, limit)
        else:
            # 获取所有状态的任务
            tasks = []
            for s in TaskStatus:
                tasks.extend(await self._task_repo.list_by_status(s, limit // 5))
            return tasks[:limit]

    def clear_completed(self, days: int = 7) -> int:
        """清理已完成的任务。"""
        # TODO: 实现清理逻辑
        pass

    # ========== 生命周期 ==========

    def shutdown(self):
        """关闭任务管理器。"""
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._shutdown_async())
            else:
                loop.run_until_complete(self._shutdown_async())
        except RuntimeError:
            pass

    async def _shutdown_async(self):
        """异步关闭。"""
        # 关闭存储连接
        if hasattr(self._task_repo, 'close'):
            await self._task_repo.close()
        if hasattr(self._queue_repo, 'close'):
            await self._queue_repo.close()

        # 清空缓存
        self._cache.clear()
        self._stats_cache = None

    # ========== 工具方法 ==========

    def _generate_task_id(self) -> str:
        """Generate unique task ID."""
        return "TSK" + datetime.now(timezone.utc).strftime("%Y%m%d") + uuid.uuid4().hex[:10]

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """获取事件循环。"""
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            try:
                return asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop
