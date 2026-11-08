"""
@FileName: lifecycle.py
@Description: 任务生命周期管理 - 单一职责原则
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

import asyncio
import random
import threading
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

from neotask.common.exceptions import TaskNotFoundError, TaskAlreadyExistsError
from neotask.common.constants import (
    DEFAULT_LIFECYCLE_CACHE_MAX_SIZE,
    DEFAULT_PROGRESS_FLUSH_BATCH_INTERVAL,
    DEFAULT_PROGRESS_FLUSH_DELTA,
    DEFAULT_PROGRESS_FLUSH_INTERVAL,
)
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
            cache_enabled: bool = True,
            progress_flush_interval: float = DEFAULT_PROGRESS_FLUSH_INTERVAL,
            progress_flush_delta: float = DEFAULT_PROGRESS_FLUSH_DELTA,
            progress_flush_batch_interval: float = DEFAULT_PROGRESS_FLUSH_BATCH_INTERVAL,
            cache_max_size: int = DEFAULT_LIFECYCLE_CACHE_MAX_SIZE,
    ):
        self._task_repo = task_repo
        self._event_bus = event_bus
        self._future_manager = FutureManager()
        self._cache: Dict[str, Task] = {}
        self._cache_enabled = cache_enabled
        self._cache_max_size = cache_max_size
        # 缓存可能被 TaskPool 内部循环与调用方循环同时访问，使用线程锁保护
        self._lock = threading.Lock()
        # 进度写入节流：task_id -> (最近落库时间, 最近落库进度)
        self._progress_flush_interval = progress_flush_interval
        self._progress_flush_delta = progress_flush_delta
        self._progress_flush_state: Dict[str, tuple] = {}
        # 写回缓存：待批量落库的进度，task_id -> (progress, message)
        self._progress_flush_batch_interval = progress_flush_batch_interval
        self._pending_progress: Dict[str, Tuple[float, str]] = {}
        self._flusher_task: Optional[asyncio.Task] = None
        self._write_behind_enabled = False

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
            self._cache_put(task_id, task)

        await self._emit_event("task.created", task_id, task.to_dict())

        return task

    async def get_task(self, task_id: str) -> Optional[Task]:
        """获取任务

        同一实例内的所有状态写入（start/complete/fail/cancel/update_status）都会
        同步刷新缓存，故缓存对本实例始终权威。
        """
        # 从缓存获取
        if self._cache_enabled:
            with self._lock:
                if task_id in self._cache:
                    return self._cache[task_id]

        # 从存储获取
        task = await self._task_repo.get(task_id)

        if task and self._cache_enabled:
            with self._lock:
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

        # 只落库发生变化的字段，避免重写整条任务记录
        await self._task_repo.save_fields(
            task,
            {"status", *(key for key in kwargs if hasattr(task, key))}
        )

        if self._cache_enabled:
            self._cache_put(task_id, task)

        await self._emit_event(
            "task.status_changed",
            task_id,
            {"old_status": old_status.value, "new_status": status.value}
        )

        return True

    async def update_progress(
            self,
            task_id: str,
            progress: float,
            message: str = ""
    ) -> bool:
        """更新任务进度并发布 ``task.progress`` 事件

        仅对未结束的任务生效；进度值会被收敛到 0.0 - 1.0。

        内存中的任务对象每次都更新，但落库按「时间间隔 / 增量阈值」节流，
        避免业务代码高频上报把 SQLite 写入放大。终态由 ``complete_task`` 等
        方法强制写入，不会停留在旧进度。

        Returns:
            是否成功更新（任务不存在或已处于终态时返回 False）
        """
        task = await self.get_task(task_id)
        if not task or task.status.is_terminal():
            return False

        task.update_progress(progress, message)

        if self._cache_enabled:
            self._cache_put(task_id, task)

        if self._should_flush_progress(task_id, task.progress):
            if self._write_behind_enabled:
                # 写回模式：只记入待刷队列，由后台 flusher 合并落库
                with self._lock:
                    self._pending_progress[task_id] = (
                        task.progress,
                        task.progress_message,
                    )
            else:
                await self._task_repo.update_progress(
                    task_id,
                    task.progress,
                    task.progress_message
                )

        await self._emit_event(
            "task.progress",
            task_id,
            {
                "progress": task.progress,
                "message": task.progress_message,
                "timestamp": time.time(),
            }
        )
        return True

    def _should_flush_progress(self, task_id: str, progress: float) -> bool:
        """判断本次进度是否需要落库

        三个条件任一满足即写入：
        - 进度到达 100%（终态前的最后一次进度）
        - 距上次落库超过 ``progress_flush_interval``
        - 进度增量达到 ``progress_flush_delta``
        """
        last = self._progress_flush_state.get(task_id)
        if progress >= 1.0 or last is None:
            self._progress_flush_state[task_id] = (time.monotonic(), progress)
            return True

        last_time, last_progress = last
        now = time.monotonic()
        if now - last_time >= self._progress_flush_interval or \
                progress - last_progress >= self._progress_flush_delta:
            self._progress_flush_state[task_id] = (now, progress)
            return True

        return False

    def _clear_progress_flush(self, task_id: str) -> None:
        """任务结束时清理进度节流与待刷状态"""
        self._progress_flush_state.pop(task_id, None)
        with self._lock:
            self._pending_progress.pop(task_id, None)

    def _cache_put(self, task_id: str, task: Task) -> None:
        """写入缓存，并在超出上限时优先淘汰已结束任务"""
        with self._lock:
            self._cache[task_id] = task
            overflow = len(self._cache) - self._cache_max_size
            if overflow > 0:
                # 只淘汰终态任务：RUNNING/PENDING 可能仍被 Worker 使用
                terminal = sorted(
                    (
                        (item.completed_at or item.created_at, item.task_id)
                        for item in self._cache.values()
                        if item.status.is_terminal()
                    ),
                    key=lambda pair: pair[0],
                )
                for _, cached_id in terminal[:overflow]:
                    self._cache.pop(cached_id, None)

    def start_progress_flusher(self) -> None:
        """启动后台进度写回任务（需在事件循环内调用）

        启用后 ``update_progress`` 只更新内存并把进度放入待刷队列，由后台任务
        每隔 ``progress_flush_batch_interval`` 秒合并写入一次，显著减少提交次数。
        """
        if self._progress_flush_batch_interval <= 0:
            return
        self._write_behind_enabled = True
        if self._flusher_task is not None and not self._flusher_task.done():
            return
        self._flusher_task = asyncio.create_task(self._progress_flush_loop())

    async def _progress_flush_loop(self) -> None:
        """后台批量落库循环"""
        try:
            while True:
                await asyncio.sleep(self._progress_flush_batch_interval)
                try:
                    await self.flush_pending_progress()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    warning(f"Flush pending progress failed: {exc}")
        except asyncio.CancelledError:
            pass

    async def stop_progress_flusher(self) -> None:
        """停止后台任务并把剩余进度全部落库"""
        self._write_behind_enabled = False
        task = self._flusher_task
        self._flusher_task = None
        if task is not None and not task.done():
            try:
                task.cancel()
            except RuntimeError:
                pass
            try:
                await task
            except asyncio.CancelledError:
                pass
            except RuntimeError as exc:
                # 关闭路径可能跨事件循环，无法等待原 loop 上的任务
                warning(f"Cancel progress flusher failed: {exc}")
        await self.flush_pending_progress()

    async def flush_pending_progress(self) -> int:
        """把待刷进度合并写入存储

        失败时把这一批进度放回待刷队列，避免静默丢失。
        """
        with self._lock:
            if not self._pending_progress:
                return 0
            batch = self._pending_progress
            self._pending_progress = {}

        try:
            return await self._task_repo.update_progress_batch(batch)
        except Exception:
            with self._lock:
                for task_id, value in batch.items():
                    self._pending_progress.setdefault(task_id, value)
            raise

    async def restore_pending_tasks(
            self,
            push: Callable[[str, int], Awaitable[bool]],
            limit: int = 1000,
    ) -> int:
        """把任务表中仍为 PENDING 的任务重新入队

        队列未持久化（``persist_queue=False``）时，进程重启后由任务表恢复待执行
        任务，避免队列从内存清空导致任务静默丢失。

        Args:
            push: 入队函数，签名为 ``(task_id, priority) -> Awaitable[bool]``
            limit: 单次最多恢复的任务数量

        Returns:
            成功恢复的任务数量
        """
        tasks = await self._task_repo.list_by_status(TaskStatus.PENDING, limit=limit)
        restored = 0
        for task in tasks:
            if await push(task.task_id, task.priority.value):
                restored += 1

        if restored:
            info(f"Restored {restored} pending task(s) into queue")
        return restored

    async def start_task(self, task_id: str, node_id: str) -> bool:
        """开始执行任务"""
        task = await self.get_task(task_id)
        if not task or task.status != TaskStatus.PENDING:
            warning(f"Cannot start task {task_id}: status={task.status if task else 'not found'}")
            return False

        previous = (task.status, task.node_id, task.started_at)
        task.start(node_id)
        # 只更新状态相关字段：data 等不可变字段无需重写
        try:
            await self._task_repo.save_fields(
                task,
                ("status", "node_id", "started_at")
            )
        except Exception:
            # 落库失败必须回滚内存状态：缓存里存的就是同一个对象，若保持 RUNNING，
            # 任务被重新入队后再次启动会被状态机拒绝，最终永久卡死在 RUNNING。
            task.status, task.node_id, task.started_at = previous
            raise

        if self._cache_enabled:
            self._cache_put(task_id, task)

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
        # 成功完成即视为 100%，避免 UI 停留在最后一次业务上报的进度上。
        task.progress = 1.0
        await self._task_repo.save_fields(
            task,
            ("status", "completed_at", "result", "progress")
        )
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, task)

        # 通知等待者
        await self._future_manager.complete(task_id, result=result)

        # 直接传递 result，不包装
        await self._emit_event("task.completed", task_id, result)

        debug(f"Task {task_id} completed successfully")
        return True

    async def fail_task(self, task_id: str, error: str, allow_pending: bool = False) -> bool:
        """任务失败

        正常情况下只有 RUNNING 状态的任务可以标记为失败。`allow_pending=True`
        用于入队失败的回滚：任务已落库为 PENDING 但从未进入队列，只能在这里终结。
        """
        task = await self.get_task(task_id)

        # 检查任务是否存在
        if not task:
            return False

        # 检查是否已经是终态
        if task.status.is_terminal():
            warning(f"Cannot fail task {task_id}: already in terminal state {task.status.value}")
            return False

        allowed = (TaskStatus.PENDING, TaskStatus.RUNNING) if allow_pending else (TaskStatus.RUNNING,)
        if task.status not in allowed:
            warning(
                f"Cannot fail task {task_id}: status={task.status.value}, "
                f"expected {'PENDING or RUNNING' if allow_pending else 'RUNNING'}"
            )
            return False

        debug(f"[DEBUG] Failing task {task_id}: {error}")

        task.fail(error)
        await self._task_repo.save_fields(
            task,
            ("status", "error", "completed_at")
        )
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, task)

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
        await self._task_repo.save_fields(
            task,
            ("status", "completed_at")
        )
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, task)

        # 通知等待者
        await self._future_manager.complete(task_id, error="Task cancelled")

        await self._emit_event("task.cancelled", task_id)

        return True

    async def reset_for_retry(self, task_id: str) -> bool:
        """把已结束的任务重置为 PENDING，供手动重试

        与 Worker 内部的自动重试不同，手动 ``retry()`` 面对的是已经进入终态的
        任务，必须先恢复成 PENDING 才能再次执行。旧的 future 一并丢弃，
        否则等待方会立刻拿到上一次的失败结果。

        Returns:
            是否重置成功（任务不存在或仍在运行 / 已完成时返回 False）
        """
        task = await self.get_task(task_id)
        if not task or not task.status.is_terminal():
            return False

        task.reset_for_retry()
        await self._task_repo.save_fields(
            task,
            (
                "status", "started_at", "completed_at", "result", "error",
                "node_id", "progress", "progress_message",
            )
        )
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, task)

        await self._future_manager.remove(task_id)

        await self._emit_event(
            "task.retry",
            task_id,
            {"retry_count": task.retry_count, "manual": True}
        )

        debug(f"Task {task_id} reset to PENDING for manual retry")
        return True

    async def delete_task(self, task_id: str) -> bool:
        """删除任务"""
        await self._task_repo.delete(task_id)
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            with self._lock:
                self._cache.pop(task_id, None)

        await self._future_manager.remove(task_id)

        await self._emit_event("task.deleted", task_id)

        return True

    async def wait_for_task(
            self,
            task_id: str,
            timeout: float = 300
    ) -> Any:
        """等待任务完成

        等待策略：优先依赖 ``FutureManager`` 的即时通知（同节点执行时毫秒级返回）；
        对于跨节点执行的任务，完成事件只发生在执行节点，本节点的 Future 不会被唤醒，
        因此额外按固定时间片直接回查仓储的终态，避免必须等满整个 ``timeout``。
        """
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

        from neotask.common.exceptions import TimeoutError

        poll_interval = 0.2
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(task_id, timeout)

            try:
                return await future.wait(min(poll_interval, remaining))
            except Exception:
                # future 的超时/失败/取消都交给下面的存储回查判定
                pass

            # 绕过生命周期缓存直接读取仓储：跨节点执行时本地缓存可能仍是 PENDING
            task = await self._task_repo.get(task_id)
            if task:
                if task.status == TaskStatus.SUCCESS:
                    self._refresh_cache(task)
                    return task.result
                elif task.status == TaskStatus.FAILED:
                    self._refresh_cache(task)
                    raise Exception(task.error or "Task failed")
                elif task.status == TaskStatus.CANCELLED:
                    self._refresh_cache(task)
                    raise Exception("Task was cancelled")

    def _refresh_cache(self, task: Task) -> None:
        """用仓储中的终态刷新缓存（终态不会回退，覆盖是安全的）"""
        if not self._cache_enabled:
            return
        self._cache_put(task.task_id, task)

    async def get_task_stats(self) -> TaskStats:
        """获取任务统计

        从存储层聚合统计信息，支持内存、SQLite、Redis 后端。
        """
        stats = TaskStats()

        # 如果缓存启用且有数据，优先使用缓存
        if self._cache_enabled and self._cache:
            with self._lock:
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
                        with self._lock:
                            self._cache.pop(task.task_id, None)
                    self._clear_progress_flush(task.task_id)

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
                            with self._lock:
                                self._cache.pop(task.task_id, None)
                        self._clear_progress_flush(task.task_id)

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
        with self._lock:
            self._cache.clear()
        self._progress_flush_state.clear()
