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

from collections import deque
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

from neotask.common.constants import (
    DEFAULT_LIFECYCLE_CACHE_MAX_SIZE,
    DEFAULT_PROGRESS_FLUSH_BATCH_INTERVAL,
    DEFAULT_PROGRESS_FLUSH_DELTA,
    DEFAULT_PROGRESS_FLUSH_INTERVAL,
    DEFAULT_RUNNING_PERSIST_DELAY,
)
from neotask.common.exceptions import TaskAlreadyExistsError, TaskNotFoundError
from neotask.common.logger import debug, info, warning
from neotask.core.future import FutureManager
from neotask.event.bus import EventBus, TaskEvent
from neotask.models.task import Task, TaskPriority, TaskStats, TaskStatus
from neotask.storage.base import TaskRepository


#: 进度落库硬性最小间隔（秒）。同一任务两次进度写入间隔小于该值时跳过存储
#: 写入，但 ``task.progress`` 事件仍会正常推送，保证订阅方实时可见。
#: 该行为固定、不提供配置项，避免业务方高频上报把存储写放大。
PROGRESS_MIN_WRITE_INTERVAL = 0.5


class _DeferredRunningWrite:
    """一条延迟落库的 RUNNING 状态句柄

    - ``done``：任务在窗口内完成时置位，后台协程据此跳过这次 RUNNING 写入
    - ``force``：关闭时置位，即使 ``done`` 已置位也强制落库当前 RUNNING 状态
    - ``flush_task``：后台协程，终态写入前会先等待它结束，保证写入顺序
    """

    __slots__ = ("task_obj", "done", "force", "flush_task")

    def __init__(self, task: Task, done: asyncio.Event):
        self.task_obj = task
        self.done = done
        self.force = False
        self.flush_task: Optional[asyncio.Task] = None


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
            running_persist_delay: float = DEFAULT_RUNNING_PERSIST_DELAY,
    ):
        self._task_repo = task_repo
        self._event_bus = event_bus
        self._future_manager = FutureManager()
        self._cache: Dict[str, Task] = {}
        self._cache_enabled = cache_enabled
        self._cache_max_size = cache_max_size
        # 一旦发生过淘汰，缓存不再完整，统计需回退到存储层
        self._cache_evicted = False
        # 终态任务按进入缓存的时间排队，淘汰时直接取队首，避免全量扫描
        self._terminal_cache_ids: "deque[str]" = deque()
        self._terminal_cache_set: set = set()
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
        # 执行侧写入优化：RUNNING 状态延迟落库窗口（0 = 立即落库）
        self._running_persist_delay = running_persist_delay
        self._deferred_running: Dict[str, _DeferredRunningWrite] = {}


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

        # 先创建任务，再由存储判定是否重复：Redis 用单条 Lua、SQLite 用
        # INSERT OR IGNORE 原子完成「查重 + 写入」，避免额外的查询往返
        task = Task(
            task_id=task_id,
            data=data,
            priority=priority_enum,
            node_id=node_id,
            ttl=ttl
        )

        if not await self._task_repo.save_if_absent(task):
            raise TaskAlreadyExistsError(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, task)

        await self._emit_event("task.created", task_id, task.to_dict())

        return task

    async def create_task_and_enqueue(
            self,
            data: Dict[str, Any],
            max_size: int,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            node_id: str = "default",
            ttl: int = 3600,
    ) -> Tuple[Task, Optional[bool]]:
        """创建任务并尝试一次原子入队（提交快路径）

        Returns:
            ``(task, queued)``:
            - ``queued=True``：存储层已原子完成「创建 + 入队」
            - ``queued=False``：任务已创建，但队列已满
            - ``queued=None``：后端不支持合并，调用方需自行 ``push``
        """
        task_id = task_id or self._generate_task_id()

        priority_enum = TaskPriority(priority) if isinstance(priority, int) else priority

        task = Task(
            task_id=task_id,
            data=data,
            priority=priority_enum,
            node_id=node_id,
            ttl=ttl
        )

        queued = await self._task_repo.save_if_absent_and_enqueue(task, max_size)
        if queued is None and not await self._task_repo.save_if_absent(task):
            raise TaskAlreadyExistsError(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, task)

        await self._emit_event("task.created", task_id, task.to_dict())

        return task, queued

    async def create_tasks(
            self,
            data_list: List[Dict[str, Any]],
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            node_id: str = "default",
            ttl: int = 3600,
    ) -> List[str]:
        """批量创建任务，返回实际创建成功的 task_id 列表。

        与逐条 :meth:`create_task` 的区别：落库只做一次批量写入（SQLite 单事务
        / Redis pipeline），且不逐条查询是否存在；已存在的 task_id 会被跳过。
        """
        tasks = self.build_tasks(data_list, priority, node_id, ttl)
        return await self.persist_tasks(tasks)

    def build_tasks(
            self,
            data_list: List[Dict[str, Any]],
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            node_id: str = "default",
            ttl: int = 3600,
    ) -> List[Task]:
        """构造一批任务对象（纯内存操作，不落库）

        供批量提交先完成全部序列化/校验，再一次性写入，避免中途失败留下
        半批数据。
        """
        if not data_list:
            return []

        priority_enum = TaskPriority(priority) if isinstance(priority, int) else priority

        # 批内去重：任务 ID 精度到微秒 + 随机后缀，批量生成时仍可能撞车
        tasks: List[Task] = []
        generated: set = set()
        for data in data_list:
            task_id = self._generate_task_id()
            while task_id in generated:
                task_id = self._generate_task_id()
            generated.add(task_id)
            tasks.append(Task(
                task_id=task_id,
                data=data,
                priority=priority_enum,
                node_id=node_id,
                ttl=ttl,
            ))
        return tasks

    async def persist_tasks(self, tasks: List[Task]) -> List[str]:
        """把已构造的任务一次性写入存储，并发布创建事件

        批量写入失败时由存储层整体回滚（SQLite 单事务），调用方不会看到
        半批任务。返回实际新增的 task_id 列表。
        """
        if not tasks:
            return []

        saved_ids = await self._task_repo.save_batch(tasks)
        saved_set = set(saved_ids)

        created: List[str] = []
        for task in tasks:
            if task.task_id not in saved_set:
                continue
            if self._cache_enabled:
                self._cache_put(task.task_id, task)
            await self._emit_event("task.created", task.task_id, task.to_dict())
            created.append(task.task_id)

        return created

    async def delete_tasks(self, task_ids: List[str]) -> int:
        """批量删除任务（批量提交失败时的回滚手段）"""
        if not task_ids:
            return 0

        deleted = await self._task_repo.delete_batch(task_ids)
        if self._cache_enabled:
            with self._lock:
                for task_id in task_ids:
                    self._cache.pop(task_id, None)
                    self._terminal_cache_set.discard(task_id)
        for task_id in task_ids:
            self._clear_progress_flush(task_id)
            await self._future_manager.remove(task_id)
        return deleted

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

        硬性节流：同一任务两次写入间隔小于 ``PROGRESS_MIN_WRITE_INTERVAL``
        时直接跳过存储写入（100% 除外），事件总线仍会收到每次上报。

        其余条件任一满足即写入：
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
        if now - last_time < PROGRESS_MIN_WRITE_INTERVAL:
            return False

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
        """写入缓存，并在超出上限时优先淘汰已结束任务

        终态任务按入缓存顺序维护在 ``_terminal_cache_ids`` 队列中，超限时
        直接从队首回收，单次写入的开销均摊为 O(1)。

        只淘汰终态任务：RUNNING/PENDING 可能仍被 Worker 使用；因此当活跃
        任务本身超过上限时，缓存会短暂超出上限，等待任务结束后再回收。
        """
        with self._lock:
            self._cache[task_id] = task
            if task.status.is_terminal() and task_id not in self._terminal_cache_set:
                self._terminal_cache_set.add(task_id)
                self._terminal_cache_ids.append(task_id)
            limit = max(1, self._cache_max_size)
            while self._terminal_cache_ids and len(self._cache) > limit:
                evict_id = self._terminal_cache_ids.popleft()
                self._terminal_cache_set.discard(evict_id)
                cached = self._cache.get(evict_id)
                if cached is not None and cached.status.is_terminal():
                    self._cache.pop(evict_id, None)
                    self._cache_evicted = True

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
        await self.flush_deferred_running()

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

    # ========== RUNNING 状态延迟落库（执行侧写入优化） ==========

    def _defer_running_persist(self, task: Task) -> None:
        """把 RUNNING 状态放入延迟落库队列（需在事件循环内调用）"""
        handle = _DeferredRunningWrite(task, asyncio.Event())
        handle.flush_task = asyncio.ensure_future(self._persist_running_later(handle))
        with self._lock:
            self._deferred_running[task.task_id] = handle

    async def _persist_running_later(self, handle: _DeferredRunningWrite) -> None:
        """等待窗口结束，把仍处于 RUNNING 的任务状态落库

        任务在窗口内结束（``done`` 被置位且未 ``force``）时跳过本次写入，
        由终态写入一次性补齐 ``started_at``。
        """
        task = handle.task_obj
        try:
            try:
                await asyncio.wait_for(handle.done.wait(), timeout=self._running_persist_delay)
                if not handle.force:
                    return
            except asyncio.TimeoutError:
                pass

            if task.status != TaskStatus.RUNNING:
                return
            await self._task_repo.save_fields(task, ("status", "node_id", "started_at"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            warning(f"Persist running state for {task.task_id} failed: {exc}")
        finally:
            with self._lock:
                if self._deferred_running.get(task.task_id) is handle:
                    self._deferred_running.pop(task.task_id, None)

    async def _resolve_deferred_running(self, task_id: str) -> None:
        """终态写入前结束该任务的延迟 RUNNING 落库

        - 仍在窗口内：跳过写入（终态写入包含 ``started_at``）
        - 已开始落库：等待它结束，避免 RUNNING 写入晚于终态写入
        """
        with self._lock:
            handle = self._deferred_running.get(task_id)
        if handle is None:
            return

        handle.done.set()
        flush_task = handle.flush_task
        if flush_task is None or flush_task.done():
            return
        try:
            await asyncio.shield(flush_task)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    async def flush_deferred_running(self) -> int:
        """把所有仍在等待窗口的 RUNNING 状态立即落库（关闭前调用）

        Returns:
            成功落库的任务数量
        """
        with self._lock:
            handles = list(self._deferred_running.values())
            self._deferred_running.clear()

        flushed = 0
        for handle in handles:
            handle.force = True
            handle.done.set()
            flush_task = handle.flush_task
            if flush_task is not None and flush_task.done():
                continue
            if flush_task is not None:
                try:
                    await asyncio.shield(flush_task)
                    flushed += 1
                    continue
                except asyncio.CancelledError:
                    raise
                except RuntimeError:
                    # 跨事件循环无法等待后台协程，退回直接写入
                    pass
                except Exception:
                    continue

            task = handle.task_obj
            if task.status != TaskStatus.RUNNING:
                continue
            try:
                await self._task_repo.save_fields(task, ("status", "node_id", "started_at"))
                flushed += 1
            except Exception as exc:
                warning(f"Flush running state for {task.task_id} failed: {exc}")

        return flushed

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
        if self._running_persist_delay > 0:
            # 延迟窗口内完成的任务只需写一次终态，先只更新内存
            self._defer_running_persist(task)
        else:
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

        # 先结束延迟的 RUNNING 落库，保证终态写入之后不会再出现 RUNNING 写入
        await self._resolve_deferred_running(task_id)

        task.complete(result)
        # 成功完成即视为 100%，避免 UI 停留在最后一次业务上报的进度上。
        task.progress = 1.0

        await self._task_repo.save_fields(
            task,
            ("status", "started_at", "completed_at", "result", "progress")
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

        await self._resolve_deferred_running(task_id)

        old_status = task.status
        task.fail(error)
        await self._task_repo.save_fields(
            task,
            ("status", "started_at", "error", "completed_at")
        )
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, task)


        # 通知等待者 - 确保错误被传递
        await self._future_manager.complete(task_id, error=error)

        await self._emit_event(
            "task.failed",
            task_id,
            {"error": error, "old_status": old_status.value}
        )

        return True


    async def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        task = await self.get_task(task_id)
        if not task:
            return False

        # 只有待处理或运行中的任务可以取消
        if task.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
            return False

        await self._resolve_deferred_running(task_id)

        old_status = task.status
        task.cancel()
        await self._task_repo.save_fields(
            task,
            ("status", "started_at", "completed_at")
        )
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, task)


        # 通知等待者
        await self._future_manager.complete(task_id, error="Task cancelled")

        await self._emit_event(
            "task.cancelled",
            task_id,
            {"old_status": old_status.value}
        )

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

        old_status = task.status
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
            {
                "retry_count": task.retry_count,
                "manual": True,
                "old_status": old_status.value,
            }
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
                self._terminal_cache_set.discard(task_id)

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


        # 如果缓存启用且有数据，优先使用缓存；缓存发生过淘汰时已不完整，
        # 必须回退到存储层聚合，避免统计结果随着淘汰静默缩水。
        if self._cache_enabled and self._cache and not self._cache_evicted:
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
                            self._terminal_cache_set.discard(task.task_id)
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
                                self._terminal_cache_set.discard(task.task_id)
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
            self._cache_evicted = False
            self._terminal_cache_ids.clear()
            self._terminal_cache_set.clear()
        self._progress_flush_state.clear()
