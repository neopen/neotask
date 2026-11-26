"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: lifecycle.py
@Description: 任务生命周期管理 - 单一职责原则
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

import asyncio
import copy
import random
import threading
import time

from collections import deque
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

from neotask.common.constants import (
    DEFAULT_LIFECYCLE_CACHE_MAX_SIZE,
    DEFAULT_PROGRESS_FLUSH_BATCH_INTERVAL,
    DEFAULT_PROGRESS_FLUSH_DELTA,
    DEFAULT_PROGRESS_FLUSH_INTERVAL,
    DEFAULT_RUNNING_PERSIST_DELAY,
    EventType,
)
from neotask.common.exceptions import TaskAlreadyExistsError, TaskNotFoundError
from neotask.common.logger import debug, info, is_debug_enabled, warning
from neotask.core.checkpoint import CHECKPOINT_DATA_KEY
from neotask.core.future import FutureManager
from neotask.core.signal import SIGNAL_DATA_KEY
from neotask.event.bus import EventBus, TaskEvent
from neotask.models.task import Task, TaskPriority, TaskStats, TaskStatus
from neotask.storage.base import CheckpointRepository, TaskRepository


#: 进度落库硬性最小间隔（秒）。同一任务两次进度写入间隔小于该值时跳过存储
#: 写入，但 ``task.progress`` 事件仍会正常推送，保证订阅方实时可见。
#: 该行为固定、不提供配置项，避免业务方高频上报把存储写放大。
PROGRESS_MIN_WRITE_INTERVAL = 0.5


class _DeferredRunningWrite:
    """一条延迟落库的 RUNNING 状态句柄

    - ``done``：任务在窗口内完成时置位，后台协程据此跳过这次 RUNNING 写入
    - ``force``：关闭时置位，即使 ``done`` 已置位也强制落库当前 RUNNING 状态
    - ``persisted``：这次延迟写入是否真的改写了存储（False = 存储仍是 PENDING）
    - ``flush_task``：后台协程，终态写入前会先等待它结束，保证写入顺序
    """

    __slots__ = ("task_obj", "done", "force", "persisted", "flush_task")

    def __init__(self, task: Task, done: asyncio.Event):
        self.task_obj = task
        self.done = done
        self.force = False
        self.persisted = False
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
            checkpoint_repo: Optional[CheckpointRepository] = None,
            checkpoint_keep_versions: int = 5,
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
        # checkpoint：None 表示功能未启用（save_checkpoint 返回 0，不抛异常）
        self._checkpoint_repo = checkpoint_repo
        self._checkpoint_keep_versions = max(0, int(checkpoint_keep_versions))


    async def create_task(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            node_id: str = "default",
            ttl: int = 3600,
            parent_task_id: Optional[str] = None,
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
            ttl=ttl,
            parent_task_id=parent_task_id,
        )

        if not await self._task_repo.save_if_absent(task):
            raise TaskAlreadyExistsError(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, task)

        await self._emit_event(EventType.CREATED, task_id, task.to_dict())

        return task

    async def create_task_and_enqueue(
            self,
            data: Dict[str, Any],
            max_size: int,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            node_id: str = "default",
            ttl: int = 3600,
            parent_task_id: Optional[str] = None,
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
            ttl=ttl,
            parent_task_id=parent_task_id,
        )

        queued = await self._task_repo.save_if_absent_and_enqueue(task, max_size)
        if queued is None and not await self._task_repo.save_if_absent(task):
            raise TaskAlreadyExistsError(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, task)

        await self._emit_event(EventType.CREATED, task_id, task.to_dict())

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
            await self._emit_event(EventType.CREATED, task.task_id, task.to_dict())
            created.append(task.task_id)

        return created

    async def delete_tasks(self, task_ids: List[str]) -> int:
        """批量删除任务（批量提交失败时的回滚手段）"""
        if not task_ids:
            return 0

        deleted = await self._task_repo.delete_batch(task_ids)
        await self._delete_checkpoints(task_ids)
        if self._cache_enabled:
            with self._lock:
                for task_id in task_ids:
                    self._cache.pop(task_id, None)
                    self._terminal_cache_set.discard(task_id)
        for task_id in task_ids:
            self._clear_progress_flush(task_id)
            await self._future_manager.remove(task_id)
        return deleted

    async def get_task(self, task_id: str, refresh: bool = False) -> Optional[Task]:
        """获取任务

        同一实例内的所有状态写入（start/complete/fail/cancel/update_status）都会
        同步刷新缓存，故缓存对本实例始终权威。跨实例场景（多进程/多节点共享
        同一存储）下缓存可能落后于其它实例写入的状态与 ``__signal__`` 数据，
        执行与唤醒判定需要最新数据时应传 ``refresh=True`` 回查存储。

        Args:
            task_id: 任务 ID
            refresh: 为 True 时绕过缓存直接读存储，并用读到的对象刷新缓存

        Returns:
            任务对象；不存在时返回 None
        """
        if self._cache_enabled and not refresh:
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
        """更新任务状态

        写入以读取到的旧状态为 CAS 条件：并发转换时只有一方生效，
        失败方不会覆盖对方的写入。
        """
        task = await self.get_task(task_id)
        if not task:
            return False

        old_status = task.status
        shadow = self._shadow_task(task)
        shadow.status = status

        # 更新额外字段
        fields = ["status"]
        for key, value in kwargs.items():
            if hasattr(shadow, key):
                setattr(shadow, key, value)
                fields.append(key)

        # 只落库发生变化的字段，避免重写整条任务记录
        if not await self._task_repo.save_fields_if_status(
                shadow, tuple(fields), old_status
        ):
            self._cache_discard(task_id)
            warning(
                f"Cannot update task {task_id}: status changed from "
                f"{old_status.value} by another executor"
            )
            return False

        if self._cache_enabled:
            self._cache_put(task_id, shadow)

        await self._emit_event(
            EventType.STATUS_CHANGED,
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
            EventType.PROGRESS,
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

    def _cache_discard(self, task_id: str) -> None:
        """丢弃本地缓存副本，强制下次读取回查存储

        状态转换的 CAS 失败说明存储已被其它实例改写，本地缓存不再权威；
        保留旧对象会让后续读写继续基于过期状态做判断。
        """
        if not self._cache_enabled:
            return
        with self._lock:
            self._cache.pop(task_id, None)
            self._terminal_cache_set.discard(task_id)

    @staticmethod
    def _shadow_task(task: Task) -> Task:
        """复制一份任务用于状态转换

        直接修改从缓存取出的对象会在 CAS 失败时把本地缓存改脏；``data`` 可能
        含嵌套的信号条目，必须深拷贝，其余字段在转换中只做整体赋值。
        """
        shadow = copy.copy(task)
        shadow.data = copy.deepcopy(task.data) if task.data else {}
        return shadow

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
            # 存储中仍是 PENDING：延迟窗口内状态可能已被其它实例改写（取消/超时）
            written = await self._task_repo.save_fields_if_status(
                task,
                ("status", "node_id", "started_at", "queue_wait_ms"),
                TaskStatus.PENDING,
            )
            handle.persisted = written
            if not written:
                self._cache_discard(task.task_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            warning(f"Persist running state for {task.task_id} failed: {exc}")
        finally:
            with self._lock:
                if self._deferred_running.get(task.task_id) is handle:
                    self._deferred_running.pop(task.task_id, None)

    async def _resolve_deferred_running(self, task_id: str) -> Optional[bool]:
        """终态写入前结束该任务的延迟 RUNNING 落库

        - 仍在窗口内：跳过写入（终态写入包含 ``started_at``）
        - 已开始落库：等待它结束，避免 RUNNING 写入晚于终态写入

        Returns:
            None: 没有延迟写入
            False: 延迟写入被跳过或失败，存储中仍是 PENDING
            True: 延迟写入已把状态改为 RUNNING
        """
        with self._lock:
            handle = self._deferred_running.get(task_id)
        if handle is None:
            return None

        handle.done.set()
        flush_task = handle.flush_task
        if flush_task is not None and not flush_task.done():
            try:
                await asyncio.shield(flush_task)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
        return handle.persisted

    async def _running_origin(
            self, task_id: str, observed: TaskStatus
    ) -> TaskStatus:
        """解析 RUNNING 状态转换的 CAS 期望值

        启用延迟 RUNNING 落库时，短任务在窗口内完成/挂起前存储里仍是 PENDING；
        此时 CAS 必须以 PENDING 为条件，否则永远匹配不到。其它来源的状态
        （超时的 WAITING、入队失败的 PENDING）不受影响。
        """
        persisted = await self._resolve_deferred_running(task_id)
        if observed == TaskStatus.RUNNING and persisted is False:
            return TaskStatus.PENDING
        return observed

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
                written = await self._task_repo.save_fields_if_status(
                    task,
                    ("status", "node_id", "started_at", "queue_wait_ms"),
                    TaskStatus.PENDING,
                )
                if written:
                    flushed += 1
                else:
                    self._cache_discard(task.task_id)
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
        """开始执行任务

        状态检查与写入由仓储 CAS 合并：同一任务被多个 Worker 同时领取时，
        只有第一个 PENDING → RUNNING 生效，其余返回 False 且不改动存储。
        """
        task = await self.get_task(task_id)
        if not task or task.status != TaskStatus.PENDING:
            warning(f"Cannot start task {task_id}: status={task.status if task else 'not found'}")
            return False

        shadow = self._shadow_task(task)
        shadow.start(node_id)
        if self._running_persist_delay > 0:
            # 延迟窗口内完成的任务只需写一次终态，先只更新内存
            self._defer_running_persist(shadow)
        else:
            # 只更新状态相关字段：data 等不可变字段无需重写
            try:
                started = await self._task_repo.save_fields_if_status(
                    shadow,
                    ("status", "node_id", "started_at", "queue_wait_ms"),
                    TaskStatus.PENDING,
                )
            except Exception:
                # 落库失败必须放弃本地 RUNNING：缓存里保持旧状态，任务被重新
                # 入队后再次启动会被状态机拒绝，最终永久卡死在 RUNNING。
                self._cache_discard(task_id)
                raise
            if not started:
                self._cache_discard(task_id)
                warning(
                    f"Cannot start task {task_id}: "
                    "status changed by another executor"
                )
                return False

        if self._cache_enabled:
            self._cache_put(task_id, shadow)


        await self._emit_event(EventType.STARTED, task_id, {"node_id": node_id})

        if is_debug_enabled():
            debug(f"Task {task_id} started on node {node_id}")
        return True

    @staticmethod
    def _merge_cost(task: Task, cost: Optional[Dict[str, Any]]) -> None:
        """把一次成本上报累加进任务（金额与明细均按字段求和）"""
        if not cost:
            return

        detail = dict(task.cost_tracking or {})
        for key in ("tokens_in", "tokens_out", "api_calls"):
            detail[key] = int(detail.get(key) or 0) + int(cost.get(key) or 0)
        task.estimated_cost = (
            float(task.estimated_cost or 0.0)
            + float(cost.get("estimated_cost") or 0.0)
        )

        currency = str(cost.get("currency") or "")
        current = str(detail.get("currency") or "")
        if currency:
            if not current:
                detail["currency"] = currency
            elif current != currency:
                detail["currency"] = "MIXED"
        task.cost_tracking = detail

    async def apply_cost(self, task_id: str, cost: Dict[str, Any]) -> bool:
        """把成本明细累加到任务并发布 ``task.cost_reported`` 事件

        终态任务同样接受追加成本：进程执行器的父进程通常在子任务结束后
        才拿到用量数据，此处不做 RUNNING 限制。
        """
        if not cost:
            return False

        task = await self.get_task(task_id)
        if not task:
            return False

        self._merge_cost(task, cost)
        await self._task_repo.save_fields(
            task, ("estimated_cost", "cost_tracking")
        )
        if self._cache_enabled:
            self._cache_put(task_id, task)

        await self._emit_event(EventType.COST_REPORTED, task_id, cost)
        return True

    # ========== checkpoint ==========

    @property
    def checkpoint_enabled(self) -> bool:
        """checkpoint 仓储是否已装配"""
        return self._checkpoint_repo is not None

    async def save_checkpoint(
            self, task_id: str, state: Dict[str, Any]
    ) -> int:
        """保存一个 checkpoint 版本并发布 ``task.checkpointed``

        Returns:
            新版本号；任务不存在或 checkpoint 未启用时返回 0
        """
        if self._checkpoint_repo is None:
            return 0

        task = await self.get_task(task_id)
        if not task:
            return 0

        version = int(task.checkpoint_version or 0) + 1
        await self._checkpoint_repo.save(task_id, version, state)

        task.checkpoint_version = version
        if not getattr(self._checkpoint_repo, "handles_task_marker", False):
            await self._task_repo.save_fields(task, ("checkpoint_version",))
        if self._cache_enabled:
            self._cache_put(task_id, task)

        # 只有累计版本数超过保留上限时才可能淘汰旧版本；窗口内跳过 prune，
        # 避免每次写入都多跑一次 DELETE 事务（SQLite 下约 0.65ms）
        keep = self._checkpoint_keep_versions
        if keep == 0 or version > keep:
            pruned = await self._checkpoint_repo.prune(task_id, keep)
            if pruned:
                debug(f"Pruned {pruned} old checkpoint(s) for task {task_id}")

        await self._emit_event(
            EventType.CHECKPOINTED,
            task_id,
            {"version": version},
        )
        return version

    async def load_checkpoint(self, task_id: str) -> Optional[Dict[str, Any]]:
        """读取任务最近一次 checkpoint 的状态；没有则返回 None"""
        if self._checkpoint_repo is None:
            return None
        record = await self._checkpoint_repo.load_latest(task_id)
        return record.state if record else None

    async def resume_from_checkpoint(self, task: Task) -> bool:
        """把最近一次 checkpoint 注入 ``task.data``（不落库）

        执行器通过 ``data[CHECKPOINT_DATA_KEY]`` 拿到恢复状态；调用方负责把
        ``data`` 与 ``checkpoint_version`` 一并写回存储。

        Returns:
            是否找到并注入了 checkpoint
        """
        if self._checkpoint_repo is None:
            return False
        record = await self._checkpoint_repo.load_latest(task.task_id)
        if record is None:
            return False
        task.data[CHECKPOINT_DATA_KEY] = record.state
        if record.version > task.checkpoint_version:
            task.checkpoint_version = record.version
        return True

    async def apply_checkpoint_resume(self, task: Task) -> bool:
        """恢复最近 checkpoint 并立即落库（供提交/恢复路径使用）

        Returns:
            是否找到并写入了 checkpoint
        """
        if not await self.resume_from_checkpoint(task):
            return False
        await self._task_repo.save_fields(
            task, ("data", "checkpoint_version")
        )
        if self._cache_enabled:
            self._cache_put(task.task_id, task)
        return True

    # ========== 人工介入（等待 / 信号） ==========

    @staticmethod
    def _signal_entry(task: Task) -> Optional[Dict[str, Any]]:
        entry = (task.data or {}).get(SIGNAL_DATA_KEY)
        return entry if isinstance(entry, dict) else None

    async def suspend_for_signal(
            self,
            task_id: str,
            signal: str,
            timeout: Optional[float] = None,
    ) -> bool:
        """把运行中的任务持久化为 ``waiting``，等待外部信号

        进入等待前确保存在可恢复的 checkpoint（Handler 未保存时以空状态占位），
        并记录等待键、序号、超时时刻，供 signal 唤醒与超时扫描使用。

        Returns:
            任务存在且处于 RUNNING 时返回 True
        """
        task = await self.get_task(task_id)
        if not task or task.status != TaskStatus.RUNNING:
            warning(
                f"Cannot suspend task {task_id}: "
                f"status={task.status.value if task else 'not found'}, expected RUNNING"
            )
            return False

        # 延迟落库的 RUNNING 写入必须在 WAITING 之前收尾，避免状态回退
        expected = await self._running_origin(task_id, TaskStatus.RUNNING)

        if self._checkpoint_repo is not None:
            if await self._checkpoint_repo.load_latest(task_id) is None:
                await self.save_checkpoint(task_id, {})

        previous = self._signal_entry(task)
        sequence = int(previous.get("sequence") or 0) + 1 if previous else 1
        timeout_at = time.time() + timeout if timeout and timeout > 0 else None

        shadow = self._shadow_task(task)
        shadow.data[SIGNAL_DATA_KEY] = {
            "signal": signal,
            "sequence": sequence,
            "payload": None,
            "arrived": False,
            "consumed": False,
            "timeout_at": timeout_at,
        }
        shadow.status = TaskStatus.WAITING

        if not await self._task_repo.save_fields_if_status(
                shadow,
                ("status", "data", "node_id", "started_at", "queue_wait_ms"),
                expected,
        ):
            self._cache_discard(task_id)
            warning(
                f"Cannot suspend task {task_id}: "
                "status changed by another executor"
            )
            return False
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, shadow)

        await self._emit_event(
            EventType.WAITING,
            task_id,
            {
                "old_status": TaskStatus.RUNNING.value,
                "signal": signal,
                "timeout_at": timeout_at,
            },
        )
        return True

    async def consume_signal(self, task_id: str, signal: str) -> bool:
        """标记当前信号已被 Handler 消费并从 ``task.data`` 清除 payload

        清除后同一等待键再次挂起不会命中旧信号；payload 已通过本次调用的
        返回值交付给业务代码。
        """
        task = await self.get_task(task_id)
        if not task:
            return False

        entry = self._signal_entry(task)
        if not entry or entry.get("signal") != signal or not entry.get("arrived"):
            return False

        entry["arrived"] = False
        entry["consumed"] = True
        entry["payload"] = None

        await self._task_repo.save_fields(task, ("data",))
        if self._cache_enabled:
            self._cache_put(task_id, task)
        return True

    async def signal_task(
            self,
            task_id: str,
            payload: Any = None,
    ) -> bool:
        """唤醒等待中的任务：写入 payload、转 ``pending`` 并发布事件

        只接受 ``waiting`` 状态；先到先得，重复 signal 或任务已结束返回 False。
        调用方负责把任务重新入队，入队失败时可调用 :meth:`revert_signal` 回滚。

        signal 可由任意节点发起：本实例缓存可能仍是任务创建/上次挂起前的
        副本（例如执行发生在其它节点），必须回查存储取得 WAITING 快照后再判定。
        """
        task = await self.get_task(task_id, refresh=True)
        if not task or task.status != TaskStatus.WAITING:
            return False

        shadow = self._shadow_task(task)
        entry = self._signal_entry(shadow)
        if not entry or entry.get("arrived"):
            return False

        signal = str(entry.get("signal") or "")
        entry["payload"] = payload
        entry["arrived"] = True

        shadow.status = TaskStatus.PENDING
        shadow.node_id = ""
        fields = ["status", "data", "node_id"]
        if await self.resume_from_checkpoint(shadow):
            fields.append("checkpoint_version")

        if not await self._task_repo.save_fields_if_status(
                shadow, tuple(fields), TaskStatus.WAITING
        ):
            # 其它实例已先一步唤醒（或已超时终结）该任务：本次 signal 不生效
            self._cache_discard(task_id)
            return False
        if self._cache_enabled:
            self._cache_put(task_id, shadow)

        await self._emit_event(
            EventType.SIGNAL_RECEIVED,
            task_id,
            {"signal": signal, "payload": payload},
        )
        return True

    async def revert_signal(self, task_id: str) -> bool:
        """signal 入队失败时回滚：撤销已到达标记并恢复 ``waiting``

        以 PENDING 为 CAS 条件：若任务已被 Worker 领取（RUNNING）或已被取消，
        说明 signal 实际生效，不能再回退成 WAITING。
        """
        task = await self.get_task(task_id)
        if not task or task.status != TaskStatus.PENDING:
            return False

        shadow = self._shadow_task(task)
        entry = self._signal_entry(shadow)
        if not entry or not entry.get("arrived"):
            return False

        entry["arrived"] = False
        entry["payload"] = None
        shadow.status = TaskStatus.WAITING

        if not await self._task_repo.save_fields_if_status(
                shadow, ("status", "data"), TaskStatus.PENDING
        ):
            self._cache_discard(task_id)
            return False
        if self._cache_enabled:
            self._cache_put(task_id, shadow)

        # 通知订阅方把 pending 计数挪回 waiting；只有真正回滚成功才发布
        await self._emit_event(
            EventType.SIGNAL_REVERTED,
            task_id,
            {"old_status": TaskStatus.PENDING.value},
        )
        return True

    async def fail_expired_waiting(self, now: Optional[float] = None) -> int:
        """把等待超时的任务置为 FAILED（``failure_reason="waiting_timeout"``）

        只向仓储要到期任务（索引/过滤由后端负责），先完整取回再统一失败，
        避免边扫描边改状态导致分页偏移。扫描与 signal 并发时以先完成落库
        的一方为准。
        """
        current = time.time() if now is None else now
        waiting: List[Task] = []
        offset = 0
        batch = 1000
        while True:
            tasks = await self._task_repo.list_expired_waiting(
                current, limit=batch, offset=offset
            )
            waiting.extend(tasks)
            if len(tasks) < batch:
                break
            offset += len(tasks)

        failed = 0
        for task in waiting:
            entry = self._signal_entry(task)
            if not entry or entry.get("arrived"):
                continue
            timeout_at = entry.get("timeout_at")
            if timeout_at is None or float(timeout_at) > current:
                continue
            signal = str(entry.get("signal") or "")
            if await self.fail_task(
                task.task_id,
                f"Timed out waiting for signal '{signal}'",
                allow_waiting=True,
                failure_reason="waiting_timeout",
                # 扫描可能在非执行节点上运行：直接用在存储中读到的快照判定，
                # 不能回读本实例缓存（可能仍是任务创建时的 PENDING 副本）
                snapshot=task,
            ):
                failed += 1
        return failed

    async def _delete_checkpoints(self, task_ids: List[str]) -> None:
        """删除任务时清理其 checkpoint（尽力而为，失败不影响删除主流程）"""
        if self._checkpoint_repo is None:
            return
        for task_id in task_ids:
            try:
                await self._checkpoint_repo.delete(task_id)
            except Exception as exc:
                debug(f"Failed to delete checkpoint for task {task_id}: {exc}")

    async def complete_task(
            self,
            task_id: str,
            result: Any,
            cost: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """完成任务

        Args:
            task_id: 任务ID
            result: 执行器返回值
            cost: 本次执行上报的成本明细；非空时随 ``task.completed`` 事件携带
        """
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
        expected = await self._running_origin(task_id, TaskStatus.RUNNING)

        shadow = self._shadow_task(task)
        fields = ["status", "started_at", "completed_at", "result", "progress", "duration_ms"]
        if cost:
            self._merge_cost(shadow, cost)
            fields.extend(("estimated_cost", "cost_tracking"))

        shadow.complete(result)
        # 成功完成即视为 100%，避免 UI 停留在最后一次业务上报的进度上。
        shadow.progress = 1.0

        if not await self._task_repo.save_fields_if_status(
                shadow, tuple(fields), expected
        ):
            # 状态已被其它实例改写（取消/超时）：不再覆盖终态，也不通知等待者
            self._cache_discard(task_id)
            warning(
                f"Cannot complete task {task_id}: "
                "status changed by another executor"
            )
            return False
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, shadow)


        # 通知等待者
        await self._future_manager.complete(task_id, result=result)

        # 无成本时保持「data 直接是 result」的既有契约；有成本时包一层信封
        event_data: Any = result
        if cost:
            event_data = {"result": result, "cost": cost}
        await self._emit_event(EventType.COMPLETED, task_id, event_data)

        if is_debug_enabled():
            debug(f"Task {task_id} completed successfully")
        return True

    async def fail_task(
            self,
            task_id: str,
            error: str,
            allow_pending: bool = False,
            failure_reason: Optional[str] = None,
            cost: Optional[Dict[str, Any]] = None,
            allow_waiting: bool = False,
            snapshot: Optional[Task] = None,
    ) -> bool:
        """任务失败

        正常情况下只有 RUNNING 状态的任务可以标记为失败。`allow_pending=True`
        用于入队失败的回滚：任务已落库为 PENDING 但从未进入队列，只能在这里终结。
        `allow_waiting=True` 用于等待超时扫描终结 WAITING 任务。

        Args:
            task_id: 任务ID
            error: 失败信息
            allow_pending: 是否允许终结尚未开始的任务
            failure_reason: 业务级失败原因（如 ``qa_failed``）。系统异常时留空，
                两者靠该字段区分，状态同为 ``FAILED``。
            cost: 本次执行上报的成本明细；非空时随 ``task.failed`` 事件携带
            allow_waiting: 是否允许终结等待中的任务（等待超时）
            snapshot: 调用方已从存储读到的最新任务快照（如超时扫描）。
                给出时直接用它判定，避免本实例缓存落后于其它节点的写入。
        """
        task = snapshot if snapshot is not None else await self.get_task(task_id)

        # 检查任务是否存在
        if not task:
            return False

        # 检查是否已经是终态
        if task.status.is_terminal():
            warning(f"Cannot fail task {task_id}: already in terminal state {task.status.value}")
            return False

        allowed = [TaskStatus.RUNNING]
        if allow_pending:
            allowed.append(TaskStatus.PENDING)
        if allow_waiting:
            allowed.append(TaskStatus.WAITING)
        if task.status not in allowed:
            expected_text = " / ".join(status.value.upper() for status in allowed)
            warning(
                f"Cannot fail task {task_id}: status={task.status.value}, "
                f"expected {expected_text}"
            )
            return False

        if is_debug_enabled():
            debug(f"[DEBUG] Failing task {task_id}: {error}")

        old_status = task.status
        expected = await self._running_origin(task_id, old_status)
        shadow = self._shadow_task(task)
        fields = ["status", "started_at", "error", "completed_at", "failure_reason", "duration_ms"]
        if cost:
            self._merge_cost(shadow, cost)
            fields.extend(("estimated_cost", "cost_tracking"))

        shadow.fail(error, failure_reason)
        if not await self._task_repo.save_fields_if_status(
                shadow, tuple(fields), expected
        ):
            # 超时扫描与 signal/完成并发时，先落库的一方生效，这里放弃终结
            self._cache_discard(task_id)
            warning(
                f"Cannot fail task {task_id}: "
                "status changed by another executor"
            )
            return False
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, shadow)


        # 通知等待者 - 确保错误被传递
        await self._future_manager.complete(task_id, error=error)

        await self._emit_event(
            EventType.FAILED,
            task_id,
            {
                "error": error,
                "old_status": old_status.value,
                "reason": shadow.failure_reason,
                "cost": cost,
            }
        )

        return True


    async def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        task = await self.get_task(task_id)
        if not task:
            return False

        # 待处理、运行中与等待中的任务都可以取消
        if task.status not in (
            TaskStatus.PENDING,
            TaskStatus.RUNNING,
            TaskStatus.WAITING,
        ):
            return False

        old_status = task.status
        expected = await self._running_origin(task_id, old_status)
        shadow = self._shadow_task(task)
        shadow.cancel()
        if not await self._task_repo.save_fields_if_status(
                shadow,
                ("status", "started_at", "completed_at", "duration_ms"),
                expected,
        ):
            self._cache_discard(task_id)
            warning(
                f"Cannot cancel task {task_id}: "
                "status changed by another executor"
            )
            return False
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, shadow)


        # 通知等待者
        await self._future_manager.complete(task_id, error="Task cancelled")

        await self._emit_event(
            EventType.CANCELLED,
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
        shadow = self._shadow_task(task)
        shadow.reset_for_retry()

        # 手动重试同样从最近一次 checkpoint 恢复（data 与标记一起写回）
        fields = [
            "status", "started_at", "completed_at", "result", "error",
            "node_id", "progress", "progress_message",
            "queue_wait_ms", "duration_ms", "data", "checkpoint_version",
        ]
        await self.resume_from_checkpoint(shadow)

        if not await self._task_repo.save_fields_if_status(
                shadow, tuple(fields), old_status
        ):
            # 终态被别人先重置（或已删除）时不再重复重试
            self._cache_discard(task_id)
            return False
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            self._cache_put(task_id, shadow)


        await self._future_manager.remove(task_id)

        await self._emit_event(
            EventType.RETRY,
            task_id,
            {
                "retry_count": shadow.retry_count,
                "manual": True,
                "old_status": old_status.value,
            }
        )

        debug(f"Task {task_id} reset to PENDING for manual retry")
        return True

    async def prepare_retry(
            self,
            task_id: str,
            retry_count: int,
            error: str,
    ) -> bool:
        """自动重试前把 RUNNING 任务重置为 PENDING

        与 :meth:`reset_for_retry` 的区别是这里面对的是仍在 RUNNING 的任务：
        只写重试计数、错误信息与状态，保留 started_at 等执行痕迹，由下一次
        执行覆盖。以 RUNNING 为 CAS 条件，任务已被取消/超时时不再复活。

        Returns:
            是否成功（状态已被其它执行者改写时返回 False）
        """
        task = await self.get_task(task_id)
        if not task or task.status != TaskStatus.RUNNING:
            return False

        # 延迟的 RUNNING 落库必须先收尾，否则它会在 PENDING 写入之后再次生效
        expected = await self._running_origin(task_id, TaskStatus.RUNNING)

        shadow = self._shadow_task(task)
        shadow.retry_count = retry_count
        shadow.error = error
        shadow.status = TaskStatus.PENDING
        shadow.node_id = ""

        fields = ["retry_count", "error", "status", "node_id"]
        # 自动重试同样续跑：把最近 checkpoint 注入 task.data 并落库
        if await self.resume_from_checkpoint(shadow):
            fields.extend(("data", "checkpoint_version"))

        if not await self._task_repo.save_fields_if_status(
                shadow, tuple(fields), expected
        ):
            self._cache_discard(task_id)
            return False
        if self._cache_enabled:
            self._cache_put(task_id, shadow)
        return True

    async def delete_task(self, task_id: str) -> bool:
        """删除任务"""
        await self._task_repo.delete(task_id)
        await self._delete_checkpoints([task_id])
        self._clear_progress_flush(task_id)

        if self._cache_enabled:
            with self._lock:
                self._cache.pop(task_id, None)
                self._terminal_cache_set.discard(task_id)

        await self._future_manager.remove(task_id)

        await self._emit_event(EventType.DELETED, task_id)

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
                    elif task.status == TaskStatus.WAITING:
                        stats.waiting += 1
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

        支持所有存储后端（内存、SQLite、Redis）。各后端按状态做计数查询，
        不加载任务实体，任务量增长不会放大单次统计的开销。
        """
        counts = {}
        for status in TaskStatus:
            counts[status] = await self._task_repo.count_by_status(status)

        stats = TaskStats()
        stats.total = sum(counts.values())
        stats.pending = counts.get(TaskStatus.PENDING, 0)
        stats.running = counts.get(TaskStatus.RUNNING, 0)
        stats.waiting = counts.get(TaskStatus.WAITING, 0)
        stats.completed = counts.get(TaskStatus.SUCCESS, 0)
        stats.failed = counts.get(TaskStatus.FAILED, 0)
        stats.cancelled = counts.get(TaskStatus.CANCELLED, 0)
        return stats

    async def cleanup_terminal(self, max_age_seconds: Optional[int] = None) -> int:
        """删除已过保留期的终态任务并同步本地缓存与等待器

        Args:
            max_age_seconds: None/0 时按任务自身 ttl；大于 0 时使用统一保留时长

        Returns:
            清理的任务数量
        """
        deleted_ids = await self._task_repo.delete_expired_terminal(max_age_seconds)
        await self._delete_checkpoints(deleted_ids)

        for task_id in deleted_ids:
            if self._cache_enabled:
                with self._lock:
                    self._cache.pop(task_id, None)
                    self._terminal_cache_set.discard(task_id)
            self._clear_progress_flush(task_id)
            await self._future_manager.remove(task_id)

        if deleted_ids:
            info(f"Cleaned {len(deleted_ids)} expired tasks")

        return len(deleted_ids)

    async def cleanup_expired(self) -> int:
        """清理超过任务自身 ttl 的终态任务（兼容旧接口）"""
        return await self.cleanup_terminal(None)

    async def cleanup_expired_by_time(self, max_age_seconds: int = 86400) -> int:
        """按统一保留时长清理终态任务（兼容旧接口）

        Args:
            max_age_seconds: 最大存活时间（秒），默认 24 小时

        Returns:
            清理的任务数量
        """
        return await self.cleanup_terminal(max_age_seconds)

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
