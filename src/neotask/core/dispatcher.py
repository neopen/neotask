"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: dispatcher.py
@Description: 任务分发器 - 负责将任务放入队列
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

from typing import Dict, Any, Optional, Union, List
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.queue.queue_scheduler import QueueScheduler
from neotask.models.task import TaskPriority, TaskStatus, BatchSubmitResult
from neotask.common.exceptions import QueueFullError, BatchSubmitError
from neotask.common.logger import debug, warning


class TaskDispatcher:
    """任务分发器

    职责：
    - 接收任务提交
    - 创建任务记录
    - 将任务放入队列
    - 处理延迟任务

    设计模式：Facade Pattern - 封装生命周期和队列
    """

    def __init__(
        self,
        lifecycle_manager: TaskLifecycleManager,
        queue_scheduler: QueueScheduler,
        node_id: str = "default"
    ):
        self._lifecycle = lifecycle_manager
        self._queue = queue_scheduler
        self._node_id = node_id

    async def dispatch(
        self,
        data: Dict[str, Any],
        task_id: Optional[str] = None,
        priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
        delay: float = 0,
        ttl: int = 3600,
        parent_task_id: Optional[str] = None,
    ) -> str:
        """分发任务

        Args:
            data: 任务数据
            task_id: 任务ID（可选）
            priority: 优先级
            delay: 延迟执行时间（秒）
            ttl: 任务超时时间（秒）
            parent_task_id: 父任务 ID（仅建立关联，不做自动汇总）

        Returns:
            task_id
        """
        # 确保 priority 是 TaskPriority 枚举（越界整数按 NORMAL 处理，不抛异常）
        if isinstance(priority, int):
            priority_enum = TaskPriority.from_value(priority)
        else:
            priority_enum = priority

        debug(f"Dispatching task: task_id={task_id}, priority={priority_enum.value}, delay={delay}")

        # 创建任务并入队：即时任务优先走「存储层原子创建 + 入队」快路径
        # （Redis 用单条 Lua，提交只需 1 次跨 loop 往返）
        if delay > 0:
            task = await self._lifecycle.create_task(
                data=data,
                task_id=task_id,
                priority=priority_enum,
                node_id=self._node_id,
                ttl=ttl,
                parent_task_id=parent_task_id,
            )
            queued = await self._queue.schedule_delayed(
                task.task_id, task.priority.value, delay
            )
        else:
            if self._queue.is_disabled:
                # 队列已禁用：Redis 原子脚本无法感知该状态，走普通创建后统一失败处理
                task = await self._lifecycle.create_task(
                    data=data,
                    task_id=task_id,
                    priority=priority_enum,
                    node_id=self._node_id,
                    ttl=ttl,
                    parent_task_id=parent_task_id,
                )
                queued = False
            else:
                task, queued = await self._lifecycle.create_task_and_enqueue(
                    data=data,
                    max_size=self._queue.max_size,
                    task_id=task_id,
                    priority=priority_enum,
                    node_id=self._node_id,
                    ttl=ttl,
                    parent_task_id=parent_task_id,
                )
                if queued is None:
                    # 后端不支持合并（memory / SQLite）：按原两步流程入队
                    queued = await self._queue.push(task.task_id, task.priority.value)

        if not queued:
            reason = "queue is disabled" if self._queue.is_disabled else "queue is full"
            await self._lifecycle.fail_task(
                task.task_id,
                f"Failed to enqueue task: {reason}",
                allow_pending=True
            )
            raise QueueFullError(self._queue.max_size)

        debug(f"Task {task.task_id} dispatched successfully")
        return task.task_id

    async def dispatch_batch(
        self,
        tasks: List[Dict[str, Any]],
        priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
        partial_on_error: bool = False
    ) -> Union[List[str], BatchSubmitResult]:
        """批量分发任务（默认全有或全无）

        Args:
            tasks: 任务数据列表
            priority: 优先级
            partial_on_error: ``False``（默认）时任一任务创建或入队失败都会
                回滚整批并抛出 :class:`BatchSubmitError`；``True`` 时允许部分
                成功，返回 :class:`BatchSubmitResult`。

        Returns:
            ``partial_on_error=False`` 时返回全部成功的 task_id 列表；
            ``True`` 时返回包含成功/失败明细的 :class:`BatchSubmitResult`。
        """
        return await self.dispatch_batch_atomic(
            tasks, priority=priority, partial_on_error=partial_on_error
        )

    async def dispatch_batch_atomic(
        self,
        items: List[Dict[str, Any]],
        priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
        ttl: int = 3600,
        partial_on_error: bool = False,
    ) -> Union[List[str], BatchSubmitResult]:
        """批量分发即时任务，默认整批原子提交。

        实现要点：
        1. 先按队列剩余容量预检（``partial_on_error=True`` 时只提交容量允许的部分）
        2. 任务记录一次性写入（SQLite 单事务 / Memory 全量构造后一次写入）
        3. 入队同样批量写入；默认模式下若仍有任务被拒，则撤销已入队任务并
           删除已写入记录，保证调用方看不到半批数据
        """
        if not items:
            return BatchSubmitResult() if partial_on_error else []

        priority_enum = TaskPriority(priority) if isinstance(priority, int) else priority
        reason = "queue is disabled" if self._queue.is_disabled else "queue is full"

        result = BatchSubmitResult()
        remaining = self._queue.max_size - await self._queue.size()
        allowed_items = items
        if remaining < len(items):
            if not partial_on_error:
                raise BatchSubmitError(
                    f"Batch submit aborted: {len(items)} task(s) requested, "
                    f"only {max(remaining, 0)} slot(s) available ({reason})",
                    index=-1,
                    cause=QueueFullError(self._queue.max_size),
                )
            allowed_items = items[:max(remaining, 0)]
            result.failed.extend(
                {"index": index, "data": items[index], "error": reason}
                for index in range(len(allowed_items), len(items))
            )

        if not allowed_items:
            return result

        try:
            tasks = self._lifecycle.build_tasks(
                data_list=allowed_items,
                priority=priority_enum,
                node_id=self._node_id,
                ttl=ttl,
            )
            task_ids = await self._lifecycle.persist_tasks(tasks)
        except Exception as exc:
            if partial_on_error:
                result.failed = [
                    {"index": index, "data": data, "error": str(exc)}
                    for index, data in enumerate(allowed_items)
                ] + result.failed
                return result
            raise BatchSubmitError(
                f"Batch submit aborted while persisting tasks: {exc}",
                index=-1,
                cause=exc,
            ) from exc

        queue_results = await self._queue.push_batch(
            [(task_id, priority_enum.value, 0) for task_id in task_ids]
        )
        accepted = [tid for tid, ok in zip(task_ids, queue_results) if ok]
        rejected = [tid for tid, ok in zip(task_ids, queue_results) if not ok]

        if rejected and not partial_on_error:
            # 回滚：撤销已入队任务，再删除整批任务记录，确保“全无”
            for task_id in accepted:
                await self._queue.remove(task_id)
            await self._lifecycle.delete_tasks(task_ids)
            raise BatchSubmitError(
                f"Batch submit aborted: {len(rejected)} task(s) rejected by queue "
                f"({reason}); the whole batch was rolled back",
                index=len(accepted),
                cause=QueueFullError(self._queue.max_size),
            )

        if rejected:
            # 部分成功：未入队的记录直接删除，不留下“库里存在但永不执行”的任务
            rejected_set = set(rejected)
            await self._lifecycle.delete_tasks(rejected)
            for index, (task_id, data) in enumerate(zip(task_ids, allowed_items)):
                if task_id in rejected_set:
                    result.failed.append(
                        {"index": index, "data": data, "error": reason}
                    )

        result.succeeded = accepted
        if partial_on_error:
            # 容量溢出部分保持失败明细，与已创建任务的失败合并
            result.failed.sort(key=lambda item: item.get("index", 0))
            return result
        return accepted

    async def dispatch_many(
        self,
        items: List[Dict[str, Any]],
        priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
        ttl: int = 3600,
    ) -> List[str]:
        """批量分发即时任务。

        与逐条 :meth:`dispatch` 的关键区别：任务创建与入队各只做一次批量写入，
        避免每条任务 3 次跨事件循环往返（SELECT 查重 + 2 次写事务）。

        Args:
            items: 任务数据列表
            priority: 优先级
            ttl: 任务超时时间（秒）

        Returns:
            实际提交成功的 task_id 列表；若队列容量不足，未能入队的任务会被
            标记为 FAILED 并记录 warning（不抛异常，批量接口按可用容量尽力提交）。
        """
        if not items:
            return []

        priority_enum = TaskPriority(priority) if isinstance(priority, int) else priority

        # 先按容量预检，避免写完库才发现入不了队（队列在并发下仍可能被占满，
        # 故下面仍按 push_batch 的返回值做最终判定）
        remaining = self._queue.max_size - await self._queue.size()
        if remaining < len(items):
            raise QueueFullError(self._queue.max_size)

        task_ids = await self._lifecycle.create_tasks(
            data_list=items,
            priority=priority_enum,
            node_id=self._node_id,
            ttl=ttl,
        )
        if not task_ids:
            return []

        results = await self._queue.push_batch(
            [(task_id, priority_enum.value, 0) for task_id in task_ids]
        )

        accepted = [tid for tid, ok in zip(task_ids, results) if ok]
        rejected = [tid for tid, ok in zip(task_ids, results) if not ok]
        if rejected:
            reason = "queue is disabled" if self._queue.is_disabled else "queue is full"
            warning(f"{len(rejected)} task(s) rejected by queue: {reason}")
            for task_id in rejected:
                await self._lifecycle.fail_task(
                    task_id,
                    f"Failed to enqueue task: {reason}",
                    allow_pending=True
                )

        return accepted

    async def redispatch(self, task_id: str, delay: float = 0) -> bool:
        """重新分发任务（用于手动重试）

        终态（failed / cancelled）任务需要先重置为 PENDING 才能再次入队，否则
        Worker 出队时会因状态不是 PENDING 而直接跳过。已在队列中的 PENDING 任务
        不会重复入队；RUNNING / SUCCESS 任务不允许重试，避免重复执行或覆盖结果。
        """
        task = await self._lifecycle.get_task(task_id)
        if not task:
            return False

        if task.status.is_terminal():
            if not await self._lifecycle.reset_for_retry(task_id):
                return False
        elif task.status != TaskStatus.PENDING:
            warning(
                f"Cannot retry task {task_id}: status={task.status.value}, "
                f"expected FAILED or CANCELLED"
            )
            return False
        elif await self._queue.contains(task_id):
            # 任务已在队列中排队，重复入队只会让它被执行多次
            debug(f"Task {task_id} already queued, skip duplicate redispatch")
            return True

        # 获取优先级整数值
        priority_value = task.priority.value

        if delay > 0:
            return await self._queue.schedule_delayed(task_id, priority_value, delay)
        return await self._queue.push(task_id, priority_value)
