"""
@FileName: dispatcher.py
@Description: 任务分发器 - 负责将任务放入队列
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

from typing import Dict, Any, Optional, Union, List
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.queue.queue_scheduler import QueueScheduler
from neotask.models.task import TaskPriority
from neotask.common.exceptions import QueueFullError
from neotask.common.logger import debug


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
        ttl: int = 3600
    ) -> str:
        """分发任务

        Args:
            data: 任务数据
            task_id: 任务ID（可选）
            priority: 优先级
            delay: 延迟执行时间（秒）
            ttl: 任务超时时间（秒）

        Returns:
            task_id
        """
        # 确保 priority 是 TaskPriority 枚举
        if isinstance(priority, int):
            priority_enum = TaskPriority(priority)
        else:
            priority_enum = priority

        debug(f"Dispatching task: task_id={task_id}, priority={priority_enum.value}, delay={delay}")

        # 创建任务
        task = await self._lifecycle.create_task(
            data=data,
            task_id=task_id,
            priority=priority_enum,
            node_id=self._node_id,
            ttl=ttl
        )

        # 获取优先级整数值用于队列
        priority_value = task.priority.value

        # 如果是延迟任务，使用延迟队列
        if delay > 0:
            queued = await self._queue.schedule_delayed(task.task_id, priority_value, delay)
        else:
            # 立即入队
            queued = await self._queue.push(task.task_id, priority_value)

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
        priority: Union[int, TaskPriority] = TaskPriority.NORMAL
    ) -> List[str]:
        """批量分发任务"""
        task_ids = []
        for task_data in tasks:
            task_id = await self.dispatch(task_data, priority=priority)
            task_ids.append(task_id)
        return task_ids

    async def redispatch(self, task_id: str, delay: float = 0) -> bool:
        """重新分发任务（用于重试）"""
        task = await self._lifecycle.get_task(task_id)
        if not task:
            return False

        # 获取优先级整数值
        priority_value = task.priority.value

        if delay > 0:
            return await self._queue.schedule_delayed(task_id, priority_value, delay)
        return await self._queue.push(task_id, priority_value)
