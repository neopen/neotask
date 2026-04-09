"""
@FileName: dispatcher.py
@Description: 任务分发器 - 负责将任务放入队列
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

from typing import Dict, Any, Optional, Union, List
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.queue.scheduler import QueueScheduler
from neotask.models.task import TaskPriority


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
    ):
        """分发任务

        Args:
            data: 任务数据
            task_id: 任务ID（可选）
            priority: 优先级
            delay: 延迟执行时间（秒）
            ttl: 任务超时时间（秒）

        Returns:
            创建的Task对象或task_id
        """
        # 转换优先级
        if isinstance(priority, int):
            priority_enum = TaskPriority.from_value(priority)
        else:
            priority_enum = priority

        # 创建任务
        task = await self._lifecycle.create_task(
            data=data,
            task_id=task_id,
            priority=priority_enum,
            node_id=self._node_id,
            ttl=ttl
        )

        # 如果是延迟任务，使用延迟队列
        if delay > 0:
            # 使用 push 方法传入 delay 参数
            await self._queue.push(task.task_id, priority_enum.value, delay)
        else:
            # 立即入队
            await self._queue.push(task.task_id, priority_enum.value)

        return task

    async def dispatch_batch(
        self,
        tasks: List[Dict[str, Any]],
        priority: Union[int, TaskPriority] = TaskPriority.NORMAL
    ) -> List[str]:
        """批量分发任务"""
        task_ids = []
        for task_data in tasks:
            task = await self.dispatch(task_data, priority=priority)
            task_ids.append(task.task_id if hasattr(task, 'task_id') else task)
        return task_ids

    async def redispatch(self, task_id: str, delay: float = 0) -> bool:
        """重新分发任务（用于重试）"""
        task = await self._lifecycle.get_task(task_id)
        if not task:
            return False

        if delay > 0:
            await self._queue.push(task_id, task.priority.value, delay)
        else:
            await self._queue.push(task_id, task.priority.value)

        return True