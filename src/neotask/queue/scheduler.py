"""
@FileName: scheduler.py
@Description: 队列调度器 - 整合优先级队列和延迟队列
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import time
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from neotask.queue.delayed_queue import DelayedQueue
from neotask.queue.priority_queue import PriorityQueue
from neotask.storage.base import QueueRepository


@dataclass
class QueueSchedulerStats:
    """队列调度器统计信息"""
    priority_queue_size: int = 0
    delayed_queue_size: int = 0
    total_size: int = 0
    is_paused: bool = False
    is_disabled: bool = False
    max_size: int = 10000
    usage_ratio: float = 0.0


class QueueScheduler:
    """队列调度器

    整合优先级队列和延迟队列，提供统一的调度接口。

    设计模式：Facade Pattern - 封装优先级队列和延迟队列
    """

    def __init__(
            self,
            queue_repo: Optional[QueueRepository] = None,
            max_size: int = 10000
    ):
        self._priority_queue = PriorityQueue(queue_repo, max_size)
        self._delayed_queue = DelayedQueue()
        self._max_size = max_size
        self._paused = False
        self._disabled = False
        self._started = False

    async def start(self) -> None:
        """启动调度器"""
        if self._started:
            return
        self._started = True
        await self._delayed_queue.start(self._on_delayed_task_ready)

    async def stop(self) -> None:
        """停止调度器"""
        self._started = False
        await self._delayed_queue.stop()

    async def push(self, task_id: str, priority: int, delay: float = 0) -> bool:
        """推送任务到队列

        Args:
            task_id: 任务ID
            priority: 优先级（数字越小优先级越高）
            delay: 延迟执行时间（秒）

        Returns:
            是否成功入队
        """
        if self._disabled:
            return False

        current_size = await self.size()
        if current_size >= self._max_size:
            return False

        if delay > 0:
            return await self._delayed_queue.schedule(task_id, priority, delay)
        else:
            return await self._priority_queue.push(task_id, priority)

    async def push_batch(
            self,
            tasks: List[tuple],  # (task_id, priority, delay)
    ) -> List[bool]:
        """批量推送任务

        Args:
            tasks: 任务列表，每个元素为 (task_id, priority, delay)

        Returns:
            成功标志列表
        """
        results = []
        for task_id, priority, delay in tasks:
            result = await self.push(task_id, priority, delay)
            results.append(result)
        return results

    async def pop(self, count: int = 1) -> List[str]:
        """弹出任务"""
        if self._paused or self._disabled:
            return []

        return await self._priority_queue.pop(count)

    async def pop_with_priority(self, count: int = 1) -> List[tuple]:
        """弹出任务并返回优先级"""
        if self._paused or self._disabled:
            return []

        return await self._priority_queue.pop_with_priority(count)

    async def remove(self, task_id: str) -> bool:
        """从队列移除任务"""
        # 先从优先级队列移除
        removed = await self._priority_queue.remove(task_id)
        if not removed:
            # 再从延迟队列移除
            removed = await self._delayed_queue.cancel(task_id)
        return removed

    async def size(self) -> int:
        """获取队列总大小"""
        priority_size = await self._priority_queue.size()
        delayed_size = await self._delayed_queue.size()
        return priority_size + delayed_size

    async def priority_size(self) -> int:
        """获取优先级队列大小"""
        return await self._priority_queue.size()

    async def delayed_size(self) -> int:
        """获取延迟队列大小"""
        return await self._delayed_queue.size()

    async def schedule_delayed(self, task_id: str, priority: int, delay: float) -> None:
        """调度延迟任务（别名方法）"""
        await self.push(task_id, priority, delay)

    async def is_empty(self) -> bool:
        """检查队列是否为空"""
        return await self.size() == 0

    async def clear(self) -> None:
        """清空队列"""
        await self._priority_queue.clear()
        await self._delayed_queue.clear()

    async def contains(self, task_id: str) -> bool:
        """检查任务是否在队列中"""
        if await self._priority_queue.contains(task_id):
            return True
        return await self._delayed_queue.contains(task_id)

    async def pause(self) -> None:
        """暂停出队（但可以继续入队）"""
        self._paused = True

    async def resume(self) -> None:
        """恢复出队"""
        self._paused = False

    async def disable(self) -> None:
        """禁用入队"""
        self._disabled = True

    async def enable(self) -> None:
        """启用入队"""
        self._disabled = False

    async def wait_until_empty(self, timeout: float = 300) -> bool:
        """等待队列清空

        Args:
            timeout: 超时时间（秒）

        Returns:
            是否在超时前清空
        """
        start = time.time()
        while time.time() - start < timeout:
            if await self.is_empty():
                return True
            await asyncio.sleep(0.5)
        return False

    async def get_stats(self) -> QueueSchedulerStats:
        """获取统计信息"""
        priority_size = await self._priority_queue.size()
        delayed_size = await self._delayed_queue.size()
        total_size = priority_size + delayed_size

        return QueueSchedulerStats(
            priority_queue_size=priority_size,
            delayed_queue_size=delayed_size,
            total_size=total_size,
            is_paused=self._paused,
            is_disabled=self._disabled,
            max_size=self._max_size,
            usage_ratio=total_size / self._max_size if self._max_size > 0 else 0
        )

    async def get_detailed_stats(self) -> Dict[str, Any]:
        """获取详细统计信息"""
        priority_stats = await self._priority_queue.get_stats()
        delayed_stats = await self._delayed_queue.get_stats()

        return {
            "priority_queue": priority_stats,
            "delayed_queue": delayed_stats,
            "paused": self._paused,
            "disabled": self._disabled,
            "max_size": self._max_size,
            "started": self._started
        }

    async def _on_delayed_task_ready(self, task_id: str, priority: int) -> None:
        """延迟任务到期回调"""
        if not self._disabled:
            await self._priority_queue.push(task_id, priority)

    @property
    def is_paused(self) -> bool:
        """检查是否暂停"""
        return self._paused

    @property
    def is_disabled(self) -> bool:
        """检查是否禁用"""
        return self._disabled

    @property
    def is_started(self) -> bool:
        """检查是否已启动"""
        return self._started
