"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: queue_scheduler.py
@Description: 队列调度器 - 整合优先级队列和延迟队列
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

import asyncio
import time
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from neotask.common.logger import debug, error
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
        # 上一次 shutdown 会 disable 队列，重启时需要恢复接收新任务
        self._disabled = False

        # 包装回调以匹配 delayed_queue 期望的签名 (task_id, priority, data)
        async def wrapped_callback(task_id: str, priority: int, data: Dict = None):
            await self._on_delayed_task_ready(task_id, priority, data)

        await self._delayed_queue.start(wrapped_callback)

    async def _on_delayed_task_ready(self, task_id: str, priority: int, data: Dict = None) -> None:
        """延迟任务到期回调"""
        debug(f"[QUEUE_SCHEDULER] Delayed task ready: {task_id}, priority={priority}")
        if self._disabled:
            return
        if not await self._priority_queue.push(task_id, priority):
            error(f"Delayed task {task_id} could not be promoted: priority queue is full")

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
        debug(f"[QUEUE_SCHEDULER] push: task_id={task_id}, priority={priority}, delay={delay}")
        if self._disabled:
            return False

        current_size = await self.size()
        if current_size >= self._max_size:
            return False

        if delay > 0:
            return await self._delayed_queue.schedule(task_id, priority, delay)
        else:
            # 容量已在上面查过，避免 PriorityQueue 再查一次 size()
            return await self._priority_queue.push(
                task_id, priority, check_capacity=False
            )

    async def re_enqueue(self, task_id: str, priority: int) -> bool:
        """把已出队但尚未执行的任务放回优先队列（停机回滚用）

        与 ``push`` 的区别：停机时队列已 ``disable()``（不再接收新任务），但
        预取器本地队列里的任务是**已经出队**的，放回属于撤销出队而不是新任务，
        必须放行；否则这些任务会变成孤儿，只能等 ttl 兜底重排。

        Args:
            task_id: 任务ID
            priority: 任务原始优先级

        Returns:
            是否成功放回
        """
        if await self.size() >= self._max_size:
            return False
        return await self._priority_queue.push(task_id, priority, check_capacity=False)

    async def re_enqueue(self, task_id: str, priority: int) -> bool:
        """把已出队但尚未执行的任务放回优先队列（停机回滚用）

        与 ``push`` 的区别：停机时队列已 ``disable()``（不再接收新任务），但
        预取器本地队列里的任务是**已经出队**的，放回属于撤销出队而不是新任务，
        必须放行；否则这些任务会变成孤儿，只能等 ttl 兜底重排。

        Args:
            task_id: 任务ID
            priority: 任务原始优先级

        Returns:
            是否成功放回
        """
        if await self.size() >= self._max_size:
            return False
        return await self._priority_queue.push(task_id, priority)

    async def push_batch(
            self,
            tasks: List[tuple],  # (task_id, priority, delay)
    ) -> List[bool]:
        """批量推送任务

        与逐条 ``push`` 的区别：容量只检查一次、即时任务一次批量写入仓储，
        避免每条任务都做一次 ``size()`` 查询和一次跨事件循环往返。延迟任务
        仍逐条调度（每个任务有独立的到期时间）。

        Args:
            tasks: 任务列表，每个元素为 (task_id, priority, delay)

        Returns:
            成功标志列表（与输入顺序一致）
        """
        results = [False] * len(tasks)
        if self._disabled or not tasks:
            return results

        current_size = await self.size()
        available = max(0, self._max_size - current_size)

        immediate: List[tuple] = []  # (index, task_id, priority)
        for index, (task_id, priority, delay) in enumerate(tasks):
            if delay > 0:
                results[index] = await self._delayed_queue.schedule(task_id, priority, delay)
            elif len(immediate) < available:
                immediate.append((index, task_id, priority))

        if immediate:
            pushed = await self._priority_queue.push_batch(
                [(task_id, priority) for _, task_id, priority in immediate]
            )
            for index, _, _ in immediate[:pushed]:
                results[index] = True

        return results

    async def pop(self, count: int = 1) -> List[str]:
        """弹出任务

        注意：``disable()`` 只表示停止接收新任务（禁止入队），优雅关闭时仍需
        把已排队的 pending 任务出队执行完，因此出队只受 ``pause`` 约束，不受
        ``disable`` 约束。
        """
        debug(f"[QUEUE_SCHEDULER] pop called, paused={self._paused}, disabled={self._disabled}")  # 调试
        if self._paused:
            return []

        return await self._priority_queue.pop(count)

    async def pop_with_priority(self, count: int = 1) -> List[tuple]:
        """弹出任务并返回优先级（同 pop，不受 disable 约束）"""
        if self._paused:
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
        """获取队列总大小（包括正在处理的任务？）"""
        priority_size = await self._priority_queue.size()
        delayed_size = await self._delayed_queue.size()
        return priority_size + delayed_size

    async def priority_size(self) -> int:
        """获取优先级队列大小"""
        return await self._priority_queue.size()

    async def delayed_size(self) -> int:
        """获取延迟队列大小"""
        return await self._delayed_queue.size()

    async def drain_delayed(self) -> int:
        """把全部未触发的延迟任务立即转入优先队列（停机补偿）

        队列在停机时已 ``disable()``，但这里属于“撤销延迟、立即执行”而不是
        接收新任务，因此直接写入优先队列并跳过容量检查。

        Returns:
            被转移的任务数量
        """
        drained = await self._delayed_queue.drain()
        for task_id, priority in drained:
            await self._priority_queue.push(task_id, priority, check_capacity=False)
        return len(drained)

    async def schedule_delayed(self, task_id: str, priority: int, delay: float) -> bool:
        """调度延迟任务（别名方法）"""
        return await self.push(task_id, priority, delay)

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


    @property
    def max_size(self) -> int:
        """队列容量上限"""
        return self._max_size

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
