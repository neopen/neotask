"""
@FileName: delayed_queue.py
@Description: 延迟队列实现 - 支持延时任务调度
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import heapq
import time
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any, Callable

from neotask.models.schedule import DelayedTask


class DelayedQueue:
    """延迟队列

    支持任务延迟执行，使用时间堆实现。

    设计模式：Scheduler Pattern - 定时调度延迟任务

    使用示例：
        >>> dq = DelayedQueue()
        >>> await dq.start(callback)
        >>> await dq.schedule("task-1", 5, delay=10)  # 10秒后执行
        >>> await dq.schedule("task-2", 1, delay=5)   # 5秒后执行
    """

    def __init__(self, check_interval: float = 0.1):
        self._heap: List[Tuple[float, int, str]] = []  # (execute_time, priority, task_id)
        self._tasks: Dict[str, DelayedTask] = {}  # task_id -> DelayedTask
        self._lock = asyncio.Lock()
        self._scheduler_task: Optional[asyncio.Task] = None
        self._running = False
        self._callback: Optional[Callable] = None
        self._check_interval = check_interval
        self._stats = {"scheduled": 0, "executed": 0, "cancelled": 0}

    async def start(self, callback: Callable) -> None:
        """启动延迟队列调度器

        Args:
            callback: 延迟到期时的回调函数，签名为 async def callback(task_id: str, priority: int, data: Dict)
        """
        self._callback = callback
        self._running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    async def stop(self) -> None:
        """停止延迟队列"""
        self._running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

    async def schedule(
            self,
            task_id: str,
            priority: int,
            delay: float,
            data: Optional[Dict] = None
    ) -> bool:
        """调度延迟任务

        Args:
            task_id: 任务ID
            priority: 优先级
            delay: 延迟时间（秒）
            data: 任务数据（可选）

        Returns:
            是否成功调度
        """
        if delay <= 0:
            return False

        execute_time = time.time() + delay
        delayed_task = DelayedTask(
            task_id=task_id,
            execute_at=execute_time,
            priority=priority,
            data=data
        )

        async with self._lock:
            heapq.heappush(self._heap, (execute_time, priority, task_id))
            self._tasks[task_id] = delayed_task
            self._stats["scheduled"] += 1

        return True

    async def schedule_at(
            self,
            task_id: str,
            priority: int,
            execute_at: datetime,
            data: Optional[Dict] = None
    ) -> bool:
        """在指定时间点执行任务

        Args:
            task_id: 任务ID
            priority: 优先级
            execute_at: 执行时间点
            data: 任务数据

        Returns:
            是否成功调度
        """
        delay = max(0.0, (execute_at - datetime.now()).total_seconds())
        return await self.schedule(task_id, priority, delay, data)

    async def cancel(self, task_id: str) -> bool:
        """取消延迟任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功取消
        """
        async with self._lock:
            if task_id not in self._tasks:
                return False

            # 从堆中移除（延迟删除策略）
            self._heap = [item for item in self._heap if item[2] != task_id]
            heapq.heapify(self._heap)
            del self._tasks[task_id]
            self._stats["cancelled"] += 1
            return True

    async def size(self) -> int:
        """获取延迟队列大小"""
        async with self._lock:
            return len(self._heap)

    async def is_empty(self) -> bool:
        """检查队列是否为空"""
        return await self.size() == 0

    async def clear(self) -> None:
        """清空延迟队列"""
        async with self._lock:
            self._heap.clear()
            self._tasks.clear()

    async def contains(self, task_id: str) -> bool:
        """检查任务是否在队列中"""
        async with self._lock:
            return task_id in self._tasks

    async def get_next_execution_time(self) -> Optional[float]:
        """获取下一个任务的执行时间"""
        async with self._lock:
            if not self._heap:
                return None
            return self._heap[0][0]

    async def get_stats(self) -> Dict[str, Any]:
        """获取队列统计信息"""
        size = await self.size()
        next_time = await self.get_next_execution_time()
        return {
            "type": "delayed_queue",
            "size": size,
            "check_interval": self._check_interval,
            "next_execution_time": next_time,
            "next_execution_in": next_time - time.time() if next_time else None,
            "stats": self._stats.copy()
        }

    async def _scheduler_loop(self) -> None:
        """调度循环"""
        while self._running:
            try:
                now = time.time()
                tasks_to_execute = []

                async with self._lock:
                    # 获取所有到期的任务
                    while self._heap and self._heap[0][0] <= now:
                        execute_time, priority, task_id = heapq.heappop(self._heap)
                        task = self._tasks.pop(task_id, None)
                        if task:
                            tasks_to_execute.append((task_id, priority, task.data))
                            self._stats["executed"] += 1

                # 触发回调执行任务
                for task_id, priority, data in tasks_to_execute:
                    if self._callback:
                        try:
                            # 兼容不同签名的回调函数
                            try:
                                await self._callback(task_id, priority, data)
                            except TypeError:
                                # 如果回调只接受两个参数
                                await self._callback(task_id, priority)
                        except Exception:
                            pass

                # 计算下次检查时间
                next_time = self._heap[0][0] if self._heap else now + 1
                wait_time = min(self._check_interval, max(0.0, next_time - now))
                await asyncio.sleep(wait_time)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self._check_interval)
