"""
@FileName: delayed_queue.py
@Description: 延迟队列实现
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import heapq
import time
from typing import List, Tuple, Optional, Dict, Any, Callable


class DelayedQueue:
    """延迟队列

    支持任务延迟执行，使用时间堆实现。

    设计模式：Scheduler Pattern - 定时调度延迟任务
    """

    def __init__(self, check_interval: float = 0.1):
        self._heap: List[Tuple[float, int, str]] = []  # (execute_time, priority, task_id)
        self._lock = asyncio.Lock()
        self._scheduler_task: Optional[asyncio.Task] = None
        self._running = False
        self._callback: Optional[Callable] = None
        self._check_interval = check_interval
        self._task_index: Dict[str, Tuple[float, int, str]] = {}  # task_id -> item

    async def start(self, callback: Callable) -> None:
        """启动延迟队列调度器

        Args:
            callback: 延迟到期时的回调函数，签名为 async def callback(task_id: str, priority: int)
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

    async def schedule(self, task_id: str, priority: int, delay: float) -> bool:
        """调度延迟任务

        Args:
            task_id: 任务ID
            priority: 优先级
            delay: 延迟时间（秒）

        Returns:
            是否成功调度
        """
        if delay <= 0:
            return False

        execute_time = time.time() + delay

        async with self._lock:
            item = (execute_time, priority, task_id)
            heapq.heappush(self._heap, item)
            self._task_index[task_id] = item
        return True

    async def cancel(self, task_id: str) -> bool:
        """取消延迟任务"""
        async with self._lock:
            if task_id not in self._task_index:
                return False

            self._heap = [item for item in self._heap if item[2] != task_id]
            heapq.heapify(self._heap)
            del self._task_index[task_id]
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
            self._task_index.clear()

    async def contains(self, task_id: str) -> bool:
        """检查任务是否在队列中"""
        async with self._lock:
            return task_id in self._task_index

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
            "next_execution_in": next_time - time.time() if next_time else None
        }

    async def _scheduler_loop(self) -> None:
        """调度循环"""
        while self._running:
            try:
                now = time.time()
                tasks_to_move = []

                async with self._lock:
                    while self._heap and self._heap[0][0] <= now:
                        execute_time, priority, task_id = heapq.heappop(self._heap)
                        tasks_to_move.append((task_id, priority))
                        self._task_index.pop(task_id, None)

                # 触发回调
                for task_id, priority in tasks_to_move:
                    if self._callback:
                        try:
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
