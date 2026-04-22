"""
@FileName: priority_queue.py
@Description: 优先级队列实现
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import heapq
from typing import List, Tuple, Optional, Dict, Any

from neotask.common.logger import debug
from neotask.queue.base import BaseQueue
from neotask.storage.base import QueueRepository


class PriorityQueue(BaseQueue):
    """优先级队列

    使用堆实现的优先级队列，支持内存和持久化两种模式。

    设计模式：Strategy Pattern - 支持内存/持久化两种存储策略
    """

    def __init__(self, repository: Optional[QueueRepository] = None, max_size: int = 10000):
        self._repository = repository
        self._heap: List[Tuple[int, float, str]] = []  # (priority, sequence, task_id)
        self._lock = asyncio.Lock()
        self._use_repository = repository is not None
        self._max_size = max_size
        self._sequence = 0  # 用于稳定排序（相同优先级时FIFO）
        self._task_index: Dict[str, Tuple[int, float, str]] = {}  # task_id -> item 快速查找

    async def push(self, task_id: str, priority: int, delay: float = 0) -> bool:
        """入队"""
        debug(f"[PRIORITY_QUEUE] push: task_id={task_id}, priority={priority}, use_repo={self._use_repository}")
        if delay > 0:
            # 延迟任务不应直接入优先级队列
            return False

        if await self.size() >= self._max_size:
            return False

        if self._use_repository:
            await self._repository.push(task_id, priority)
        else:
            async with self._lock:
                self._sequence += 1
                item = (priority, self._sequence, task_id)
                heapq.heappush(self._heap, item)
                self._task_index[task_id] = item
        return True

    async def pop(self, count: int = 1) -> List[str]:
        """出队，返回任务ID列表"""
        debug(f"[PRIORITY_QUEUE] pop: use_repo={self._use_repository}")
        if self._use_repository:
            return await self._repository.pop(count)

        async with self._lock:
            result = []
            for _ in range(min(count, len(self._heap))):
                priority, seq, task_id = heapq.heappop(self._heap)
                result.append(task_id)
                self._task_index.pop(task_id, None)
            return result

    async def pop_with_priority(self, count: int = 1) -> List[Tuple[str, int]]:
        """出队并返回优先级"""
        if self._use_repository:
            task_ids = await self._repository.pop(count)
            # 从存储获取优先级（简化处理）
            return [(tid, 5) for tid in task_ids]

        async with self._lock:
            result = []
            for _ in range(min(count, len(self._heap))):
                priority, seq, task_id = heapq.heappop(self._heap)
                result.append((task_id, priority))
                self._task_index.pop(task_id, None)
            return result

    async def remove(self, task_id: str) -> bool:
        """移除任务"""
        if self._use_repository:
            return await self._repository.remove(task_id)

        async with self._lock:
            if task_id not in self._task_index:
                return False

            # 重建堆（延迟删除策略）
            self._heap = [item for item in self._heap if item[2] != task_id]
            heapq.heapify(self._heap)
            del self._task_index[task_id]
            return True

    async def size(self) -> int:
        """获取队列大小"""
        if self._use_repository:
            return await self._repository.size()

        async with self._lock:
            return len(self._heap)

    async def peek(self, count: int = 1) -> List[str]:
        """查看队首任务"""
        if self._use_repository:
            return await self._repository.peek(count)

        async with self._lock:
            sorted_heap = sorted(self._heap)
            return [item[2] for item in sorted_heap[:count]]

    async def peek_with_priority(self, count: int = 1) -> List[Tuple[str, int]]:
        """查看队首任务及优先级"""
        if self._use_repository:
            task_ids = await self._repository.peek(count)
            return [(tid, 5) for tid in task_ids]

        async with self._lock:
            sorted_heap = sorted(self._heap)
            return [(item[2], item[0]) for item in sorted_heap[:count]]

    async def clear(self) -> None:
        """清空队列"""
        if self._use_repository:
            await self._repository.clear()

        async with self._lock:
            self._heap.clear()
            self._task_index.clear()
            self._sequence = 0

    async def contains(self, task_id: str) -> bool:
        """检查任务是否在队列中"""
        if self._use_repository:
            # 存储层需要实现 contains 方法
            return False

        async with self._lock:
            return task_id in self._task_index

    async def get_stats(self) -> Dict[str, Any]:
        """获取队列统计信息"""
        size = await self.size()
        return {
            "type": "priority_queue",
            "size": size,
            "max_size": self._max_size,
            "usage_ratio": size / self._max_size if self._max_size > 0 else 0,
            "use_repository": self._use_repository
        }
