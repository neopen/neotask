"""
@FileName: time_wheel.py
@Description: 时间轮实现 - 高性能定时器
@Author: HiPeng
@Time: 2026/4/21
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from collections import deque


@dataclass
class TimeWheelTask:
    """时间轮任务"""
    task_id: str
    priority: int
    data: Optional[Dict] = None
    round: int = 0
    execute_at: float = 0.0


class TimeWheel:
    """时间轮

    高性能定时器，适用于大量延时任务的场景。

    设计模式：Circular Buffer Pattern - 循环数组实现

    使用示例：
        >>> wheel = TimeWheel(slot_count=60, tick_interval=0.1)
        >>> await wheel.start(callback)
        >>> await wheel.add_task("task_1", 1, delay=0.5)
        >>> await wheel.stop()
    """

    def __init__(
        self,
        slot_count: int = 60,
        tick_interval: float = 0.1,
        max_rounds: int = 100
    ):
        self._slot_count = slot_count
        self._tick_interval = tick_interval
        self._max_rounds = max_rounds
        # 每个槽位是一个队列
        self._slots: List[deque] = [deque() for _ in range(slot_count)]
        self._current_slot = 0
        self._running = False
        self._ticker_task: Optional[asyncio.Task] = None
        self._callback: Optional[Callable] = None
        self._lock = asyncio.Lock()
        self._task_index: Dict[str, TimeWheelTask] = {}  # 快速查找

    async def start(self, callback: Callable) -> None:
        """启动时间轮

        Args:
            callback: 任务到期回调，签名为 async def callback(task_id: str, priority: int, data: Dict)
        """
        self._callback = callback
        self._running = True
        self._ticker_task = asyncio.create_task(self._ticker_loop())

    async def stop(self) -> None:
        """停止时间轮"""
        self._running = False
        if self._ticker_task:
            self._ticker_task.cancel()
            try:
                await self._ticker_task
            except asyncio.CancelledError:
                pass

    async def add_task(
        self,
        task_id: str,
        priority: int,
        delay: float,
        data: Optional[Dict] = None
    ) -> int:
        """添加延时任务

        Args:
            task_id: 任务ID
            priority: 优先级
            delay: 延迟时间（秒）
            data: 任务数据

        Returns:
            任务所在的槽位索引，-1表示超过最大轮数限制
        """
        if delay <= 0:
            # 延迟为0的任务，直接执行
            if self._callback:
                await self._callback(task_id, priority, data)
            return 0

        # 计算需要经过的槽位数
        slots_needed = int(delay / self._tick_interval)

        if slots_needed >= self._slot_count * self._max_rounds:
            return -1

        # 计算槽位和轮数
        target_slot = (self._current_slot + slots_needed) % self._slot_count
        rounds = slots_needed // self._slot_count

        task = TimeWheelTask(
            task_id=task_id,
            priority=priority,
            data=data,
            round=rounds,
            execute_at=time.time() + delay
        )

        async with self._lock:
            self._slots[target_slot].append(task)
            self._task_index[task_id] = task

        return target_slot

    async def cancel_task(self, task_id: str) -> bool:
        """取消任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功取消
        """
        async with self._lock:
            if task_id not in self._task_index:
                return False

            # 从槽位中移除
            for slot in self._slots:
                for i, task in enumerate(slot):
                    if task.task_id == task_id:
                        del slot[i]
                        break

            del self._task_index[task_id]
            return True

    async def size(self) -> int:
        """获取任务总数"""
        async with self._lock:
            return len(self._task_index)

    async def _ticker_loop(self) -> None:
        """时间轮转动循环"""
        while self._running:
            try:
                start_time = time.time()
                await self._tick()
                # 计算实际耗时，调整睡眠时间
                elapsed = time.time() - start_time
                sleep_time = max(0.0, self._tick_interval - elapsed)
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self._tick_interval)

    async def _tick(self) -> None:
        """执行一次滴答"""
        tasks_to_execute = []

        async with self._lock:
            # 获取当前槽位的任务
            current_slot_tasks = self._slots[self._current_slot]
            remaining_tasks = deque()

            while current_slot_tasks:
                task = current_slot_tasks.popleft()
                if task.round == 0:
                    # 轮数为0，立即执行
                    tasks_to_execute.append((task.task_id, task.priority, task.data))
                    del self._task_index[task.task_id]
                else:
                    # 轮数减1，继续等待
                    task.round -= 1
                    remaining_tasks.append(task)

            # 更新当前槽位
            self._slots[self._current_slot] = remaining_tasks

            # 移动到下一个槽位
            self._current_slot = (self._current_slot + 1) % self._slot_count

        # 执行回调（在锁外执行，避免阻塞）
        for task_id, priority, data in tasks_to_execute:
            if self._callback:
                try:
                    await self._callback(task_id, priority, data)
                except Exception:
                    pass

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "slot_count": self._slot_count,
            "tick_interval": self._tick_interval,
            "current_slot": self._current_slot,
            "total_tasks": len(self._task_index),
            "is_running": self._running
        }