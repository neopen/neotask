"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: time_wheel.py
@Description: 时间轮实现 - 高性能定时器
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/21
"""

import asyncio
import math
import threading
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
        # 时间轮可能被任务池内部循环与调用方循环同时访问，使用线程锁
        self._lock = threading.Lock()
        self._task_index: Dict[str, TimeWheelTask] = {}  # 快速查找
        # 下一次滴答的绝对时间（time.time 口径），用于按相位计算槽位，
        # 避免"刚好错过本 tick 就要多等一整轮"的抖动。
        self._next_tick_at = 0.0

    async def start(self, callback: Callable) -> None:
        """启动时间轮

        Args:
            callback: 任务到期回调，签名为 async def callback(task_id: str, priority: int, data: Dict)
        """
        self._callback = callback
        self._running = True
        self._next_tick_at = time.time()
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

        execute_at = time.time() + delay

        with self._lock:
            # 按相位取最近的滴答槽位：下次滴答（k=0）处理 current_slot，之后
            # 每 tick 前进一格。向上取整在"刚好错过本 tick 几毫秒"时会多等
            # 一整轮；取最近值把误差压到 ±半个 tick，执行前再由 execute_at
            # 容忍窗口防止明显提前。
            if self._next_tick_at > 0:
                slots_needed = max(0, math.floor(
                    (execute_at - self._next_tick_at) / self._tick_interval + 0.5
                ))
            else:
                slots_needed = max(1, math.ceil(
                    delay / self._tick_interval - 1e-9
                ))

            if slots_needed >= self._slot_count * self._max_rounds:
                return -1

            target_slot = (self._current_slot + slots_needed) % self._slot_count
            rounds = slots_needed // self._slot_count

            task = TimeWheelTask(
                task_id=task_id,
                priority=priority,
                data=data,
                round=rounds,
                execute_at=execute_at
            )

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
        with self._lock:
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
        with self._lock:
            return len(self._task_index)

    async def drain(self) -> List[TimeWheelTask]:
        """取出并清空所有等待中的任务

        时间轮是纯内存结构，停机时若直接丢弃，未到期任务会变成"已落库但永不入队"。
        调用方可用返回的 ``execute_at`` 重新计算剩余延时，转投延时队列。

        Returns:
            尚未到期的任务列表
        """
        with self._lock:
            drained: List[TimeWheelTask] = []
            for slot in self._slots:
                drained.extend(slot)
                slot.clear()
            self._task_index.clear()
            return drained

    async def _ticker_loop(self) -> None:
        """时间轮转动循环

        使用绝对滴答时间累加，避免每轮 ``tick - elapsed`` 的睡眠误差累积
        （Windows 上 asyncio.sleep 粒度约 15ms，按相对间隔睡会持续漂移）。
        """
        next_tick_at = time.time()
        while self._running:
            try:
                with self._lock:
                    self._next_tick_at = next_tick_at

                wait = next_tick_at - time.time()
                if wait > 0:
                    await asyncio.sleep(wait)

                await self._tick()

                next_tick_at += self._tick_interval
                # 轻微落后时下一轮 wait<=0，立即补处理落后槽位（追帧），
                # 不重置绝对时间轴，避免反复右移累积延迟。
                if time.time() - next_tick_at > 1.0:
                    # 落后过多（如长回调阻塞）：放弃追帧，重新对齐
                    next_tick_at = time.time() + self._tick_interval
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self._tick_interval)
                next_tick_at = time.time() + self._tick_interval

    async def _tick(self) -> None:
        """执行一次滴答"""
        tasks_to_execute = []
        now = time.time()

        with self._lock:
            # 获取当前槽位的任务
            current_slot_tasks = self._slots[self._current_slot]
            remaining_tasks = deque()
            deferred_tasks = []

            while current_slot_tasks:
                task = current_slot_tasks.popleft()
                if task.round == 0:
                    # 允许最多 10% tick 的提前量：槽位取最近滴答时，恰好
                    # 落在 tick 边界附近的任务会差几毫秒；超出则推迟一槽复检。
                    if task.execute_at > now + self._tick_interval * 0.1:
                        deferred_tasks.append(task)
                    else:
                        tasks_to_execute.append((task.task_id, task.priority, task.data))
                        del self._task_index[task.task_id]
                else:
                    # 轮数减1，继续等待
                    task.round -= 1
                    remaining_tasks.append(task)

            # 更新当前槽位
            self._slots[self._current_slot] = remaining_tasks

            # 移动到下一个槽位，未到期任务随下一槽复检
            next_slot = (self._current_slot + 1) % self._slot_count
            for task in deferred_tasks:
                self._slots[next_slot].append(task)
            self._current_slot = next_slot

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
