"""
@FileName: bus.py
@Description: 事件总线 - 发布订阅模式
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Callable, Any, Optional

from neotask.common.logger import debug, error


@dataclass
class TaskEvent:
    """任务事件"""
    event_type: str
    task_id: str
    data: Optional[Any] = None


class EventBus:
    """事件总线

    设计模式：Observer Pattern / Pub-Sub Pattern
    """

    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = {}
        self._global_handlers: List[Callable] = []
        self._lock = asyncio.Lock()
        self._running = False
        self._queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None

    def subscribe(self, event_type: str, handler: Callable = None) -> Callable:
        """订阅特定类型事件

        可以用作装饰器：
        @bus.subscribe("task.completed")
        async def on_complete(event):
            pass

        也可以直接调用：
        bus.subscribe("task.completed", on_complete)

        Args:
            event_type: 事件类型
            handler: 事件处理器（可选，用于直接调用）

        Returns:
            装饰器函数或原始handler
        """
        def decorator(func: Callable) -> Callable:
            # 直接存储原始函数，不包装
            if event_type not in self._handlers:
                self._handlers[event_type] = []
            self._handlers[event_type].append(func)
            return func

        if handler is None:
            # 用作装饰器
            return decorator
        else:
            # 直接调用
            return decorator(handler)

    def subscribe_global(self, handler: Callable = None) -> Callable:
        """订阅所有事件

        可以用作装饰器：
        @bus.subscribe_global
        async def on_event(event):
            pass

        也可以直接调用：
        bus.subscribe_global(on_event)

        Args:
            handler: 事件处理器（可选，用于直接调用）

        Returns:
            装饰器函数或原始handler
        """
        def decorator(func: Callable) -> Callable:
            self._global_handlers.append(func)
            return func

        if handler is None:
            return decorator
        else:
            return decorator(handler)

    def unsubscribe(self, event_type: str, handler: Callable) -> bool:
        """取消订阅"""
        if event_type in self._handlers:
            original_count = len(self._handlers[event_type])
            self._handlers[event_type] = [
                h for h in self._handlers[event_type]
                if h != handler
            ]
            return len(self._handlers[event_type]) < original_count
        return False

    async def emit(self, event: TaskEvent) -> None:
        """发送事件"""
        debug(f"[EventBus] Emitting event: {event.event_type} for task {event.task_id}")

        if not self._running:
            # 同步处理
            await self._process_event(event)
        else:
            # 异步队列处理
            await self._queue.put(event)

    async def _process_event(self, event: TaskEvent) -> None:
        """处理事件"""
        # 调用全局处理器
        for handler in self._global_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                error(f"Global handler error: {e}")

        # 调用特定事件处理器
        handlers = self._handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                error(f"Handler for {event.event_type} error: {e}")

    async def start(self) -> None:
        """启动事件总线"""
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())

    async def stop(self) -> None:
        """停止事件总线"""
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

    async def _worker_loop(self) -> None:
        """工作循环"""
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._process_event(event)
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                error(f"Worker loop error: {e}")
                continue

    def clear(self) -> None:
        """清空所有处理器"""
        self._handlers.clear()
        self._global_handlers.clear()