"""
@FileName: bus.py
@Description: 事件总线 - 发布订阅模式
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Callable, Any, Optional


@dataclass
class TaskEvent:
    """任务事件"""
    event_type: str
    task_id: str
    data: Optional[Any] = None


class EventBus:
    """事件总线

    设计模式：Observer Pattern / Pub-Sub Pattern

    使用示例：
        >>> bus = EventBus()
        >>>
        >>> # 方式1：装饰器注册
        >>> @bus.subscribe("task.completed")
        ... async def on_complete(event):
        ...     print(f"Task {event.task_id} completed")
        >>>
        >>> # 方式2：方法调用注册
        >>> bus.subscribe("task.started", on_start)
        >>>
        >>> # 方式3：全局装饰器
        >>> @bus.subscribe_global
        ... async def on_any(event):
        ...     print(f"Event: {event.event_type}")
    """

    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = {}
        self._global_handlers: List[Callable] = []
        self._lock = asyncio.Lock()
        self._running = False
        self._queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None

    def subscribe(self, event_type: str, handler: Optional[Callable] = None):
        """订阅特定类型事件

        支持两种使用方式：
        1. 作为装饰器: @bus.subscribe("task.completed")
        2. 作为方法: bus.subscribe("task.completed", handler)

        Args:
            event_type: 事件类型
            handler: 事件处理器（可选）

        Returns:
            如果 handler 为 None，返回装饰器；否则返回 handler
        """

        def decorator(func: Callable) -> Callable:
            # 包装为异步函数
            async def wrapper(event: TaskEvent):
                if asyncio.iscoroutinefunction(func):
                    await func(event)
                else:
                    func(event)

            if event_type not in self._handlers:
                self._handlers[event_type] = []
            self._handlers[event_type].append(wrapper)
            return func

        # 如果提供了 handler，直接注册
        if handler is not None:
            return decorator(handler)

        # 否则返回装饰器
        return decorator

    def subscribe_global(self, handler: Optional[Callable] = None):
        """订阅所有事件

        支持两种使用方式：
        1. 作为装饰器: @bus.subscribe_global
        2. 作为方法: bus.subscribe_global(handler)

        Args:
            handler: 事件处理器（可选）

        Returns:
            如果 handler 为 None，返回装饰器；否则返回 handler
        """

        def decorator(func: Callable) -> Callable:
            # 包装为异步函数
            async def wrapper(event: TaskEvent):
                if asyncio.iscoroutinefunction(func):
                    await func(event)
                else:
                    func(event)

            self._global_handlers.append(wrapper)
            return func

        # 如果提供了 handler，直接注册
        if handler is not None:
            return decorator(handler)

        # 否则返回装饰器
        return decorator

    def unsubscribe(self, event_type: str, handler: Callable) -> bool:
        """取消订阅

        Args:
            event_type: 事件类型
            handler: 事件处理器

        Returns:
            是否成功取消订阅
        """
        if event_type in self._handlers:
            original_count = len(self._handlers[event_type])
            self._handlers[event_type] = [
                h for h in self._handlers[event_type]
                if h != handler
            ]
            return len(self._handlers[event_type]) < original_count
        return False

    def unsubscribe_global(self, handler: Callable) -> bool:
        """取消全局订阅

        Args:
            handler: 事件处理器

        Returns:
            是否成功取消订阅
        """
        original_count = len(self._global_handlers)
        self._global_handlers = [h for h in self._global_handlers if h != handler]
        return len(self._global_handlers) < original_count

    async def emit(self, event: TaskEvent) -> None:
        """发送事件

        Args:
            event: 事件对象
        """
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
                await handler(event)
            except Exception:
                pass

        # 调用特定事件处理器
        handlers = self._handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                await handler(event)
            except Exception:
                pass

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
            except Exception:
                continue

    def clear(self) -> None:
        """清空所有处理器"""
        self._handlers.clear()
        self._global_handlers.clear()

    def has_subscribers(self, event_type: Optional[str] = None) -> bool:
        """检查是否有订阅者

        Args:
            event_type: 事件类型，如果为 None 则检查全局订阅者

        Returns:
            是否有订阅者
        """
        if event_type:
            return event_type in self._handlers and bool(self._handlers[event_type])
        return bool(self._global_handlers)

    def subscriber_count(self, event_type: Optional[str] = None) -> int:
        """获取订阅者数量

        Args:
            event_type: 事件类型，如果为 None 则返回全局订阅者数量

        Returns:
            订阅者数量
        """
        if event_type:
            return len(self._handlers.get(event_type, []))
        return len(self._global_handlers)
