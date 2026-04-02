"""
@FileName: event_bus.py
@Description: 事件总线。Event bus using Observer pattern.
@Author: HiPeng
@Time: 2026/4/1 18:20
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Callable, Any


@dataclass
class Event:
    """Base event class."""

    event_type: str
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class TaskEvent(Event):
    """Task-related event."""

    def __init__(self, event_type: str, task_id: str, data: Any = None):
        super().__init__(event_type)
        self.task_id = task_id
        self.data = data


class EventBus:
    """Event bus implementing Observer pattern."""

    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = {}
        self._async_handlers: Dict[str, List[Callable]] = {}

    def subscribe(self, event_type: str, handler: Callable, async_handler: bool = False):
        """Subscribe to an event."""
        target = self._async_handlers if async_handler else self._handlers
        target.setdefault(event_type, []).append(handler)

    def unsubscribe(self, event_type: str, handler: Callable):
        """Unsubscribe from an event."""
        if event_type in self._handlers:
            self._handlers[event_type].remove(handler)
        if event_type in self._async_handlers:
            self._async_handlers[event_type].remove(handler)

    async def emit(self, event: Event) -> None:
        """Emit an event to all subscribers."""
        # Sync handlers
        for handler in self._handlers.get(event.event_type, []):
            try:
                handler(event)
            except Exception as e:
                print(f"Error in event handler: {e}")

        # Async handlers
        for handler in self._async_handlers.get(event.event_type, []):
            try:
                await handler(event)
            except Exception as e:
                print(f"Error in async event handler: {e}")

    def on_task_submitted(self, handler: Callable, async_handler: bool = False):
        """Decorator for task submitted event."""
        self.subscribe("task.submitted", handler, async_handler)
        return handler

    def on_task_started(self, handler: Callable, async_handler: bool = False):
        """Decorator for task started event."""
        self.subscribe("task.started", handler, async_handler)
        return handler

    def on_task_completed(self, handler: Callable, async_handler: bool = False):
        """Decorator for task completed event."""
        self.subscribe("task.completed", handler, async_handler)
        return handler

    def on_task_failed(self, handler: Callable, async_handler: bool = False):
        """Decorator for task failed event."""
        self.subscribe("task.failed", handler, async_handler)
        return handler
