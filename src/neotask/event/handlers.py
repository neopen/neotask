"""
@FileName: handlers.py
@Description: 事件处理器 - 内置事件处理器
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

import logging
from typing import Dict, Any, Optional

from neotask.event.bus import TaskEvent, EventBus

logger = logging.getLogger(__name__)


class LoggingHandler:
    """日志事件处理器"""

    def __init__(self, level: int = logging.INFO):
        self.level = level

    async def handle(self, event: TaskEvent) -> None:
        """处理事件"""
        logger.log(
            self.level,
            f"Event: {event.event_type} | Task: {event.task_id} | Data: {event.data}"
        )


class MetricsHandler:
    """指标事件处理器"""

    def __init__(self, metrics_collector):
        self._metrics = metrics_collector

    async def handle(self, event: TaskEvent) -> None:
        """处理事件"""
        if event.event_type == "task.started":
            self._metrics.record_task_start(event.task_id)
        elif event.event_type == "task.completed":
            self._metrics.record_task_complete(event.task_id)
        elif event.event_type == "task.failed":
            self._metrics.record_task_failed(event.task_id)
        elif event.event_type == "task.retry":
            self._metrics.record_task_retry(event.task_id)


class PersistenceHandler:
    """持久化事件处理器"""

    def __init__(self, storage):
        self._storage = storage

    async def handle(self, event: TaskEvent) -> None:
        """处理事件"""
        # 将重要事件持久化到存储
        event_data = event.to_dict()
        await self._storage.save_event(event_data)


class NotificationHandler:
    """通知事件处理器"""

    def __init__(self, notifiers: Dict[str, Any]):
        self._notifiers = notifiers

    async def handle(self, event: TaskEvent) -> None:
        """处理事件"""
        # 根据事件类型发送通知
        if event.event_type in ["task.failed", "task.completed"]:
            for name, notifier in self._notifiers.items():
                try:
                    await notifier.send(event)
                except Exception as e:
                    logger.error(f"Failed to send notification via {name}: {e}")


def setup_default_handlers(
        event_bus: EventBus,
        metrics_collector=None,
        storage=None,
        notifiers: Optional[Dict[str, Any]] = None
) -> None:
    """设置默认事件处理器"""

    # 日志处理器
    logging_handler = LoggingHandler()
    event_bus.subscribe_global(logging_handler.handle)

    # 指标处理器
    if metrics_collector:
        metrics_handler = MetricsHandler(metrics_collector)
        event_bus.subscribe_global(metrics_handler.handle)

    # 持久化处理器
    if storage:
        persistence_handler = PersistenceHandler(storage)
        event_bus.subscribe("task.created", persistence_handler.handle)
        event_bus.subscribe("task.completed", persistence_handler.handle)
        event_bus.subscribe("task.failed", persistence_handler.handle)

    # 通知处理器
    if notifiers:
        notification_handler = NotificationHandler(notifiers)
        event_bus.subscribe("task.failed", notification_handler.handle)
        if notifiers.get("on_complete"):
            event_bus.subscribe("task.completed", notification_handler.handle)
