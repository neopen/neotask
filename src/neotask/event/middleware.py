"""
@FileName: middleware.py
@Description: 事件中间件 - 事件处理管道
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

import logging
import time
from typing import Callable, Awaitable, List

from neotask.event.bus import TaskEvent

logger = logging.getLogger(__name__)


class EventMiddleware:
    """事件中间件基类"""

    async def process(self, event: TaskEvent, next_handler: Callable[[TaskEvent], Awaitable[None]]) -> None:
        """处理事件

        Args:
            event: 事件
            next_handler: 下一个处理器
        """
        await next_handler(event)


class LoggingMiddleware(EventMiddleware):
    """日志中间件"""

    async def process(self, event: TaskEvent, next_handler: Callable[[TaskEvent], Awaitable[None]]) -> None:
        logger.debug(f"Processing event: {event.event_type}")
        start = time.time()

        try:
            await next_handler(event)
            duration = time.time() - start
            logger.debug(f"Event {event.event_type} processed in {duration:.3f}s")
        except Exception as e:
            logger.error(f"Error processing event {event.event_type}: {e}")
            raise


class TimingMiddleware(EventMiddleware):
    """计时中间件"""

    def __init__(self):
        self.timings: dict = {}

    async def process(self, event: TaskEvent, next_handler: Callable[[TaskEvent], Awaitable[None]]) -> None:
        start = time.time()

        await next_handler(event)

        duration = time.time() - start
        key = f"{event.event_type}"
        if key not in self.timings:
            self.timings[key] = []
        self.timings[key].append(duration)

    def get_average_timing(self, event_type: str) -> float:
        """获取平均处理时间"""
        timings = self.timings.get(event_type, [])
        if not timings:
            return 0.0
        return sum(timings) / len(timings)


class FilterMiddleware(EventMiddleware):
    """过滤中间件"""

    def __init__(self, allowed_types: List[str] = None, blocked_types: List[str] = None):
        self.allowed_types = set(allowed_types or [])
        self.blocked_types = set(blocked_types or [])

    async def process(self, event: TaskEvent, next_handler: Callable[[TaskEvent], Awaitable[None]]) -> None:
        # 检查是否被阻止
        if event.event_type in self.blocked_types:
            logger.debug(f"Event {event.event_type} blocked by filter")
            return

        # 检查是否允许
        if self.allowed_types and event.event_type not in self.allowed_types:
            logger.debug(f"Event {event.event_type} not in allowed list")
            return

        await next_handler(event)


class MiddlewarePipeline:
    """中间件管道"""

    def __init__(self):
        self._middlewares: List[EventMiddleware] = []

    def add(self, middleware: EventMiddleware) -> 'MiddlewarePipeline':
        """添加中间件"""
        self._middlewares.append(middleware)
        return self

    def remove(self, middleware_type: type) -> bool:
        """移除中间件"""
        for i, m in enumerate(self._middlewares):
            if isinstance(m, middleware_type):
                self._middlewares.pop(i)
                return True
        return False

    def wrap_handler(self, handler: Callable[[TaskEvent], Awaitable[None]]) -> Callable[[TaskEvent], Awaitable[None]]:
        """包装处理器，应用中间件"""

        async def wrapped(event: TaskEvent) -> None:
            # 构建中间件链
            async def dispatch(index: int) -> None:
                if index >= len(self._middlewares):
                    await handler(event)
                else:
                    await self._middlewares[index].process(event, lambda e: dispatch(index + 1))

            await dispatch(0)

        return wrapped
