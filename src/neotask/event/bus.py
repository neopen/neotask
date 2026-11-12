"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: bus.py
@Description: 事件总线 - 发布订阅模式
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Callable, Any, Optional, cast

from neotask.common.logger import error, warning


@dataclass
class TaskEvent:
    """任务事件

    事件信封在 v1.1.0 起稳定：``timestamp`` / ``node_id`` / ``schema_version``
    为远程上报预留字段，本版本只做信封扩展，不做任何上报。

    Attributes:
        event_type: 事件名，取值见 :class:`neotask.common.constants.EventType`
        task_id: 关联任务 ID
        data: 事件负载
        timestamp: 事件产生时刻（epoch 秒），构造时自动填充
        node_id: 产生该事件的实例标识，由 EventBus 按自身配置补齐
        schema_version: 信封结构版本，当前为 1
    """
    event_type: str
    task_id: str
    data: Optional[Any] = None
    timestamp: float = field(default_factory=time.time)
    node_id: str = ""
    schema_version: int = 1


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

    # 无界队列的堆积告警步长：每多堆积这么多事件就告警一次
    QUEUE_WARN_STEP = 1000

    def __init__(
            self,
            max_queue_size: Optional[int] = None,
            overflow_policy: str = "drop",
            slow_handler_threshold: float = 1.0,
            node_id: str = "",
    ):
        """初始化事件总线

        Args:
            max_queue_size: 待处理事件队列上限；None 表示不设硬上限（默认，
                保持向后兼容），仅在水位越线时告警。
            overflow_policy: 队列满时的策略，目前支持 ``"drop"``：丢弃新事件
                并计数告警，避免慢回调把发布方一起拖死。
            slow_handler_threshold: 单个回调耗时超过该秒数时告警并计数。
            node_id: 写入事件信封 ``TaskEvent.node_id`` 的实例标识；留空则保持
                事件自带的 ``node_id``（调用方未指定时为 ``""``）。
        """
        if overflow_policy != "drop":
            raise ValueError(f"Unsupported overflow_policy: {overflow_policy}")
        self._node_id = node_id
        self._handlers: Dict[str, List[Callable]] = {}
        self._global_handlers: List[Callable] = []
        # 订阅关系可能被不同线程修改，使用线程锁（临界区无 await）
        self._lock = threading.Lock()
        self._running = False
        self._max_queue_size = max_queue_size
        self._overflow_policy = overflow_policy
        self._slow_handler_threshold = slow_handler_threshold
        # 队列延迟到 start() 中创建：Python 3.8 的 asyncio.Queue 在构造时就会
        # 绑定当前线程的事件循环，在无循环的调用方线程里构造会直接抛 RuntimeError。
        self._queue: asyncio.Queue = cast(asyncio.Queue, None)
        self._worker_task: Optional[asyncio.Task] = None
        # 消费队列的事件循环（由 start() 记录），用于跨循环投递
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # 监控计数：丢弃/已处理事件、慢回调次数、水位告警状态
        self._dropped_events = 0
        self._processed_events = 0
        self._slow_handler_calls = 0
        self._queue_warned = False
        self._unbounded_warn_at = self.QUEUE_WARN_STEP

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

            # 保存原始函数引用，用于 unsubscribe
            wrapper.__wrapped__ = func
            with self._lock:
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

            with self._lock:
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
            with self._lock:
                original_count = len(self._handlers[event_type])
                self._handlers[event_type] = [
                    h for h in self._handlers[event_type]
                    if h != handler and getattr(h, '__wrapped__', None) != handler
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
        with self._lock:
            original_count = len(self._global_handlers)
            self._global_handlers = [h for h in self._global_handlers if h != handler]
            return len(self._global_handlers) < original_count

    async def emit(self, event: TaskEvent) -> None:
        """发送事件

        事件总线启动后，消费队列的协程运行在 TaskPool 的内部事件循环上；
        ``emit`` 可能由调用方线程/另一个事件循环触发。直接 ``await
        self._queue.put(...)`` 会跨循环唤醒 getter，目标循环不会被唤醒
        （最多拖延到一个轮询超时）。因此跨循环时必须用
        ``call_soon_threadsafe`` 在目标循环中投递。
        """
        if self._node_id and not event.node_id:
            event.node_id = self._node_id

        if not self._running:
            # 同步处理
            await self._process_event(event)
            return

        loop = self._loop
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        if loop is None or loop.is_closed() or running_loop is loop:
            # 同一事件循环（或尚未记录消费循环）：直接入队即可
            self._put_event(event)
            return

        try:
            loop.call_soon_threadsafe(self._put_event, event)
        except RuntimeError:
            # 目标循环已关闭：降级为在当前循环同步处理
            await self._process_event(event)

    def _put_event(self, event: TaskEvent) -> None:
        """入队事件（调用方须位于消费循环线程）"""
        if self._max_queue_size is not None and self._queue.full():
            # 有界队列满：丢弃新事件并限频告警，避免发布方与消费方一起挂死
            self._dropped_events += 1
            if self._dropped_events == 1 or self._dropped_events % self.QUEUE_WARN_STEP == 0:
                warning(
                    f"EventBus queue full (max={self._max_queue_size}), "
                    f"dropped {self._dropped_events} event(s) total"
                )
            return
        self._queue.put_nowait(event)
        self._update_queue_watermark()

    def _update_queue_watermark(self) -> None:
        """队列水位监控：越线告警，回落后复位"""
        size = self._queue.qsize()
        if self._max_queue_size is not None:
            high = max(1, int(self._max_queue_size * 0.8))
            low = max(1, int(self._max_queue_size * 0.5))
            if size >= high and not self._queue_warned:
                warning(
                    f"EventBus pending events reached {size}/{self._max_queue_size}; "
                    f"a slow subscriber is likely backing up event processing"
                )
                self._queue_warned = True
            elif size <= low:
                self._queue_warned = False
        elif size >= self._unbounded_warn_at:
            warning(
                f"EventBus pending events reached {size}; "
                f"a slow subscriber is likely backing up event processing"
            )
            self._unbounded_warn_at = size + self.QUEUE_WARN_STEP
        elif size == 0:
            self._unbounded_warn_at = self.QUEUE_WARN_STEP

    async def _invoke_handler(self, handler: Callable, event: TaskEvent) -> None:
        """调用单个处理器，隔离异常并统计慢回调"""
        start = time.perf_counter()
        try:
            await handler(event)
        except Exception:
            pass
        finally:
            elapsed = time.perf_counter() - start
            if elapsed >= self._slow_handler_threshold:
                self._slow_handler_calls += 1
                name = getattr(
                    getattr(handler, "__wrapped__", handler), "__name__", repr(handler)
                )
                warning(
                    f"EventBus slow handler '{name}' took {elapsed:.3f}s "
                    f"for event '{event.event_type}'"
                )

    async def _process_event(self, event: TaskEvent) -> None:
        """处理事件

        同一事件的各订阅者并发执行：一个慢回调不会拖慢其余订阅者。订阅者之间
        本就无顺序约定，事件之间的顺序仍由单消费协程保证。
        """
        with self._lock:
            global_handlers = list(self._global_handlers)
            handlers = list(self._handlers.get(event.event_type, []))

        handlers_to_run = global_handlers + handlers
        if handlers_to_run:
            await asyncio.gather(
                *(self._invoke_handler(handler, event) for handler in handlers_to_run)
            )

    async def start(self) -> None:
        """启动事件总线"""
        if self._running:
            return
        # 队列必须在每次启动时重建：asyncio.Queue 会绑定所在事件循环，复用旧
        # 队列会让重启后的消费任务在别的循环上抛 RuntimeError。先创建队列与
        # 循环记录再置运行标记，避免并发 emit 看到 None 队列。
        self._queue = asyncio.Queue(maxsize=self._max_queue_size or 0)
        # 记录消费队列的事件循环，供 emit 判断是否需要跨循环投递
        self._loop = asyncio.get_running_loop()
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
            self._worker_task = None
        self._loop = None

    async def _worker_loop(self) -> None:
        """工作循环

        直接阻塞等待队列：退出依赖 ``stop()`` 取消本任务，因此不需要轮询超时。
        此前用 ``wait_for(..., timeout=1.0)`` 轮询，取消恰好撞上超时竞态时
        ``wait_for`` 会吞掉 CancelledError 继续下一轮等待，外部取消方（如
        pytest-asyncio 关闭事件循环）会一直等不到任务结束。
        """
        while self._running:
            try:
                event = await self._queue.get()
                await self._process_event(event)
                self._processed_events += 1
                self._queue.task_done()
                self._update_queue_watermark()
            except asyncio.CancelledError:
                break
            except RuntimeError as exc:
                # 事件循环绑定异常无法通过重试恢复，直接退出避免忙旋卡死循环
                error(f"EventBus worker stopped: {exc}")
                break
            except Exception as exc:
                error(f"EventBus worker error: {exc}")
                # 避免持续异常时空转占满事件循环
                await asyncio.sleep(0.05)
                continue

    def get_stats(self) -> Dict[str, Any]:
        """获取事件处理统计

        用于监控慢订阅者：``queue_size`` 持续上涨说明消费跟不上生产，
        ``dropped_events`` 非零说明有界队列已开始丢弃事件。
        """
        return {
            "queue_size": self._queue.qsize() if self._queue is not None else 0,
            "max_queue_size": self._max_queue_size,
            "processed_events": self._processed_events,
            "dropped_events": self._dropped_events,
            "slow_handler_calls": self._slow_handler_calls,
            "slow_handler_threshold": self._slow_handler_threshold,
            "is_running": self._running,
        }

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

    async def __aenter__(self) -> "EventBus":
        """异步上下文管理器进入"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """异步上下文管理器退出"""
        await self.stop()
