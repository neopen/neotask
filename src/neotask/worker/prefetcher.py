"""
@FileName: prefetcher.py
@Description: 任务预取器 - 批量预取任务到本地队列，减少远程访问
@Author: HiPeng
@Time: 2026/4/15
"""

import asyncio
import time
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from enum import Enum

from neotask.queue.scheduler import QueueScheduler


class PrefetchStrategy(Enum):
    """预取策略"""
    SIZE_BASED = "size_based"      # 基于队列大小
    TIME_BASED = "time_based"      # 基于时间间隔
    HYBRID = "hybrid"              # 混合策略


@dataclass
class PrefetchConfig:
    """预取配置"""
    # 预取数量
    prefetch_size: int = 20              # 每次预取数量
    local_queue_size: int = 100          # 本地队列最大大小
    min_threshold: int = 5               # 低于此阈值触发预取
    max_threshold: int = 80              # 高于此阈值停止预取

    # 时间策略
    prefetch_interval: float = 0.1       # 预取检查间隔（秒）
    idle_timeout: float = 5.0            # 空闲超时（秒）

    # 策略配置
    strategy: PrefetchStrategy = PrefetchStrategy.HYBRID
    enable_batch_pop: bool = True        # 启用批量弹出
    enable_priority_filter: bool = True  # 启用优先级过滤

    # 统计配置
    enable_stats: bool = True
    stats_window: int = 100              # 统计窗口大小


@dataclass
class PrefetchStats:
    """预取统计"""
    total_prefetch: int = 0              # 总预取次数
    total_fetched: int = 0               # 总获取任务数
    total_errors: int = 0                # 总错误次数
    avg_batch_size: float = 0.0          # 平均批次大小
    last_prefetch_time: Optional[float] = None
    last_prefetch_size: int = 0

    # 性能指标
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0

    # 最近N次批次大小
    recent_batch_sizes: List[int] = field(default_factory=list)
    recent_latencies: List[float] = field(default_factory=list)


class TaskPrefetcher:
    """任务预取器

    从共享队列批量预取任务到本地队列，减少远程访问次数。

    设计模式：Producer-Consumer Pattern - 预取器作为生产者

    使用示例：
        >>> prefetcher = TaskPrefetcher(queue_scheduler)
        >>> await prefetcher.start()
        >>>
        >>> # Worker 获取任务
        >>> task_id = await prefetcher.get()
        >>>
        >>> # 获取统计信息
        >>> stats = prefetcher.get_stats()
    """

    def __init__(
        self,
        queue_scheduler: QueueScheduler,
        config: Optional[PrefetchConfig] = None
    ):
        self._queue = queue_scheduler
        self._config = config or PrefetchConfig()
        self._local_queue: asyncio.Queue = asyncio.Queue(
            maxsize=self._config.local_queue_size
        )
        self._prefetch_task: Optional[asyncio.Task] = None
        self._running = False
        self._last_activity = time.time()
        self._lock = asyncio.Lock()

        # 统计
        self._stats = PrefetchStats()
        self._prefetch_in_progress = False

    async def start(self) -> None:
        """启动预取器"""
        if self._running:
            return

        self._running = True
        self._prefetch_task = asyncio.create_task(self._prefetch_loop())
        self._last_activity = time.time()

    async def stop(self, graceful: bool = True, timeout: float = 5.0) -> None:
        """停止预取器

        Args:
            graceful: 是否优雅停止（等待本地队列清空）
            timeout: 超时时间
        """
        self._running = False

        if graceful and not self._local_queue.empty():
            try:
                await asyncio.wait_for(
                    self._wait_until_empty(),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                pass

        if self._prefetch_task:
            self._prefetch_task.cancel()
            try:
                await self._prefetch_task
            except asyncio.CancelledError:
                pass

    async def get(self, timeout: float = 1.0) -> Optional[str]:
        """从本地队列获取任务

        Args:
            timeout: 超时时间（秒）

        Returns:
            任务ID，超时返回 None
        """
        self._last_activity = time.time()

        try:
            task_id = await asyncio.wait_for(
                self._local_queue.get(),
                timeout=timeout
            )
            return task_id
        except asyncio.TimeoutError:
            return None

    async def get_batch(self, max_count: int, timeout: float = 0.1) -> List[str]:
        """批量获取任务

        Args:
            max_count: 最大获取数量
            timeout: 超时时间

        Returns:
            任务ID列表
        """
        self._last_activity = time.time()
        tasks = []

        try:
            # 尝试获取第一个任务（可能等待）
            first = await asyncio.wait_for(
                self._local_queue.get(),
                timeout=timeout
            )
            tasks.append(first)

            # 非阻塞获取剩余任务
            for _ in range(max_count - 1):
                try:
                    task_id = self._local_queue.get_nowait()
                    tasks.append(task_id)
                except asyncio.QueueEmpty:
                    break

        except asyncio.TimeoutError:
            pass

        return tasks

    async def size(self) -> int:
        """获取本地队列大小"""
        return self._local_queue.qsize()

    async def is_empty(self) -> bool:
        """检查本地队列是否为空"""
        return self._local_queue.empty()

    async def clear(self) -> None:
        """清空本地队列"""
        while not self._local_queue.empty():
            try:
                self._local_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def _prefetch_loop(self) -> None:
        """预取循环"""
        while self._running:
            try:
                current_size = self._local_queue.qsize()

                # 检查是否需要预取
                if self._should_prefetch(current_size):
                    await self._do_prefetch()

                # 根据策略决定等待时间
                wait_time = self._calculate_wait_time(current_size)
                await asyncio.sleep(wait_time)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.5)

    def _should_prefetch(self, current_size: int) -> bool:
        """判断是否需要预取"""
        # 防止并发预取
        if self._prefetch_in_progress:
            return False

        # 检查空闲超时
        idle_time = time.time() - self._last_activity
        if idle_time > self._config.idle_timeout:
            return False

        # 根据策略判断
        if self._config.strategy == PrefetchStrategy.SIZE_BASED:
            return current_size < self._config.min_threshold

        elif self._config.strategy == PrefetchStrategy.TIME_BASED:
            last_prefetch = self._stats.last_prefetch_time or 0
            time_since_last = time.time() - last_prefetch
            return time_since_last > self._config.prefetch_interval * 10

        else:  # HYBRID
            return (
                current_size < self._config.min_threshold or
                (current_size < self._config.max_threshold and
                 self._is_consuming_fast())
            )

    def _is_consuming_fast(self) -> bool:
        """判断是否消费速度快"""
        if len(self._stats.recent_batch_sizes) < 5:
            return False

        recent = self._stats.recent_batch_sizes[-5:]
        avg_size = sum(recent) / len(recent)
        return avg_size > self._config.prefetch_size * 0.5

    def _calculate_wait_time(self, current_size: int) -> float:
        """计算等待时间"""
        if current_size >= self._config.max_threshold:
            return self._config.prefetch_interval * 5
        elif current_size >= self._config.min_threshold:
            return self._config.prefetch_interval * 2
        else:
            return self._config.prefetch_interval

    async def _do_prefetch(self) -> None:
        """执行预取"""
        self._prefetch_in_progress = True
        start_time = time.time()

        try:
            # 计算需要预取的数量
            needed = self._config.prefetch_size
            available = self._config.local_queue_size - self._local_queue.qsize()
            to_fetch = min(needed, available)

            if to_fetch <= 0:
                return

            # 从共享队列获取任务
            if self._config.enable_batch_pop:
                task_ids = await self._queue.pop(to_fetch)
            else:
                task_ids = []
                for _ in range(to_fetch):
                    task_id = await self._queue.pop(1)
                    if task_id:
                        task_ids.extend(task_id)
                    else:
                        break

            # 放入本地队列
            for task_id in task_ids:
                try:
                    self._local_queue.put_nowait(task_id)
                except asyncio.QueueFull:
                    # 队列已满，停止放入
                    break

            # 更新统计
            await self._update_stats(len(task_ids), start_time)

        except Exception as e:
            self._stats.total_errors += 1
        finally:
            self._prefetch_in_progress = False

    async def _update_stats(self, fetched_count: int, start_time: float) -> None:
        """更新统计信息"""
        if not self._config.enable_stats:
            return

        latency = (time.time() - start_time) * 1000  # 毫秒

        async with self._lock:
            self._stats.total_prefetch += 1
            self._stats.total_fetched += fetched_count
            self._stats.last_prefetch_time = time.time()
            self._stats.last_prefetch_size = fetched_count

            # 记录批次大小
            self._stats.recent_batch_sizes.append(fetched_count)
            if len(self._stats.recent_batch_sizes) > self._config.stats_window:
                self._stats.recent_batch_sizes = self._stats.recent_batch_sizes[-self._config.stats_window:]

            # 计算平均批次大小
            if self._stats.recent_batch_sizes:
                self._stats.avg_batch_size = sum(self._stats.recent_batch_sizes) / len(self._stats.recent_batch_sizes)

            # 记录延迟
            self._stats.recent_latencies.append(latency)
            if len(self._stats.recent_latencies) > self._config.stats_window:
                self._stats.recent_latencies = self._stats.recent_latencies[-self._config.stats_window:]

            # 计算平均延迟
            if self._stats.recent_latencies:
                self._stats.avg_latency_ms = sum(self._stats.recent_latencies) / len(self._stats.recent_latencies)

                # 计算 P95
                sorted_latencies = sorted(self._stats.recent_latencies)
                p95_idx = int(len(sorted_latencies) * 0.95)
                self._stats.p95_latency_ms = sorted_latencies[p95_idx] if p95_idx < len(sorted_latencies) else 0

    async def _wait_until_empty(self) -> None:
        """等待本地队列清空"""
        while not self._local_queue.empty():
            await asyncio.sleep(0.1)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "local_queue_size": self._local_queue.qsize(),
            "local_queue_maxsize": self._config.local_queue_size,
            "total_prefetch": self._stats.total_prefetch,
            "total_fetched": self._stats.total_fetched,
            "total_errors": self._stats.total_errors,
            "avg_batch_size": self._stats.avg_batch_size,
            "last_prefetch_size": self._stats.last_prefetch_size,
            "avg_latency_ms": self._stats.avg_latency_ms,
            "p95_latency_ms": self._stats.p95_latency_ms,
            "prefetch_in_progress": self._prefetch_in_progress,
            "is_running": self._running,
            "config": {
                "prefetch_size": self._config.prefetch_size,
                "min_threshold": self._config.min_threshold,
                "max_threshold": self._config.max_threshold,
                "strategy": self._config.strategy.value
            }
        }

    def reset_stats(self) -> None:
        """重置统计信息"""
        self._stats = PrefetchStats()