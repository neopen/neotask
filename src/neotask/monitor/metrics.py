"""
@FileName: metrics.py
@Description: 指标收集 - 任务执行指标
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from statistics import mean
from typing import Dict, Any, Optional


class MetricType(Enum):
    """指标类型"""
    COUNTER = "counter"  # 计数器
    GAUGE = "gauge"  # 瞬时值
    HISTOGRAM = "histogram"  # 分布统计


@dataclass
class TaskMetrics:
    """任务指标

    包含任务执行的核心统计信息。
    """
    total_submitted: int = 0
    total_completed: int = 0
    total_failed: int = 0
    total_cancelled: int = 0
    total_retried: int = 0

    # 当前状态（瞬时值）
    pending: int = 0
    running: int = 0
    scheduled: int = 0

    # 执行时间统计（滑动窗口）
    execution_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    queue_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    retry_counts: deque = field(default_factory=lambda: deque(maxlen=1000))

    # 按优先级统计
    priority_counts: Dict[int, int] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        """成功率"""
        total = self.total_completed + self.total_failed
        if total == 0:
            return 1.0
        return self.total_completed / total

    @property
    def failure_rate(self) -> float:
        """失败率"""
        return 1.0 - self.success_rate

    @property
    def avg_execution_time(self) -> float:
        """平均执行时间（秒）"""
        if not self.execution_times:
            return 0.0
        return mean(self.execution_times)

    @property
    def p50_execution_time(self) -> float:
        """P50执行时间（秒）"""
        return self._percentile(self.execution_times, 0.5)

    @property
    def p90_execution_time(self) -> float:
        """P90执行时间（秒）"""
        return self._percentile(self.execution_times, 0.9)

    @property
    def p95_execution_time(self) -> float:
        """P95执行时间（秒）"""
        return self._percentile(self.execution_times, 0.95)

    @property
    def p99_execution_time(self) -> float:
        """P99执行时间（秒）"""
        return self._percentile(self.execution_times, 0.99)

    @property
    def avg_queue_time(self) -> float:
        """平均排队时间（秒）"""
        if not self.queue_times:
            return 0.0
        return mean(self.queue_times)

    @property
    def p95_queue_time(self) -> float:
        """P95排队时间（秒）"""
        return self._percentile(self.queue_times, 0.95)

    @property
    def avg_retry_count(self) -> float:
        """平均重试次数"""
        if not self.retry_counts:
            return 0.0
        return mean(self.retry_counts)

    @property
    def throughput(self) -> float:
        """吞吐量（任务/秒）"""
        # 基于最近完成的1000个任务计算
        if not self.execution_times:
            return 0.0
        # 简化实现：平均执行时间的倒数
        return 1.0 / (self.avg_execution_time + 0.001)

    def _percentile(self, data: deque, p: float) -> float:
        """计算百分位数"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * p)
        return sorted_data[min(idx, len(sorted_data) - 1)]

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "total_submitted": self.total_submitted,
            "total_completed": self.total_completed,
            "total_failed": self.total_failed,
            "total_cancelled": self.total_cancelled,
            "total_retried": self.total_retried,
            "success_rate": self.success_rate,
            "failure_rate": self.failure_rate,
            "pending": self.pending,
            "running": self.running,
            "scheduled": self.scheduled,
            "avg_execution_time": self.avg_execution_time,
            "p50_execution_time": self.p50_execution_time,
            "p90_execution_time": self.p90_execution_time,
            "p95_execution_time": self.p95_execution_time,
            "p99_execution_time": self.p99_execution_time,
            "avg_queue_time": self.avg_queue_time,
            "p95_queue_time": self.p95_queue_time,
            "avg_retry_count": self.avg_retry_count,
            "throughput": self.throughput,
            "priority_counts": self.priority_counts.copy(),
            "execution_samples": len(self.execution_times),
            "queue_samples": len(self.queue_times),
        }


@dataclass
class SystemMetrics:
    """系统指标"""
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    memory_used_mb: float = 0.0
    thread_count: int = 0
    task_count: int = 0
    uptime_seconds: float = 0.0


class MetricsCollector:
    """指标收集器

    收集任务执行的各项指标，支持滑动窗口统计。

    使用示例：
        >>> collector = MetricsCollector()
        >>> collector.record_task_submit("task-1")
        >>> collector.record_task_start("task-1")
        >>> collector.record_task_complete("task-1", execution_time=1.5)
        >>> summary = collector.get_summary()
    """

    def __init__(
            self,
            window_size: int = 1000,
            enable_system_metrics: bool = True,
            system_metrics_interval: float = 60.0
    ):
        """初始化指标收集器

        Args:
            window_size: 滑动窗口大小
            enable_system_metrics: 是否启用系统指标收集
            system_metrics_interval: 系统指标收集间隔（秒）
        """
        self._window_size = window_size
        self._enable_system_metrics = enable_system_metrics
        self._system_metrics_interval = system_metrics_interval

        # 任务指标
        self._metrics = TaskMetrics()
        self._metrics.execution_times = deque(maxlen=window_size)
        self._metrics.queue_times = deque(maxlen=window_size)
        self._metrics.retry_counts = deque(maxlen=window_size)

        # 内部状态
        self._task_start_times: Dict[str, float] = {}
        self._task_queue_times: Dict[str, float] = {}
        self._task_retry_counts: Dict[str, int] = {}

        # 系统指标
        self._system_metrics: Optional[SystemMetrics] = None
        self._system_metrics_task: Optional[asyncio.Task] = None
        self._start_time: float = time.time()

        # 并发控制
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """启动指标收集器（启动系统指标收集）"""
        if self._enable_system_metrics and not self._system_metrics_task:
            self._system_metrics_task = asyncio.create_task(self._collect_system_metrics_loop())

    async def stop(self) -> None:
        """停止指标收集器"""
        if self._system_metrics_task:
            self._system_metrics_task.cancel()
            try:
                await self._system_metrics_task
            except asyncio.CancelledError:
                pass
            self._system_metrics_task = None

    # ========== 任务指标记录 ==========

    async def record_task_submit(self, task_id: str, priority: int = 2) -> None:
        """记录任务提交

        Args:
            task_id: 任务ID
            priority: 优先级
        """
        self._task_queue_times[task_id] = time.time()

        async with self._lock:
            self._metrics.total_submitted += 1
            self._metrics.pending += 1
            self._metrics.priority_counts[priority] = self._metrics.priority_counts.get(priority, 0) + 1

    async def record_task_start(self, task_id: str) -> None:
        """记录任务开始执行

        Args:
            task_id: 任务ID
        """
        self._task_start_times[task_id] = time.time()

        # 计算排队时间
        if task_id in self._task_queue_times:
            queue_time = time.time() - self._task_queue_times[task_id]
            async with self._lock:
                self._metrics.queue_times.append(queue_time)

        async with self._lock:
            self._metrics.pending = max(0, self._metrics.pending - 1)
            self._metrics.running += 1

    async def record_task_complete(self, task_id: str) -> None:
        """记录任务完成

        Args:
            task_id: 任务ID
        """
        # 计算执行时间
        if task_id in self._task_start_times:
            execution_time = time.time() - self._task_start_times[task_id]
            async with self._lock:
                self._metrics.execution_times.append(execution_time)

        # 记录重试次数
        if task_id in self._task_retry_counts:
            retry_count = self._task_retry_counts[task_id]
            async with self._lock:
                self._metrics.retry_counts.append(retry_count)

        async with self._lock:
            self._metrics.total_completed += 1
            self._metrics.running = max(0, self._metrics.running - 1)

        # 清理
        self._cleanup_task(task_id)

    async def record_task_failed(self, task_id: str) -> None:
        """记录任务失败

        Args:
            task_id: 任务ID
        """
        # 计算执行时间
        if task_id in self._task_start_times:
            execution_time = time.time() - self._task_start_times[task_id]
            async with self._lock:
                self._metrics.execution_times.append(execution_time)

        async with self._lock:
            self._metrics.total_failed += 1
            self._metrics.running = max(0, self._metrics.running - 1)

        # 清理
        self._cleanup_task(task_id)

    async def record_task_cancelled(self, task_id: str) -> None:
        """记录任务取消

        Args:
            task_id: 任务ID
        """
        async with self._lock:
            self._metrics.total_cancelled += 1
            self._metrics.running = max(0, self._metrics.running - 1)

        # 清理
        self._cleanup_task(task_id)

    async def record_task_retry(self, task_id: str, retry_count: int) -> None:
        """记录任务重试

        Args:
            task_id: 任务ID
            retry_count: 重试次数
        """
        self._task_retry_counts[task_id] = retry_count

        async with self._lock:
            self._metrics.total_retried += 1

    async def record_task_scheduled(self, task_id: str) -> None:
        """记录任务被调度（延迟任务）

        Args:
            task_id: 任务ID
        """
        async with self._lock:
            self._metrics.scheduled += 1

    async def record_task_unscheduled(self, task_id: str) -> None:
        """记录任务从调度中移除

        Args:
            task_id: 任务ID
        """
        async with self._lock:
            self._metrics.scheduled = max(0, self._metrics.scheduled - 1)

    async def update_pending_count(self, count: int) -> None:
        """更新待处理任务计数

        Args:
            count: 新的计数
        """
        async with self._lock:
            self._metrics.pending = count

    def _cleanup_task(self, task_id: str) -> None:
        """清理任务相关状态"""
        self._task_start_times.pop(task_id, None)
        self._task_queue_times.pop(task_id, None)
        self._task_retry_counts.pop(task_id, None)

    # ========== 系统指标收集 ==========

    async def _collect_system_metrics_loop(self) -> None:
        """系统指标收集循环"""
        while True:
            try:
                await self._collect_system_metrics()
                await asyncio.sleep(self._system_metrics_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    async def _collect_system_metrics(self) -> None:
        """收集系统指标"""
        import psutil
        import threading

        try:
            process = psutil.Process()

            self._system_metrics = SystemMetrics(
                cpu_percent=process.cpu_percent(interval=0.1),
                memory_percent=process.memory_percent(),
                memory_used_mb=process.memory_info().rss / 1024 / 1024,
                thread_count=threading.active_count(),
                task_count=len(self._task_start_times),
                uptime_seconds=time.time() - self._start_time
            )
        except Exception:
            # psutil 可能不可用
            self._system_metrics = SystemMetrics()

    # ========== 指标查询 ==========

    def get_metrics(self) -> TaskMetrics:
        """获取任务指标（副本）"""
        return self._metrics

    async def get_metrics_async(self) -> TaskMetrics:
        """异步获取任务指标"""
        async with self._lock:
            # 返回副本
            return self._metrics

    def get_system_metrics(self) -> Optional[SystemMetrics]:
        """获取系统指标"""
        return self._system_metrics

    async def get_system_metrics_async(self) -> Optional[SystemMetrics]:
        """异步获取系统指标"""
        return self._system_metrics

    def get_summary(self) -> Dict[str, Any]:
        """获取指标摘要"""
        return self._metrics.to_dict()

    async def get_summary_async(self) -> Dict[str, Any]:
        """异步获取指标摘要"""
        async with self._lock:
            return self._metrics.to_dict()

    def get_full_summary(self) -> Dict[str, Any]:
        """获取完整摘要（包含系统指标）"""
        result = self._metrics.to_dict()

        if self._system_metrics:
            result["system"] = {
                "cpu_percent": self._system_metrics.cpu_percent,
                "memory_percent": self._system_metrics.memory_percent,
                "memory_used_mb": self._system_metrics.memory_used_mb,
                "thread_count": self._system_metrics.thread_count,
                "task_count": self._system_metrics.task_count,
                "uptime_seconds": self._system_metrics.uptime_seconds,
            }

        return result

    async def get_full_summary_async(self) -> Dict[str, Any]:
        """异步获取完整摘要"""
        async with self._lock:
            result = self._metrics.to_dict()

        if self._system_metrics:
            result["system"] = {
                "cpu_percent": self._system_metrics.cpu_percent,
                "memory_percent": self._system_metrics.memory_percent,
                "memory_used_mb": self._system_metrics.memory_used_mb,
                "thread_count": self._system_metrics.thread_count,
                "task_count": self._system_metrics.task_count,
                "uptime_seconds": self._system_metrics.uptime_seconds,
            }

        return result

    # ========== 指标重置 ==========

    async def reset(self) -> None:
        """重置所有指标"""
        async with self._lock:
            self._metrics = TaskMetrics()
            self._metrics.execution_times = deque(maxlen=self._window_size)
            self._metrics.queue_times = deque(maxlen=self._window_size)
            self._metrics.retry_counts = deque(maxlen=self._window_size)

            self._task_start_times.clear()
            self._task_queue_times.clear()
            self._task_retry_counts.clear()
            self._start_time = time.time()

    # ========== 指标导出 ==========

    def to_prometheus_format(self) -> str:
        """转换为 Prometheus 格式

        Returns:
            Prometheus 格式的指标字符串
        """
        metrics = self._metrics
        lines = []

        # 计数器
        lines.append(f"# HELP neotask_total_submitted Total number of submitted tasks")
        lines.append(f"# TYPE neotask_total_submitted counter")
        lines.append(f"neotask_total_submitted {metrics.total_submitted}")

        lines.append(f"# HELP neotask_total_completed Total number of completed tasks")
        lines.append(f"# TYPE neotask_total_completed counter")
        lines.append(f"neotask_total_completed {metrics.total_completed}")

        lines.append(f"# HELP neotask_total_failed Total number of failed tasks")
        lines.append(f"# TYPE neotask_total_failed counter")
        lines.append(f"neotask_total_failed {metrics.total_failed}")

        lines.append(f"# HELP neotask_total_cancelled Total number of cancelled tasks")
        lines.append(f"# TYPE neotask_total_cancelled counter")
        lines.append(f"neotask_total_cancelled {metrics.total_cancelled}")

        lines.append(f"# HELP neotask_total_retried Total number of retried tasks")
        lines.append(f"# TYPE neotask_total_retried counter")
        lines.append(f"neotask_total_retried {metrics.total_retried}")

        # 瞬时值
        lines.append(f"# HELP neotask_pending Current number of pending tasks")
        lines.append(f"# TYPE neotask_pending gauge")
        lines.append(f"neotask_pending {metrics.pending}")

        lines.append(f"# HELP neotask_running Current number of running tasks")
        lines.append(f"# TYPE neotask_running gauge")
        lines.append(f"neotask_running {metrics.running}")

        lines.append(f"# HELP neotask_scheduled Current number of scheduled tasks")
        lines.append(f"# TYPE neotask_scheduled gauge")
        lines.append(f"neotask_scheduled {metrics.scheduled}")

        # 比率
        lines.append(f"# HELP neotask_success_rate Task success rate")
        lines.append(f"# TYPE neotask_success_rate gauge")
        lines.append(f"neotask_success_rate {metrics.success_rate}")

        # 时间统计（毫秒）
        lines.append(f"# HELP neotask_avg_execution_time_ms Average execution time in milliseconds")
        lines.append(f"# TYPE neotask_avg_execution_time_ms gauge")
        lines.append(f"neotask_avg_execution_time_ms {metrics.avg_execution_time * 1000}")

        lines.append(f"# HELP neotask_p95_execution_time_ms P95 execution time in milliseconds")
        lines.append(f"# TYPE neotask_p95_execution_time_ms gauge")
        lines.append(f"neotask_p95_execution_time_ms {metrics.p95_execution_time * 1000}")

        lines.append(f"# HELP neotask_p99_execution_time_ms P99 execution time in milliseconds")
        lines.append(f"# TYPE neotask_p99_execution_time_ms gauge")
        lines.append(f"neotask_p99_execution_time_ms {metrics.p99_execution_time * 1000}")

        lines.append(f"# HELP neotask_avg_queue_time_ms Average queue time in milliseconds")
        lines.append(f"# TYPE neotask_avg_queue_time_ms gauge")
        lines.append(f"neotask_avg_queue_time_ms {metrics.avg_queue_time * 1000}")

        # 吞吐量
        lines.append(f"# HELP neotask_throughput Task throughput per second")
        lines.append(f"# TYPE neotask_throughput gauge")
        lines.append(f"neotask_throughput {metrics.throughput}")

        # 系统指标
        if self._system_metrics:
            lines.append(f"# HELP neotask_cpu_percent CPU usage percentage")
            lines.append(f"# TYPE neotask_cpu_percent gauge")
            lines.append(f"neotask_cpu_percent {self._system_metrics.cpu_percent}")

            lines.append(f"# HELP neotask_memory_percent Memory usage percentage")
            lines.append(f"# TYPE neotask_memory_percent gauge")
            lines.append(f"neotask_memory_percent {self._system_metrics.memory_percent}")

            lines.append(f"# HELP neotask_memory_used_mb Memory used in megabytes")
            lines.append(f"# TYPE neotask_memory_used_mb gauge")
            lines.append(f"neotask_memory_used_mb {self._system_metrics.memory_used_mb}")

            lines.append(f"# HELP neotask_thread_count Number of active threads")
            lines.append(f"# TYPE neotask_thread_count gauge")
            lines.append(f"neotask_thread_count {self._system_metrics.thread_count}")

            lines.append(f"# HELP neotask_uptime_seconds System uptime in seconds")
            lines.append(f"# TYPE neotask_uptime_seconds gauge")
            lines.append(f"neotask_uptime_seconds {self._system_metrics.uptime_seconds}")

        # 按优先级统计
        for priority, count in metrics.priority_counts.items():
            lines.append(f"# HELP neotask_priority_count Number of tasks by priority")
            lines.append(f"# TYPE neotask_priority_count gauge")
            lines.append(f"neotask_priority_count{{priority=\"{priority}\"}} {count}")

        return "\n".join(lines)

    def __repr__(self) -> str:
        return (f"MetricsCollector(window_size={self._window_size}, "
                f"total_submitted={self._metrics.total_submitted}, "
                f"success_rate={self._metrics.success_rate:.2%})")
