"""
@FileName: reclaimer.py
@Description: 任务回收器 - 回收超时、孤儿、僵尸任务和锁
@Author: HiPeng
@Time: 2026/4/15
"""

import asyncio
import time
from typing import Optional, List, Dict, Any, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

from neotask.models.task import Task, TaskStatus
from neotask.storage.base import TaskRepository
from neotask.queue.scheduler import QueueScheduler
from neotask.lock.base import TaskLock
from neotask.lock.scanner import LockScanner, LockScannerConfig
from neotask.event.bus import EventBus, TaskEvent


class ReclaimReason(Enum):
    """回收原因"""
    TIMEOUT = "timeout"              # 执行超时
    ORPHAN = "orphan"                # 孤儿任务（节点宕机）
    STALE_LOCK = "stale_lock"        # 僵尸锁
    MAX_RETRIES = "max_retries"      # 超过重试次数
    CANCELLED = "cancelled"          # 已取消


@dataclass
class ReclaimResult:
    """回收结果"""
    task_id: str
    reason: ReclaimReason
    success: bool
    message: str = ""
    timestamp: float = field(default_factory=time.time)


@dataclass
class ReclaimerConfig:
    """回收器配置"""
    # 调度配置
    interval: float = 30.0               # 回收检查间隔（秒）
    max_reclaim_per_cycle: int = 100     # 每周期最大回收数量

    # 超时配置
    task_timeout: int = 300              # 任务超时时间（秒）
    lock_timeout: int = 30               # 锁超时时间（秒）
    node_heartbeat_timeout: int = 60     # 节点心跳超时（秒）

    # 回收策略
    enable_timeout_reclaim: bool = True      # 启用超时回收
    enable_orphan_reclaim: bool = True       # 启用孤儿回收
    enable_stale_lock_reclaim: bool = True   # 启用僵尸锁回收
    enable_auto_requeue: bool = True         # 启用自动重新入队

    # 锁扫描配置
    lock_scan_interval: float = 60.0         # 锁扫描间隔（秒）
    stale_lock_ttl_threshold: int = 300      # 僵尸锁TTL阈值（秒）

    # 重试配置
    max_retries: int = 3                     # 最大重试次数
    retry_delay: float = 1.0                 # 重试延迟（秒）


class TaskReclaimer:
    """任务回收器

    负责回收：
    - 超时未完成的任务
    - 孤儿任务（节点宕机）
    - 僵尸锁（通过内部 LockScanner）

    设计模式：Composite Pattern - 组合 LockScanner 实现锁清理

    使用示例：
        >>> reclaimer = TaskReclaimer(task_repo, queue, lock, event_bus)
        >>> await reclaimer.start()
        >>>
        >>> # 手动触发回收
        >>> results = await reclaimer.reclaim_now()
        >>>
        >>> # 获取统计
        >>> stats = reclaimer.get_stats()
    """

    def __init__(
        self,
        task_repo: TaskRepository,
        queue_scheduler: QueueScheduler,
        lock: Optional[TaskLock] = None,
        event_bus: Optional[EventBus] = None,
        config: Optional[ReclaimerConfig] = None
    ):
        self._task_repo = task_repo
        self._queue = queue_scheduler
        self._lock = lock
        self._event_bus = event_bus
        self._config = config or ReclaimerConfig()

        self._running = False
        self._reclaim_loop_task: Optional[asyncio.Task] = None

        # 初始化锁扫描器（组合模式）
        self._lock_scanner: Optional[LockScanner] = None
        if lock is not None and self._config.enable_stale_lock_reclaim:
            lock_scanner_config = LockScannerConfig(
                scan_interval=self._config.lock_scan_interval,
                stale_ttl_threshold=self._config.stale_lock_ttl_threshold,
                max_scan_per_cycle=self._config.max_reclaim_per_cycle
            )
            self._lock_scanner = LockScanner(lock, lock_scanner_config)

        # 统计信息
        self._stats: Dict[ReclaimReason, int] = {reason: 0 for reason in ReclaimReason}
        self._total_reclaimed = 0
        self._last_reclaim_time: Optional[float] = None
        self._reclaim_history: List[ReclaimResult] = []
        self._max_history = 1000

        self._stats_lock = asyncio.Lock()

    async def start(self) -> None:
        """启动回收器"""
        if self._running:
            return

        self._running = True

        # 启动锁扫描器
        if self._lock_scanner:
            await self._lock_scanner.start()

        # 启动回收循环
        self._reclaim_loop_task = asyncio.create_task(self._reclaim_loop())

    async def stop(self, graceful: bool = True) -> None:
        """停止回收器

        Args:
            graceful: 是否优雅停止
        """
        self._running = False

        # 停止锁扫描器
        if self._lock_scanner:
            await self._lock_scanner.stop()

        # 停止回收循环
        if self._reclaim_loop_task:
            self._reclaim_loop_task.cancel()
            try:
                await self._reclaim_loop_task
            except asyncio.CancelledError:
                pass

    async def reclaim_now(self) -> List[ReclaimResult]:
        """立即执行一次回收

        Returns:
            回收结果列表
        """
        return await self._execute_reclaim()

    async def cleanup_stale_locks_now(self) -> int:
        """立即执行一次僵尸锁清理

        Returns:
            清理的锁数量
        """
        if self._lock_scanner:
            return await self._lock_scanner.scan_now()
        return 0

    async def _reclaim_loop(self) -> None:
        """回收循环"""
        while self._running:
            try:
                await self._execute_reclaim()
                await asyncio.sleep(self._config.interval)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    async def _execute_reclaim(self) -> List[ReclaimResult]:
        """执行回收"""
        self._last_reclaim_time = time.time()
        results = []

        # 1. 回收超时任务
        if self._config.enable_timeout_reclaim:
            timeout_results = await self._reclaim_timeout_tasks()
            results.extend(timeout_results)

        # 2. 回收孤儿任务
        if self._config.enable_orphan_reclaim:
            orphan_results = await self._reclaim_orphan_tasks()
            results.extend(orphan_results)

        # 3. 回收僵尸锁（通过锁扫描器）
        # 注意：锁扫描器有自己的调度循环，这里不再重复执行
        # 如果需要同步执行，可以取消注释
        # if self._config.enable_stale_lock_reclaim and self._lock_scanner:
        #     stale_results = await self._reclaim_stale_locks()
        #     results.extend(stale_results)

        # 更新统计
        await self._update_stats(results)

        return results

    async def _reclaim_timeout_tasks(self) -> List[ReclaimResult]:
        """回收超时任务"""
        results = []

        # 获取所有 RUNNING 状态的任务
        running_tasks = await self._task_repo.list_by_status(
            TaskStatus.RUNNING,
            limit=self._config.max_reclaim_per_cycle
        )

        now = time.time()
        for task in running_tasks:
            # 检查是否超时
            if task.started_at:
                elapsed = now - task.started_at.timestamp()
                timeout = getattr(task, 'ttl', self._config.task_timeout)

                if elapsed > timeout:
                    result = await self._reclaim_single_task(task, ReclaimReason.TIMEOUT)
                    results.append(result)

        return results

    async def _reclaim_orphan_tasks(self) -> List[ReclaimResult]:
        """回收孤儿任务（节点宕机）"""
        results = []

        # 获取所有 RUNNING 状态的任务
        running_tasks = await self._task_repo.list_by_status(
            TaskStatus.RUNNING,
            limit=self._config.max_reclaim_per_cycle
        )

        # 获取活跃节点
        active_nodes = await self._get_active_nodes()

        for task in running_tasks:
            # 检查节点是否存活
            if task.node_id and task.node_id not in active_nodes:
                result = await self._reclaim_single_task(task, ReclaimReason.ORPHAN)
                results.append(result)

        return results

    async def _reclaim_stale_locks(self) -> List[ReclaimResult]:
        """回收僵尸锁（与任务关联的锁）

        注意：独立的僵尸锁（无任务关联）由 LockScanner 自动处理
        此方法只处理与现有任务相关的僵尸锁
        """
        results = []

        if not self._lock:
            return results

        try:
            # 1. 获取所有锁键
            lock_keys = await self._lock.scan_locks(pattern="task:*", count=self._config.max_reclaim_per_cycle)

            for lock_key in lock_keys:
                # 从锁键中提取任务ID
                task_id = self._extract_task_id_from_lock_key(lock_key)

                if not task_id:
                    continue

                # 检查任务是否存在
                task = await self._task_repo.get(task_id)

                # 如果任务不存在，或者任务状态不是 RUNNING，则清理锁
                if task is None:
                    # 任务不存在，清理僵尸锁
                    await self._lock.release(lock_key)
                    results.append(ReclaimResult(
                        task_id=task_id,
                        reason=ReclaimReason.STALE_LOCK,
                        success=True,
                        message=f"Cleaned stale lock (task not found)"
                    ))
                elif task.status != TaskStatus.RUNNING:
                    # 任务状态不是 RUNNING，清理锁
                    await self._lock.release(lock_key)
                    results.append(ReclaimResult(
                        task_id=task_id,
                        reason=ReclaimReason.STALE_LOCK,
                        success=True,
                        message=f"Cleaned stale lock (task status: {task.status.value})"
                    ))

        except Exception as e:
            # 记录错误但不中断回收流程
            pass

        return results

    async def _reclaim_single_task(self, task: Task, reason: ReclaimReason) -> ReclaimResult:
        """回收单个任务

        Args:
            task: 任务对象
            reason: 回收原因

        Returns:
            回收结果
        """
        try:
            # 检查重试次数
            retry_count = getattr(task, 'retry_count', 0)

            if self._config.enable_auto_requeue and retry_count < self._config.max_retries:
                # 重试：更新重试计数，重新入队
                new_retry_count = retry_count + 1

                # 更新任务
                task.status = TaskStatus.PENDING
                task.retry_count = new_retry_count
                task.error = f"Reclaimed due to {reason.value}"
                task.node_id = ""
                await self._task_repo.save(task)

                # 重新入队（带延迟）
                delay = self._config.retry_delay * new_retry_count
                await self._queue.push(task.task_id, task.priority.value, delay=delay)

                # 释放锁
                if self._lock:
                    lock_key = f"task:{task.task_id}"
                    await self._lock.release(lock_key)

                # 发送事件
                if self._event_bus:
                    await self._event_bus.emit(TaskEvent(
                        "task.reclaimed",
                        task.task_id,
                        {
                            "reason": reason.value,
                            "retry_count": new_retry_count,
                            "action": "requeued"
                        }
                    ))

                return ReclaimResult(
                    task_id=task.task_id,
                    reason=reason,
                    success=True,
                    message=f"Requeued (retry {new_retry_count}/{self._config.max_retries})"
                )
            else:
                # 超过重试次数，标记为失败
                task.status = TaskStatus.FAILED
                task.error = f"Task failed after {retry_count} retries: reclaimed due to {reason.value}"
                task.completed_at = datetime.now()
                await self._task_repo.save(task)

                # 释放锁
                if self._lock:
                    lock_key = f"task:{task.task_id}"
                    await self._lock.release(lock_key)

                # 发送事件
                if self._event_bus:
                    await self._event_bus.emit(TaskEvent(
                        "task.reclaimed",
                        task.task_id,
                        {
                            "reason": reason.value,
                            "action": "failed"
                        }
                    ))

                return ReclaimResult(
                    task_id=task.task_id,
                    reason=reason,
                    success=True,
                    message=f"Marked as failed (max retries exceeded)"
                )

        except Exception as e:
            return ReclaimResult(
                task_id=task.task_id,
                reason=reason,
                success=False,
                message=str(e)
            )

    def _extract_task_id_from_lock_key(self, lock_key: str) -> Optional[str]:
        """从锁键中提取任务ID

        Args:
            lock_key: 锁键名，如 "lock:task:abc123" 或 "task:abc123"

        Returns:
            任务ID
        """
        # 移除前缀
        prefixes = ["lock:task:", "lock:", "task:"]
        for prefix in prefixes:
            if lock_key.startswith(prefix):
                return lock_key[len(prefix):]

        # 如果没有匹配的前缀，返回原键（可能本身就是任务ID）
        return lock_key

    async def _get_active_nodes(self) -> Set[str]:
        """获取活跃节点列表"""
        active_nodes = set()

        # TODO: 从存储获取活跃节点
        # 这需要实现节点心跳管理

        return active_nodes

    async def _update_stats(self, results: List[ReclaimResult]) -> None:
        """更新统计信息"""
        async with self._stats_lock:
            for result in results:
                if result.success:
                    self._stats[result.reason] = self._stats.get(result.reason, 0) + 1
                    self._total_reclaimed += 1

                    # 记录历史
                    self._reclaim_history.append(result)
                    if len(self._reclaim_history) > self._max_history:
                        self._reclaim_history = self._reclaim_history[-self._max_history:]

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = {
            "total_reclaimed": self._total_reclaimed,
            "last_reclaim_time": self._last_reclaim_time,
            "by_reason": {k.value: v for k, v in self._stats.items()},
            "recent_reclaims": [
                {
                    "task_id": r.task_id,
                    "reason": r.reason.value,
                    "success": r.success,
                    "timestamp": r.timestamp
                }
                for r in self._reclaim_history[-10:]
            ],
            "is_running": self._running,
            "config": {
                "interval": self._config.interval,
                "task_timeout": self._config.task_timeout,
                "max_retries": self._config.max_retries,
                "enable_timeout_reclaim": self._config.enable_timeout_reclaim,
                "enable_orphan_reclaim": self._config.enable_orphan_reclaim,
                "enable_stale_lock_reclaim": self._config.enable_stale_lock_reclaim
            }
        }

        # 添加锁扫描器统计
        if self._lock_scanner:
            stats["lock_scanner"] = self._lock_scanner.get_stats()

        return stats

    def reset_stats(self) -> None:
        """重置统计信息"""
        self._stats = {reason: 0 for reason in ReclaimReason}
        self._total_reclaimed = 0
        self._reclaim_history.clear()
        if self._lock_scanner:
            self._lock_scanner.reset_stats()

    @property
    def lock_scanner(self) -> Optional[LockScanner]:
        """获取锁扫描器实例"""
        return self._lock_scanner