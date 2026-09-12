"""
@FileName: supervisor.py
@Description: Worker监督者 - 监控和管理worker健康状态
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

from neotask.worker.pool import WorkerPool


@dataclass
class SupervisorConfig:
    """监督者配置"""
    health_check_interval: float = 30.0  # 健康检查间隔（秒）
    worker_timeout: float = 300.0  # Worker超时时间（秒）
    auto_restart: bool = True  # 自动重启失败的worker
    max_restart_attempts: int = 5  # 最大重启尝试次数
    restart_delay: float = 5.0  # 重启延迟（秒）


class WorkerSupervisor:
    """Worker监督者

    监控worker池的健康状态，自动恢复失败的worker。
    """

    def __init__(
            self,
            worker_pool: WorkerPool,
            config: Optional[SupervisorConfig] = None
    ):
        self._worker_pool = worker_pool
        self._config = config or SupervisorConfig()
        self._restart_counts: Dict[int, int] = {}
        self._last_restart: Dict[int, datetime] = {}
        self._monitor_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """启动监督者"""
        if self._running:
            return

        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def stop(self) -> None:
        """停止监督者"""
        self._running = False

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

    async def _monitor_loop(self) -> None:
        """监控循环"""
        while self._running:
            try:
                await self._health_check()
                await asyncio.sleep(self._config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    async def _health_check(self) -> None:
        """执行健康检查"""
        stats = self._worker_pool.get_stats()

        for worker_id, worker_stats in stats.items():
            # 检查worker是否挂起
            if worker_stats.is_busy and worker_stats.last_active:
                idle_time = datetime.now() - worker_stats.last_active
                if idle_time.total_seconds() > self._config.worker_timeout:
                    await self._restart_worker(worker_id, reason="timeout")

            # 检查失败率
            total = worker_stats.completed_tasks + worker_stats.failed_tasks
            if total > 10:
                failure_rate = worker_stats.failed_tasks / total
                if failure_rate > 0.5:  # 失败率超过50%
                    await self._restart_worker(worker_id, reason="high_failure_rate")

    async def _restart_worker(self, worker_id: int, reason: str) -> None:
        """重启worker"""
        # 检查重启次数
        restart_count = self._restart_counts.get(worker_id, 0)
        if restart_count >= self._config.max_restart_attempts:
            # 超过最大重启次数，不重启
            return

        # 检查重启频率
        last_restart = self._last_restart.get(worker_id)
        if last_restart:
            time_since_restart = datetime.now() - last_restart
            if time_since_restart.total_seconds() < self._config.restart_delay:
                return

        # 记录重启
        self._restart_counts[worker_id] = restart_count + 1
        self._last_restart[worker_id] = datetime.now()

        if self._config.auto_restart:
            # 重启worker池
            await self._worker_pool.stop(graceful=False)
            await asyncio.sleep(self._config.restart_delay)
            await self._worker_pool.start()

    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        stats = self._worker_pool.get_stats()

        return {
            "healthy": True,
            "total_workers": len(stats),
            "active_workers": self._worker_pool.active_count(),
            "restart_counts": self._restart_counts.copy(),
            "timestamp": datetime.now().isoformat()
        }
