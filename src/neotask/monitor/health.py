"""
@FileName: health.py
@Description: 健康检查 - 系统健康状态监控
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional


# 单项检查的超时（秒）。存储/队列检查在依赖不可达时可能一直挂到 TCP 超时，
# 没有这层保护会连带卡住调用健康接口的一方。
DEFAULT_CHECK_TIMEOUT = 5.0


class HealthStatus(Enum):
    """健康状态"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class CheckResult:
    """检查结果"""
    name: str
    status: HealthStatus
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    duration_ms: float = 0.0


class HealthChecker:
    """健康检查器

    执行各项健康检查，汇总系统状态。
    """

    def __init__(self, check_timeout: float = DEFAULT_CHECK_TIMEOUT):
        self._checks: Dict[str, callable] = {}
        self._results: Dict[str, CheckResult] = {}
        self._last_check: Optional[datetime] = None
        self._check_timeout = check_timeout

    def register(self, name: str, check_func) -> None:
        """注册健康检查函数

        Args:
            name: 检查项名称
            check_func: 异步检查函数，返回 CheckResult
        """
        self._checks[name] = check_func

    def unregister(self, name: str) -> None:
        """注销健康检查"""
        self._checks.pop(name, None)
        self._results.pop(name, None)

    @property
    def last_check(self) -> Optional[datetime]:
        """上次执行检查的时间（从未执行过为 None）"""
        return self._last_check

    async def check_all(self) -> Dict[str, CheckResult]:
        """执行所有健康检查（逐项串行，单项超时则记 UNHEALTHY）"""
        self._last_check = datetime.now()

        for name, check_func in self._checks.items():
            start = datetime.now()
            try:
                result = await asyncio.wait_for(
                    check_func(), timeout=self._check_timeout
                )
                result.duration_ms = (datetime.now() - start).total_seconds() * 1000
                self._results[name] = result
            except asyncio.TimeoutError:
                self._results[name] = CheckResult(
                    name=name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Check timed out after {self._check_timeout}s",
                    duration_ms=(datetime.now() - start).total_seconds() * 1000
                )
            except Exception as e:
                self._results[name] = CheckResult(
                    name=name,
                    status=HealthStatus.UNHEALTHY,
                    message=str(e),
                    duration_ms=(datetime.now() - start).total_seconds() * 1000
                )

        return self._results

    def get_status(self) -> HealthStatus:
        """获取整体健康状态"""
        if not self._results:
            return HealthStatus.UNHEALTHY

        unhealthy = any(r.status == HealthStatus.UNHEALTHY for r in self._results.values())
        degraded = any(r.status == HealthStatus.DEGRADED for r in self._results.values())

        if unhealthy:
            return HealthStatus.UNHEALTHY
        if degraded:
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY

    def get_summary(self) -> Dict[str, Any]:
        """获取健康检查摘要"""
        return {
            "status": self.get_status().value,
            "last_check": self._last_check.isoformat() if self._last_check else None,
            "checks": {
                name: {
                    "status": result.status.value,
                    "message": result.message,
                    "duration_ms": result.duration_ms
                }
                for name, result in self._results.items()
            }
        }


class SystemHealthChecker:
    """系统健康检查器

    提供内置的系统健康检查。
    """

    def __init__(
            self,
            task_repo=None,
            queue=None,
            storage=None,
            check_timeout: float = DEFAULT_CHECK_TIMEOUT
    ):
        self._task_repo = task_repo
        self._queue = queue
        self._storage = storage
        self._checker = HealthChecker(check_timeout=check_timeout)

        # 注册内置检查
        self._register_builtin_checks()

    def _register_builtin_checks(self) -> None:
        """注册内置检查"""
        self._checker.register("storage", self._check_storage)
        self._checker.register("queue", self._check_queue)
        self._checker.register("system", self._check_system)

    async def _check_storage(self) -> CheckResult:
        """检查存储健康状态"""
        if not self._task_repo:
            return CheckResult(
                name="storage",
                status=HealthStatus.UNHEALTHY,
                message="Storage not configured"
            )

        try:
            # 尝试执行简单查询
            await self._task_repo.exists("health_check")
            return CheckResult(
                name="storage",
                status=HealthStatus.HEALTHY,
                message="Storage is healthy"
            )
        except Exception as e:
            return CheckResult(
                name="storage",
                status=HealthStatus.UNHEALTHY,
                message=f"Storage error: {e}"
            )

    async def _check_queue(self) -> CheckResult:
        """检查队列健康状态"""
        if not self._queue:
            return CheckResult(
                name="queue",
                status=HealthStatus.DEGRADED,
                message="Queue not configured"
            )

        try:
            size = await self._queue.size()
            if size > 10000:
                return CheckResult(
                    name="queue",
                    status=HealthStatus.DEGRADED,
                    message=f"Queue size is large: {size}",
                    details={"queue_size": size}
                )

            return CheckResult(
                name="queue",
                status=HealthStatus.HEALTHY,
                message="Queue is healthy",
                details={"queue_size": size}
            )
        except Exception as e:
            return CheckResult(
                name="queue",
                status=HealthStatus.UNHEALTHY,
                message=f"Queue error: {e}"
            )

    async def _check_system(self) -> CheckResult:
        """检查系统健康状态"""
        try:
            import psutil
        except ImportError:
            # psutil 是可选依赖：缺它只说明这项检查做不了，不代表系统不健康。
            # 若记为 DEGRADED，默认安装（不带 [monitor] extra）会永远上报 degraded。
            return CheckResult(
                name="system",
                status=HealthStatus.HEALTHY,
                message="psutil not installed, system check skipped. "
                        "Install with: pip install neotask[monitor]",
                details={"available": False}
            )

        try:
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=0.1)

            # 内存使用率
            memory = psutil.virtual_memory()
            memory_percent = memory.percent

            # 磁盘使用率
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent

            status = HealthStatus.HEALTHY
            messages = []

            if cpu_percent > 90:
                status = HealthStatus.DEGRADED
                messages.append(f"High CPU usage: {cpu_percent}%")

            if memory_percent > 90:
                status = HealthStatus.DEGRADED
                messages.append(f"High memory usage: {memory_percent}%")

            if disk_percent > 90:
                status = HealthStatus.DEGRADED
                messages.append(f"High disk usage: {disk_percent}%")

            return CheckResult(
                name="system",
                status=status,
                message="; ".join(messages) if messages else "System is healthy",
                details={
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory_percent,
                    "disk_percent": disk_percent
                }
            )
        except Exception as e:
            return CheckResult(
                name="system",
                status=HealthStatus.DEGRADED,
                message=f"System check error: {e}"
            )

    async def check(self) -> Dict[str, CheckResult]:
        """执行所有健康检查"""
        return await self._checker.check_all()

    @property
    def last_check(self) -> Optional[datetime]:
        """上次执行检查的时间（从未执行过为 None）"""
        return self._checker.last_check

    def get_status(self) -> HealthStatus:
        """获取整体健康状态"""
        return self._checker.get_status()

    def get_summary(self) -> Dict[str, Any]:
        """获取健康检查摘要"""
        return self._checker.get_summary()
