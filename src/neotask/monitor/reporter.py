"""
@FileName: reporter.py
@Description: 指标上报 - 将指标上报到外部系统
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class MetricsReporter(ABC):
    """指标上报器基类"""

    @abstractmethod
    async def report(self, metrics: Dict[str, Any]) -> None:
        """上报指标"""
        pass

    async def close(self) -> None:
        """关闭连接"""
        pass


class ConsoleReporter(MetricsReporter):
    """控制台上报器"""

    async def report(self, metrics: Dict[str, Any]) -> None:
        """输出到控制台"""
        print(f"[Metrics] {json.dumps(metrics, indent=2)}")


class FileReporter(MetricsReporter):
    """文件上报器"""

    def __init__(self, filepath: str):
        self._filepath = filepath

    async def report(self, metrics: Dict[str, Any]) -> None:
        """写入文件"""
        with open(self._filepath, 'a') as f:
            f.write(json.dumps(metrics) + '\n')


class PrometheusReporter(MetricsReporter):
    """Prometheus上报器"""

    def __init__(self, pushgateway_url: str, job_name: str = "neotask"):
        self._url = pushgateway_url.rstrip('/')
        self._job_name = job_name
        self._session = None

    async def _get_session(self):
        """获取HTTP会话"""
        if self._session is None:
            import aiohttp
            self._session = aiohttp.ClientSession()
        return self._session

    async def report(self, metrics: Dict[str, Any]) -> None:
        """上报到Prometheus Pushgateway"""
        session = await self._get_session()

        # 转换为Prometheus格式
        data = self._convert_to_prometheus(metrics)

        url = f"{self._url}/metrics/job/{self._job_name}"

        try:
            async with session.post(url, data=data) as resp:
                if resp.status not in (200, 202):
                    logger.error(f"Failed to push metrics: {resp.status}")
        except Exception as e:
            logger.error(f"Error pushing metrics to Prometheus: {e}")

    def _convert_to_prometheus(self, metrics: Dict[str, Any]) -> str:
        """转换为Prometheus格式"""
        lines = []

        # 任务统计
        lines.append(f"neotask_total_submitted {metrics.get('total_submitted', 0)}")
        lines.append(f"neotask_total_completed {metrics.get('total_completed', 0)}")
        lines.append(f"neotask_total_failed {metrics.get('total_failed', 0)}")
        lines.append(f"neotask_success_rate {metrics.get('success_rate', 1.0)}")

        # 队列统计
        lines.append(f"neotask_pending {metrics.get('pending', 0)}")
        lines.append(f"neotask_running {metrics.get('running', 0)}")

        # 时间统计
        lines.append(f"neotask_avg_execution_time_ms {metrics.get('avg_execution_time', 0) * 1000}")
        lines.append(f"neotask_p95_execution_time_ms {metrics.get('p95_execution_time', 0) * 1000}")

        return "\n".join(lines) + "\n"

    async def close(self) -> None:
        """关闭会话"""
        if self._session:
            await self._session.close()
            self._session = None


class ReporterManager:
    """上报器管理器

    管理多个指标上报器，定期上报指标。
    """

    def __init__(self, interval: float = 60.0):
        self._reporters: List[MetricsReporter] = []
        self._interval = interval
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._metrics_callback = None

    def add_reporter(self, reporter: MetricsReporter) -> None:
        """添加上报器"""
        self._reporters.append(reporter)

    def remove_reporter(self, reporter: MetricsReporter) -> None:
        """移除上报器"""
        self._reporters.remove(reporter)

    def set_metrics_callback(self, callback) -> None:
        """设置指标获取回调"""
        self._metrics_callback = callback

    async def start(self) -> None:
        """启动上报"""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._report_loop())

    async def stop(self) -> None:
        """停止上报"""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        # 关闭所有上报器
        for reporter in self._reporters:
            await reporter.close()

    async def report_now(self) -> None:
        """立即上报"""
        if not self._metrics_callback:
            return

        metrics = self._metrics_callback()

        for reporter in self._reporters:
            try:
                await reporter.report(metrics)
            except Exception as e:
                logger.error(f"Failed to report metrics: {e}")

    async def report_batch(self, metrics_list: List[Dict[str, Any]]) -> None:
        """批量上报指标"""
        if not self._reporters:
            return

        for reporter in self._reporters:
            try:
                # 如果上报器支持批量上报
                if hasattr(reporter, 'report_batch'):
                    await reporter.report_batch(metrics_list)
                else:
                    # 逐个上报
                    for metrics in metrics_list:
                        await reporter.report(metrics)
            except Exception as e:
                logger.error(f"Failed to report batch metrics: {e}")

    async def _report_loop(self) -> None:
        """上报循环（支持批量聚合）"""
        batch_buffer = []
        batch_size = 10
        last_flush = time.time()

        while self._running:
            try:
                if self._metrics_callback:
                    metrics = self._metrics_callback()
                    batch_buffer.append(metrics)

                    # 批量触发条件
                    should_flush = (
                            len(batch_buffer) >= batch_size or
                            time.time() - last_flush >= self._interval
                    )

                    if should_flush and batch_buffer:
                        await self.report_batch(batch_buffer)
                        batch_buffer.clear()
                        last_flush = time.time()

                await asyncio.sleep(1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in report loop: {e}")
                await asyncio.sleep(5)