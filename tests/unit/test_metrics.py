"""
@FileName: test_metrics.py
@Description: 指标收集单元测试
@Author: HiPeng
@Time: 2026/4/21
"""

import pytest
import asyncio
import time
import random

from neotask.monitor.metrics import MetricsCollector, TaskMetrics


class TestMetricsCollector:
    """指标收集器测试"""

    @pytest.mark.asyncio
    async def test_record_task_submit(self, metrics_collector):
        """测试记录任务提交"""
        await metrics_collector.record_task_submit("task_001", priority=2)

        summary = metrics_collector.get_summary()
        assert summary["total_submitted"] == 1
        assert summary["pending"] == 1

    @pytest.mark.asyncio
    async def test_record_task_start(self, metrics_collector):
        """测试记录任务开始"""
        await metrics_collector.record_task_submit("task_001", priority=2)
        await metrics_collector.record_task_start("task_001")

        summary = metrics_collector.get_summary()
        assert summary["pending"] == 0
        assert summary["running"] == 1

    @pytest.mark.asyncio
    async def test_record_task_complete(self, metrics_collector):
        """测试记录任务完成"""
        await metrics_collector.record_task_submit("task_001", priority=2)
        await metrics_collector.record_task_start("task_001")
        await metrics_collector.record_task_complete("task_001")

        summary = metrics_collector.get_summary()
        assert summary["total_completed"] == 1
        assert summary["running"] == 0
        assert summary["success_rate"] == 1.0

    @pytest.mark.asyncio
    async def test_record_task_failed(self, metrics_collector):
        """测试记录任务失败"""
        await metrics_collector.record_task_submit("task_001", priority=2)
        await metrics_collector.record_task_start("task_001")
        await metrics_collector.record_task_failed("task_001")

        summary = metrics_collector.get_summary()
        assert summary["total_failed"] == 1
        assert summary["running"] == 0
        assert summary["success_rate"] == 0.0

    @pytest.mark.asyncio
    async def test_record_task_cancelled(self, metrics_collector):
        """测试记录任务取消"""
        await metrics_collector.record_task_submit("task_001", priority=2)
        await metrics_collector.record_task_start("task_001")
        await metrics_collector.record_task_cancelled("task_001")

        summary = metrics_collector.get_summary()
        assert summary["total_cancelled"] == 1
        assert summary["running"] == 0

    @pytest.mark.asyncio
    async def test_record_task_retry(self, metrics_collector):
        """测试记录任务重试"""
        await metrics_collector.record_task_retry("task_001", 1)
        await metrics_collector.record_task_retry("task_002", 2)

        summary = metrics_collector.get_summary()
        assert summary["total_retried"] == 2

    @pytest.mark.asyncio
    async def test_record_task_scheduled(self, metrics_collector):
        """测试记录任务调度"""
        await metrics_collector.record_task_scheduled("task_001")
        await metrics_collector.record_task_scheduled("task_002")

        summary = metrics_collector.get_summary()
        assert summary["scheduled"] == 2

        await metrics_collector.record_task_unscheduled("task_001")

        summary = metrics_collector.get_summary()
        assert summary["scheduled"] == 1

    @pytest.mark.asyncio
    async def test_execution_time_tracking(self, metrics_collector):
        """测试执行时间跟踪"""
        await metrics_collector.record_task_submit("task_001", priority=2)
        await asyncio.sleep(0.01)

        await metrics_collector.record_task_start("task_001")
        await asyncio.sleep(0.1)  # 模拟执行
        await metrics_collector.record_task_complete("task_001")

        summary = metrics_collector.get_summary()
        # 执行时间应该大约 0.1 秒（允许一些误差，四舍五入到两位小数后为 0.10）
        assert summary["avg_execution_time"] >= 0.09
        assert summary["avg_execution_time"] <= 0.15

    @pytest.mark.asyncio
    async def test_queue_time_tracking(self, metrics_collector):
        """测试排队时间跟踪"""
        await metrics_collector.record_task_submit("task_001", priority=2)
        await asyncio.sleep(0.05)  # 模拟排队

        await metrics_collector.record_task_start("task_001")
        await metrics_collector.record_task_complete("task_001")

        summary = metrics_collector.get_summary()
        assert summary["avg_queue_time"] >= 0.04
        assert summary["avg_queue_time"] <= 0.07

    @pytest.mark.asyncio
    async def test_multiple_tasks(self, metrics_collector):
        """测试多个任务"""
        task_ids = [f"task_{i:03d}" for i in range(10)]

        # 提交所有任务
        for i, task_id in enumerate(task_ids):
            await metrics_collector.record_task_submit(task_id, priority=i % 4)

        # 开始所有任务
        for task_id in task_ids:
            await metrics_collector.record_task_start(task_id)

        # 完成大部分任务
        for task_id in task_ids[:7]:
            await metrics_collector.record_task_complete(task_id)

        # 失败一些任务
        for task_id in task_ids[7:9]:
            await metrics_collector.record_task_failed(task_id)

        # 取消一个任务
        await metrics_collector.record_task_cancelled(task_ids[9])

        summary = metrics_collector.get_summary()
        assert summary["total_submitted"] == 10
        assert summary["total_completed"] == 7
        assert summary["total_failed"] == 2
        assert summary["total_cancelled"] == 1
        assert summary["running"] == 0
        # 成功率 = 7 / (7+2) = 0.777... 四舍五入到两位小数为 0.78
        assert summary["success_rate"] == 0.78

    @pytest.mark.asyncio
    async def test_p95_execution_time(self, metrics_collector):
        """测试 P95 执行时间"""
        random.seed(42)

        for i in range(100):
            task_id = f"task_{i:03d}"
            await metrics_collector.record_task_submit(task_id, priority=2)
            await metrics_collector.record_task_start(task_id)

            exec_time = random.uniform(0.01, 0.2)
            await asyncio.sleep(exec_time)

            await metrics_collector.record_task_complete(task_id)

        summary = metrics_collector.get_summary()
        # P95 应该在 0.15-0.20 之间（四舍五入到两位小数）
        assert 0.15 <= summary["p95_execution_time"] <= 0.20

    @pytest.mark.asyncio
    async def test_window_size_limit(self):
        """测试窗口大小限制"""
        collector = MetricsCollector(window_size=10)
        await collector.start()

        try:
            for i in range(20):
                task_id = f"task_{i:03d}"
                await collector.record_task_submit(task_id, priority=2)
                await collector.record_task_start(task_id)
                await collector.record_task_complete(task_id)

            metrics = collector.get_metrics()
            assert len(metrics.execution_times) <= 10
            assert len(metrics.queue_times) <= 10
        finally:
            await collector.stop()

    @pytest.mark.asyncio
    async def test_reset_metrics(self, metrics_collector):
        """测试重置指标"""
        await metrics_collector.record_task_submit("task_001", priority=2)
        await metrics_collector.record_task_start("task_001")
        await metrics_collector.record_task_complete("task_001")

        summary_before = metrics_collector.get_summary()
        assert summary_before["total_submitted"] == 1

        await metrics_collector.reset()

        summary_after = metrics_collector.get_summary()
        assert summary_after["total_submitted"] == 0
        assert summary_after["pending"] == 0
        assert summary_after["running"] == 0
        assert summary_after["total_completed"] == 0
        assert summary_after["total_failed"] == 0

    @pytest.mark.asyncio
    async def test_concurrent_recording(self):
        """测试并发记录"""
        collector = MetricsCollector(window_size=100)
        await collector.start()

        try:
            async def record_operations(task_id: str):
                await collector.record_task_submit(task_id, priority=2)
                await collector.record_task_start(task_id)
                await asyncio.sleep(0.001)
                await collector.record_task_complete(task_id)

            tasks = [record_operations(f"task_{i:03d}") for i in range(50)]
            await asyncio.gather(*tasks)

            summary = collector.get_summary()
            assert summary["total_submitted"] == 50
            assert summary["total_completed"] == 50
            assert summary["running"] == 0
        finally:
            await collector.stop()

    @pytest.mark.asyncio
    async def test_get_metrics_object(self, metrics_collector):
        """测试获取指标对象"""
        await metrics_collector.record_task_submit("task_001", priority=2)
        await metrics_collector.record_task_start("task_001")
        await metrics_collector.record_task_complete("task_001")

        metrics = metrics_collector.get_metrics()
        assert isinstance(metrics, TaskMetrics)
        assert metrics.total_submitted == 1
        assert metrics.total_completed == 1

    @pytest.mark.asyncio
    async def test_priority_counts(self, metrics_collector):
        """测试优先级计数"""
        priorities = [0, 1, 2, 3, 0, 1, 2, 0]

        for i, priority in enumerate(priorities):
            task_id = f"task_{i:03d}"
            await metrics_collector.record_task_submit(task_id, priority=priority)

        summary = metrics_collector.get_summary()
        assert summary["priority_counts"][0] == 3
        assert summary["priority_counts"][1] == 2
        assert summary["priority_counts"][2] == 2
        assert summary["priority_counts"][3] == 1

    @pytest.mark.asyncio
    async def test_throughput_calculation(self, metrics_collector):
        """测试吞吐量计算"""
        for i in range(10):
            task_id = f"task_{i:03d}"
            await metrics_collector.record_task_submit(task_id, priority=2)
            await metrics_collector.record_task_start(task_id)
            await asyncio.sleep(0.01)
            await metrics_collector.record_task_complete(task_id)

        summary = metrics_collector.get_summary()
        # 吞吐量应该在 50-150 之间（两位小数）
        assert 50 <= summary["throughput"] <= 150

    @pytest.mark.asyncio
    async def test_async_get_summary(self, metrics_collector):
        """测试异步获取摘要"""
        await metrics_collector.record_task_submit("task_001", priority=2)

        summary = await metrics_collector.get_summary_async()
        assert summary["total_submitted"] == 1

    @pytest.mark.asyncio
    async def test_start_stop(self):
        """测试启动和停止"""
        collector = MetricsCollector(enable_system_metrics=True)

        await collector.start()
        assert collector._system_metrics_task is not None

        await asyncio.sleep(0.1)

        await collector.stop()
        assert collector._system_metrics_task is None


class TestMetricsCollectorPrometheus:
    """Prometheus 格式导出测试"""

    @pytest.mark.asyncio
    async def test_to_prometheus_format(self, metrics_collector):
        """测试 Prometheus 格式导出"""
        await metrics_collector.record_task_submit("task_001", priority=2)
        await metrics_collector.record_task_start("task_001")
        await metrics_collector.record_task_complete("task_001")

        prometheus_output = metrics_collector.to_prometheus_format()

        assert "neotask_total_submitted" in prometheus_output
        assert "neotask_total_completed" in prometheus_output
        assert "neotask_success_rate" in prometheus_output
        assert "neotask_avg_execution_time_ms" in prometheus_output

    @pytest.mark.asyncio
    async def test_prometheus_format_two_decimals(self, metrics_collector):
        """测试 Prometheus 格式输出两位小数"""
        await metrics_collector.record_task_submit("task_001", priority=2)
        await metrics_collector.record_task_start("task_001")
        await metrics_collector.record_task_complete("task_001")

        prometheus_output = metrics_collector.to_prometheus_format()

        # 验证浮点数格式为两位小数
        import re
        float_pattern = r"\d+\.\d{2}"
        matches = re.findall(float_pattern, prometheus_output)

        # 应该有多个两位小数的浮点数
        assert len(matches) > 0
        for match in matches:
            assert len(match.split(".")[1]) == 2


class TestTaskMetrics:
    """TaskMetrics 数据类测试"""

    def test_success_rate_two_decimals(self):
        """测试成功率两位小数"""
        metrics = TaskMetrics()
        metrics.total_completed = 8
        metrics.total_failed = 2

        # 8/10 = 0.8，两位小数为 0.8
        assert metrics.success_rate == 0.8
        assert metrics.failure_rate == 0.2

    def test_success_rate_empty(self):
        """测试空数据时的成功率"""
        metrics = TaskMetrics()
        assert metrics.success_rate == 1.0

    def test_avg_execution_time_two_decimals(self):
        """测试平均执行时间两位小数"""
        metrics = TaskMetrics()
        metrics.execution_times.extend([0.123, 0.456, 0.789])

        # (0.123+0.456+0.789)/3 = 0.456，四舍五入到两位小数为 0.46
        assert metrics.avg_execution_time == 0.46

    def test_percentiles_two_decimals(self):
        """测试百分位数两位小数"""
        metrics = TaskMetrics()
        metrics.execution_times.extend([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1])

        # 中位数应该是 0.6
        assert metrics.p50_execution_time == 0.6
        # P95 应该是 1.05 四舍五入到 1.05
        assert metrics.p95_execution_time == 1.05

    def test_to_dict_two_decimals(self):
        """测试转换为字典（两位小数）"""
        metrics = TaskMetrics()
        metrics.total_submitted = 100
        metrics.total_completed = 80
        metrics.total_failed = 20
        metrics.execution_times.extend([0.123, 0.456])

        result = metrics.to_dict()
        assert result["total_submitted"] == 100
        assert result["success_rate"] == 0.8
        assert result["avg_execution_time"] == 0.29  # (0.123+0.456)/2=0.2895 -> 0.29

    def test_avg_retry_count_two_decimals(self):
        """测试平均重试次数两位小数"""
        metrics = TaskMetrics()
        metrics.retry_counts.extend([0, 1, 2, 0, 1])

        # (0+1+2+0+1)/5 = 0.8
        assert metrics.avg_retry_count == 0.8

    def test_throughput_two_decimals(self):
        """测试吞吐量两位小数"""
        metrics = TaskMetrics()
        metrics.execution_times.extend([0.1, 0.1, 0.1, 0.1, 0.1])

        # 平均执行时间 0.1，吞吐量 = 1/0.1 = 10.0
        assert metrics.throughput == 10.0

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
