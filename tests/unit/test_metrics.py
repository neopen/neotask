"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: test_metrics.py
@Description: 指标收集单元测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/21
"""

import time

import pytest
import asyncio
import random
import threading

from neotask import TaskPool, TaskPoolConfig
from neotask.models.task import TaskPriority
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
    async def test_p95_execution_time(self, metrics_collector, monkeypatch):
        """测试 P95 执行时间"""
        import neotask.monitor.metrics as metrics_module

        class _FakeClock:
            now = 1000.0

            @classmethod
            def time(cls):
                return cls.now

        # 用假时钟推进执行耗时，避免 100 次真实 sleep
        monkeypatch.setattr(metrics_module, "time", _FakeClock)
        random.seed(42)

        for i in range(100):
            task_id = f"task_{i:03d}"
            await metrics_collector.record_task_submit(task_id, priority=2)
            await metrics_collector.record_task_start(task_id)

            _FakeClock.now += random.uniform(0.01, 0.2)

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
    async def test_summary_read_survives_concurrent_recording(self):
        """同步摘要/导出与记录并发时不应抛 deque mutated during iteration"""
        collector = MetricsCollector(window_size=1000)
        stop = threading.Event()
        errors = []

        def reader():
            while not stop.is_set():
                try:
                    collector.get_full_summary()
                    collector.to_prometheus_format()
                except Exception as exc:
                    errors.append(exc)
                    stop.set()

        thread = threading.Thread(target=reader, daemon=True)
        thread.start()

        try:
            for i in range(300):
                task_id = f"task_{i}"
                await collector.record_task_submit(task_id)
                await collector.record_task_start(task_id)
                await collector.record_task_complete(task_id)
                if errors:
                    break
        finally:
            stop.set()
            thread.join(5)

        assert not errors, errors

    async def test_summary_read_takes_collector_lock(self):
        """同步摘要读取必须与 record_* 的写锁互斥（不再有无锁遍历 deque 的窗口）"""
        collector = MetricsCollector()
        done = threading.Event()

        def reader():
            collector.get_full_summary()
            done.set()

        with collector._lock:
            thread = threading.Thread(target=reader, daemon=True)
            thread.start()
            assert not done.wait(0.2)
        assert done.wait(2)
        thread.join(2)

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

class TestTaskPoolMetrics:
    """TaskPool 轻量监控快照与周期推送"""

    def test_get_metrics_before_start_raises(self):
        pool = TaskPool(lambda x: x)
        with pytest.raises(RuntimeError):
            pool.get_metrics()

    def test_get_metrics_snapshot_reflects_queue_and_counters(self):
        pool = TaskPool(lambda x: x, config=TaskPoolConfig(worker_concurrency=3))
        pool.start()
        try:
            # 先暂停出队，让任务稳定停在队列中再断言深度
            pool.pause()
            normal_id = pool.submit(1, priority=TaskPriority.NORMAL.value)
            critical_id = pool.submit(2, priority=TaskPriority.CRITICAL.value)

            snapshot = pool.get_metrics()
            assert snapshot["worker_concurrency"] == 3
            assert snapshot["storage_backend"] == "memory"
            assert snapshot["queue_depth_by_priority"] == {0: 1, 1: 0, 2: 1, 3: 0}
            assert snapshot["retry_queue_size"] == 0
            assert snapshot["checkpoint_count"] == 0
            assert snapshot["context_key_count"] == 0
            assert snapshot["active_workers"] == 0
            assert snapshot["oldest_pending_age_seconds"] >= 0

            # 计数器由事件驱动，异步消费后才是 2
            deadline = time.time() + 3
            while time.time() < deadline and pool.get_metrics()["pending"] < 2:
                time.sleep(0.01)
            assert pool.get_metrics()["pending"] == 2

            pool.resume()
            pool.wait_for_result(normal_id, timeout=5)
            pool.wait_for_result(critical_id, timeout=5)

            deadline = time.time() + 3
            while time.time() < deadline and pool.get_metrics()["completed"] < 2:
                time.sleep(0.01)
            final = pool.get_metrics()
            assert final["completed"] == 2
            assert final["running"] == 0
            assert final["active_workers"] == 0
            assert sum(final["queue_depth_by_priority"].values()) == 0
        finally:
            pool.shutdown()

    def test_on_metrics_pushes_and_cancels(self):
        pool = TaskPool(lambda x: x)
        pool.start()
        received = []
        event = threading.Event()
        try:
            def handler(snapshot):
                received.append(snapshot)
                if len(received) >= 2:
                    event.set()

            subscription = pool.on_metrics(handler, interval=0.02)
            try:
                assert event.wait(5)
                assert received[0]["storage_backend"] == "memory"
            finally:
                subscription.cancel()

            deadline = time.time() + 3
            while time.time() < deadline and subscription.active:
                time.sleep(0.01)
            assert subscription.active is False
        finally:
            pool.shutdown()

    def test_on_metrics_supports_async_handler(self):
        pool = TaskPool(lambda x: x)
        pool.start()
        received = []
        event = threading.Event()
        try:
            async def handler(snapshot):
                received.append(snapshot)
                event.set()

            subscription = pool.on_metrics(handler, interval=0.02)
            try:
                assert event.wait(5)
                assert "queue_depth_by_priority" in received[0]
            finally:
                subscription.cancel()
        finally:
            pool.shutdown()

    def test_metrics_handler_error_does_not_stop_subscription(self):
        pool = TaskPool(lambda x: x)
        pool.start()
        calls = []
        event = threading.Event()
        try:
            def handler(snapshot):
                calls.append(snapshot)
                if len(calls) == 1:
                    raise RuntimeError("boom")
                if len(calls) >= 3:
                    event.set()

            subscription = pool.on_metrics(handler, interval=0.02)
            try:
                assert event.wait(5), "推送在 handler 抛错后应继续"
                assert len(calls) >= 3
            finally:
                subscription.cancel()
        finally:
            pool.shutdown()

    def test_on_metrics_rejects_invalid_arguments(self):
        pool = TaskPool(lambda x: x)
        pool.start()
        try:
            with pytest.raises(ValueError):
                pool.on_metrics(lambda snapshot: None, interval=0)
            with pytest.raises(TypeError):
                pool.on_metrics("not-callable")  # type: ignore[arg-type]
        finally:
            pool.shutdown()

    def test_retry_queue_size_tracks_delayed_retries(self):
        calls = {"count": 0}

        def flaky(value):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("transient")
            return value

        pool = TaskPool(
            flaky,
            config=TaskPoolConfig(
                worker_concurrency=1,
                max_retries=2,
                retry_delay=0.3,
            ),
        )
        pool.start()
        try:
            task_id = pool.submit("ok")

            deadline = time.time() + 5
            seen = 0
            while time.time() < deadline:
                seen = max(seen, pool.get_metrics()["retry_queue_size"])
                if seen == 1:
                    break
                time.sleep(0.01)
            assert seen == 1, "重试任务在延迟窗口内应计入 retry_queue_size"

            pool.wait_for_result(task_id, timeout=5)

            deadline = time.time() + 3
            while (
                time.time() < deadline
                and pool.get_metrics()["retry_queue_size"] != 0
            ):
                time.sleep(0.01)
            final = pool.get_metrics()
            assert final["retry_queue_size"] == 0
            assert final["completed"] == 1
        finally:
            pool.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
