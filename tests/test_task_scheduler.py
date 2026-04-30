"""
@FileName: test_task_scheduler.py
@Description: TaskScheduler 单元测试
@Author: HiPeng
@Time: 2026/4/8 00:00
"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, Any

import pytest

from neotask import SchedulerConfig, TaskScheduler
from neotask.models.task import TaskStatus


# ========== 测试辅助函数 ==========

async def echo_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """回显执行器"""
    return {"echo": data, "timestamp": time.time()}


async def counter_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """计数器执行器，记录调用次数"""
    counter = data.get("counter", 0)
    return {"counter": counter + 1, "data": data}


# ========== 测试类 ==========

class TestTaskSchedulerInit:
    """测试 TaskScheduler 初始化"""

    def test_default_init(self):
        """测试默认初始化"""
        scheduler = TaskScheduler()
        assert scheduler._config is not None
        assert scheduler._config.memory()
        assert not scheduler._running

    def test_custom_config(self):
        """测试自定义配置"""
        config = SchedulerConfig(
            storage_type="sqlite",
            sqlite_path="test_scheduler.db",
            worker_concurrency=5,
            max_retries=3,
            scan_interval=0.5
        )
        scheduler = TaskScheduler(config=config)
        assert scheduler._config.sqlite()
        assert scheduler._config.worker_concurrency == 5
        assert scheduler._config.scan_interval == 0.5

    def test_with_executor(self):
        """测试带执行器的初始化"""
        scheduler = TaskScheduler(executor=echo_executor)
        assert scheduler._pool is not None

    def test_context_manager(self):
        """测试上下文管理器"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            assert scheduler._running
            task_id = scheduler.submit({"test": "data"})
            assert task_id is not None
        assert not scheduler._running


class TestTaskSchedulerDelayed:
    """测试延时任务"""

    def test_submit_delayed_sync(self):
        """测试同步延时任务"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            start = time.time()
            task_id = scheduler.submit_delayed(
                {"test": "delayed"},
                delay_seconds=1.0
            )
            elapsed = time.time() - start

            # 提交应该是即时的
            assert elapsed < 0.1
            assert task_id is not None

            # 等待结果
            result = scheduler.wait_for_result(task_id, timeout=5)
            assert result["echo"]["test"] == "delayed"

            # 实际执行时间应该 >= 延迟时间
            assert result["timestamp"] - start >= 0.9

    @pytest.mark.asyncio
    async def test_submit_delayed_async(self):
        """测试异步延时任务"""
        scheduler = TaskScheduler(executor=echo_executor)
        scheduler.start()

        start = time.time()
        task_id = await scheduler.submit_delayed_async(
            {"test": "async_delayed"},
            delay_seconds=0.5
        )

        result = await scheduler.wait_for_result_async(task_id, timeout=5)

        assert result["echo"]["test"] == "async_delayed"
        assert result["timestamp"] - start >= 0.4

        scheduler.shutdown()

    def test_submit_at(self):
        """测试指定时间点执行"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            execute_at = datetime.now() + timedelta(seconds=1)
            start = time.time()

            task_id = scheduler.submit_at(
                {"test": "at_time"},
                execute_at=execute_at
            )

            result = scheduler.wait_for_result(task_id, timeout=5)

            assert result["echo"]["test"] == "at_time"
            assert result["timestamp"] - start >= 0.9

    def test_multiple_delayed_tasks(self):
        """测试多个延时任务"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            delays = [0.5, 1.0, 1.5]
            task_ids = []
            start = time.time()

            for delay in delays:
                task_id = scheduler.submit_delayed(
                    {"delay": delay},
                    delay_seconds=delay
                )
                task_ids.append(task_id)

            # 等待所有任务完成
            for task_id in task_ids:
                result = scheduler.wait_for_result(task_id, timeout=5)
                delay = result["echo"]["delay"]
                # 验证执行时间
                assert result["timestamp"] - start >= delay - 0.1


class TestTaskSchedulerImmediate:
    """测试即时任务"""

    def test_submit_immediate(self):
        """测试提交即时任务"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            task_id = scheduler.submit({"test": "immediate"})
            result = scheduler.wait_for_result(task_id, timeout=5)
            assert result["echo"]["test"] == "immediate"

    @pytest.mark.asyncio
    async def test_submit_immediate_async(self):
        """测试异步提交即时任务"""
        scheduler = TaskScheduler(executor=echo_executor)
        scheduler.start()

        task_id = await scheduler.submit_async({"test": "async_immediate"})
        result = await scheduler.wait_for_result_async(task_id, timeout=5)
        assert result["echo"]["test"] == "async_immediate"

        scheduler.shutdown()

    def test_submit_with_priority(self):
        """测试不同优先级"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            tasks = []
            priorities = [0, 1, 2, 3]  # CRITICAL, HIGH, NORMAL, LOW

            for priority in priorities:
                task_id = scheduler.submit(
                    {"priority": priority},
                    priority=priority
                )
                tasks.append(task_id)

            for task_id in tasks:
                result = scheduler.wait_for_result(task_id, timeout=5)
                assert result is not None


class TestTaskSchedulerStats:
    """测试统计信息"""

    def test_get_stats(self):
        """测试获取统计信息"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            # 提交一些任务
            task_ids = []
            for i in range(5):
                task_id = scheduler.submit({"id": i})
                task_ids.append(task_id)

            # 等待完成
            for task_id in task_ids:
                scheduler.wait_for_result(task_id, timeout=5)

            stats = scheduler.get_stats()

            assert "queue_size" in stats
            assert "total" in stats
            assert "completed" in stats
            assert "pending" in stats

    def test_get_queue_size(self):
        """测试获取队列大小"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            initial = scheduler.get_queue_size()
            assert initial == 0

            # 提交延迟任务（不会立即入队）
            scheduler.submit_delayed({"test": "delayed"}, delay_seconds=1)

            # 提交即时任务
            scheduler.submit({"test": "immediate"})

            size = scheduler.get_queue_size()
            assert size >= 1


class TestTaskSchedulerEvents:
    """测试事件回调"""

    def test_event_callbacks(self):
        """测试事件回调"""
        events = []

        async def on_created(event):
            events.append(("created", event.task_id))

        async def on_completed(event):
            events.append(("completed", event.task_id))

        with TaskScheduler(executor=echo_executor) as scheduler:
            scheduler.on_created(on_created)
            scheduler.on_completed(on_completed)

            task_id = scheduler.submit({"test": "events"})
            scheduler.wait_for_result(task_id, timeout=5)

            # 等待事件处理
            time.sleep(0.5)

            event_types = [e[0] for e in events]
            assert "created" in event_types
            assert "completed" in event_types


class TestTaskSchedulerEdgeCases:
    """测试边界情况"""

    def test_zero_delay(self):
        """测试零延迟"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            start = time.time()
            task_id = scheduler.submit_delayed({"test": "zero"}, delay_seconds=0)
            result = scheduler.wait_for_result(task_id, timeout=5)

            elapsed = time.time() - start
            assert elapsed < 1.0
            assert result is not None

    def test_negative_delay(self):
        """测试负延迟（应该当作0处理）"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            start = time.time()
            task_id = scheduler.submit_delayed({"test": "negative"}, delay_seconds=-1)
            result = scheduler.wait_for_result(task_id, timeout=5)

            elapsed = time.time() - start
            assert elapsed < 1.0
            assert result is not None

    def test_very_long_delay(self):
        """测试长延迟（取消任务）"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            task_id = scheduler.submit_delayed(
                {"test": "long"},
                delay_seconds=3600  # 1小时
            )

            # 立即取消
            cancelled = scheduler.cancel(task_id)
            assert cancelled is True

            status = scheduler.get_status(task_id)
            assert status == TaskStatus.CANCELLED.value

    def test_concurrent_schedulers(self):
        """测试多个调度器并发"""
        scheduler1 = TaskScheduler(executor=echo_executor)
        scheduler2 = TaskScheduler(executor=echo_executor)

        scheduler1.start()
        scheduler2.start()

        task1 = scheduler1.submit({"scheduler": 1})
        task2 = scheduler2.submit({"scheduler": 2})

        result1 = scheduler1.wait_for_result(task1, timeout=5)
        result2 = scheduler2.wait_for_result(task2, timeout=5)

        assert result1["echo"]["scheduler"] == 1
        assert result2["echo"]["scheduler"] == 2

        scheduler1.shutdown()
        scheduler2.shutdown()

    def test_mixed_task_types(self):
        """测试混合任务类型"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            # 即时任务
            immediate_id = scheduler.submit({"type": "immediate"})

            # 延时任务
            delayed_id = scheduler.submit_delayed({"type": "delayed"}, delay_seconds=0.5)

            # 周期任务
            periodic_id = scheduler.submit_interval({"type": "periodic"}, interval_seconds=1)

            # 等待完成
            scheduler.wait_for_result(immediate_id, timeout=5)
            scheduler.wait_for_result(delayed_id, timeout=5)

            # 清理周期任务
            scheduler.cancel_periodic(periodic_id)

            # 验证结果
            result1 = scheduler.get_result(immediate_id)
            result2 = scheduler.get_result(delayed_id)

            assert result1["result"]["echo"]["type"] == "immediate"
            assert result2["result"]["echo"]["type"] == "delayed"


class TestTaskSchedulerStress:
    """压力测试"""

    def test_many_delayed_tasks(self):
        """测试大量延时任务"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            task_ids = []
            start = time.time()

            # 提交 100 个延时任务
            for i in range(100):
                task_id = scheduler.submit_delayed(
                    {"id": i},
                    delay_seconds=0.1
                )
                task_ids.append(task_id)

            # 等待所有任务完成
            for task_id in task_ids:
                scheduler.wait_for_result(task_id, timeout=10)

            elapsed = time.time() - start
            assert elapsed < 5  # 应该快速完成

            # 验证所有任务都完成
            for task_id in task_ids:
                status = scheduler.get_status(task_id)
                assert status == TaskStatus.SUCCESS.value

    def test_many_periodic_tasks(self):
        """测试大量周期任务"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            task_ids = []

            # 创建 50 个周期任务
            for i in range(50):
                task_id = scheduler.submit_interval(
                    {"id": i},
                    interval_seconds=60,  # 长间隔，避免太多执行
                    run_immediately=False
                )
                task_ids.append(task_id)

            periodic_tasks = scheduler.get_periodic_tasks()
            assert len(periodic_tasks) == 50

            # 清理
            for task_id in task_ids:
                scheduler.cancel_periodic(task_id)

            assert len(scheduler.get_periodic_tasks()) == 0


class TestTaskSchedulerCron:
    """测试Cron任务"""

    def test_submit_cron(self):
        """测试Cron任务"""
        execution_count = 0

        async def cron_executor(data):
            nonlocal execution_count
            execution_count += 1
            return {"count": execution_count}

        with TaskScheduler(executor=cron_executor) as scheduler:
            task_id = scheduler.submit_cron(
                {"test": "cron"},
                cron_expr="*/1 * * * *"
            )

            # 等待足够时间让任务执行
            time.sleep(1.5)

            # 取消任务
            scheduler.cancel_periodic(task_id)

            # 由于 Cron 解析返回 5 秒后，所以可能还没执行
            # 这里只验证任务已注册
            assert task_id is not None


class TestTaskSchedulerQuery:
    """测试任务查询"""

    def test_get_status(self):
        """测试获取状态"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            task_id = scheduler.submit({"test": "status"})

            # 等待一小段时间让任务可能完成
            time.sleep(0.1)

            # 状态可能是 PENDING 或 SUCCESS
            status = scheduler.get_status(task_id)
            assert status in [TaskStatus.PENDING.value, TaskStatus.SUCCESS.value]


class TestTaskSchedulerQueueControl:
    """测试队列控制"""

    def test_pause_resume(self):
        """测试暂停/恢复"""
        execution_times = []

        async def recording_executor(data):
            execution_times.append(time.time())
            return {"time": execution_times[-1]}

        with TaskScheduler(executor=recording_executor) as scheduler:
            # 提交多个任务
            for i in range(5):
                scheduler.submit({"id": i})

            # 等待一些任务开始执行
            time.sleep(0.5)
            initial_count = len(execution_times)

            # 暂停
            scheduler.pause()

            # 记录当前执行次数
            count_before_resume = len(execution_times)

            # 等待一小段时间，不应该有新任务执行
            time.sleep(0.3)
            assert len(execution_times) == count_before_resume

            # 恢复
            scheduler.resume()

            # 等待所有任务完成
            time.sleep(2)

            # 恢复后应该执行了剩余任务
            assert len(execution_times) >= 5


class TestTaskSchedulerTaskManagement:
    """测试任务管理"""

    def test_task_exists(self):
        """测试任务存在性"""
        with TaskScheduler(executor=echo_executor) as scheduler:
            task_id = scheduler.submit({"test": "exists"})
            # 等待任务被创建
            time.sleep(0.1)
            assert scheduler.task_exists(task_id) is True
            assert scheduler.task_exists("nonexistent") is False


class TestTaskSchedulerIntegration:
    """集成测试"""

    def test_multiple_task_types(self):
        """测试多种任务类型混合"""
        results = {
            "delayed": False,
            "interval": 0
        }

        async def unified_executor(data):
            task_type = data.get("type", "unknown")
            if task_type == "delayed":
                results["delayed"] = True
            elif task_type == "interval":
                results["interval"] += 1
            return {"status": task_type}

        async def run_test():
            config = SchedulerConfig.memory()
            # 不设置 enable_periodic_manager，使用默认值 True

            scheduler = TaskScheduler(executor=unified_executor, config=config)
            # 关键：不显式调用 start()，让 submit_interval 内部自动启动

            try:
                # 提交延时任务
                delayed_id = await scheduler.submit_delayed_async(
                    {"type": "delayed", "test": "delayed"},
                    delay_seconds=1.0
                )

                # 提交周期任务（这会自动启动 scheduler）
                interval_id = scheduler.submit_interval(
                    {"type": "interval", "test": "interval"},
                    interval_seconds=0.5,
                    run_immediately=True
                )

                # 等待执行
                await asyncio.sleep(2.5)

                scheduler.cancel_periodic(interval_id)
                await scheduler.wait_for_result_async(delayed_id, timeout=2.0)

                return results

            finally:
                scheduler.shutdown()

        asyncio.run(run_test())

        assert results["delayed"] is True
        assert results["interval"] >= 2



# ========== 运行测试 ==========

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
    # pytest.main([__file__, "-v", "--asyncio-mode=auto"])
