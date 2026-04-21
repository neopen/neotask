import time

import pytest
import asyncio
from datetime import datetime, timedelta

from neotask.api.task_scheduler import TaskScheduler
from neotask.models.config import SchedulerConfig


class TestTaskSchedulerIntegration:
    """TaskScheduler 集成测试"""

    @pytest.fixture
    def scheduler(self):
        """创建调度器 fixture - 使用简单模式（enable_periodic_manager=False）"""
        config = SchedulerConfig(
            storage_type="memory",
            scan_interval=0.05,
            enable_periodic_manager=False,
            enable_time_wheel=False
        )

        async def executor(data):
            await asyncio.sleep(0.01)
            return {"result": "processed", "data": data}

        scheduler = TaskScheduler(executor=executor, config=config)
        scheduler.start()
        
        # 等待调度器完全启动
        time.sleep(0.2)
        
        yield scheduler
        
        scheduler.shutdown()

    @pytest.fixture
    def scheduler_with_periodic(self):
        """创建带周期管理器的调度器 fixture"""
        config = SchedulerConfig(
            storage_type="memory",
            scan_interval=0.05,
            enable_periodic_manager=True,
            enable_time_wheel=False
        )

        async def executor(data):
            await asyncio.sleep(0.01)
            return {"result": "processed", "data": data}

        scheduler = TaskScheduler(executor=executor, config=config)
        scheduler.start()
        
        # 等待调度器完全启动
        time.sleep(0.2)
        
        yield scheduler
        
        scheduler.shutdown()

    # ========== 延时任务测试 ==========

    def test_submit_delayed(self, scheduler):
        """测试提交延时任务"""
        task_id = scheduler.submit_delayed(
            data={"action": "test"},
            delay_seconds=0.05,
            priority=1
        )

        assert task_id is not None
        assert isinstance(task_id, str)

        time.sleep(0.15)

        status = scheduler.get_status(task_id)
        assert status in ["success", "running", "pending"]

    def test_submit_delayed_async(self, scheduler):
        """测试异步提交延时任务"""
        # 同步调用异步方法需要使用 run_coroutine_threadsafe
        loop = scheduler._pool._loop
        future = asyncio.run_coroutine_threadsafe(
            scheduler.submit_delayed_async(
                data={"action": "test"},
                delay_seconds=0.05,
                priority=1
            ),
            loop
        )
        task_id = future.result()

        assert task_id is not None

        time.sleep(0.15)
        status = scheduler.get_status(task_id)
        assert status in ["success", "running", "pending"]

    def test_submit_at(self, scheduler):
        """测试指定时间点执行"""
        execute_at = datetime.now() + timedelta(seconds=0.05)
        task_id = scheduler.submit_at(
            data={"action": "test"},
            execute_at=execute_at,
            priority=1
        )

        assert task_id is not None

        time.sleep(0.15)
        status = scheduler.get_status(task_id)
        assert status in ["success", "running", "pending"]

    # ========== 周期任务测试 ==========

    def test_submit_interval(self, scheduler):
        """测试提交周期任务（简单模式）"""
        task_id = scheduler.submit_interval(
            data={"action": "heartbeat"},
            interval_seconds=0.05,
            run_immediately=True,
            max_runs=3,
            name="Test Heartbeat"
        )

        assert task_id is not None
        assert isinstance(task_id, str)

        time.sleep(0.2)

        tasks = scheduler.get_periodic_tasks()
        assert tasks is not None

    def test_submit_interval_with_periodic_manager(self, scheduler_with_periodic):
        """测试提交周期任务（使用周期管理器）"""
        task_id = scheduler_with_periodic.submit_interval(
            data={"action": "heartbeat"},
            interval_seconds=0.05,
            run_immediately=True,
            max_runs=3,
            name="Test Heartbeat"
        )

        assert task_id is not None
        assert isinstance(task_id, str)

        time.sleep(0.2)

        tasks = scheduler_with_periodic.get_periodic_tasks()
        assert tasks is not None

    def test_pause_resume_periodic(self, scheduler_with_periodic):
        """测试暂停和恢复周期任务"""
        task_id = scheduler_with_periodic.submit_interval(
            data={"action": "heartbeat"},
            interval_seconds=0.05,
            run_immediately=True,
            name="Test Pause"
        )

        assert task_id is not None
        
        time.sleep(0.05)

        result = scheduler_with_periodic.pause_periodic(task_id)
        assert result is True

        task = scheduler_with_periodic.get_periodic_task(task_id)
        if task:
            assert task.get("is_paused") is True

        result = scheduler_with_periodic.resume_periodic(task_id)
        assert result is True

        task = scheduler_with_periodic.get_periodic_task(task_id)
        if task:
            assert task.get("is_paused") is False

    def test_cancel_periodic(self, scheduler_with_periodic):
        """测试取消周期任务"""
        task_id = scheduler_with_periodic.submit_interval(
            data={"action": "heartbeat"},
            interval_seconds=0.05,
            run_immediately=True,
            name="Test Cancel"
        )

        assert task_id is not None
        
        time.sleep(0.05)

        result = scheduler_with_periodic.cancel_periodic(task_id)
        assert result is True

        tasks = scheduler_with_periodic.get_periodic_tasks()
        task_ids = [t.get("task_id") for t in tasks]
        assert task_id not in task_ids

    # ========== Cron 任务测试 ==========

    def test_submit_cron(self, scheduler_with_periodic):
        """测试提交Cron任务"""
        task_id = scheduler_with_periodic.submit_cron(
            data={"action": "daily"},
            cron_expr="* * * * *",
            max_runs=2,
            name="Test Cron"
        )

        assert task_id is not None
        assert isinstance(task_id, str)

        tasks = scheduler_with_periodic.get_periodic_tasks()
        assert len(tasks) >= 1

        task = scheduler_with_periodic.get_periodic_task(task_id)
        assert task is not None
        assert task.get("cron_expr") == "* * * * *"

    def test_submit_cron_invalid(self, scheduler_with_periodic):
        """测试提交无效Cron表达式"""
        with pytest.raises(ValueError, match="Invalid cron expression"):
            scheduler_with_periodic.submit_cron(
                data={"action": "test"},
                cron_expr="invalid"
            )

    # ========== 任务查询测试 ==========

    def test_get_periodic_tasks(self, scheduler_with_periodic):
        """测试获取所有周期任务"""
        scheduler_with_periodic.submit_interval(
            data={"action": "task1"},
            interval_seconds=60,
            name="Task One"
        )
        scheduler_with_periodic.submit_interval(
            data={"action": "task2"},
            interval_seconds=120,
            name="Task Two"
        )

        tasks = scheduler_with_periodic.get_periodic_tasks()
        assert len(tasks) == 2

        for task in tasks:
            assert "task_id" in task

    def test_get_periodic_task(self, scheduler_with_periodic):
        """测试获取单个周期任务"""
        task_id = scheduler_with_periodic.submit_interval(
            data={"action": "test"},
            interval_seconds=60,
            name="Test Task"
        )

        task = scheduler_with_periodic.get_periodic_task(task_id)
        assert task is not None
        assert task.get("task_id") == task_id

        task = scheduler_with_periodic.get_periodic_task("nonexistent")
        assert task is None

    # ========== 统计信息测试 ==========

    def test_get_stats(self, scheduler):
        """测试获取统计信息"""
        stats = scheduler.get_stats()
        assert "periodic_tasks_count" in stats
        assert "queue_size" in stats
        assert isinstance(stats["periodic_tasks_count"], int)

    # ========== 生命周期测试 ==========

    def test_context_manager(self):
        """测试上下文管理器"""
        async def executor(data):
            await asyncio.sleep(0.01)
            return {"result": "done"}

        with TaskScheduler(executor=executor) as scheduler:
            task_id = scheduler.submit_delayed({"test": 1}, delay_seconds=0.05)
            assert scheduler._running is True
            time.sleep(0.1)

    def test_ensure_running_lazy_start(self):
        """测试懒加载启动"""
        config = SchedulerConfig(storage_type="memory")
        
        async def executor(data):
            await asyncio.sleep(0.01)
            return {"result": "done"}

        scheduler = TaskScheduler(executor=executor, config=config)
        assert scheduler._running is False

        task_id = scheduler.submit_delayed({"test": 1}, delay_seconds=0.05)
        assert scheduler._running is True

        time.sleep(0.1)
        scheduler.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])