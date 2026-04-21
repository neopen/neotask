"""
@FileName: test_end_to_end.py
@Description: 端到端集成测试
@Author: HiPeng
@Time: 2026/4/21
"""

import asyncio
import time
from typing import Dict, Any

import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.api.task_scheduler import TaskScheduler, SchedulerConfig
from neotask.event.bus import TaskEvent
from neotask.models.task import TaskStatus, TaskPriority


class TestEndToEnd:
    """端到端测试"""

    @pytest.fixture
    def task_pool(self):
        """创建 TaskPool"""
        events_received = []

        async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
            return {"result": f"processed_{data.get('id', 'unknown')}"}

        config = TaskPoolConfig(storage_type="memory")
        pool = TaskPool(executor, config)

        # 注册事件回调
        def on_complete(event: TaskEvent):
            events_received.append(("complete", event.task_id))

        def on_failed(event: TaskEvent):
            events_received.append(("failed", event.task_id))

        pool.on_completed(on_complete)
        pool.on_failed(on_failed)

        pool.start()
        yield pool, events_received
        pool.shutdown()

    @pytest.mark.asyncio
    async def test_complete_workflow(self, task_pool):
        """测试完整工作流：提交 → 执行 → 获取结果"""
        pool, events = task_pool

        # 1. 提交任务
        task_id = await pool.submit_async({"id": 1, "data": "test"})
        assert task_id is not None

        # 2. 检查状态
        status = await pool.get_status_async(task_id)
        assert status == TaskStatus.PENDING.value

        # 3. 等待结果
        result = await pool.wait_for_result_async(task_id, timeout=5)
        assert result is not None
        assert result["result"] == "processed_1"

        # 4. 验证最终状态
        status = await pool.get_status_async(task_id)
        assert status == TaskStatus.SUCCESS.value

        # 5. 验证事件
        await asyncio.sleep(0.1)
        assert len(events) >= 1
        assert events[0][0] == "complete"

    @pytest.mark.asyncio
    async def test_concurrent_tasks(self, task_pool):
        """测试并发任务处理"""
        pool, _ = task_pool

        # 提交多个任务
        task_ids = []
        for i in range(20):
            task_id = await pool.submit_async({"id": i, "data": f"task_{i}"})
            task_ids.append(task_id)

        # 等待所有完成
        results = []
        for task_id in task_ids:
            result = await pool.wait_for_result_async(task_id, timeout=10)
            results.append(result)

        assert len(results) == 20
        for i, result in enumerate(results):
            assert result["result"] == f"processed_{i}"

    @pytest.mark.asyncio
    async def test_task_with_error_handling(self):
        """测试错误处理"""

        async def failing_executor(data):
            raise ValueError("Task execution failed")

        config = TaskPoolConfig(storage_type="memory", max_retries=1, retry_delay=0.1)
        pool = TaskPool(failing_executor, config)
        pool.start()

        try:
            task_id = await pool.submit_async({"test": "data"})

            with pytest.raises(Exception):
                await pool.wait_for_result_async(task_id, timeout=2)

            status = await pool.get_status_async(task_id)
            assert status == TaskStatus.FAILED.value
        finally:
            pool.shutdown()

    @pytest.mark.asyncio
    async def test_bulk_operations(self, task_pool):
        """测试批量操作"""
        pool, _ = task_pool

        # 批量提交
        tasks = [{"id": i, "data": f"task_{i}"} for i in range(50)]
        task_ids = await pool.submit_batch_async(tasks, priority=TaskPriority.NORMAL)

        assert len(task_ids) == 50

        # 批量等待
        results = await pool.wait_all_async(task_ids, timeout=30)

        assert len(results) == 50
        for task_id in task_ids:
            assert task_id in results

    @pytest.mark.asyncio
    async def test_queue_management(self, task_pool):
        """测试队列管理"""
        pool, _ = task_pool

        # 获取初始队列大小
        initial_size = await pool.get_queue_size_async()

        # 提交任务
        for i in range(10):
            await pool.submit_async({"id": i})

        # 队列应该增加了
        size = await pool.get_queue_size_async()
        assert size >= initial_size + 10

        # 清空队列
        await pool.clear_queue_async()

        # 队列应该为空
        size = await pool.get_queue_size_async()
        assert size == 0

    @pytest.mark.asyncio
    async def test_health_check(self, task_pool):
        """测试健康检查"""
        pool, _ = task_pool

        health = pool.get_health_status()
        assert "status" in health


class TestEndToEndWithDelayed:
    """延迟任务端到端测试"""

    @pytest.fixture
    def task_scheduler(self):
        """创建 TaskScheduler"""
        execution_times = []

        async def executor(data):
            execution_times.append(time.time())
            return {"executed_at": time.time()}

        config = SchedulerConfig.memory()
        scheduler = TaskScheduler(executor, config)
        scheduler.start()
        yield scheduler, execution_times
        scheduler.shutdown()

    @pytest.mark.asyncio
    async def test_delayed_execution(self, task_scheduler):
        """测试延迟执行"""
        scheduler, execution_times = task_scheduler

        start_time = time.time()
        delay = 1.0

        task_id = scheduler.submit_delayed(
            {"test": "delayed"},
            delay_seconds=delay
        )

        # 等待执行
        result = scheduler.wait_for_result(task_id, timeout=5)

        exec_time = result["executed_at"]
        actual_delay = exec_time - start_time

        # 延迟应该大约是指定时间
        assert actual_delay >= delay - 0.1
        assert actual_delay <= delay + 0.5

    @pytest.mark.asyncio
    async def test_interval_execution(self, task_scheduler):
        """测试周期执行"""
        scheduler, execution_times = task_scheduler

        interval = 0.5
        task_id = scheduler.submit_interval(
            {"test": "interval"},
            interval_seconds=interval,
            run_immediately=True
        )

        # 等待多次执行
        await asyncio.sleep(2.0)

        # 取消周期任务
        scheduler.cancel_periodic(task_id)

        # 应该执行了多次
        assert len(execution_times) >= 3
        assert len(execution_times) <= 5


class TestEndToEndWithEvents:
    """事件驱动端到端测试"""

    @pytest.mark.asyncio
    async def test_event_flow(self):
        """测试完整事件流"""
        events_log = []

        async def executor(data):
            return {"result": data}

        config = TaskPoolConfig(storage_type="memory")
        pool = TaskPool(executor, config)

        # 注册事件回调
        def log_event(event_type, task_id, data=None):
            events_log.append({
                "type": event_type,
                "task_id": task_id,
                "data": data
            })

        pool.on_created(lambda e: log_event("created", e.task_id))
        pool.on_started(lambda e: log_event("started", e.task_id))
        pool.on_completed(lambda e: log_event("completed", e.task_id, e.data))

        pool.start()

        try:
            task_id = await pool.submit_async({"test": "event_flow"})
            await pool.wait_for_result_async(task_id)

            await asyncio.sleep(0.2)

            # 验证事件顺序
            event_types = [e["type"] for e in events_log]

            assert "created" in event_types
            assert "started" in event_types
            assert "completed" in event_types

            # 事件顺序应该是 created -> started -> completed
            created_idx = event_types.index("created")
            started_idx = event_types.index("started")
            completed_idx = event_types.index("completed")

            assert created_idx < started_idx < completed_idx

        finally:
            pool.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
