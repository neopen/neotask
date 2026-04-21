"""
@FileName: test_task_pool.py
@Description: TaskPool 集成测试
@Author: HiPeng
@Time: 2026/4/21
"""

import pytest
import asyncio
from typing import Dict, Any

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.models.task import TaskStatus, TaskPriority


class TestTaskPoolMemory:
    """TaskPool 内存存储集成测试"""

    @pytest.fixture
    def task_pool(self):
        """创建内存 TaskPool"""
        async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
            return {"result": "processed", "input": data}

        config = TaskPoolConfig(storage_type="memory")
        pool = TaskPool(executor, config)
        pool.start()
        yield pool
        pool.shutdown()

    @pytest.mark.asyncio
    async def test_submit_and_wait(self, task_pool):
        """测试提交任务并等待结果"""
        task_id = await task_pool.submit_async({"test": "data"})
        assert task_id is not None

        result = await task_pool.wait_for_result_async(task_id, timeout=5)
        assert result is not None
        assert result["result"] == "processed"

    @pytest.mark.asyncio
    async def test_get_task_status(self, task_pool):
        """测试获取任务状态"""
        task_id = await task_pool.submit_async({"test": "data"})

        status = await task_pool.get_status_async(task_id)
        assert status == TaskStatus.PENDING.value

        # 等待完成
        await task_pool.wait_for_result_async(task_id)

        status = await task_pool.get_status_async(task_id)
        assert status == TaskStatus.SUCCESS.value

    @pytest.mark.asyncio
    async def test_get_task_result(self, task_pool):
        """测试获取任务结果"""
        task_id = await task_pool.submit_async({"key": "value"})

        await task_pool.wait_for_result_async(task_id)

        result = await task_pool.get_result_async(task_id)
        assert result is not None
        assert result["task_id"] == task_id
        assert result["status"] == TaskStatus.SUCCESS.value

    @pytest.mark.asyncio
    async def test_submit_with_priority(self, task_pool):
        """测试带优先级的任务提交"""
        # 提交不同优先级的任务
        task_ids = []

        # 低优先级
        task_id_low = await task_pool.submit_async(
            {"priority": "low"},
            priority=TaskPriority.LOW
        )
        task_ids.append(task_id_low)

        # 高优先级
        task_id_high = await task_pool.submit_async(
            {"priority": "high"},
            priority=TaskPriority.HIGH
        )
        task_ids.append(task_id_high)

        # 关键优先级
        task_id_critical = await task_pool.submit_async(
            {"priority": "critical"},
            priority=TaskPriority.CRITICAL
        )
        task_ids.append(task_id_critical)

        # 等待所有完成
        for task_id in task_ids:
            await task_pool.wait_for_result_async(task_id)

    @pytest.mark.asyncio
    async def test_submit_batch(self, task_pool):
        """测试批量提交"""
        tasks_data = [
            {"index": i, "data": f"task_{i}"}
            for i in range(10)
        ]

        task_ids = await task_pool.submit_batch_async(tasks_data, priority=TaskPriority.NORMAL)

        assert len(task_ids) == 10

        # 等待所有完成
        for task_id in task_ids:
            result = await task_pool.wait_for_result_async(task_id)
            assert result["result"] == "processed"

    @pytest.mark.asyncio
    async def test_cancel_task(self, task_pool):
        """测试取消任务"""
        # 提交一个长时间运行的任务
        async def long_executor(data):
            await asyncio.sleep(10)
            return {"result": "done"}

        config = TaskPoolConfig(storage_type="memory")
        pool = TaskPool(long_executor, config)
        pool.start()

        try:
            task_id = await pool.submit_async({"test": "data"})

            # 取消任务
            cancelled = await pool.cancel_async(task_id)
            assert cancelled is True

            status = await pool.get_status_async(task_id)
            assert status == TaskStatus.CANCELLED.value
        finally:
            pool.shutdown()

    @pytest.mark.asyncio
    async def test_get_stats(self, task_pool):
        """测试获取统计信息"""
        # 提交一些任务
        for i in range(5):
            await task_pool.submit_async({"index": i})

        await asyncio.sleep(0.5)

        stats = await task_pool.get_stats_async()
        assert "queue_size" in stats
        assert "total" in stats
        assert "pending" in stats
        assert "running" in stats
        assert "completed" in stats

    @pytest.mark.asyncio
    async def test_pause_and_resume(self, task_pool):
        """测试暂停和恢复"""
        # 提交任务
        task_id = await task_pool.submit_async({"test": "data"})

        # 暂停
        await task_pool.pause_async()

        # 等待一段时间，任务不应被执行
        await asyncio.sleep(0.5)

        status = await task_pool.get_status_async(task_id)
        assert status == TaskStatus.PENDING.value

        # 恢复
        await task_pool.resume_async()

        # 等待完成
        await task_pool.wait_for_result_async(task_id)

        status = await task_pool.get_status_async(task_id)
        assert status == TaskStatus.SUCCESS.value


class TestTaskPoolSQLite:
    """TaskPool SQLite 存储集成测试"""

    @pytest.fixture
    def task_pool_sqlite(self, tmp_path):
        """创建 SQLite TaskPool"""
        db_path = str(tmp_path / "test_tasks.db")

        async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
            return {"result": "processed", "input": data}

        config = TaskPoolConfig(
            storage_type="sqlite",
            sqlite_path=db_path
        )
        pool = TaskPool(executor, config)
        pool.start()
        yield pool
        pool.shutdown()

    @pytest.mark.asyncio
    async def test_persistence(self, task_pool_sqlite):
        """测试任务持久化"""
        task_id = await task_pool_sqlite.submit_async({"test": "persistence"})

        # 等待完成
        result = await task_pool_sqlite.wait_for_result_async(task_id)
        assert result is not None

        # 重启后任务应该还在
        # 注意：这里只是验证任务存在，实际重启需要重新创建 pool

    @pytest.mark.asyncio
    async def test_task_exists_after_restart(self, tmp_path):
        """测试重启后任务存在"""
        db_path = str(tmp_path / "test_tasks.db")

        async def executor(data):
            return {"result": "done"}

        # 第一个实例
        config1 = TaskPoolConfig(storage_type="sqlite", sqlite_path=db_path)
        pool1 = TaskPool(executor, config1)
        pool1.start()

        task_id = await pool1.submit_async({"test": "data"})
        await pool1.wait_for_result_async(task_id)
        pool1.shutdown()

        # 第二个实例
        config2 = TaskPoolConfig(storage_type="sqlite", sqlite_path=db_path)
        pool2 = TaskPool(executor, config2)
        pool2.start()

        # 任务应该仍然存在
        result = await pool2.get_result_async(task_id)
        assert result is not None
        assert result["task_id"] == task_id

        pool2.shutdown()


class TestTaskPoolPriority:
    """TaskPool 优先级集成测试"""

    @pytest.fixture
    def task_pool(self):
        """创建 TaskPool"""
        execution_order = []

        async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
            execution_order.append(data["priority"])
            await asyncio.sleep(0.05)
            return {"order": execution_order.copy()}

        config = TaskPoolConfig(storage_type="memory", worker_concurrency=1)  # 单 worker 确保顺序
        pool = TaskPool(executor, config)
        pool.start()
        yield pool, execution_order
        pool.shutdown()

    @pytest.mark.asyncio
    async def test_priority_execution_order(self, task_pool):
        """测试优先级执行顺序"""
        pool, execution_order = task_pool

        # 提交不同优先级的任务
        await pool.submit_async({"priority": "normal"}, priority=TaskPriority.NORMAL)
        await pool.submit_async({"priority": "high"}, priority=TaskPriority.HIGH)
        await pool.submit_async({"priority": "critical"}, priority=TaskPriority.CRITICAL)
        await pool.submit_async({"priority": "low"}, priority=TaskPriority.LOW)

        # 等待所有任务完成
        await asyncio.sleep(1)

        # 优先级高的应该先执行
        assert execution_order[0] == "critical"
        assert execution_order[1] == "high"

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
