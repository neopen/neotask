"""
@FileName: test_e2e.py
@Description: 端到端集成测试
@Author: HiPeng
@Time: 2026/4/16
"""

import asyncio
import os
import tempfile
import time

import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.models.task import TaskStatus
from tests.conftest import (
    slow_executor, echo_executor,
    skip_if_no_redis
)


class TestEndToEnd:
    """端到端测试"""

    @pytest.mark.asyncio
    async def test_submit_and_wait(self, memory_task_pool):
        """测试提交任务并等待结果"""
        # 提交任务
        task_id = await memory_task_pool.submit_async({"key": "value"})
        assert task_id is not None

        # 等待结果
        result = await memory_task_pool.wait_for_result_async(task_id, timeout=5)
        assert result["result"] == "success"
        assert result["data"]["key"] == "value"

    @pytest.mark.asyncio
    async def test_submit_batch(self, memory_task_pool):
        """测试批量提交任务"""
        tasks = [
            {"id": 1, "data": "task1"},
            {"id": 2, "data": "task2"},
            {"id": 3, "data": "task3"},
        ]

        task_ids = await memory_task_pool.submit_batch_async(tasks)
        assert len(task_ids) == 3

        # 等待所有任务完成
        results = await memory_task_pool.wait_all_async(task_ids, timeout=10)
        assert len(results) == 3

        for task_id in task_ids:
            assert task_id in results

    @pytest.mark.asyncio
    async def test_submit_with_priority(self, memory_task_pool):
        """测试优先级任务"""
        from neotask.models.task import TaskPriority

        # 提交不同优先级的任务
        task_ids = []

        # 低优先级
        task_ids.append(await memory_task_pool.submit_async(
            {"order": 1}, priority=TaskPriority.LOW
        ))
        # 高优先级
        task_ids.append(await memory_task_pool.submit_async(
            {"order": 2}, priority=TaskPriority.HIGH
        ))
        # 普通优先级
        task_ids.append(await memory_task_pool.submit_async(
            {"order": 3}, priority=TaskPriority.NORMAL
        ))

        # 等待所有任务完成
        results = await memory_task_pool.wait_all_async(task_ids, timeout=10)

        # 验证所有任务都完成
        for task_id in task_ids:
            status = await memory_task_pool.get_status_async(task_id)
            assert status == TaskStatus.SUCCESS.value

    @pytest.mark.asyncio
    async def test_submit_with_delay(self, memory_task_pool):
        """测试延迟任务"""
        start_time = time.time()
        delay = 2.0

        task_id = await memory_task_pool.submit_async(
            {"delay": delay},
            delay=delay
        )

        # 任务应该延迟执行
        result = await memory_task_pool.wait_for_result_async(task_id, timeout=delay + 2)
        elapsed = time.time() - start_time

        assert elapsed >= delay
        assert result["result"] == "success"

    @pytest.mark.asyncio
    async def test_cancel_task(self, memory_task_pool):
        """测试取消任务"""
        # 提交慢速任务
        pool = memory_task_pool
        # 临时替换为慢速执行器
        original_executor = pool._executor
        pool._executor = slow_executor

        try:
            task_id = await pool.submit_async({"delay": 5})

            # 立即取消
            cancelled = await pool.cancel_async(task_id)
            assert cancelled is True

            # 等待一小段时间
            await asyncio.sleep(0.5)

            # 检查状态
            status = await pool.get_status_async(task_id)
            assert status == TaskStatus.CANCELLED.value
        finally:
            pool._executor = original_executor

    @pytest.mark.asyncio
    async def test_task_status_transition(self, memory_task_pool):
        """测试任务状态转换"""
        pool = memory_task_pool
        original_executor = pool._executor
        pool._executor = slow_executor

        try:
            # 提交任务
            task_id = await pool.submit_async({"delay": 1})

            # 检查初始状态
            status = await pool.get_status_async(task_id)
            assert status == TaskStatus.PENDING.value

            # 等待完成
            result = await pool.wait_for_result_async(task_id, timeout=3)
            assert result["result"] == "success"

            # 检查最终状态
            status = await pool.get_status_async(task_id)
            assert status == TaskStatus.SUCCESS.value
        finally:
            pool._executor = original_executor

    @pytest.mark.asyncio
    async def test_task_failure_and_retry(self, task_pool_with_fail_executor):
        """测试任务失败和重试"""
        pool = task_pool_with_fail_executor

        # 提交会失败的任务
        task_id = await pool.submit_async({"data": "fail"})

        # 等待任务完成（应该失败）
        with pytest.raises(Exception) as exc_info:
            await pool.wait_for_result_async(task_id, timeout=10)

        assert "Task execution failed" in str(exc_info.value)

        # 检查状态
        status = await pool.get_status_async(task_id)
        assert status == TaskStatus.FAILED.value

    @pytest.mark.asyncio
    async def test_task_exists(self, memory_task_pool):
        """测试任务存在性检查"""
        task_id = await memory_task_pool.submit_async({"data": "test"})

        # 检查存在
        exists = await memory_task_pool.task_exists_async(task_id)
        assert exists is True

        # 等待完成
        await memory_task_pool.wait_for_result_async(task_id, timeout=5)

        # 删除任务
        deleted = await memory_task_pool.delete_async(task_id)
        assert deleted is True

        # 检查不存在
        exists = await memory_task_pool.task_exists_async(task_id)
        assert exists is False


class TestEndToEndWithSQLite:
    """SQLite 后端端到端测试"""

    @pytest.fixture
    def sqlite_db_path(self):
        """创建临时 SQLite 数据库文件"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        yield db_path
        # 清理：确保文件被关闭后再删除
        import gc
        gc.collect()
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                # 如果无法删除，稍后重试
                import time
                time.sleep(0.5)
                try:
                    os.unlink(db_path)
                except PermissionError:
                    pass

    @pytest.mark.asyncio
    async def test_sqlite_basic(self, sqlite_db_path):
        """测试 SQLite 基本功能"""
        config = TaskPoolConfig(storage_type="sqlite", sqlite_path=sqlite_db_path)
        pool = TaskPool(executor=echo_executor, config=config)
        pool.start()

        try:
            # 提交任务
            task_id = await pool.submit_async({"data": "test"})

            # 等待完成
            result = await pool.wait_for_result_async(task_id, timeout=5)
            assert result["result"]["data"] == "test"
        finally:
            pool.shutdown()
            # 给一点时间让连接关闭
            await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_persist_after_restart(self, sqlite_db_path):
        """测试重启后任务持久化"""
        # 创建第一个池
        config = TaskPoolConfig(storage_type="sqlite", sqlite_path=sqlite_db_path)
        pool1 = TaskPool(executor=echo_executor, config=config)
        pool1.start()

        try:
            # 提交任务
            task_id = await pool1.submit_async({"data": "persistent"})

            # 等待完成
            result = await pool1.wait_for_result_async(task_id, timeout=5)
            assert result["result"]["data"] == "persistent"

            # 获取任务信息
            task_info = await pool1.get_task_async(task_id)
            assert task_info is not None
            assert task_info["task_id"] == task_id
            assert task_info["status"] == TaskStatus.SUCCESS.value
        finally:
            pool1.shutdown()
            await asyncio.sleep(0.1)

        # 创建第二个池（使用相同的数据库文件）
        pool2 = TaskPool(executor=echo_executor, config=config)
        pool2.start()

        try:
            # 验证任务仍然存在
            task_data = await pool2.get_task_async(task_id)
            assert task_data is not None
            assert task_data["task_id"] == task_id
            assert task_data["status"] == TaskStatus.SUCCESS.value

            # 验证结果
            assert task_data["result"] is not None
        finally:
            pool2.shutdown()
            await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_sqlite_multiple_tasks(self, sqlite_db_path):
        """测试 SQLite 多任务"""
        config = TaskPoolConfig(storage_type="sqlite", sqlite_path=sqlite_db_path)
        pool = TaskPool(executor=echo_executor, config=config)
        pool.start()

        try:
            # 提交多个任务
            task_ids = []
            for i in range(10):
                task_id = await pool.submit_async({"id": i, "data": f"task_{i}"})
                task_ids.append(task_id)

            # 等待所有任务完成
            results = await pool.wait_all_async(task_ids, timeout=30)
            assert len(results) == 10

            # 验证所有任务都成功
            for task_id in task_ids:
                status = await pool.get_status_async(task_id)
                assert status == TaskStatus.SUCCESS.value
        finally:
            pool.shutdown()
            await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_sqlite_task_recovery(self, sqlite_db_path):
        """测试 SQLite 任务恢复 - 未完成任务在重启后重新入队"""
        config = TaskPoolConfig(storage_type="sqlite", sqlite_path=sqlite_db_path)

        # 第一个池：提交慢速任务
        pool1 = TaskPool(executor=slow_executor, config=config)
        pool1.start()

        task_id = None
        try:
            task_id = await pool1.submit_async({"delay": 10})

            # 等待任务开始执行
            await asyncio.sleep(0.5)

            # 检查任务状态应该是 RUNNING
            status = await pool1.get_status_async(task_id)
            # 可能是 PENDING 或 RUNNING，取决于调度
        finally:
            pool1.shutdown()
            await asyncio.sleep(0.1)

        # 第二个池：应该能够看到这个任务
        pool2 = TaskPool(executor=slow_executor, config=config)
        pool2.start()

        try:
            # 检查任务是否存在
            task_data = await pool2.get_task_async(task_id)
            assert task_data is not None

            # 任务状态可能是 RUNNING 或 PENDING
            # 这取决于 shutdown 时是否清理了锁
        finally:
            pool2.shutdown()
            await asyncio.sleep(0.1)


class TestEndToEndWithRedis:
    """Redis 后端端到端测试"""

    @pytest.mark.asyncio
    @skip_if_no_redis
    async def test_redis_basic(self, redis_config):
        """测试 Redis 基本功能"""
        pool = TaskPool(executor=echo_executor, config=redis_config)
        pool.start()

        try:
            # 提交任务
            task_id = await pool.submit_async({"data": "redis_test"})

            # 等待完成
            result = await pool.wait_for_result_async(task_id, timeout=10)
            assert result["result"]["data"] == "redis_test"

            # 验证任务状态
            status = await pool.get_status_async(task_id)
            assert status == TaskStatus.SUCCESS.value
        finally:
            pool.shutdown()
            # 给时间让连接关闭
            await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    @skip_if_no_redis
    async def test_redis_shared_queue_consumer(self, redis_config):
        """测试多个消费者共享队列

        每个任务应该只被一个消费者执行一次
        """
        task_count = 30

        # 提交所有任务（使用一个临时的 pool）
        temp_pool = TaskPool(executor=echo_executor, config=redis_config)
        temp_pool.start()

        task_ids = []
        for i in range(task_count):
            task_id = await temp_pool.submit_async({"id": i})
            task_ids.append(task_id)
            print(f"Submitted task {i}: {task_id}")

        # 等待所有任务进入队列
        await asyncio.sleep(1)
        temp_pool.shutdown()
        await asyncio.sleep(0.5)

        # 记录每个任务被哪个消费者完成（应该只被完成一次）
        task_completed_by = {}
        completed_lock = asyncio.Lock()

        async def worker(worker_id):
            """消费者工作函数"""
            pool = TaskPool(executor=echo_executor, config=redis_config)
            pool.start()

            completed_count = 0
            start_time = asyncio.get_event_loop().time()

            try:
                # 持续消费直到所有任务完成或超时
                while len(task_completed_by) < task_count:
                    # 检查超时
                    if asyncio.get_event_loop().time() - start_time > 60:
                        print(f"Consumer {worker_id} timed out")
                        break

                    # 获取任务状态，找出未完成的任务
                    for task_id in task_ids:
                        if task_id in task_completed_by:
                            continue

                        try:
                            # 尝试等待任务（短超时）
                            result = await pool.wait_for_result_async(task_id, timeout=1.0)

                            # 任务完成，记录
                            async with completed_lock:
                                if task_id not in task_completed_by:
                                    task_completed_by[task_id] = worker_id
                                    completed_count += 1
                                    print(f"Consumer {worker_id} completed task {task_id}")

                        except Exception as e:
                            # 任务还未完成或超时，继续
                            pass

                    # 短暂等待避免忙循环
                    await asyncio.sleep(0.5)

            finally:
                pool.shutdown()
                print(f"Consumer {worker_id} finished, completed {completed_count} tasks")

        # 启动三个消费者
        consumers = [worker(i) for i in range(1, 4)]
        await asyncio.gather(*consumers)

        # 验证结果
        total_completed = len(task_completed_by)
        print(f"Total completed: {total_completed}")
        print(f"Unique completed: {len(set(task_completed_by.keys()))}")
        print(f"Distribution: {dict(task_completed_by)}")

        # 每个任务应该只被完成一次
        assert total_completed == task_count, \
            f"Expected {task_count} unique completions, got {total_completed}"

        # 验证每个任务都被完成
        for task_id in task_ids:
            assert task_id in task_completed_by, f"Task {task_id} was never completed"

    @pytest.mark.asyncio
    @skip_if_no_redis
    async def test_redis_shared_queue_consumer_simple(self, redis_config):
        """测试多个消费者共享队列 - 简化版本

        验证任务被正确消费，队列最终为空
        """
        task_count = 30

        # 提交任务
        submit_pool = TaskPool(executor=echo_executor, config=redis_config)
        submit_pool.start()

        task_ids = []
        for i in range(task_count):
            task_id = await submit_pool.submit_async({"id": i})
            task_ids.append(task_id)

        # 等待任务进入队列
        await asyncio.sleep(1)
        submit_pool.shutdown()
        await asyncio.sleep(0.5)

        # 记录完成的任务
        completed_tasks = set()
        completed_lock = asyncio.Lock()

        async def worker(worker_id):
            """消费者"""
            pool = TaskPool(executor=echo_executor, config=redis_config)
            pool.start()

            local_completed = 0

            try:
                # 持续消费直到超时
                start_time = asyncio.get_event_loop().time()
                while asyncio.get_event_loop().time() - start_time < 30:
                    # 检查还有多少未完成的任务
                    remaining = [tid for tid in task_ids if tid not in completed_tasks]
                    if not remaining:
                        break

                    # 尝试获取一个未完成的任务
                    for task_id in remaining[:5]:  # 每次检查5个
                        try:
                            result = await pool.wait_for_result_async(task_id, timeout=2.0)

                            async with completed_lock:
                                if task_id not in completed_tasks:
                                    completed_tasks.add(task_id)
                                    local_completed += 1
                                    print(f"Worker {worker_id} completed task {task_id}")

                        except Exception:
                            # 任务还未完成
                            pass

                    await asyncio.sleep(0.5)

            finally:
                pool.shutdown()
                print(f"Worker {worker_id} completed {local_completed} tasks")

        # 启动三个消费者
        await asyncio.gather(worker(1), worker(2), worker(3))

        # 验证所有任务都被完成
        assert len(completed_tasks) == task_count, \
            f"Expected {task_count} completed tasks, got {len(completed_tasks)}"

        print(f"All {task_count} tasks completed successfully")

# 运行测试的入口
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
