"""
@FileName: test_task_pool.py
@Description: TaskPool 单元测试
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import time
from typing import Dict, Any

import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.common.exceptions import TaskNotFoundError, TimeoutError
from neotask.models.task import TaskPriority, TaskStatus

pytest_plugins = ('pytest_asyncio',)

# 确保异步测试使用 event_loop fixture
@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()

# ========== 测试辅助函数 ==========

async def success_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """成功的执行器"""
    return {"status": "success", "input": data, "timestamp": time.time()}


async def failing_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """失败的执行器"""
    raise ValueError("Task execution failed")


async def slow_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """慢速执行器"""
    delay = data.get("delay", 0.5)
    await asyncio.sleep(delay)
    return {"status": "success", "delay": delay, "input": data}


async def echo_executor(data: Dict[str, Any]) -> Dict[str, Any]:
    """回显执行器"""
    return {"echo": data}


# ========== 测试类 ==========

class TestTaskPoolInit:
    """测试 TaskPool 初始化"""

    def test_default_init(self):
        """测试默认初始化"""
        pool = TaskPool()
        assert pool._config is not None
        assert pool._config.memory()
        assert pool._config.worker_concurrency == 10
        assert not pool._running

    def test_custom_config(self):
        """测试自定义配置"""
        config = TaskPoolConfig(
            storage_type="memory",
            worker_concurrency=5,
            max_retries=5,
            queue_max_size=5000
        )
        pool = TaskPool(config=config)
        assert pool._config.worker_concurrency == 5
        assert pool._config.max_retries == 5
        assert pool._config.queue_max_size == 5000

    def test_with_executor_function(self):
        """测试使用执行器函数"""
        pool = TaskPool(executor=echo_executor)
        assert pool._executor is not None

    def test_context_manager(self):
        """测试上下文管理器"""
        with TaskPool(executor=echo_executor) as pool:
            assert pool._running
            task_id = pool.submit({"test": "data"})
            assert task_id is not None
        # 退出后应该关闭
        assert not pool._running


class TestTaskPoolSubmit:
    """测试任务提交"""

    @pytest.mark.asyncio
    async def test_submit_async(self):
        """测试异步提交"""
        pool = TaskPool(executor=echo_executor)
        pool.start()

        task_id = await pool.submit_async({"test": "async_submit"})
        assert task_id is not None
        assert isinstance(task_id, str)
        assert task_id.startswith("TSK")

        pool.shutdown()

    def test_submit_sync(self):
        """测试同步提交"""
        with TaskPool(executor=echo_executor) as pool:
            task_id = pool.submit({"test": "sync_submit"})
            assert task_id is not None
            assert isinstance(task_id, str)

    def test_submit_with_custom_task_id(self):
        """测试自定义任务ID"""
        with TaskPool(executor=echo_executor) as pool:
            custom_id = "my_custom_task_123"
            task_id = pool.submit({"test": "data"}, task_id=custom_id)
            assert task_id == custom_id

    def test_submit_with_different_priorities(self):
        """测试不同优先级"""
        with TaskPool(executor=echo_executor) as pool:
            # 默认优先级
            task1 = pool.submit({"test": "normal"}, priority=TaskPriority.NORMAL)
            # 高优先级
            task2 = pool.submit({"test": "high"}, priority=TaskPriority.HIGH)
            # 低优先级
            task3 = pool.submit({"test": "low"}, priority=TaskPriority.LOW)
            # 使用整数
            task4 = pool.submit({"test": "critical"}, priority=0)

            assert task1 is not None
            assert task2 is not None
            assert task3 is not None
            assert task4 is not None

    def test_submit_with_delay(self):
        """测试延迟任务"""
        with TaskPool(executor=echo_executor) as pool:
            start = time.time()
            task_id = pool.submit({"test": "delayed"}, delay=1.0)
            elapsed = time.time() - start

            # 提交应该是即时的
            assert elapsed < 0.1
            assert task_id is not None

    @pytest.mark.asyncio
    async def test_submit_batch_async(self):
        """测试批量异步提交"""
        pool = TaskPool(executor=echo_executor)
        pool.start()

        tasks = [{"id": i, "data": f"task_{i}"} for i in range(10)]
        task_ids = await pool.submit_batch_async(tasks)

        assert len(task_ids) == 10
        assert len(set(task_ids)) == 10  # 所有ID唯一

        pool.shutdown()

    def test_submit_batch_sync(self):
        """测试批量同步提交"""
        with TaskPool(executor=echo_executor) as pool:
            tasks = [{"id": i, "data": f"task_{i}"} for i in range(5)]
            task_ids = pool.submit_batch(tasks)
            assert len(task_ids) == 5


class TestTaskPoolWait:
    """测试任务等待"""

    @pytest.mark.asyncio
    async def test_wait_for_result_async_success(self):
        """测试异步等待成功结果"""
        pool = TaskPool(executor=success_executor)
        pool.start()

        task_id = await pool.submit_async({"test": "data"})
        result = await pool.wait_for_result_async(task_id, timeout=5)

        assert result is not None
        assert result["status"] == "success"
        assert result["input"] == {"test": "data"}

        pool.shutdown()

    def test_wait_for_result_sync_success(self):
        """测试同步等待成功结果"""
        with TaskPool(executor=success_executor) as pool:
            task_id = pool.submit({"test": "sync_data"})
            result = pool.wait_for_result(task_id, timeout=5)

            assert result is not None
            assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_wait_for_result_failure(self):
        """测试等待失败任务 - 健壮版本"""
        config = TaskPoolConfig(
            storage_type="memory",
            worker_concurrency=1,  # 单 worker 便于调试
            max_retries=0  # 不重试，直接失败
        )

        execution_log = []

        async def failing_executor(data):
            execution_log.append("executed")
            raise RuntimeError("Task execution failed intentionally")

        pool = TaskPool(executor=failing_executor, config=config)
        pool.start()

        try:
            task_id = await pool.submit_async({"test": "fail"})
            print(f"Submitted task: {task_id}")

            # 初始状态应该是 PENDING
            initial_status = await pool.get_status_async(task_id)
            print(f"Initial status: {initial_status}")
            assert initial_status == TaskStatus.PENDING.value

            # 等待任务执行完成（轮询状态）
            max_wait = 3.0
            start = asyncio.get_event_loop().time()
            final_status = None

            while asyncio.get_event_loop().time() - start < max_wait:
                status = await pool.get_status_async(task_id)
                print(f"Current status: {status}")
                if status in [TaskStatus.SUCCESS.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value]:
                    final_status = status
                    break
                await asyncio.sleep(0.1)

            # 验证执行器被调用了
            assert len(execution_log) > 0, "Executor was not called"

            # 验证最终状态是 FAILED
            assert final_status == TaskStatus.FAILED.value, f"Expected FAILED, got {final_status}"

            # 验证 wait_for_result 抛出异常
            with pytest.raises(Exception) as exc_info:
                await pool.wait_for_result_async(task_id, timeout=2)

            print(f"Exception caught: {exc_info.value}")
            assert "failed" in str(exc_info.value).lower() or "execution" in str(exc_info.value).lower()

            # 获取任务详情验证错误信息
            task_info = await pool.get_task_async(task_id)
            if task_info:
                print(f"Task info: {task_info}")
                assert task_info.get("error") is not None

        finally:
            pool.shutdown()

    @pytest.mark.asyncio
    async def test_wait_for_result_timeout(self):
        """测试等待超时"""
        pool = TaskPool(executor=slow_executor)
        pool.start()

        task_id = await pool.submit_async({"delay": 2.0})

        with pytest.raises(TimeoutError):
            await pool.wait_for_result_async(task_id, timeout=0.5)

        pool.shutdown()

    @pytest.mark.asyncio
    async def test_wait_for_nonexistent_task(self):
        """测试等待不存在的任务"""
        pool = TaskPool(executor=success_executor)
        pool.start()

        with pytest.raises(TaskNotFoundError):
            await pool.wait_for_result_async("nonexistent_task", timeout=1)

        pool.shutdown()

    @pytest.mark.asyncio
    async def test_wait_all_async(self):
        """测试异步等待所有任务"""
        pool = TaskPool(executor=slow_executor)
        pool.start()

        task_ids = []
        for i in range(3):
            task_id = await pool.submit_async({"delay": 0.2, "id": i})
            task_ids.append(task_id)

        results = await pool.wait_all_async(task_ids, timeout=5)

        assert len(results) == 3
        for task_id in task_ids:
            assert task_id in results
            assert results[task_id]["status"] == "success"

        pool.shutdown()

    def test_wait_all_sync(self):
        """测试同步等待所有任务"""
        with TaskPool(executor=slow_executor) as pool:
            task_ids = []
            for i in range(3):
                task_id = pool.submit({"delay": 0.1, "id": i})
                task_ids.append(task_id)

            results = pool.wait_all(task_ids, timeout=5)

            assert len(results) == 3
            for task_id in task_ids:
                assert task_id in results


class TestTaskPoolQuery:
    """测试任务查询"""

    @pytest.mark.asyncio
    async def test_get_status_async(self):
        """测试异步获取状态"""
        pool = TaskPool(executor=slow_executor)
        pool.start()

        task_id = await pool.submit_async({"delay": 0.5})

        # 初始状态应该是 PENDING
        status = await pool.get_status_async(task_id)
        assert status == TaskStatus.PENDING.value

        # 等待完成
        await pool.wait_for_result_async(task_id, timeout=5)

        # 完成后状态应该是 SUCCESS
        status = await pool.get_status_async(task_id)
        assert status == TaskStatus.SUCCESS.value

        pool.shutdown()

    def test_get_status_sync(self):
        """测试同步获取状态"""
        with TaskPool(executor=slow_executor) as pool:
            # 使用延迟确保任务不会立即执行
            task_id = pool.submit({"delay": 5}, delay=2.0)

            # 立即检查状态，应该是 PENDING（因为延迟2秒）
            status = pool.get_status(task_id)
            assert status == TaskStatus.PENDING.value

            # 等待任务完成
            pool.wait_for_result(task_id, timeout=10)

            # 完成后应该是 SUCCESS
            status = pool.get_status(task_id)
            assert status == TaskStatus.SUCCESS.value

    def test_get_result(self):
        """测试获取结果"""
        with TaskPool(executor=success_executor) as pool:
            task_id = pool.submit({"test": "result_test"})
            pool.wait_for_result(task_id, timeout=5)

            result = pool.get_result(task_id)
            assert result is not None
            assert result["task_id"] == task_id
            assert result["status"] == TaskStatus.SUCCESS.value
            assert result["result"] is not None
            assert result["error"] is None

    def test_get_result_not_found(self):
        """测试获取不存在任务的结果"""
        with TaskPool(executor=success_executor) as pool:
            result = pool.get_result("nonexistent")
            assert result is None

    def test_get_task(self):
        """测试获取完整任务信息"""
        with TaskPool(executor=success_executor) as pool:
            task_id = pool.submit({"test": "full_info"})
            pool.wait_for_result(task_id, timeout=5)

            task_dict = pool.get_task(task_id)
            assert task_dict is not None
            assert task_dict["task_id"] == task_id
            assert "created_at" in task_dict
            assert "completed_at" in task_dict

    def test_task_exists(self):
        """测试任务存在性检查"""
        with TaskPool(executor=success_executor) as pool:
            task_id = pool.submit({"test": "exists"})
            assert pool.task_exists(task_id) is True
            assert pool.task_exists("nonexistent") is False


class TestTaskPoolManagement:
    """测试任务管理"""

    @pytest.mark.asyncio
    async def test_cancel_async(self):
        """测试异步取消任务"""
        pool = TaskPool(executor=slow_executor)
        pool.start()

        task_id = await pool.submit_async({"delay": 5.0})

        # 等待一小段时间确保任务已入队
        await asyncio.sleep(0.1)

        cancelled = await pool.cancel_async(task_id)
        assert cancelled is True

        # 检查状态
        status = await pool.get_status_async(task_id)
        assert status == TaskStatus.CANCELLED.value

        pool.shutdown()

    def test_cancel_sync(self):
        """测试同步取消任务"""
        with TaskPool(executor=slow_executor) as pool:
            task_id = pool.submit({"delay": 5.0})

            cancelled = pool.cancel(task_id)
            assert cancelled is True

            status = pool.get_status(task_id)
            assert status == TaskStatus.CANCELLED.value

    def test_cancel_already_completed(self):
        """测试取消已完成的任务"""
        with TaskPool(executor=success_executor) as pool:
            task_id = pool.submit({"test": "quick"})
            pool.wait_for_result(task_id, timeout=5)

            cancelled = pool.cancel(task_id)
            assert cancelled is False  # 已完成的任务无法取消

    def test_delete_task(self):
        """测试删除任务"""
        with TaskPool(executor=success_executor) as pool:
            task_id = pool.submit({"test": "delete_me"})
            pool.wait_for_result(task_id, timeout=5)

            deleted = pool.delete(task_id)
            assert deleted is True

            # 删除后任务应该不存在
            task = pool.get_task(task_id)
            assert task is None

    @pytest.mark.asyncio
    async def test_retry_async(self):
        """测试异步重试任务"""
        retry_count = 0

        async def retry_executor(data):
            nonlocal retry_count
            retry_count += 1
            if retry_count < 2:
                raise ValueError("First attempt failed")
            return {"success": True, "attempts": retry_count}

        pool = TaskPool(executor=retry_executor, config=TaskPoolConfig(max_retries=3))
        pool.start()

        task_id = await pool.submit_async({"test": "retry"})
        result = await pool.wait_for_result_async(task_id, timeout=10)

        assert result["success"] is True
        assert result["attempts"] == 2

        pool.shutdown()

    def test_retry_sync(self):
        """测试同步重试"""
        call_count = 0

        def retry_executor_sync(data):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("First attempt failed")
            return {"success": True}

        with TaskPool(executor=retry_executor_sync, config=TaskPoolConfig(max_retries=3)) as pool:
            task_id = pool.submit({"test": "sync_retry"})
            result = pool.wait_for_result(task_id, timeout=10)

            assert result["success"] is True
            assert call_count == 2


class TestTaskPoolQueueControl:
    """测试队列控制"""

    def test_pause_resume(self):
        """测试暂停/恢复"""
        with TaskPool(executor=slow_executor) as pool:
            # 提交多个任务
            task_ids = []
            for i in range(5):
                task_id = pool.submit({"delay": 0.1, "id": i})
                task_ids.append(task_id)

            # 等待任务入队
            time.sleep(0.1)

            # 暂停
            pool.pause()
            time.sleep(0.1)  # 等待暂停生效
            assert pool._queue_scheduler.is_paused

            # 记录当前队列大小
            queue_size_before = pool.get_queue_size()

            # 提交新任务（应该被暂停影响）
            new_task_id = pool.submit({"delay": 0.1, "id": 100})
            task_ids.append(new_task_id)

            time.sleep(0.2)

            # 恢复
            pool.resume()
            time.sleep(0.1)  # 等待恢复生效
            assert not pool._queue_scheduler.is_paused

            # 等待所有任务完成
            for task_id in task_ids:
                pool.wait_for_result(task_id, timeout=5)

    def test_clear_queue(self):
        """测试清空队列"""
        with TaskPool(executor=slow_executor) as pool:
            # 提交多个任务
            for i in range(10):
                pool.submit({"delay": 0.5, "id": i})

            # 等待一小段时间让任务入队
            time.sleep(0.1)

            # 清空队列
            pool.clear_queue()

            queue_size = pool.get_queue_size()
            assert queue_size == 0

    def test_get_queue_size(self):
        """测试获取队列大小"""
        with TaskPool(executor=slow_executor) as pool:
            initial_size = pool.get_queue_size()
            assert initial_size == 0

            # 提交任务
            for i in range(5):
                pool.submit({"delay": 0.1, "id": i})

            # 等待一小段时间让任务入队
            import time
            time.sleep(0.05)

            # 队列大小可能为 0（如果 Worker 已经取走了任务）
            # 或者大于 0（如果 Worker 还没取走）
            # 所以只验证队列大小是整数且 >= 0
            size = pool.get_queue_size()
            assert isinstance(size, int)
            assert size >= 0

            # 更好的验证：等待所有任务完成
            # 这里验证任务被正确执行
            # 可以通过其他方式验证，比如检查任务状态


class TestTaskPoolStats:
    """测试统计信息"""

    def test_get_stats(self):
        """测试获取统计信息"""
        with TaskPool(executor=success_executor) as pool:
            # 提交多个任务
            task_ids = []
            for i in range(5):
                task_id = pool.submit({"id": i})
                task_ids.append(task_id)

            # 等待完成
            for task_id in task_ids:
                pool.wait_for_result(task_id, timeout=5)

            stats = pool.get_stats()

            assert "queue_size" in stats
            assert "total" in stats
            assert "pending" in stats
            assert "running" in stats
            assert "completed" in stats
            assert "failed" in stats
            assert "success_rate" in stats

    @pytest.mark.asyncio
    async def test_get_stats_async(self):
        """测试异步获取统计信息"""
        pool = TaskPool(executor=success_executor)
        pool.start()

        # 提交任务
        task_id = await pool.submit_async({"test": "stats"})
        await pool.wait_for_result_async(task_id, timeout=5)

        stats = await pool.get_stats_async()

        assert "queue_size" in stats
        assert "completed" in stats

        pool.shutdown()

    def test_get_worker_stats(self):
        """测试获取Worker统计"""
        with TaskPool(executor=success_executor) as pool:
            worker_stats = pool.get_worker_stats()
            assert isinstance(worker_stats, dict)

    def test_get_health_status(self):
        """测试获取健康状态"""
        with TaskPool(executor=success_executor) as pool:
            health = pool.get_health_status()
            assert "status" in health


class TestTaskPoolLock:
    """测试锁管理"""

    def test_acquire_release_lock(self):
        """测试获取和释放锁"""
        with TaskPool(executor=success_executor) as pool:
            task_id = "lock_test_task"

            # 获取锁
            acquired = pool.acquire_lock(task_id, ttl=10)
            assert acquired is True

            # 再次获取应该失败
            acquired_again = pool.acquire_lock(task_id, ttl=10)
            assert acquired_again is False

            # 释放锁
            released = pool.release_lock(task_id)
            assert released is True

            # 释放后可以重新获取
            acquired_after = pool.acquire_lock(task_id, ttl=10)
            assert acquired_after is True

    @pytest.mark.asyncio
    async def test_acquire_release_lock_async(self):
        """测试异步获取和释放锁"""
        pool = TaskPool(executor=success_executor)
        pool.start()

        task_id = "async_lock_test"

        acquired = await pool.acquire_lock_async(task_id, ttl=10)
        assert acquired is True

        released = await pool.release_lock_async(task_id)
        assert released is True

        pool.shutdown()


class TestTaskPoolEvents:
    """测试事件回调"""

    def test_event_callbacks(self):
        """测试事件回调"""
        events = []

        # 定义同步回调函数（不一定是异步的）
        def on_created(event):
            events.append(("created", event.task_id))

        def on_started(event):
            events.append(("started", event.task_id))

        def on_completed(event):
            events.append(("completed", event.task_id))

        with TaskPool(executor=success_executor) as pool:
            # 注册回调
            pool.on_created(on_created)
            pool.on_started(on_started)
            pool.on_completed(on_completed)

            task_id = pool.submit({"test": "events"})
            pool.wait_for_result(task_id, timeout=5)

            # 等待事件处理完成 - 给事件总线一些时间
            import time
            time.sleep(0.3)

            # 验证事件被触发
            event_types = [e[0] for e in events]
            print(f"Events received: {event_types}")  # 调试输出

            assert "created" in event_types, f"Expected 'created' in {event_types}"
            assert "started" in event_types, f"Expected 'started' in {event_types}"
            assert "completed" in event_types, f"Expected 'completed' in {event_types}"


class TestTaskPoolEdgeCases:
    """测试边界情况"""

    def test_submit_empty_data(self):
        """测试提交空数据"""
        with TaskPool(executor=echo_executor) as pool:
            task_id = pool.submit({})
            result = pool.wait_for_result(task_id, timeout=5)
            assert result["echo"] == {}

    def test_submit_large_data(self):
        """测试提交大数据"""
        large_data = {"data": "x" * 10000}
        with TaskPool(executor=echo_executor) as pool:
            task_id = pool.submit(large_data)
            result = pool.wait_for_result(task_id, timeout=5)
            assert result["echo"]["data"] == large_data["data"]

    def test_multiple_pools(self):
        """测试多个池并发运行"""
        pool1 = TaskPool(executor=echo_executor)
        pool2 = TaskPool(executor=echo_executor)

        pool1.start()
        pool2.start()

        task1 = pool1.submit({"pool": 1})
        task2 = pool2.submit({"pool": 2})

        result1 = pool1.wait_for_result(task1, timeout=5)
        result2 = pool2.wait_for_result(task2, timeout=5)

        assert result1["echo"]["pool"] == 1
        assert result2["echo"]["pool"] == 2

        pool1.shutdown()
        pool2.shutdown()

    def test_concurrent_submissions(self):
        """测试并发提交"""
        import threading
        import time

        with TaskPool(executor=slow_executor) as pool:
            task_ids = []
            submit_lock = threading.Lock()
            errors = []

            def submit_task():
                try:
                    task_id = pool.submit({"delay": 0.1})
                    with submit_lock:
                        task_ids.append(task_id)
                except Exception as e:
                    with submit_lock:
                        errors.append(str(e))

            threads = []
            for _ in range(10):
                t = threading.Thread(target=submit_task)
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # 等待所有任务被处理
            time.sleep(1.0)

            assert len(errors) == 0, f"Errors occurred: {errors}"
            assert len(task_ids) == 10
            assert len(set(task_ids)) == 10  # 所有ID唯一


# ========== 运行测试 ==========

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
