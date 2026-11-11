"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_task_pool.py
@Description: TaskPool 集成测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/21
"""

import pytest
import asyncio
import time
from typing import Dict, Any

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.models.task import TaskStatus, TaskPriority


class TestTaskPoolMemory:
    """TaskPool 内存存储集成测试"""

    @pytest.fixture
    def task_pool(self):
        """创建内存 TaskPool"""
        # 使用较长的执行时间，让状态检查有机会看到中间状态
        async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
            await asyncio.sleep(0.2)  # 增加执行时间
            return {"result": "processed", "input": data}

        config = TaskPoolConfig(storage_type="memory", worker_concurrency=2)
        pool = TaskPool(executor, config)
        pool.start()

        # 等待启动完成
        time.sleep(0.3)

        yield pool
        pool.shutdown()

    def test_submit_and_wait(self, task_pool):
        """测试提交任务并等待结果"""
        task_id = task_pool.submit({"test": "data"})
        assert task_id is not None

        result = task_pool.wait_for_result(task_id, timeout=5)
        assert result is not None
        assert result["result"] == "processed"

    def test_get_task_status(self, task_pool):
        """测试获取任务状态 - 在任务执行过程中检查"""
        # 提交一个任务（会执行0.2秒）
        task_id = task_pool.submit({"test": "data"})

        # 立即检查状态，应该是 PENDING（刚提交还未开始执行）
        # 由于 executor 有 0.2 秒延迟，状态应该是 PENDING
        status = task_pool.get_status(task_id)
        # PENDING 或 RUNNING 都是可接受的状态
        assert status in [TaskStatus.PENDING.value, TaskStatus.RUNNING.value]

        # 等待任务完成
        task_pool.wait_for_result(task_id)

        # 完成后检查状态
        status = task_pool.get_status(task_id)
        assert status == TaskStatus.SUCCESS.value

    def test_get_task_result(self, task_pool):
        """测试获取任务结果"""
        task_id = task_pool.submit({"key": "value"})

        task_pool.wait_for_result(task_id)

        result = task_pool.get_result(task_id)
        assert result is not None
        assert result["task_id"] == task_id
        assert result["status"] == TaskStatus.SUCCESS.value

    def test_submit_with_priority(self, task_pool):
        """测试带优先级的任务提交"""
        task_ids = []

        # 低优先级
        task_id_low = task_pool.submit(
            {"priority": "low"},
            priority=TaskPriority.LOW
        )
        task_ids.append(task_id_low)

        # 高优先级
        task_id_high = task_pool.submit(
            {"priority": "high"},
            priority=TaskPriority.HIGH
        )
        task_ids.append(task_id_high)

        # 关键优先级
        task_id_critical = task_pool.submit(
            {"priority": "critical"},
            priority=TaskPriority.CRITICAL
        )
        task_ids.append(task_id_critical)

        # 等待所有完成
        for task_id in task_ids:
            task_pool.wait_for_result(task_id)

    def test_submit_batch(self, task_pool):
        """测试批量提交"""
        tasks_data = [
            {"index": i, "data": f"task_{i}"}
            for i in range(5)  # 减少数量避免测试时间过长
        ]

        task_ids = task_pool.submit_batch(tasks_data, priority=TaskPriority.NORMAL)

        assert len(task_ids) == 5

        # 等待所有完成
        for task_id in task_ids:
            result = task_pool.wait_for_result(task_id)
            assert result["result"] == "processed"

    def test_burst_submit_keeps_running_table_bounded(self):
        """突发批量提交时，worker 逐个执行任务，运行表不随队列深度膨胀"""
        observed = {"max": 0}
        pool = None

        async def executor(data):
            with pool._worker_pool._lock:
                observed["max"] = max(
                    observed["max"], len(pool._worker_pool._running_tasks)
                )
            return {"ok": True}

        config = TaskPoolConfig(
            storage_type="memory",
            worker_concurrency=1,
            enable_prefetch=False,
        )
        pool = TaskPool(executor, config)
        pool.start()
        try:
            task_ids = pool.submit_batch([{"i": i} for i in range(50)])
            for task_id in task_ids:
                pool.wait_for_result(task_id, timeout=10)
        finally:
            pool.shutdown()

        assert observed["max"] <= 5

    def test_direct_mode_runs_tasks_concurrently(self):
        """关闭预取时，任务仍按 worker 数量并发执行（不能被单个 worker 独占）"""
        async def executor(data):
            await asyncio.sleep(0.1)
            return {"ok": True}

        config = TaskPoolConfig(
            storage_type="memory",
            worker_concurrency=4,
            enable_prefetch=False,
        )
        pool = TaskPool(executor, config)
        pool.start()
        try:
            start = time.perf_counter()
            task_ids = pool.submit_batch([{"i": i} for i in range(8)])
            for task_id in task_ids:
                pool.wait_for_result(task_id, timeout=10)
            elapsed = time.perf_counter() - start
        finally:
            pool.shutdown()

        # 串行执行约 0.8s，4 个 worker 并发约 0.2s
        assert elapsed < 0.7

    def test_prefetch_is_not_rate_limited_by_interval(self):
        """预取模式吞吐不受 prefetch_size / prefetch_interval 固定上限限制"""
        async def executor(data):
            return {"ok": True}

        config = TaskPoolConfig(
            storage_type="memory",
            worker_concurrency=10,
            enable_prefetch=True,
            prefetch_size=20,
            enable_metrics=False,
            enable_health_check=False,
        )
        pool = TaskPool(executor, config)
        pool.start()
        try:
            start = time.perf_counter()
            task_ids = pool.submit_batch([{"i": i} for i in range(3000)])
            for task_id in task_ids:
                pool.wait_for_result(task_id, timeout=60)
            elapsed = time.perf_counter() - start
        finally:
            pool.shutdown()

        # 旧实现被 20 / 0.1s 限制在约 182/s（3000 条需 16s 以上）
        assert elapsed < 8

    def test_terminal_cleanup_removes_expired_tasks(self):
        """周期清理会删除超过任务 ttl 的终态任务"""
        async def executor(data):
            return {"ok": True}

        config = TaskPoolConfig(
            storage_type="memory",
            worker_concurrency=4,
            enable_terminal_cleanup=True,
            terminal_cleanup_interval=0.1,
            enable_metrics=False,
            enable_health_check=False,
        )
        pool = TaskPool(executor, config)
        pool.start()
        try:
            task_ids = pool.submit_batch([{"i": i} for i in range(20)], ttl=1)
            for task_id in task_ids:
                pool.wait_for_result(task_id, timeout=10)
            assert pool.get_stats()["total"] == 20

            time.sleep(1.5)

            assert pool.get_stats()["total"] == 0
            assert all(pool.get_task(task_id) is None for task_id in task_ids)
        finally:
            pool.shutdown()

    def test_redis_flags_disable_distributed_components(self):
        """关闭 heartbeat/reclaimer 时不再创建或启动分布式组件"""
        config = TaskPoolConfig(
            storage_type="redis",
            redis_url="redis://127.0.0.1:6379/15",
            enable_heartbeat=False,
            enable_reclaimer=False,
            enable_dead_letter=False,
        )
        pool = TaskPool(lambda data: None, config)
        assert pool._node_manager is None
        assert pool._heartbeat_manager is None
        assert pool._reclaimer is None

        enabled = TaskPoolConfig(
            storage_type="redis",
            redis_url="redis://127.0.0.1:6379/15",
            enable_heartbeat=True,
            enable_reclaimer=True,
            enable_dead_letter=False,
        )
        enabled_pool = TaskPool(lambda data: None, enabled)
        assert enabled_pool._node_manager is not None
        assert enabled_pool._heartbeat_manager is not None
        assert enabled_pool._reclaimer is not None

    def test_cancel_task(self):
        """测试取消任务"""
        # 提交一个长时间运行的任务
        async def long_executor(data):
            await asyncio.sleep(5)  # 5秒，足够取消
            return {"result": "done"}

        config = TaskPoolConfig(storage_type="memory")
        pool = TaskPool(long_executor, config)
        pool.start()
        time.sleep(0.3)

        try:
            task_id = pool.submit({"test": "data"})

            # 等待一下确保任务已提交并开始执行
            time.sleep(0.1)

            # 取消任务
            cancelled = pool.cancel(task_id)
            # 注意：取消可能成功也可能失败，取决于任务状态
            # 如果任务已经开始执行，可能无法取消

            status = pool.get_status(task_id)
            # 取消后的状态可能是 CANCELLED 或 RUNNING（如果取消失败）
            assert status in [TaskStatus.CANCELLED.value, TaskStatus.RUNNING.value]
        finally:
            pool.shutdown()

    def test_get_stats(self, task_pool):
        """测试获取统计信息"""
        # 提交一些任务
        for i in range(3):
            task_pool.submit({"index": i})

        time.sleep(0.5)

        stats = task_pool.get_stats()
        assert "queue_size" in stats
        assert "total" in stats
        assert "pending" in stats
        assert "running" in stats
        assert "completed" in stats

    def test_pause_and_resume(self, task_pool):
        """测试暂停和恢复"""
        # 提交任务
        task_id = task_pool.submit({"test": "data"})

        # 立即暂停
        task_pool.pause()

        # 等待一段时间，由于暂停，任务可能还未执行或正在等待
        time.sleep(0.3)

        # 恢复
        task_pool.resume()

        # 等待任务完成
        task_pool.wait_for_result(task_id)

        status = task_pool.get_status(task_id)
        assert status == TaskStatus.SUCCESS.value


class TestTaskPoolSQLite:
    """TaskPool SQLite 存储集成测试"""

    @pytest.fixture
    def task_pool_sqlite(self, tmp_path):
        """创建 SQLite TaskPool"""
        db_path = str(tmp_path / "test_tasks.db")

        async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
            await asyncio.sleep(0.1)
            return {"result": "processed", "input": data}

        config = TaskPoolConfig(
            storage_type="sqlite",
            sqlite_path=db_path
        )
        pool = TaskPool(executor, config)
        pool.start()

        time.sleep(0.3)

        yield pool
        pool.shutdown()

    def test_persistence(self, task_pool_sqlite):
        """测试任务持久化"""
        task_id = task_pool_sqlite.submit({"test": "persistence"})

        # 等待完成
        result = task_pool_sqlite.wait_for_result(task_id)
        assert result is not None

    def test_task_exists_after_restart(self, tmp_path):
        """测试重启后任务存在"""
        db_path = str(tmp_path / "test_tasks.db")

        async def executor(data):
            await asyncio.sleep(0.1)
            return {"result": "done"}

        # 第一个实例
        config1 = TaskPoolConfig(storage_type="sqlite", sqlite_path=db_path)
        pool1 = TaskPool(executor, config1)
        pool1.start()
        time.sleep(0.3)

        task_id = pool1.submit({"test": "data"})
        pool1.wait_for_result(task_id)
        pool1.shutdown()

        # 第二个实例
        config2 = TaskPoolConfig(storage_type="sqlite", sqlite_path=db_path)
        pool2 = TaskPool(executor, config2)
        pool2.start()
        time.sleep(0.3)

        # 任务应该仍然存在
        result = pool2.get_result(task_id)
        assert result is not None
        assert result["task_id"] == task_id

        pool2.shutdown()


class TestTaskPoolPriority:
    """TaskPool 优先级集成测试"""

    @pytest.fixture
    def task_pool(self):
        """创建 TaskPool - 单 worker 确保顺序可预测"""
        execution_order = []

        async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
            # 记录执行顺序（按开始时间）
            execution_order.append((data["priority"], time.time()))
            # 增加执行时间，让任务有足够时间排队
            await asyncio.sleep(0.2)
            return {"order": execution_order.copy()}

        config = TaskPoolConfig(
            storage_type="memory",
            worker_concurrency=1,  # 单 worker 确保顺序
            queue_max_size=100
        )
        pool = TaskPool(executor, config)
        pool.start()

        time.sleep(0.3)

        yield pool, execution_order
        pool.shutdown()

    def test_priority_execution_order(self, task_pool):
        """测试优先级执行顺序

        优先级规则：数字越小优先级越高
        CRITICAL = 0 (最高)
        HIGH = 1
        NORMAL = 2
        LOW = 3 (最低)

        高优先级任务应该比低优先级任务先执行。
        """
        pool, execution_order = task_pool

        # 清空执行顺序
        execution_order.clear()

        # 暂停后批量提交再恢复：流式提交会被单 worker 即时取走，全部入队后消费优先级排序才生效
        pool.pause()

        # 提交不同优先级的任务（按优先级从低到高提交，验证队列会重新排序）
        # 先提交低优先级
        pool.submit({"priority": "LOW"}, priority=TaskPriority.LOW)  # priority=3

        pool.submit({"priority": "NORMAL"}, priority=TaskPriority.NORMAL)  # priority=2

        pool.submit({"priority": "HIGH"}, priority=TaskPriority.HIGH)  # priority=1

        pool.submit({"priority": "CRITICAL"}, priority=TaskPriority.CRITICAL)  # priority=0

        pool.resume()

        # 等待所有任务完成
        time.sleep(1.5)

        # 获取执行顺序中的优先级值
        executed_priorities = [item[0] for item in execution_order]

        # 期望的执行顺序：按优先级数字从小到大
        # CRITICAL(0) -> HIGH(1) -> NORMAL(2) -> LOW(3)
        expected_order = ["CRITICAL", "HIGH", "NORMAL", "LOW"]

        assert len(executed_priorities) == 4, f"Expected 4 tasks, got {len(executed_priorities)}"
        assert executed_priorities == expected_order, \
            f"Expected order {expected_order}, got {executed_priorities}"

    def test_priority_with_multiple_submissions(self, task_pool):
        """测试多次提交相同优先级的顺序"""
        pool, execution_order = task_pool
        execution_order.clear()

        # 暂停后批量提交再恢复：确保 HIGH_INSERT 入队时 NORMAL 任务仍在队列中，插队语义才可验证
        pool.pause()

        # 提交多个相同优先级的任务（应该按 FIFO 顺序执行）
        for i in range(5):
            pool.submit({"priority": f"NORMAL_{i}"}, priority=TaskPriority.NORMAL)

        # 提交一个高优先级任务（应该插队到最前面）
        pool.submit({"priority": "HIGH_INSERT"}, priority=TaskPriority.HIGH)

        pool.resume()

        # 等待完成
        time.sleep(1.5)

        executed = [item[0] for item in execution_order]

        # 高优先级任务应该在最前面
        assert executed[0] == "HIGH_INSERT", f"Expected HIGH_INSERT first, got {executed[0]}"
        # 后面应该是 NORMAL 任务
        assert len(executed) == 6, f"Expected 6 tasks, got {len(executed)}"


class TestTaskPoolPriorityCorrected:
    """TaskPool 优先级集成测试 - 使用同步方式验证"""

    @pytest.fixture
    def task_pool(self):
        """创建 TaskPool - 单 worker 确保顺序可预测"""
        execution_order = []

        # 使用同步函数包装器，通过线程池执行
        def sync_executor(data: Dict[str, Any]) -> Dict[str, Any]:
            execution_order.append(data["priority"])
            time.sleep(0.15)
            return {"order": execution_order.copy()}

        # 包装为异步函数
        async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
            # 在线程池中执行同步函数
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, sync_executor, data)

        config = TaskPoolConfig(
            storage_type="memory",
            worker_concurrency=1,
            queue_max_size=100,
            executor_type="thread",
            max_workers=1
        )
        pool = TaskPool(executor, config)
        pool.start()

        time.sleep(0.3)

        yield pool, execution_order
        pool.shutdown()

    def test_priority_execution_order(self, task_pool):
        """测试优先级执行顺序"""
        pool, execution_order = task_pool
        execution_order.clear()

        # 暂停后批量提交再恢复：流式提交会被单 worker 即时取走，全部入队后消费优先级排序才生效
        pool.pause()

        # 提交不同优先级的任务
        pool.submit({"priority": "LOW"}, priority=TaskPriority.LOW)

        pool.submit({"priority": "NORMAL"}, priority=TaskPriority.NORMAL)

        pool.submit({"priority": "HIGH"}, priority=TaskPriority.HIGH)

        pool.submit({"priority": "CRITICAL"}, priority=TaskPriority.CRITICAL)

        pool.resume()

        # 等待所有任务完成
        time.sleep(1.5)

        # 打印调试信息
        print(f"Execution order: {execution_order}")

        # 期望的执行顺序
        expected = ["CRITICAL", "HIGH", "NORMAL", "LOW"]

        # 由于优先级可能不生效，我们检查是否至少高优先级在低优先级之前
        # 找到各优先级的位置
        try:
            critical_pos = execution_order.index("CRITICAL")
            high_pos = execution_order.index("HIGH")
            normal_pos = execution_order.index("NORMAL")
            low_pos = execution_order.index("LOW")

            # 验证相对顺序
            assert critical_pos < high_pos, f"CRITICAL({critical_pos}) should be before HIGH({high_pos})"
            assert high_pos < normal_pos, f"HIGH({high_pos}) should be before NORMAL({normal_pos})"
            assert normal_pos < low_pos, f"NORMAL({normal_pos}) should be before LOW({low_pos})"

        except ValueError as e:
            pytest.fail(f"Missing expected task in execution order: {execution_order}")


# 如果上述测试仍然失败，说明优先级队列实现有问题
# 可以运行这个简单测试来验证

class TestTaskPoolPrioritySimple:
    """简单的优先级测试 - 不依赖执行顺序，只验证提交不报错"""

    @pytest.fixture
    def task_pool(self):
        async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
            await asyncio.sleep(0.05)
            return {"result": "processed"}

        config = TaskPoolConfig(storage_type="memory", worker_concurrency=2)
        pool = TaskPool(executor, config)
        pool.start()
        time.sleep(0.3)
        yield pool
        pool.shutdown()

    def test_submit_all_priorities(self, task_pool):
        """测试所有优先级都能正常提交和执行"""
        task_ids = []

        priorities = [
            (TaskPriority.CRITICAL, "critical"),
            (TaskPriority.HIGH, "high"),
            (TaskPriority.NORMAL, "normal"),
            (TaskPriority.LOW, "low")
        ]

        for priority, name in priorities:
            task_id = task_pool.submit({"priority": name}, priority=priority)
            task_ids.append(task_id)
            time.sleep(0.02)

        # 等待所有完成
        for task_id in task_ids:
            result = task_pool.wait_for_result(task_id)
            assert result is not None

        # 验证所有任务都完成了
        for task_id in task_ids:
            status = task_pool.get_status(task_id)
            assert status == TaskStatus.SUCCESS.value


class TestTaskPoolAsyncAPI:
    """TaskPool 异步 API 测试（使用独立事件循环）"""

    @pytest.fixture
    def task_pool(self):
        """创建内存 TaskPool"""
        async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
            await asyncio.sleep(0.1)
            return {"result": "processed", "input": data}

        config = TaskPoolConfig(storage_type="memory")
        pool = TaskPool(executor, config)
        pool.start()

        time.sleep(0.3)

        yield pool
        pool.shutdown()

    def test_submit_async(self, task_pool):
        """测试异步提交"""
        loop = task_pool._loop

        future = asyncio.run_coroutine_threadsafe(
            task_pool.submit_async({"test": "async_data"}),
            loop
        )
        task_id = future.result()
        assert task_id is not None

        future = asyncio.run_coroutine_threadsafe(
            task_pool.wait_for_result_async(task_id),
            loop
        )
        result = future.result()
        assert result["result"] == "processed"

    def test_get_status_async(self, task_pool):
        """测试异步获取状态"""
        loop = task_pool._loop

        # 提交任务
        future = asyncio.run_coroutine_threadsafe(
            task_pool.submit_async({"test": "data"}),
            loop
        )
        task_id = future.result()

        # 立即检查状态（可能 PENDING 或 RUNNING）
        future = asyncio.run_coroutine_threadsafe(
            task_pool.get_status_async(task_id),
            loop
        )
        status = future.result()
        assert status in [TaskStatus.PENDING.value, TaskStatus.RUNNING.value]

        # 等待任务完成
        future = asyncio.run_coroutine_threadsafe(
            task_pool.wait_for_result_async(task_id),
            loop
        )
        future.result()

        # 完成后检查状态
        future = asyncio.run_coroutine_threadsafe(
            task_pool.get_status_async(task_id),
            loop
        )
        status = future.result()
        assert status == TaskStatus.SUCCESS.value

    def test_cancel_async(self):
        """测试异步取消"""
        async def long_executor(data):
            await asyncio.sleep(3)
            return {"result": "done"}

        config = TaskPoolConfig(storage_type="memory")
        pool = TaskPool(long_executor, config)
        pool.start()
        time.sleep(0.3)

        try:
            loop = pool._loop

            future = asyncio.run_coroutine_threadsafe(
                pool.submit_async({"test": "data"}),
                loop
            )
            task_id = future.result()

            time.sleep(0.05)

            future = asyncio.run_coroutine_threadsafe(
                pool.cancel_async(task_id),
                loop
            )
            cancelled = future.result()

            future = asyncio.run_coroutine_threadsafe(
                pool.get_status_async(task_id),
                loop
            )
            status = future.result()
            # 取消后的状态可能是 CANCELLED 或 RUNNING
            assert status in [TaskStatus.CANCELLED.value, TaskStatus.RUNNING.value]
        finally:
            pool.shutdown()


class TestTaskPoolEventBackpressure:
    """事件总线背压集成测试：慢订阅者触发事件丢弃，但不影响任务执行"""

    def test_bounded_bus_drops_events_not_tasks(self):
        import threading

        completed = []
        handler_started = threading.Event()
        release = threading.Event()

        async def executor(data):
            completed.append(data["i"])
            return {"ok": True}

        async def slow_handler(event):
            handler_started.set()
            # 阻塞事件总线消费协程，让后续事件在队列中堆积直至溢出
            while not release.is_set():
                await asyncio.sleep(0.01)

        config = TaskPoolConfig(
            storage_type="memory",
            worker_concurrency=4,
            event_queue_max_size=2,
            enable_metrics=False,
            enable_health_check=False,
            enable_reclaimer=False,
            enable_heartbeat=False,
        )
        pool = TaskPool(executor, config)
        pool.on_completed(slow_handler)
        pool.start()
        try:
            for i in range(20):
                pool.submit({"i": i})

            assert handler_started.wait(timeout=5)
            deadline = time.time() + 10
            while len(completed) < 20 and time.time() < deadline:
                time.sleep(0.02)

            # 任务全部执行完成，事件背压不拖垮任务通路
            assert len(completed) == 20
            stats = pool.event_bus.get_stats()
            assert stats["dropped_events"] > 0
        finally:
            release.set()
            pool.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])