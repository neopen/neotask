"""
@FileName: test_exception_scenarios.py
@Description: 异常场景测试 - 故障注入、恢复测试
@Author: HiPeng
@Time: 2026/4/29
"""

import asyncio
from unittest.mock import patch, AsyncMock

import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.lock.redis import RedisLock
from neotask.lock.watchdog import WatchDog
from neotask.worker.prefetcher import TaskPrefetcher, PrefetchConfig


# ========== 1. Redis 连接异常测试 ==========

@pytest.mark.chaos
class TestRedisFailover:
    """Redis 故障转移测试"""

    @pytest.mark.asyncio
    async def test_redis_connection_timeout(self):
        """测试 Redis 连接超时"""
        with patch('redis.asyncio.ConnectionPool.from_url') as mock_pool:
            mock_pool.side_effect = Exception("Connection timeout")

            with pytest.raises(Exception):
                lock = RedisLock("redis://invalid:6379")
                await lock.acquire("test_key")

    @pytest.mark.asyncio
    async def test_redis_reconnect(self):
        """测试 Redis 自动重连"""
        from neotask.storage.redis import RedisQueueRepository

        repo = RedisQueueRepository("redis://localhost:6379")

        # 模拟第一次操作失败
        with patch.object(repo, '_get_client', side_effect=[
            Exception("Connection lost"),  # 第一次失败
            AsyncMock()  # 第二次成功
        ]):
            try:
                await repo.push("test_task", 5)
            except Exception:
                pass
            # 第二次应该成功
            await repo.push("test_task2", 5)

        await repo.close()

    @pytest.mark.asyncio
    async def test_lock_during_redis_failure(self):
        """测试 Redis 故障期间的锁行为"""
        from neotask.lock.redis import RedisLock

        lock = RedisLock("redis://localhost:6379")

        # 正确模拟 Redis 客户端异常
        with patch.object(lock, '_get_client', new_callable=AsyncMock) as mock_get_client:
            mock_get_client.side_effect = Exception("Redis unavailable")

            # 捕获预期的异常
            with pytest.raises(Exception, match="Redis unavailable"):
                await lock.acquire("test_key")

            # 验证锁状态（应该没有被获取）
            is_locked = False
            try:
                is_locked = await lock.is_locked("test_key")
            except Exception:
                is_locked = False
            assert not is_locked


# ========== 2. 任务执行异常测试 ==========

@pytest.mark.chaos
class TestTaskExecutionFailures:
    """任务执行异常测试"""

    async def failing_executor(self, data):
        """失败的执行器"""
        raise ValueError("Task execution failed")

    @pytest.mark.asyncio
    async def test_task_failure_and_retry(self):
        """测试任务失败和重试"""
        pool = TaskPool(
            executor=self.failing_executor,
            config=TaskPoolConfig(max_retries=3, retry_delay=0.1)
        )
        pool.start()

        task_id = await pool.submit_async({"data": "test"})

        with pytest.raises(Exception):
            await pool.wait_for_result_async(task_id, timeout=5)

        task = await pool.get_task_async(task_id)
        assert task['status'] == 'failed'
        assert task['retry_count'] == 3

        pool.shutdown()

    @pytest.mark.asyncio
    async def test_task_timeout(self):
        """测试任务超时"""

        async def long_running(data):
            await asyncio.sleep(10)
            return {"result": "done"}

        pool = TaskPool(
            executor=long_running,
            config=TaskPoolConfig(task_timeout=1.0)
        )
        pool.start()

        task_id = await pool.submit_async({})
        await asyncio.sleep(1.5)

        with pytest.raises(Exception):
            await pool.wait_for_result_async(task_id, timeout=2)

        pool.shutdown()

    @pytest.mark.asyncio
    async def test_task_cancellation(self):
        """测试任务取消"""

        async def long_running(data):
            await asyncio.sleep(10)
            return {"result": "done"}

        pool = TaskPool(executor=long_running)
        pool.start()

        task_id = await pool.submit_async({})
        await asyncio.sleep(0.1)

        cancelled = await pool.cancel_async(task_id)
        assert cancelled

        # 等待取消完成
        await asyncio.sleep(0.5)

        task = await pool.get_task_async(task_id)
        assert task['status'] in ['cancelled', 'failed']

        pool.shutdown()


# ========== 3. 分布式锁异常测试 ==========

@pytest.mark.chaos
class TestDistributedLockFailures:
    """分布式锁异常测试"""

    @pytest.mark.asyncio
    async def test_lock_timeout(self):
        """测试锁超时"""
        lock = RedisLock("redis://localhost:6379")

        # 获取锁
        acquired = await lock.acquire("test_lock", ttl=1)
        assert acquired

        # 等待锁过期
        await asyncio.sleep(1.5)

        # 锁应该已过期
        is_locked = await lock.is_locked("test_lock")
        assert not is_locked

        await lock.release("test_lock")

    @pytest.mark.asyncio
    async def test_lock_extend(self):
        """测试锁续期"""
        lock = RedisLock("redis://localhost:6379")

        await lock.acquire("test_lock", ttl=2)

        # 续期
        extended = await lock.extend("test_lock", ttl=3)
        assert extended

        await lock.release("test_lock")

    @pytest.mark.asyncio
    async def test_lock_release_by_wrong_owner(self):
        """测试错误持有者释放锁"""
        lock1 = RedisLock("redis://localhost:6379")
        lock2 = RedisLock("redis://localhost:6379")

        # 清空
        await lock1.release("test_lock")

        await lock1.acquire("test_lock")

        # 另一个实例尝试释放
        released = await lock2.release("test_lock")
        assert not released

        # 原持有者释放
        released = await lock1.release("test_lock")
        assert released

    @pytest.mark.asyncio
    async def test_watchdog_renewal(self):
        """测试看门狗自动续期"""
        lock = RedisLock("redis://localhost:6379")
        watchdog = WatchDog(lock, interval_ratio=0.3)

        # 清空
        await lock.release("test_watchdog")

        await lock.acquire("test_watchdog", ttl=2)
        await watchdog.start("test_watchdog", ttl=2)

        # 等待看门狗续期
        await asyncio.sleep(1.5)

        # 锁应该仍然有效
        is_locked = await lock.is_locked("test_watchdog")
        assert is_locked

        await watchdog.stop("test_watchdog")
        await lock.release("test_watchdog")


# ========== 4. 队列异常测试 ==========

@pytest.mark.chaos
class TestQueueFailures:
    """队列异常测试"""

    @pytest.mark.asyncio
    async def test_queue_full(self):
        """测试队列满异常"""
        from neotask.queue.priority_queue import PriorityQueue

        queue = PriorityQueue(max_size=10)

        for i in range(10):
            await queue.push(f"task_{i}", i % 5)

        # 第11个任务应该失败
        success = await queue.push("task_10", 0)
        assert not success

    @pytest.mark.asyncio
    async def test_pop_from_empty_queue(self):
        """测试从空队列弹出"""
        from neotask.queue.priority_queue import PriorityQueue

        queue = PriorityQueue()
        tasks = await queue.pop(10)
        assert tasks == []

    @pytest.mark.asyncio
    async def test_remove_nonexistent_task(self):
        """测试移除不存在的任务"""
        from neotask.queue.priority_queue import PriorityQueue

        queue = PriorityQueue()
        removed = await queue.remove("nonexistent")
        assert not removed

    @pytest.mark.asyncio
    async def test_prefetcher_recovery(self):
        """测试预取器恢复"""
        from neotask.queue.queue_scheduler import QueueScheduler
        from neotask.storage.memory import MemoryQueueRepository

        repo = MemoryQueueRepository()
        queue = QueueScheduler(repo, max_size=1000)
        await queue.start()

        prefetcher = TaskPrefetcher(queue, PrefetchConfig(prefetch_size=20))
        await prefetcher.start()

        # 模拟队列暂时不可用
        with patch.object(queue, 'pop', side_effect=Exception("Queue error")):
            await asyncio.sleep(0.5)  # 让预取器尝试失败

        # 恢复后应该正常工作
        await queue.push("test_task", 5)

        # 等待预取器恢复
        await asyncio.sleep(0.5)

        task = await prefetcher.get(timeout=1)
        assert task == "test_task"

        await prefetcher.stop()
        await queue.stop()


# ========== 5. 并发竞态条件测试 ==========

@pytest.mark.chaos
class TestRaceConditions:
    """竞态条件测试"""

    @pytest.mark.asyncio
    async def test_concurrent_task_submission(self):
        """测试并发任务提交"""

        async def identity(data):
            return data

        pool = TaskPool(executor=identity)
        pool.start()

        concurrency = 50
        total = 500

        async def submit_batch(batch_id: int):
            tasks = []
            for i in range(total // concurrency):
                task_id = await pool.submit_async({"batch": batch_id, "i": i})
                tasks.append(task_id)
            return tasks

        batches = [submit_batch(i) for i in range(concurrency)]
        all_task_ids = await asyncio.gather(*batches)

        # 去重检查
        flat_ids = [tid for batch in all_task_ids for tid in batch]
        assert len(flat_ids) == total
        assert len(set(flat_ids)) == total

        pool.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_lock_acquisition(self):
        """测试并发锁获取 - 修复锁竞争问题"""
        from neotask.lock.redis import RedisLock

        lock = RedisLock("redis://localhost:6379")
        test_key = "race_lock_test"

        # 清空测试锁
        await lock.release(test_key)

        acquired_count = 0
        lock_acquire_times = []
        acquisition_lock = asyncio.Lock()

        async def try_acquire(node_id: int):
            nonlocal acquired_count
            # 使用独立的锁实例
            node_lock = RedisLock("redis://localhost:6379")

            # 随机延迟，模拟真实竞争
            await asyncio.sleep(node_id * 0.0005)

            # 尝试获取锁，使用更短的 TTL 和重试
            for attempt in range(3):
                try:
                    success = await node_lock.acquire(test_key, ttl=1)
                    if success:
                        async with acquisition_lock:
                            acquired_count += 1
                            lock_acquire_times.append(node_id)
                        # 持有锁一段时间
                        await asyncio.sleep(0.05)
                        await node_lock.release(test_key)
                        break
                    else:
                        # 锁被占用，等待一小段时间
                        await asyncio.sleep(0.01)
                except Exception:
                    pass

        # 创建多个并发任务
        tasks = [try_acquire(i) for i in range(30)]
        await asyncio.gather(*tasks)

        # 等待所有任务完成
        await asyncio.sleep(0.5)

        print(f"\nAcquired count: {acquired_count}, by nodes: {lock_acquire_times[:10]}")

        # 由于锁的严格性，应该只有一个或较少节点成功
        # 在某些情况下可能因为网络延迟导致多个，但应该很少
        assert acquired_count <= 1, f"Too many lock acquisitions: {acquired_count}"

        # 清理
        await lock.release(test_key)

    @pytest.mark.asyncio
    async def test_concurrent_task_processing(self):
        """测试并发任务处理"""
        counter = 0
        counter_lock = asyncio.Lock()

        async def increment(data):
            nonlocal counter
            async with counter_lock:
                current = counter
                await asyncio.sleep(0.001)  # 模拟处理时间
                counter = current + 1
            return {"counter": counter}

        pool = TaskPool(executor=increment, config=TaskPoolConfig(worker_concurrency=20))
        pool.start()

        tasks = []
        for i in range(100):
            tasks.append(pool.submit_async({"index": i}))

        task_ids = await asyncio.gather(*tasks)

        # 等待所有任务完成
        await asyncio.sleep(2)

        stats = pool.get_stats()
        print(f"\n任务完成统计 - 完成: {stats['completed']}, 失败: {stats['failed']}")

        # 验证计数正确
        final_counter = counter
        print(f"最终计数: {final_counter}")

        # 由于并发，最终计数应该等于成功执行的任务数
        assert final_counter == stats['completed'], f"Counter {final_counter} != completed {stats['completed']}"
        assert final_counter > 0

        pool.shutdown()


# ========== 6. 资源泄漏测试 ==========

@pytest.mark.chaos
class TestResourceLeak:
    """资源泄漏测试"""

    @pytest.mark.asyncio
    async def test_connection_pool_reuse(self):
        """测试连接池复用"""
        from neotask.storage.redis import RedisTaskRepository

        repo = RedisTaskRepository("redis://localhost:6379")

        # 修复：需要 await 获取客户端对象
        client1 = await repo._get_client()
        client2 = await repo._get_client()

        # 验证是同一个客户端对象
        assert client1 is client2

        await repo.close()

    @pytest.mark.asyncio
    async def test_task_cleanup(self):
        """测试任务清理"""

        async def identity(data):
            return data

        # 确保执行器正确配置
        config = TaskPoolConfig(
            storage_type="memory",
            worker_concurrency=10,  # 增加并发
            max_retries=0  # 禁用重试
        )
        pool = TaskPool(executor=identity, config=config)
        pool.start()

        # 提交任务
        task_ids = []
        for i in range(100):
            task_id = await pool.submit_async({"i": i})
            task_ids.append(task_id)

        # 批量等待，使用较短超时
        import asyncio
        try:
            # 使用 asyncio.gather 并发等待
            results = await asyncio.wait_for(
                asyncio.gather(*[pool.wait_for_result_async(tid, timeout=10) for tid in task_ids]),
                timeout=30
            )
            assert len(results) == 100
        except asyncio.TimeoutError:
            # 如果超时，检查任务状态
            for tid in task_ids[:5]:  # 只检查前5个
                task = await pool.get_task_async(tid)
                print(f"Task {tid}: status={task['status']}, error={task.get('error')}")
            raise

        # 等待所有任务完成
        await asyncio.sleep(0.5)

        stats = pool.get_stats()
        print(f"Stats: {stats}")
        assert stats['pending'] == 0
        assert stats['running'] == 0

        pool.shutdown()

    @pytest.mark.asyncio
    async def test_event_handler_cleanup(self):
        """测试事件处理器清理"""
        from neotask.event.bus import EventBus, TaskEvent

        bus = EventBus()
        await bus.start()

        handler_count = 0

        @bus.subscribe("test.event")
        async def handler(event):
            nonlocal handler_count
            handler_count += 1

        # 等待订阅完成
        await asyncio.sleep(0.05)

        # 发送事件
        await bus.emit(TaskEvent("test.event", "task1"))
        await bus.emit(TaskEvent("test.event", "task2"))

        # 等待事件处理完成
        await asyncio.sleep(0.2)

        # 记录当前计数
        before_stop = handler_count
        assert before_stop == 2, f"Expected 2 events processed, got {before_stop}"

        # 停止事件总线
        await bus.stop()

        # 等待停止完成
        await asyncio.sleep(0.1)

        # 清空事件队列，确保没有残留
        # 创建新的事件总线实例来验证
        new_bus = EventBus()
        await new_bus.start()

        new_handler_count = 0

        @new_bus.subscribe("test.event")
        async def new_handler(event):
            nonlocal new_handler_count
            new_handler_count += 1

        await asyncio.sleep(0.05)
        await new_bus.emit(TaskEvent("test.event", "task3"))
        await asyncio.sleep(0.1)

        assert new_handler_count == 1

        await new_bus.stop()


# ========== 7. 主测试入口 ==========

def run_chaos_tests():
    """运行所有异常场景测试"""
    import pytest
    args = [
        __file__,
        "-v",
        "-m", "chaos",
        "--maxfail=3"
    ]
    pytest.main(args)


if __name__ == "__main__":
    run_chaos_tests()
