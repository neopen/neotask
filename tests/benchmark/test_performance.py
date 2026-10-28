"""
@FileName: test_performance.py
@Description: 性能基准测试（吞吐量、延迟、扩展性）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import asyncio
import statistics
import time
from typing import List, Dict

import pytest

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.worker.prefetcher import TaskPrefetcher, PrefetchConfig


# ========== 测试辅助函数 ==========

async def noop_executor(data: Dict) -> Dict:
    return {"result": "done"}


async def light_executor(data: Dict) -> Dict:
    _ = sum(range(100))
    return {"result": "done"}


async def cpu_executor(data: Dict) -> Dict:
    count = data.get("count", 5000)
    result = 0
    for i in range(count):
        result += i
    return {"result": result}


async def io_executor(data: Dict) -> Dict:
    await asyncio.sleep(data.get("sleep", 0.001))
    return {"result": "done"}


class BenchmarkResult:
    def __init__(self, name: str):
        self.name = name
        self.latencies: List[float] = []
        self.error_count: int = 0
        self.start_time: float = 0
        self.end_time: float = 0
        self.total_requests: int = 0

    def add_latency(self, latency: float):
        self.latencies.append(latency)

    def start(self):
        self.start_time = time.time()

    def finalize(self, total_requests: int = None):
        self.end_time = time.time()
        self.total_requests = total_requests if total_requests is not None else len(self.latencies)

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time

    @property
    def avg_latency_ms(self) -> float:
        return statistics.mean(self.latencies) * 1000 if self.latencies else 0

    @property
    def p50_latency_ms(self) -> float:
        return statistics.median(self.latencies) * 1000 if self.latencies else 0

    @property
    def p95_latency_ms(self) -> float:
        if not self.latencies:
            return 0
        sorted_times = sorted(self.latencies)
        return sorted_times[int(len(sorted_times) * 0.95)] * 1000

    @property
    def p99_latency_ms(self) -> float:
        if not self.latencies:
            return 0
        sorted_times = sorted(self.latencies)
        return sorted_times[int(len(sorted_times) * 0.99)] * 1000

    @property
    def throughput_ops(self) -> float:
        return self.total_requests / self.duration if self.duration > 0 else 0


# ========== 1. 基础吞吐量测试 ==========

@pytest.mark.benchmark
class TestBasicThroughput:

    @pytest.mark.asyncio
    async def test_task_pool_throughput(self):
        pool = TaskPool(executor=noop_executor)
        pool.start()

        total = 1000
        result = BenchmarkResult("task_pool_throughput")
        result.start()

        for i in range(total):
            await pool.submit_async({"index": i})

        await asyncio.sleep(0.5)
        result.finalize(total)

        stats = pool.get_stats()
        pool.shutdown()

        print("\nTaskPool 吞吐量: {:.2f} ops/sec".format(result.throughput_ops))
        print("   总任务数: {}".format(stats['total']))
        print("   成功率: {:.2%}".format(stats['success_rate']))

        assert stats['success_rate'] > 0.95

    @pytest.mark.asyncio
    async def test_task_pool_latency_distribution(self):
        pool = TaskPool(executor=noop_executor)
        pool.start()
        await asyncio.sleep(0.1)

        latencies = []
        total = 50  # 减少样本以保证测试高效且稳定

        for i in range(total):
            start = time.time()
            try:
                task_id = await pool.submit_async({"index": i})
                result = await pool.wait_for_result_async(task_id, timeout=1.0)
                if result is not None:
                    latencies.append(time.time() - start)
            except Exception:
                pass

        pool.shutdown()

        if latencies:
            sorted_latencies = sorted(latencies)
            print("\nTaskPool 延迟分布:")
            print("   成功统计任务数: {}/{}".format(len(latencies), total))
            print("   平均延迟: {:.2f} ms".format(statistics.mean(latencies) * 1000))
            print("   P50: {:.2f} ms".format(statistics.median(latencies) * 1000))
            print("   P95: {:.2f} ms".format(sorted_latencies[int(len(sorted_latencies) * 0.95)] * 1000))
            print("   P99: {:.2f} ms".format(sorted_latencies[int(len(sorted_latencies) * 0.99)] * 1000))
        else:
            pytest.fail("所有延迟测试任务均超时或失败")


# ========== 2. 队列性能测试 ==========

@pytest.mark.benchmark
class TestQueuePerformance:

    @pytest.mark.asyncio
    async def test_priority_queue_throughput(self):
        from neotask.queue.priority_queue import PriorityQueue
        queue = PriorityQueue()
        total = 5000

        start = time.time()
        for i in range(total):
            await queue.push("task_{}".format(i), i % 5)
        duration_push = time.time() - start

        start = time.time()
        for i in range(total):
            await queue.pop(1)
        duration_pop = time.time() - start

        print("\n优先级队列性能:")
        print("   入队: {:.2f} ops/sec".format(total / duration_push))
        print("   出队: {:.2f} ops/sec".format(total / duration_pop))
        assert total / duration_push > 1000

    @pytest.mark.asyncio
    async def test_prefetcher_performance(self):
        from neotask.queue.queue_scheduler import QueueScheduler
        from neotask.storage.memory import MemoryQueueRepository

        repo = MemoryQueueRepository()
        queue = QueueScheduler(repo)
        await queue.start()

        for i in range(2000):
            await queue.push("task_{}".format(i), i % 5)

        start = time.time()
        for _ in range(500):
            await queue.pop(1)
        without_prefetch = time.time() - start

        prefetcher = TaskPrefetcher(queue, PrefetchConfig(prefetch_size=20))
        await prefetcher.start()

        start = time.time()
        for _ in range(500):
            await prefetcher.get(timeout=0.01)
        with_prefetch = time.time() - start

        await prefetcher.stop()
        await queue.stop()

        if without_prefetch > 0 and with_prefetch > 0:
            throughput_without = 500 / without_prefetch
            throughput_with = 500 / with_prefetch
            print("\n预取器性能对比:")
            print("   无预取: {:.2f} ops/sec".format(throughput_without))
            print("   有预取: {:.2f} ops/sec".format(throughput_with))

    @pytest.mark.asyncio
    async def test_batch_pop_performance(self):
        from neotask.queue.priority_queue import PriorityQueue
        queue = PriorityQueue()

        for i in range(10000):
            await queue.push("task_{}".format(i), i % 5)

        start = time.time()
        for i in range(1000):
            await queue.pop(1)
        single_time = time.time() - start

        for i in range(10000):
            await queue.push("task_{}".format(i), i % 5)

        start = time.time()
        for i in range(100):
            await queue.pop(10)
        batch_time = time.time() - start

        print("\n批量弹出性能对比:")
        print("   单条弹出: {:.2f} ops/sec".format(1000 / single_time))
        print("   批量弹出: {:.2f} ops/sec".format(1000 / batch_time))


# ========== 3. 并发性能测试 ==========

@pytest.mark.benchmark
class TestConcurrencyPerformance:

    @pytest.mark.asyncio
    async def test_worker_scalability_light_task(self):
        configs = [1, 2, 4, 8, 16]
        results = []

        for concurrency in configs:
            config = TaskPoolConfig(storage_type="memory", worker_concurrency=concurrency)
            pool = TaskPool(executor=light_executor, config=config)
            pool.start()

            total = 300
            result = BenchmarkResult("workers_{}".format(concurrency))
            result.start()

            tasks = [pool.submit_async({"index": i}) for i in range(total)]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.2)

            result.finalize(total)
            results.append({"workers": concurrency, "throughput": result.throughput_ops})
            pool.shutdown()
            await asyncio.sleep(0.05)

        print("\nWorker 扩展性测试（轻量任务）:")
        for r in results:
            print("   {} workers: {:.2f} ops/sec".format(r['workers'], r['throughput']))

    @pytest.mark.asyncio
    async def test_worker_scalability_heavy_task(self):
        configs = [1, 2, 4, 8]
        results = []

        for concurrency in configs:
            # 修复：移除不支持的 executor_type="thread" 参数
            config = TaskPoolConfig(storage_type="memory", worker_concurrency=concurrency)
            pool = TaskPool(executor=cpu_executor, config=config)
            pool.start()

            total = 50
            result = BenchmarkResult("workers_{}".format(concurrency))
            result.start()

            tasks = [pool.submit_async({"count": 2000}) for i in range(total)]
            await asyncio.gather(*tasks)

            result.finalize(total)
            results.append({"workers": concurrency, "throughput": result.throughput_ops})
            pool.shutdown()
            await asyncio.sleep(0.05)

        print("\nWorker 扩展性测试（CPU密集型）:")
        for r in results:
            print("   {} workers: {:.2f} ops/sec".format(r['workers'], r['throughput']))

    @pytest.mark.asyncio
    async def test_high_concurrency_submit(self):
        pool = TaskPool(executor=light_executor)
        pool.start()

        concurrency = 20
        total = 500

        async def submit_batch(batch_id: int, start_idx: int, count: int):
            tasks = [pool.submit_async({"batch": batch_id, "i": start_idx + i}) for i in range(count)]
            return await asyncio.gather(*tasks)

        start = time.time()
        batch_size = total // concurrency
        batches = [submit_batch(i, i * batch_size, batch_size) for i in range(concurrency)]
        all_task_ids = await asyncio.gather(*batches)
        duration = time.time() - start

        await asyncio.sleep(0.5)
        stats = pool.get_stats()
        pool.shutdown()

        flat_ids = [tid for batch in all_task_ids for tid in batch]
        print("\n高并发提交 ({} 并发):".format(concurrency))
        print("   总任务: {}".format(len(flat_ids)))
        print("   吞吐量: {:.2f} ops/sec".format(len(flat_ids) / duration if duration > 0 else 0))


# ========== 4. 不同任务类型性能 ==========

@pytest.mark.benchmark
class TestTaskTypePerformance:

    @pytest.mark.asyncio
    async def test_light_task_performance(self):
        # 修复：每次测试严格新建 TaskPool 实例，避免锁跨事件循环复用
        pool = TaskPool(executor=light_executor)
        pool.start()

        result = BenchmarkResult("light_task")
        result.start()

        tasks = [pool.submit_async({"index": i}) for i in range(500)]
        await asyncio.gather(*tasks)

        await asyncio.sleep(0.2)
        result.finalize(500)
        pool.shutdown()

        print("\n轻量任务性能:")
        print("   吞吐量: {:.2f} ops/sec".format(result.throughput_ops))
        print("   平均延迟: {:.2f} ms".format(result.avg_latency_ms))

    @pytest.mark.asyncio
    async def test_io_task_performance(self):
        pool = TaskPool(executor=io_executor, config=TaskPoolConfig(worker_concurrency=20))
        pool.start()

        result = BenchmarkResult("io_task")
        result.start()

        tasks = [pool.submit_async({"sleep": 0.002}) for i in range(200)]
        await asyncio.gather(*tasks)

        result.finalize(200)
        pool.shutdown()

        print("\nIO密集型任务性能:")
        print("   吞吐量: {:.2f} ops/sec".format(result.throughput_ops))
        print("   平均延迟: {:.2f} ms".format(result.avg_latency_ms))


# ========== 5. 压力测试 ==========

@pytest.mark.benchmark
class TestStressTest:

    @pytest.mark.asyncio
    async def test_sustained_load(self):
        pool = TaskPool(executor=light_executor, config=TaskPoolConfig(worker_concurrency=10))
        pool.start()

        duration = 3
        total_submitted = 0
        errors = 0

        start = time.time()
        while time.time() - start < duration:
            try:
                await pool.submit_async({"index": total_submitted})
                total_submitted += 1
                await asyncio.sleep(0.002)
            except Exception:
                errors += 1

        await asyncio.sleep(0.5)
        stats = pool.get_stats()
        pool.shutdown()

        throughput = total_submitted / duration if duration > 0 else 0
        print("\n持续负载测试 ({}s):".format(duration))
        print("   总提交: {}, 吞吐量: {:.2f} ops/sec".format(total_submitted, throughput))

    @pytest.mark.asyncio
    async def test_burst_load(self):
        pool = TaskPool(executor=light_executor, config=TaskPoolConfig(worker_concurrency=20))
        pool.start()

        burst_size = 300
        start = time.time()

        tasks = [pool.submit_async({"index": i}) for i in range(burst_size)]
        await asyncio.gather(*tasks)
        duration = time.time() - start

        await asyncio.sleep(0.2)
        stats = pool.get_stats()
        pool.shutdown()

        print("\n突发负载测试 ({} 任务):".format(burst_size))
        print("   吞吐量: {:.2f} ops/sec".format(burst_size / duration if duration > 0 else 0))


# ========== 6. 主测试入口 ==========

def run_all_benchmarks():
    args = [
        __file__,
        "-v",
        "-m", "benchmark",
        "-x",
    ]
    pytest.main(args)


if __name__ == "__main__":
    run_all_benchmarks()
