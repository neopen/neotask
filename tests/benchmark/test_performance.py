"""
@FileName: test_performance.py
@Description: 性能基准测试 - 吞吐量、延迟、扩展性
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import asyncio
import time
import pytest
import statistics
from typing import List, Dict, Any

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.worker.prefetcher import TaskPrefetcher, PrefetchConfig


# ========== 测试辅助函数 ==========

async def noop_executor(data: Dict) -> Dict:
    return {"result": "done"}


async def light_executor(data: Dict) -> Dict:
    _ = sum(range(100))
    return {"result": "done"}


async def cpu_executor(data: Dict) -> Dict:
    count = data.get("count", 10000)
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
        if total_requests is not None:
            self.total_requests = total_requests
        else:
            self.total_requests = len(self.latencies)

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time

    @property
    def avg_latency_ms(self) -> float:
        if not self.latencies:
            return 0
        return statistics.mean(self.latencies) * 1000

    @property
    def p50_latency_ms(self) -> float:
        if not self.latencies:
            return 0
        return statistics.median(self.latencies) * 1000

    @property
    def p95_latency_ms(self) -> float:
        if not self.latencies:
            return 0
        sorted_times = sorted(self.latencies)
        idx = int(len(sorted_times) * 0.95)
        return sorted_times[idx] * 1000

    @property
    def p99_latency_ms(self) -> float:
        if not self.latencies:
            return 0
        sorted_times = sorted(self.latencies)
        idx = int(len(sorted_times) * 0.99)
        return sorted_times[idx] * 1000

    @property
    def throughput_ops(self) -> float:
        if self.duration <= 0:
            return 0
        return self.total_requests / self.duration

    def report(self) -> Dict:
        return {
            "name": self.name,
            "total_requests": self.total_requests,
            "errors": self.error_count,
            "throughput_ops": round(self.throughput_ops, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "p50_latency_ms": round(self.p50_latency_ms, 2),
            "p95_latency_ms": round(self.p95_latency_ms, 2),
            "p99_latency_ms": round(self.p99_latency_ms, 2),
        }


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

        latencies = []
        total = 500

        for i in range(total):
            start = time.time()
            task_id = await pool.submit_async({"index": i})
            await pool.wait_for_result_async(task_id)
            latencies.append(time.time() - start)

        pool.shutdown()

        if latencies:
            sorted_latencies = sorted(latencies)

            print("\nTaskPool 延迟分布:")
            print("   平均延迟: {:.2f} ms".format(statistics.mean(latencies)*1000))
            print("   P50: {:.2f} ms".format(statistics.median(latencies)*1000))
            print("   P95: {:.2f} ms".format(sorted_latencies[int(len(sorted_latencies)*0.95)]*1000))
            print("   P99: {:.2f} ms".format(sorted_latencies[int(len(sorted_latencies)*0.99)]*1000))


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
        print("   入队: {:.2f} ops/sec".format(total/duration_push))
        print("   出队: {:.2f} ops/sec".format(total/duration_pop))

        assert total/duration_push > 1000

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
            tasks = await queue.pop(1)
        without_prefetch = time.time() - start

        prefetcher = TaskPrefetcher(queue, PrefetchConfig(prefetch_size=20))
        await prefetcher.start()

        start = time.time()
        for _ in range(500):
            task = await prefetcher.get(timeout=0.01)
        with_prefetch = time.time() - start

        await prefetcher.stop()
        await queue.stop()

        if without_prefetch > 0 and with_prefetch > 0:
            throughput_without = 500 / without_prefetch
            throughput_with = 500 / with_prefetch
            improvement = throughput_with / throughput_without if throughput_without > 0 else 1

            print("\n预取器性能对比:")
            print("   无预取: {:.2f} ops/sec".format(throughput_without))
            print("   有预取: {:.2f} ops/sec".format(throughput_with))
            print("   提升: {:.2f}x".format(improvement))

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
        print("   单条弹出: {:.2f} ops/sec".format(1000/single_time))
        print("   批量弹出: {:.2f} ops/sec".format(1000/batch_time))
        if single_time > 0 and batch_time > 0:
            print("   提升: {:.2f}x".format((1000/batch_time)/(1000/single_time)))


# ========== 3. 并发性能测试 ==========

@pytest.mark.benchmark
class TestConcurrencyPerformance:

    @pytest.mark.asyncio
    async def test_worker_scalability_light_task(self):
        configs = [1, 2, 4, 8, 16]
        results = []

        for concurrency in configs:
            config = TaskPoolConfig(
                storage_type="memory",
                worker_concurrency=concurrency
            )
            pool = TaskPool(executor=light_executor, config=config)
            pool.start()

            total = 500
            result = BenchmarkResult("workers_{}".format(concurrency))
            result.start()

            tasks = []
            for i in range(total):
                tasks.append(pool.submit_async({"index": i}))

            await asyncio.gather(*tasks)
            await asyncio.sleep(0.5)

            result.finalize(total)

            results.append({
                "workers": concurrency,
                "throughput": result.throughput_ops
            })
            pool.shutdown()
            await asyncio.sleep(0.1)

        print("\nWorker 扩展性测试（轻量任务）:")
        for r in results:
            print("   {} workers: {:.2f} ops/sec".format(r['workers'], r['throughput']))

        if len(results) >= 2:
            improvement = results[2]["throughput"] / results[0]["throughput"] if results[0]["throughput"] > 0 else 1
            print("\n   扩展效率: {:.2f}x (4 workers vs 1 worker)".format(improvement))
            assert improvement > 0.8

    @pytest.mark.asyncio
    async def test_worker_scalability_heavy_task(self):
        configs = [1, 2, 4, 8]
        results = []

        for concurrency in configs:
            config = TaskPoolConfig(
                storage_type="memory",
                worker_concurrency=concurrency,
                executor_type="thread"
            )
            pool = TaskPool(executor=cpu_executor, config=config)
            pool.start()

            total = 100
            result = BenchmarkResult("workers_{}".format(concurrency))
            result.start()

            tasks = []
            for i in range(total):
                tasks.append(pool.submit_async({"count": 5000}))

            await asyncio.gather(*tasks)

            result.finalize(total)

            results.append({
                "workers": concurrency,
                "throughput": result.throughput_ops
            })
            pool.shutdown()
            await asyncio.sleep(0.1)

        print("\nWorker 扩展性测试（CPU密集型）:")
        for r in results:
            print("   {} workers: {:.2f} ops/sec".format(r['workers'], r['throughput']))

        if len(results) >= 2:
            improvement = results[2]["throughput"] / results[0]["throughput"] if results[0]["throughput"] > 0 else 1
            print("\n   扩展效率: {:.2f}x (4 workers vs 1 worker)".format(improvement))

    @pytest.mark.asyncio
    async def test_high_concurrency_submit(self):
        pool = TaskPool(executor=light_executor)
        pool.start()

        concurrency = 50
        total = 1000

        async def submit_batch(batch_id: int, start_idx: int, count: int):
            tasks = []
            for i in range(count):
                tasks.append(pool.submit_async({"batch": batch_id, "i": start_idx + i}))
            return await asyncio.gather(*tasks)

        start = time.time()
        batch_size = total // concurrency
        batches = [submit_batch(i, i * batch_size, batch_size) for i in range(concurrency)]
        all_task_ids = await asyncio.gather(*batches)
        duration = time.time() - start

        await asyncio.sleep(1)

        stats = pool.get_stats()
        pool.shutdown()

        flat_ids = [tid for batch in all_task_ids for tid in batch]

        print("\n高并发提交 ({} 并发):".format(concurrency))
        print("   总任务: {}".format(len(flat_ids)))
        print("   耗时: {:.2f}s".format(duration))
        print("   吞吐量: {:.2f} ops/sec".format(len(flat_ids)/duration))
        print("   成功率: {:.2%}".format(stats['success_rate']))

        assert len(flat_ids) == total
        assert stats['success_rate'] > 0.95


# ========== 4. 不同任务类型性能 ==========

@pytest.mark.benchmark
class TestTaskTypePerformance:

    @pytest.mark.asyncio
    async def test_light_task_performance(self):
        pool = TaskPool(executor=light_executor)
        pool.start()

        result = BenchmarkResult("light_task")
        result.start()

        tasks = []
        for i in range(1000):
            tasks.append(pool.submit_async({"index": i}))
        await asyncio.gather(*tasks)

        await asyncio.sleep(0.5)
        result.finalize(1000)
        pool.shutdown()

        print("\n轻量任务性能:")
        print("   吞吐量: {:.2f} ops/sec".format(result.throughput_ops))
        print("   平均延迟: {:.2f} ms".format(result.avg_latency_ms))

    @pytest.mark.asyncio
    async def test_io_task_performance(self):
        pool = TaskPool(executor=io_executor, config=TaskPoolConfig(worker_concurrency=50))
        pool.start()

        result = BenchmarkResult("io_task")
        result.start()

        tasks = []
        for i in range(500):
            tasks.append(pool.submit_async({"sleep": 0.005}))
        await asyncio.gather(*tasks)

        result.finalize(500)
        pool.shutdown()

        print("\nIO密集型任务性能:")
        print("   吞吐量: {:.2f} ops/sec".format(result.throughput_ops))
        print("   平均延迟: {:.2f} ms".format(result.avg_latency_ms))


# ========== 5. 压力测试 ==========

@pytest.mark.benchmark
class TestStressTest:

    @pytest.mark.asyncio
    async def test_sustained_load(self):
        pool = TaskPool(
            executor=light_executor,
            config=TaskPoolConfig(worker_concurrency=20)
        )
        pool.start()

        duration = 10
        total_submitted = 0
        errors = 0

        start = time.time()
        while time.time() - start < duration:
            try:
                await pool.submit_async({"index": total_submitted})
                total_submitted += 1
                await asyncio.sleep(0.001)
            except Exception:
                errors += 1

        await asyncio.sleep(1)

        stats = pool.get_stats()
        pool.shutdown()

        throughput = total_submitted / duration

        print("\n持续负载测试 ({}s):".format(duration))
        print("   总提交: {}".format(total_submitted))
        print("   错误: {}".format(errors))
        print("   吞吐量: {:.2f} ops/sec".format(throughput))
        print("   成功率: {:.2%}".format(stats['success_rate']))

        assert errors < total_submitted * 0.01

    @pytest.mark.asyncio
    async def test_burst_load(self):
        pool = TaskPool(
            executor=light_executor,
            config=TaskPoolConfig(worker_concurrency=50)
        )
        pool.start()

        burst_size = 500
        start = time.time()

        tasks = []
        for i in range(burst_size):
            tasks.append(pool.submit_async({"index": i}))

        await asyncio.gather(*tasks)
        duration = time.time() - start

        await asyncio.sleep(0.5)
        stats = pool.get_stats()
        pool.shutdown()

        print("\n突发负载测试 ({} 任务):".format(burst_size))
        print("   总耗时: {:.2f}s".format(duration))
        print("   吞吐量: {:.2f} ops/sec".format(burst_size/duration))
        print("   成功率: {:.2%}".format(stats['success_rate']))

        assert stats['success_rate'] > 0.95


# ========== 6. 主测试入口 ==========

def run_all_benchmarks():
    import pytest
    args = [
        __file__,
        "-v",
        "-m", "benchmark",
        "-x",
    ]
    pytest.main(args)


if __name__ == "__main__":
    run_all_benchmarks()