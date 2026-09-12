"""
@FileName: test_concurrent.py
@Description: 
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 19:05
"""
"""Concurrent task execution manual test."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import time
import threading
from neotask import TaskScheduler, TaskExecutor, SchedulerConfig


class ConcurrentExecutor(TaskExecutor):
    """Executor for concurrent testing."""

    def __init__(self):
        self.counter = 0
        self.lock = threading.Lock()

    async def execute(self, task_data: dict) -> dict:
        task_id = task_data.get("id")
        duration = task_data.get("duration", 0.5)

        with self.lock:
            self.counter += 1
            running = self.counter
            print(f"  [START] Task {task_id} (running: {running})")

        await asyncio.sleep(duration)

        with self.lock:
            self.counter -= 1
            print(f"  [END]   Task {task_id} (running: {self.counter})")

        return {"id": task_id, "duration": duration}


import asyncio


def test_concurrent_execution():
    """Test concurrent task execution."""
    print("\n" + "=" * 60)
    print("Test: Concurrent Execution")
    print("=" * 60)

    executor = ConcurrentExecutor()

    # Create scheduler with max_concurrent=3
    config = SchedulerConfig.memory()
    config.worker.max_concurrent = 3

    scheduler = TaskScheduler(
        executor=executor,
        config=config
    )

    try:
        print(f"Max concurrent: {config.worker.max_concurrent}")
        print("-" * 40)

        # Submit 10 tasks
        task_ids = []
        for i in range(10):
            task_id = scheduler.submit({"id": i, "duration": 0.5})
            task_ids.append(task_id)
            print(f"Submitted: task_{i}")

        # Wait for all
        for task_id in task_ids:
            scheduler.wait_for_result(task_id)

        print("\n✓ Concurrent test passed")

    finally:
        scheduler.shutdown()


def test_high_throughput():
    """Test high throughput with many tasks."""
    print("\n" + "=" * 60)
    print("Test: High Throughput")
    print("=" * 60)

    class FastExecutor(TaskExecutor):
        async def execute(self, task_data: dict) -> dict:
            return {"id": task_data["id"]}

    scheduler = TaskScheduler(
        executor=FastExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        count = 100
        print(f"Submitting {count} tasks...")

        start = time.time()

        task_ids = []
        for i in range(count):
            task_id = scheduler.submit({"id": i})
            task_ids.append(task_id)

        submit_time = time.time() - start
        print(f"Submit time: {submit_time:.3f}s")

        start = time.time()
        for task_id in task_ids:
            scheduler.wait_for_result(task_id)

        wait_time = time.time() - start
        print(f"Wait time: {wait_time:.3f}s")
        print(f"Throughput: {count / wait_time:.2f} tasks/s")

        print("\n✓ Throughput test passed")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    import asyncio

    test_concurrent_execution()
    test_high_throughput()
    print("\n" + "=" * 60)
    print("All concurrent tests passed!")
    print("=" * 60)