"""
@FileName: 07_batch.py
@Description: 批量任务。Batch example: Submitting and monitoring many tasks.
@Author: HiPeng
@Time: 2026/4/2 17:25
"""

import asyncio
import time
from neotask import TaskScheduler, TaskExecutor, SchedulerConfig


class BatchExecutor(TaskExecutor):
    """Executor for batch processing."""

    async def execute(self, task_data: dict) -> dict:
        # Simulate work
        await asyncio.sleep(0.1)
        return {
            "index": task_data["index"],
            "processed": True,
            "timestamp": time.time()
        }


def main():
    scheduler = TaskScheduler(
        executor=BatchExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        # Submit 100 tasks
        batch_size = 100
        task_ids = []

        print(f"Submitting {batch_size} tasks...")
        start_time = time.time()

        for i in range(batch_size):
            task_id = scheduler.submit({"index": i})
            task_ids.append(task_id)

        submit_time = time.time() - start_time
        print(f"Submitted {batch_size} tasks in {submit_time:.2f}s")

        # Wait for all tasks
        print("Waiting for all tasks to complete...")
        start_time = time.time()

        completed = 0
        failed = 0

        for task_id in task_ids:
            try:
                result = scheduler.wait_for_result(task_id, timeout=10)
                if result.get("processed"):
                    completed += 1
            except Exception:
                failed += 1

        wait_time = time.time() - start_time
        print(f"\nResults:")
        print(f"  Completed: {completed}")
        print(f"  Failed: {failed}")
        print(f"  Total time: {wait_time:.2f}s")
        print(f"  Throughput: {batch_size / wait_time:.2f} tasks/s")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()