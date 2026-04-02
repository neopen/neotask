"""
@FileName: 02_async.py
@Description: 异步使用。Async example: Using async/await pattern.
@Author: HiPeng
@Time: 2026/4/2 17:23
"""
import asyncio
from neotask import TaskScheduler, TaskExecutor, SchedulerConfig


class AsyncExecutor(TaskExecutor):
    """Async task executor."""

    async def execute(self, task_data: dict) -> dict:
        print(f"Processing: {task_data}")
        await asyncio.sleep(0.5)
        return {"result": task_data["value"] * 2}


async def main():
    # Create scheduler
    scheduler = TaskScheduler(
        executor=AsyncExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        # Submit multiple tasks concurrently
        tasks = []
        for i in range(5):
            task_id = await scheduler.submit_async({"value": i})
            tasks.append(task_id)
            print(f"Submitted: {task_id}")

        # Wait for all results
        for task_id in tasks:
            result = await scheduler.wait_for_result_async(task_id)
            print(f"Result for {task_id}: {result}")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())