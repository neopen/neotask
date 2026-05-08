"""Template for a new neotask-based project."""
import asyncio
from neotask import TaskPool, TaskPoolConfig


# 1. Define your task executor
async def my_executor(data):
    """
    Your business logic here.
    Can be sync (auto-wrapped) or async.
    """
    # Process the data...
    result = f"Processed: {data}"
    return {"status": "ok", "output": result}


# 2. Configure the pool
config = TaskPoolConfig(
    storage_type="sqlite",      # memory for dev, sqlite/redis for prod
    worker_concurrency=10,
    max_retries=3,
    retry_delay=5,
    task_timeout=300,
)


# 3. Main application
async def main():
    async with TaskPool(executor=my_executor, config=config) as pool:
        # Submit tasks
        task_ids = pool.submit_batch([
            {"id": 1, "name": "task_1"},
            {"id": 2, "name": "task_2"},
            {"id": 3, "name": "task_3"},
        ])

        # Wait for all results
        for task_id in task_ids:
            result = pool.wait_for_result(task_id)
            print(f"{task_id}: {result}")

        # Check stats
        stats = pool.get_stats()
        print(f"Success rate: {stats['success_rate']:.1%}")


if __name__ == "__main__":
    asyncio.run(main())
