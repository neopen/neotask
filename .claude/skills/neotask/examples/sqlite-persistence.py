"""TaskPool with SQLite persistence for production use."""
import asyncio
from neotask import TaskPool, TaskPoolConfig


async def process(data):
    return {"processed": True, "data": data}


async def main():
    config = TaskPoolConfig(
        storage_type="sqlite",
        sqlite_path="tasks.db",
        worker_concurrency=5,
        max_retries=3,
        retry_delay=5,
    )

    with TaskPool(executor=process, config=config) as pool:
        task_id = pool.submit({"file": "data.csv"})
        result = pool.wait_for_result(task_id)
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
