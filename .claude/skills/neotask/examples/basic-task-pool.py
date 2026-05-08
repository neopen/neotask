"""Basic TaskPool usage example."""
import asyncio
from neotask import TaskPool


async def process(data):
    await asyncio.sleep(0.1)
    return {"result": "ok", "input": data}


async def main():
    pool = TaskPool(executor=process)
    task_id = pool.submit({"name": "test"})
    result = pool.wait_for_result(task_id)
    print(f"Result: {result}")
    pool.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
