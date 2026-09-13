"""TaskPool with event callbacks."""
import asyncio
from neotask import TaskPool


async def process(data):
    await asyncio.sleep(0.1)
    if data.get("fail"):
        raise ValueError("Simulated failure")
    return data


async def on_created(event):
    print(f"[CREATED] {event.task_id}")


async def on_completed(event):
    print(f"[DONE] {event.task_id}")


async def on_failed(event):
    print(f"[FAILED] {event.task_id}: {event.data}")


async def main():
    pool = TaskPool(executor=process)
    pool.on_created(on_created)
    pool.on_completed(on_completed)
    pool.on_failed(on_failed)

    pool.submit({"work": 1})
    pool.submit({"fail": True})

    await asyncio.sleep(1)
    pool.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
