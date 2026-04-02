"""
@FileName: 04_events.py
@Description: 事件回调。Events example: Using event callbacks.
@Author: HiPeng
@Time: 2026/4/2 17:24
"""

import asyncio
from neotask import TaskScheduler, TaskExecutor, SchedulerConfig


class EventExecutor(TaskExecutor):
    """Executor that does some work."""

    async def execute(self, task_data: dict) -> dict:
        print(f"  Executing task {task_data['id']}...")
        await asyncio.sleep(0.5)
        return {"status": "done", "id": task_data["id"]}


def on_submitted(event):
    print(f"Task submitted: {event.task_id}")


def on_started(event):
    print(f"Task started: {event.task_id}")


def on_completed(event):
    print(f"Task completed: {event.task_id}")


def on_failed(event):
    print(f"Task failed: {event.task_id}")


def main():
    scheduler = TaskScheduler(
        executor=EventExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        # Register event handlers
        scheduler.on_task_submitted(on_submitted)
        scheduler.on_task_started(on_started)
        scheduler.on_task_completed(on_completed)
        scheduler.on_task_failed(on_failed)

        # Submit tasks
        for i in range(3):
            task_id = scheduler.submit({"id": i})
            print(f"  Submitted task_id: {task_id}")

        # Wait for completion
        import time
        time.sleep(3)

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()