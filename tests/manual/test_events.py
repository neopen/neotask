"""
@FileName: test_events.py
@Description: 
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 19:05
"""
"""Event system manual test."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import time
from neotask import TaskScheduler, TaskExecutor, SchedulerConfig


class EventExecutor(TaskExecutor):
    """Executor for event testing."""

    async def execute(self, task_data: dict) -> dict:
        print(f"  Executing task {task_data['id']}")
        await asyncio.sleep(0.5)
        return {"status": "done", "id": task_data["id"]}


import asyncio


def test_event_callbacks():
    """Test event callback registration."""
    print("\n" + "=" * 60)
    print("Test: Event Callbacks")
    print("=" * 60)

    # Track events
    events = []

    def on_submitted(event):
        events.append(("submitted", event.task_id))
        print(f"  [EVENT] Task submitted: {event.task_id}")

    def on_started(event):
        events.append(("started", event.task_id))
        print(f"  [EVENT] Task started: {event.task_id}")

    def on_completed(event):
        events.append(("completed", event.task_id))
        print(f"  [EVENT] Task completed: {event.task_id}")

    def on_failed(event):
        events.append(("failed", event.task_id))
        print(f"  [EVENT] Task failed: {event.task_id}")

    scheduler = TaskScheduler(
        executor=EventExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        # Register handlers
        scheduler.on_task_submitted(on_submitted)
        scheduler.on_task_started(on_started)
        scheduler.on_task_completed(on_completed)
        scheduler.on_task_failed(on_failed)

        # Submit tasks
        task_ids = []
        for i in range(3):
            task_id = scheduler.submit({"id": i})
            task_ids.append(task_id)
            print(f"Submitted: {task_id}")

        # Wait for completion
        for task_id in task_ids:
            scheduler.wait_for_result(task_id)

        # Verify events
        print(f"\nTotal events: {len(events)}")
        assert len(events) >= 9  # 3 tasks * 3 events (submitted, started, completed)
        print("✓ Event test passed")

    finally:
        scheduler.shutdown()


def test_async_events():
    """Test async event handlers."""
    print("\n" + "=" * 60)
    print("Test: Async Event Handlers")
    print("=" * 60)

    async def async_handler(event):
        print(f"  [ASYNC EVENT] {event.event_type}: {event.task_id}")
        await asyncio.sleep(0.1)

    scheduler = TaskScheduler(
        executor=EventExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        # Register async handler
        scheduler._event_bus.subscribe("task.completed", async_handler, async_handler=True)

        # Submit task
        task_id = scheduler.submit({"id": 99})
        print(f"Submitted: {task_id}")

        # Wait for completion
        result = scheduler.wait_for_result(task_id)
        print(f"Result: {result}")

        time.sleep(0.5)
        print("✓ Async event test passed")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    import asyncio

    test_event_callbacks()
    test_async_events()
    print("\n" + "=" * 60)
    print("All event tests passed!")
    print("=" * 60)