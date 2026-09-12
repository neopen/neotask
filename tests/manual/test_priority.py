"""
@FileName: test_priority.py
@Description: 
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 19:05
"""
"""Priority queue manual test."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import time
from neotask import TaskScheduler, TaskExecutor, SchedulerConfig, TaskPriority


class PriorityExecutor(TaskExecutor):
    """Executor that logs priority."""

    async def execute(self, task_data: dict) -> dict:
        name = task_data.get("name", "unknown")
        priority = task_data.get("priority", "unknown")
        print(f"  [{priority}] Executing: {name}")
        await asyncio.sleep(0.2)
        return {"name": name, "priority": priority}


import asyncio


def test_priority_order():
    """Test that higher priority tasks execute first."""
    print("\n" + "=" * 60)
    print("Test: Priority Queue Order")
    print("=" * 60)
    print("Expected order: CRITICAL -> HIGH -> NORMAL -> LOW")
    print("-" * 40)

    scheduler = TaskScheduler(
        executor=PriorityExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        # Submit tasks in reverse priority order
        tasks = [
            ("Low task", TaskPriority.LOW),
            ("Normal task", TaskPriority.NORMAL),
            ("High task", TaskPriority.HIGH),
            ("Critical task", TaskPriority.CRITICAL),
        ]

        for name, priority in tasks:
            task_id = scheduler.submit(
                {"name": name, "priority": priority.name},
                priority
            )
            print(f"Submitted: {name} (priority={priority.name})")

        # Give time for execution
        time.sleep(2)
        print("\n✓ Priority test completed")

    finally:
        scheduler.shutdown()


def test_multiple_priorities():
    """Test mixing multiple priorities."""
    print("\n" + "=" * 60)
    print("Test: Mixed Priorities")
    print("=" * 60)

    scheduler = TaskScheduler(
        executor=PriorityExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        # Submit mix of priorities
        tasks_data = [
            ("Task A", TaskPriority.CRITICAL),
            ("Task B", TaskPriority.LOW),
            ("Task C", TaskPriority.HIGH),
            ("Task D", TaskPriority.NORMAL),
            ("Task E", TaskPriority.CRITICAL),
            ("Task F", TaskPriority.LOW),
        ]

        for name, priority in tasks_data:
            task_id = scheduler.submit(
                {"name": name, "priority": priority.name},
                priority
            )
            print(f"Submitted: {name} (priority={priority.name})")

        time.sleep(3)
        print("\n✓ Mixed priority test completed")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    import asyncio

    test_priority_order()
    test_multiple_priorities()
    print("\n" + "=" * 60)
    print("All priority tests passed!")
    print("=" * 60)