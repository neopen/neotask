"""
@FileName: 03_priority.py
@Description: 优先级队列。Priority example: Demonstrating task priorities.
@Author: HiPeng
@Time: 2026/4/2 17:24
"""
import asyncio
import time
from neotask import TaskScheduler, TaskExecutor, SchedulerConfig, TaskPriority


class PriorityExecutor(TaskExecutor):
    """Executor that shows priority ordering."""

    async def execute(self, task_data: dict) -> dict:
        priority = task_data.get("priority", "unknown")
        print(f"[{priority}] Executing: {task_data['msg']}")
        await asyncio.sleep(0.2)
        return {"executed": True}


def main():
    scheduler = TaskScheduler(
        executor=PriorityExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        # Submit tasks with different priorities
        # Lower priority value = higher priority
        tasks = [
            ("Critical task", TaskPriority.CRITICAL),
            ("Normal task 1", TaskPriority.NORMAL),
            ("High task", TaskPriority.HIGH),
            ("Normal task 2", TaskPriority.NORMAL),
            ("Low task", TaskPriority.LOW),
        ]

        for msg, priority in tasks:
            task_id = scheduler.submit({"msg": msg, "priority": priority.name}, priority)
            print(f"Submitted: {msg} (priority={priority.name})")

        # Wait for all tasks to complete
        time.sleep(3)
        print("\nAll tasks completed")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()