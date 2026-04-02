"""
@FileName: 05_webui.py
@Description: Web UI example: Starting scheduler with embedded dashboard.
@Author: HiPeng
@Time: 2026/4/2 17:24
"""

import asyncio
import time
from neotask import TaskScheduler, TaskExecutor, SchedulerConfig


class DemoExecutor(TaskExecutor):
    """Demo executor that simulates various work loads."""

    async def execute(self, task_data: dict) -> dict:
        task_type = task_data.get("type", "normal")
        duration = task_data.get("duration", 1)

        print(f"Running {task_type} task for {duration}s...")
        await asyncio.sleep(duration)

        return {
            "type": task_type,
            "duration": duration,
            "completed_at": time.time()
        }


def main():
    # Create scheduler with Web UI enabled
    config = SchedulerConfig.with_webui(
        port=8080,
        auto_open=True
    )

    scheduler = TaskScheduler(
        executor=DemoExecutor(),
        config=config
    )

    print("\n" + "=" * 50)
    print("Task Scheduler with Web UI")
    print("=" * 50)
    print("Web UI: http://localhost:8080")
    print("\nSubmitting demo tasks...")

    try:
        # Submit various tasks
        tasks = [
            {"type": "quick", "duration": 0.5},
            {"type": "normal", "duration": 1},
            {"type": "slow", "duration": 2},
            {"type": "background", "duration": 0.8},
            {"type": "data_process", "duration": 1.5},
        ]

        for task_data in tasks:
            task_id = scheduler.submit(task_data)
            print(f"Submitted: {task_data['type']} -> {task_id}")

        print("\nKeep running. Press Ctrl+C to stop.\n")

        # Keep running
        while True:
            stats = scheduler.get_stats()
            print(f"\rQueue: {stats['queue_size']} | "
                  f"Node: {stats['node_id']} | "
                  f"Concurrent: {stats['max_concurrent']}", end="")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\nShutting down...")
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()