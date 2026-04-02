"""
@FileName: 08_complete.py
@Description: 完整功能演示。Complete example: Demonstrating all major features.
@Author: HiPeng
@Time: 2026/4/2 17:26
"""

import asyncio
import time

from neotask import TaskScheduler, TaskExecutor, SchedulerConfig, TaskPriority


class DemoExecutor(TaskExecutor):
    """Demonstration executor with logging."""

    async def execute(self, task_data: dict) -> dict:
        name = task_data.get("name", "unknown")
        duration = task_data.get("duration", 0.5)

        print(f"  [START] {name} (duration={duration}s)")
        await asyncio.sleep(duration)
        print(f"  [END]   {name}")

        return {
            "name": name,
            "duration": duration,
            "success": True
        }


def print_section(title: str):
    """Print section header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def main():
    # Create scheduler with Web UI
    config = SchedulerConfig.with_webui(port=8080, auto_open=False)
    config.worker.max_concurrent = 5

    scheduler = TaskScheduler(
        executor=DemoExecutor(),
        config=config
    )

    print("\n Task Scheduler Demo")
    print(f"   Node ID: {config.node_id}")
    print(f"   Web UI: http://localhost:{config.webui.port}")

    try:
        # Section 1: Basic submission
        print_section("1. Basic Task Submission")
        task_id = scheduler.submit({"name": "basic", "duration": 1})
        print(f"Submitted: {task_id}")

        result = scheduler.wait_for_result(task_id)
        print(f"Result: {result}")

        # Section 2: Priority
        print_section("2. Priority Queue")
        tasks = [
            ("Low priority", TaskPriority.LOW, 0.3),
            ("High priority", TaskPriority.HIGH, 0.3),
            ("Critical", TaskPriority.CRITICAL, 0.3),
            ("Normal", TaskPriority.NORMAL, 0.3),
        ]

        for name, priority, duration in tasks:
            task_id = scheduler.submit(
                {"name": name, "duration": duration},
                priority=priority
            )
            print(f"Submitted: {name} (priority={priority.name})")

        time.sleep(2)  # Let tasks execute

        # Section 3: Async submission
        print_section("3. Async Submission")

        async def async_demo():
            tasks = []
            for i in range(3):
                task_id = await scheduler.submit_async({"name": f"async_{i}", "duration": 0.2})
                tasks.append(task_id)
                print(f"Async submitted: {task_id}")

            for task_id in tasks:
                result = await scheduler.wait_for_result_async(task_id)
                print(f"Async result: {result}")

        asyncio.run(async_demo())

        # Section 4: Query tasks
        print_section("4. Task Query")
        stats = scheduler.get_stats()
        print(f"Statistics: {stats}")

        # Section 5: Events
        print_section("5. Event Callbacks")

        def on_complete(event):
            print(f"  [EVENT] Task {event.task_id} completed")

        scheduler.on_task_completed(on_complete)

        task_id = scheduler.submit({"name": "event_demo", "duration": 0.5})
        time.sleep(1)

        # Section 6: Cancel task
        print_section("6. Cancel Task")
        task_id = scheduler.submit({"name": "to_be_cancelled", "duration": 10})
        print(f"Submitted long task: {task_id}")

        time.sleep(0.5)
        cancelled = scheduler.cancel(task_id)
        print(f"Cancelled: {cancelled}")

        result = scheduler.get_result(task_id)
        print(f"Final status: {result['status'] if result else 'unknown'}")

        # Final stats
        print_section("Final Statistics")
        stats = scheduler.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        print("\nDemo completed successfully")

    except KeyboardInterrupt:
        print("\n\n Interrupted by user")
    finally:
        scheduler.shutdown()
        print("Shutdown complete")


if __name__ == "__main__":
    main()
