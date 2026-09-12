"""
@FileName: test_webui.py
@Description: 
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 19:05
"""
"""Web UI manual test."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import time
import webbrowser
from neotask import TaskScheduler, TaskExecutor, SchedulerConfig


class DemoExecutor(TaskExecutor):
    """Demo executor for Web UI testing."""

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


import asyncio


def test_webui_embedded():
    """Test embedded Web UI."""
    print("\n" + "=" * 60)
    print("Test: Embedded Web UI")
    print("=" * 60)

    # Create config with Web UI enabled
    config = SchedulerConfig.with_webui(
        port=8080,
        auto_open=False
    )

    scheduler = TaskScheduler(
        executor=DemoExecutor(),
        config=config
    )

    try:
        print(f"Web UI: http://localhost:{config.webui.port}")
        print("\nSubmitting demo tasks...")

        # Submit various tasks
        tasks = [
            {"type": "quick", "duration": 0.5},
            {"type": "normal", "duration": 1},
            {"type": "slow", "duration": 2},
            {"type": "background", "duration": 0.8},
        ]

        for task_data in tasks:
            task_id = scheduler.submit(task_data)
            print(f"Submitted: {task_data['type']} -> {task_id}")

        print("\n" + "=" * 60)
        print("Web UI is running at http://localhost:8080")
        print("Press Enter to stop...")
        print("=" * 60)

        input()

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        scheduler.shutdown()
        print("✓ Web UI test completed")


def test_stats_endpoint():
    """Test stats API endpoint."""
    print("\n" + "=" * 60)
    print("Test: Stats API")
    print("=" * 60)

    import httpx

    config = SchedulerConfig.with_webui(port=8081, auto_open=False)
    scheduler = TaskScheduler(
        executor=DemoExecutor(),
        config=config
    )

    try:
        # Give server time to start
        time.sleep(1)

        # Test stats endpoint
        response = httpx.get("http://localhost:8081/api/stats")
        print(f"Stats response: {response.json()}")

        assert response.status_code == 200
        print("✓ Stats API test passed")

    except Exception as e:
        print(f"Error: {e}")
        print("Make sure Web UI is enabled")
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    import asyncio

    test_webui_embedded()
    # test_stats_endpoint()