"""
@FileName: test_basic.py
@Description: 
@Author: HiPeng
@Time: 2026/4/2 19:05
"""
"""Basic functionality manual test."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import time
from neotask import TaskScheduler, TaskExecutor, SchedulerConfig


class SimpleExecutor(TaskExecutor):
    """Simple test executor."""

    async def execute(self, task_data: dict) -> dict:
        print(f"  Executing: {task_data}")
        return {"result": task_data.get("value", 0) * 2}


def test_basic_submit():
    """Test basic task submission and waiting."""
    print("\n" + "=" * 60)
    print("Test 1: Basic Task Submission")
    print("=" * 60)

    scheduler = TaskScheduler(
        executor=SimpleExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        # Submit task
        task_id = scheduler.submit({"value": 42})
        print(f"Submitted: {task_id}")

        # Wait for result
        result = scheduler.wait_for_result(task_id)
        print(f"Result: {result}")

        # Verify
        assert result == {"result": 84}
        print("Test passed")

    finally:
        scheduler.shutdown()


def test_multiple_tasks():
    """Test multiple task submissions."""
    print("\n" + "=" * 60)
    print("Test 2: Multiple Tasks")
    print("=" * 60)

    scheduler = TaskScheduler(
        executor=SimpleExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        task_ids = []
        for i in range(5):
            task_id = scheduler.submit({"value": i})
            task_ids.append(task_id)
            print(f"Submitted: {task_id}")

        # Wait for all
        for task_id in task_ids:
            result = scheduler.wait_for_result(task_id)
            print(f"Result for {task_id}: {result}")

        print("Test passed")

    finally:
        scheduler.shutdown()


def test_task_status():
    """Test task status query."""
    print("\n" + "=" * 60)
    print("Test 3: Task Status Query")
    print("=" * 60)

    scheduler = TaskScheduler(
        executor=SimpleExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        task_id = scheduler.submit({"value": 100})
        print(f"Submitted: {task_id}")

        # Check status
        status = scheduler.get_status(task_id)
        print(f"Status: {status}")

        # Wait and check again
        result = scheduler.wait_for_result(task_id)
        print(f"Result: {result}")

        status = scheduler.get_status(task_id)
        print(f"Final status: {status}")

        assert status == "success"
        print("Test passed")

    finally:
        scheduler.shutdown()


def test_get_result():
    """Test getting task result."""
    print("\n" + "=" * 60)
    print("Test 4: Get Task Result")
    print("=" * 60)

    scheduler = TaskScheduler(
        executor=SimpleExecutor(),
        config=SchedulerConfig.memory()
    )

    try:
        task_id = scheduler.submit({"value": 50})
        print(f"Submitted: {task_id}")

        # Wait for completion
        scheduler.wait_for_result(task_id)

        # Get result
        result = scheduler.get_result(task_id)
        print(f"Retrieved result: {result}")

        assert result["result"]["result"] == 100
        assert result["status"] == "success"
        print("Test passed")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    test_basic_submit()
    test_multiple_tasks()
    test_task_status()
    test_get_result()
    print("\n" + "=" * 60)
    print("All basic tests passed!")
    print("=" * 60)