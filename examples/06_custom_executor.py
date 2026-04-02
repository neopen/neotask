"""
@FileName: 06_custom_executor.py
@Description: 自定义执行器。Custom executor example: Different executor types.
@Author: HiPeng
@Time: 2026/4/2 17:25
"""

import asyncio

from neotask import TaskScheduler, TaskExecutor, SchedulerConfig
from neotask.executors.async_executor import AsyncExecutor


# Option 1: Class-based executor
class MathExecutor(TaskExecutor):
    """Executor for mathematical operations."""

    async def execute(self, task_data: dict) -> dict:
        operation = task_data.get("op")
        a = task_data.get("a", 0)
        b = task_data.get("b", 0)

        if operation == "add":
            result = a + b
        elif operation == "multiply":
            result = a * b
        elif operation == "power":
            result = a ** b
        else:
            result = None

        return {"operation": operation, "result": result}


# Option 2: Function-based executor (using AsyncExecutor)
async def shell_executor(task_data: dict) -> dict:
    """Execute shell commands."""
    cmd = task_data.get("cmd")
    if not cmd:
        return {"error": "No command provided"}

    process = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()

    return {
        "cmd": cmd,
        "returncode": process.returncode,
        "stdout": stdout.decode(),
        "stderr": stderr.decode()
    }


def main():
    # Using class-based executor
    math_scheduler = TaskScheduler(
        executor=MathExecutor(),
        config=SchedulerConfig.memory()
    )

    # Using function-based executor
    shell_scheduler = TaskScheduler(
        executor=AsyncExecutor(shell_executor),
        config=SchedulerConfig.memory()
    )

    try:
        # Math tasks
        print("=== Math Executor ===")
        task_id = math_scheduler.submit({"op": "add", "a": 10, "b": 20})
        result = math_scheduler.wait_for_result(task_id)
        print(f"Result: {result}")

        task_id = math_scheduler.submit({"op": "power", "a": 2, "b": 8})
        result = math_scheduler.wait_for_result(task_id)
        print(f"Result: {result}")

        # Shell tasks
        print("\n=== Shell Executor ===")
        task_id = shell_scheduler.submit({"cmd": "echo 'Hello from shell'"})
        result = shell_scheduler.wait_for_result(task_id)
        print(f"Result: {result}")

    finally:
        math_scheduler.shutdown()
        shell_scheduler.shutdown()


if __name__ == "__main__":
    main()
