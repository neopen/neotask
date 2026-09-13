"""Verify neotask installation and basic functionality."""
import sys


def check_version():
    import neotask
    print(f"neotask version: {neotask.__version__}")
    return True


def check_imports():
    from neotask import (
        TaskPool, TaskPoolConfig,
        TaskScheduler, SchedulerConfig,
        EventBus, TaskEvent,
    )
    print("All core imports: OK")
    return True


def check_quick_task():
    import asyncio

    async def test():
        async def dummy(data):
            return data

        pool = TaskPool(executor=dummy)
        task_id = pool.submit({"test": True})
        result = pool.wait_for_result(task_id)
        assert result == {"test": True}, f"Unexpected result: {result}"
        pool.shutdown()
        print("Quick task test: OK")

    asyncio.run(test())
    return True


def main():
    checks = [
        ("Version", check_version),
        ("Imports", check_imports),
        ("Quick task", check_quick_task),
    ]
    failures = 0
    for name, check in checks:
        try:
            check()
        except Exception as e:
            print(f"{name}: FAILED — {e}")
            failures += 1
    if failures:
        print(f"\n{failures} check(s) failed.")
        sys.exit(1)
    print("\nAll checks passed.")


if __name__ == "__main__":
    main()
