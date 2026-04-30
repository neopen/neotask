"""
@FileName: test_executors.py
@Description: Executor module tests.
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import pytest
import asyncio
from neotask.executor import (
    AsyncExecutor, ThreadExecutor, ProcessExecutor,
    ClassExecutor, ExecutorFactory, ExecutorType
)
from neotask.executor.exceptions import ExecutionTimeoutError


@pytest.mark.asyncio
async def test_async_executor():
    async def my_task(data):
        return {"result": data["value"] * 2}

    executor = AsyncExecutor(my_task)
    result = await executor.execute({"value": 10})
    assert result == {"result": 20}


@pytest.mark.asyncio
async def test_async_executor_timeout():
    async def slow_task(data):
        await asyncio.sleep(2)
        return {"result": data["value"]}

    executor = AsyncExecutor(slow_task, timeout=0.5)

    with pytest.raises(ExecutionTimeoutError):
        await executor.execute({"value": 10})


@pytest.mark.asyncio
async def test_thread_executor():
    def sync_task(data):
        return {"result": data["value"] * 2}

    executor = ThreadExecutor(sync_task)
    result = await executor.execute({"value": 10})
    assert result == {"result": 20}


@pytest.mark.asyncio
async def test_process_executor():
    def cpu_task(data):
        # Simulate CPU-intensive work
        total = sum(range(data["value"]))
        return {"result": total}

    executor = ProcessExecutor(cpu_task)
    result = await executor.execute({"value": 1000})
    assert result["result"] == sum(range(1000))


@pytest.mark.asyncio
async def test_class_executor_async():
    class AsyncWorker:
        async def execute(self, data):
            return {"result": data["value"] * 2}

    executor = ClassExecutor(AsyncWorker())
    result = await executor.execute({"value": 10})
    assert result == {"result": 20}


@pytest.mark.asyncio
async def test_factory_auto_detect():
    async def async_func(data):
        return data

    def sync_func(data):
        return data

    class Worker:
        async def execute(self, data):
            return data

    # Auto-detect async function
    executor1 = ExecutorFactory.create(async_func)
    assert isinstance(executor1, AsyncExecutor)

    # Auto-detect sync function
    executor2 = ExecutorFactory.create(sync_func)
    assert isinstance(executor2, ThreadExecutor)

    # Auto-detect class instance
    executor3 = ExecutorFactory.create(Worker())
    assert isinstance(executor3, ClassExecutor)


@pytest.mark.asyncio
async def test_factory_manual_type():
    def sync_func(data):
        return data

    # Force create as process executor
    executor = ExecutorFactory.create(sync_func, executor_type=ExecutorType.PROCESS)
    assert isinstance(executor, ProcessExecutor)


@pytest.mark.asyncio
async def test_executor_shutdown():
    def sync_task(data):
        return data

    executor = ThreadExecutor(sync_task)
    await executor.execute({"test": "data"})
    await executor.shutdown()

    # Should be able to shutdown multiple times
    await executor.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
