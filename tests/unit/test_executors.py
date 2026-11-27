"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: test_executors.py
@Description: Executor module tests.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

import pytest
import asyncio
import threading
from neotask import TaskPool, TaskPoolConfig
from neotask.core.engine import EngineConfig
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


def test_task_pool_config_normalizes_executor_type_string():
    """字符串 executor_type 在配置入口归一化为枚举，避免下游身份比较失效"""
    assert TaskPoolConfig(executor_type="thread").executor_type is ExecutorType.THREAD
    assert TaskPoolConfig(executor_type="PROCESS").executor_type is ExecutorType.PROCESS


def test_task_pool_config_rejects_unknown_executor_type():
    """未知字符串在配置构造期即报错，而不是延迟到创建执行器时"""
    with pytest.raises(ValueError):
        TaskPoolConfig(executor_type="bogus")


def test_task_pool_config_normalizes_executor_type_assignment():
    """构造后再赋字符串同样归一化，非法值在赋值处即报错"""
    config = TaskPoolConfig(storage_type="memory")
    config.executor_type = "thread"
    assert config.executor_type is ExecutorType.THREAD
    config.executor_type = "PROCESS"
    assert config.executor_type is ExecutorType.PROCESS

    with pytest.raises(ValueError):
        config.executor_type = "bogus"


def test_task_pool_honors_late_executor_type_assignment():
    """后置赋值 "thread" 也必须落到线程执行器，而不是被包装成协程"""
    def sync_func(data):
        return data

    config = TaskPoolConfig(storage_type="memory")
    config.executor_type = "thread"
    pool = TaskPool(executor=sync_func, config=config)
    assert isinstance(pool._executor, ThreadExecutor)


def test_engine_config_normalizes_executor_type_assignment():
    """EngineConfig 与 TaskPoolConfig 口径一致"""
    config = EngineConfig()
    config.executor_type = "thread"
    assert config.executor_type is ExecutorType.THREAD

    with pytest.raises(ValueError):
        config.executor_type = "bogus"


@pytest.mark.parametrize(
    "executor_type, expected_type",
    [
        ("thread", ThreadExecutor),
        ("process", ProcessExecutor),
        (ExecutorType.THREAD, ThreadExecutor),
    ],
)
def test_task_pool_honors_explicit_executor_type(executor_type, expected_type):
    """显式 thread/process 时同步函数应交给对应执行器，而不是包装成协程"""
    def sync_func(data):
        return data

    pool = TaskPool(
        executor=sync_func,
        config=TaskPoolConfig(storage_type="memory", executor_type=executor_type),
    )
    assert isinstance(pool._executor, expected_type)


def test_task_pool_default_async_keeps_sync_function_inline():
    """默认 async 模式保持原行为：同步函数包装后在事件循环内执行"""
    def sync_func(data):
        return data

    pool = TaskPool(
        executor=sync_func,
        config=TaskPoolConfig(storage_type="memory"),
    )
    assert isinstance(pool._executor, AsyncExecutor)


def test_task_pool_honors_class_executor_for_object():
    """显式 class 时带 execute() 的对象不应被包装成协程函数"""
    class Worker:
        async def execute(self, data):
            return data

    pool = TaskPool(
        executor=Worker(),
        config=TaskPoolConfig(storage_type="memory", executor_type="class"),
    )
    assert isinstance(pool._executor, ClassExecutor)


def test_task_pool_thread_executor_runs_off_event_loop():
    """thread 模式下任务实际运行在 worker 线程，而非 TaskPool 事件循环线程"""
    def sync_func(data):
        return {"thread": threading.current_thread().name}

    pool = TaskPool(
        executor=sync_func,
        config=TaskPoolConfig(storage_type="memory", executor_type="thread"),
    )
    pool.start()
    try:
        task_id = pool.submit({})
        result = pool.wait_for_result(task_id, timeout=10)
        assert result["thread"].startswith("neotask-worker")
    finally:
        pool.shutdown()


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
