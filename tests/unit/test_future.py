"""
@FileName: test_future.py
@Description: 
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 19:04
"""
"""Unit tests for TaskFuture and FutureManager."""

import pytest
import asyncio
from neotask.core.future import TaskFuture, FutureManager
from neotask.common.exceptions import TimeoutError


class TestTaskFuture:
    """Test TaskFuture class."""

    @pytest.mark.asyncio
    async def test_wait_for_result(self):
        """Test waiting for task result."""
        future = TaskFuture("test-001")

        # Simulate completion after short delay
        async def complete():
            await asyncio.sleep(0.1)
            future.set_result({"data": "success"})

        asyncio.create_task(complete())
        result = await future.wait(timeout=1)

        assert result == {"data": "success"}

    @pytest.mark.asyncio
    async def test_wait_for_error(self):
        """Test waiting for task error."""
        future = TaskFuture("test-002")

        async def fail():
            await asyncio.sleep(0.1)
            future.set_error("Task failed")

        asyncio.create_task(fail())

        with pytest.raises(Exception, match="Task failed"):
            await future.wait(timeout=1)

    @pytest.mark.asyncio
    async def test_timeout(self):
        """Test wait timeout."""
        future = TaskFuture("test-003")

        with pytest.raises(TimeoutError):
            await future.wait(timeout=0.1)

    def test_is_completed(self):
        """Test completed flag."""
        future = TaskFuture("test-004")
        assert future.is_completed is False

        future.set_result({})
        assert future.is_completed is True


class TestFutureManager:
    """Test FutureManager class."""

    @pytest.mark.asyncio
    async def test_create_and_get(self):
        """Test creating and getting future."""
        manager = FutureManager()

        future = await manager.create("task-001")
        assert future.task_id == "task-001"

        retrieved = await manager.get("task-001")
        assert retrieved is future

    @pytest.mark.asyncio
    async def test_get_creates_if_not_exists(self):
        """Test get creates future if not exists."""
        manager = FutureManager()

        future = await manager.get("task-002")
        assert future is not None
        assert future.task_id == "task-002"

    @pytest.mark.asyncio
    async def test_complete_with_result(self):
        """Test completing future with result."""
        manager = FutureManager()

        future = await manager.create("task-003")
        await manager.complete("task-003", result={"data": "done"})

        assert future.is_completed is True

    @pytest.mark.asyncio
    async def test_complete_with_error(self):
        """Test completing future with error."""
        manager = FutureManager()

        future = await manager.create("task-004")
        await manager.complete("task-004", error="Something wrong")

        assert future.is_completed is True