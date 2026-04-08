"""
@FileName: test_task.py
@Description: 
@Author: HiPeng
@Time: 2026/4/2 18:56
"""
"""Unit tests for Task model."""

import pytest
from datetime import datetime, timezone
from neotask.models.task import Task, TaskStatus, TaskPriority


class TestTask:
    """Test Task model."""

    def test_create_task(self):
        """Test creating a task."""
        task = Task(
            task_id="test-001",
            data={"key": "value"}
        )

        assert task.task_id == "test-001"
        assert task.data == {"key": "value"}
        assert task.status == TaskStatus.PENDING
        assert task.priority == TaskPriority.NORMAL
        assert task.retry_count == 0
        assert task.node_id == ""

    def test_task_with_priority(self):
        """Test task with custom priority."""
        task = Task(
            task_id="test-002",
            data={"key": "value"},
            priority=TaskPriority.HIGH
        )

        assert task.priority == TaskPriority.HIGH

    def test_task_start(self):
        """Test marking task as started."""
        task = Task(task_id="test-003", data={})
        task.start("node-1")

        assert task.status == TaskStatus.RUNNING
        assert task.node_id == "node-1"
        assert task.started_at is not None

    def test_task_complete(self):
        """Test marking task as completed."""
        task = Task(task_id="test-004", data={})
        task.start("node-1")
        task.complete({"result": "success"})

        assert task.status == TaskStatus.SUCCESS
        assert task.result == {"result": "success"}
        assert task.completed_at is not None

    def test_task_fail(self):
        """Test marking task as failed."""
        task = Task(task_id="test-005", data={})
        task.start("node-1")
        task.fail("Something went wrong")

        assert task.status == TaskStatus.FAILED
        assert task.error == "Something went wrong"
        assert task.completed_at is not None

    def test_task_cancel(self):
        """Test cancelling a task."""
        task = Task(task_id="test-006", data={})
        task.cancel()

        assert task.status == TaskStatus.CANCELLED

    def test_to_dict(self):
        """Test converting task to dictionary."""
        task = Task(
            task_id="test-007",
            data={"test": True},
            priority=TaskPriority.HIGH
        )

        data = task.to_dict()

        assert data["task_id"] == "test-007"
        assert data["priority"] == "1"  # HIGH value
        assert "data" in data

    def test_from_dict(self):
        """Test creating task from dictionary."""
        original = Task(
            task_id="test-008",
            data={"test": True},
            priority=TaskPriority.HIGH
        )

        data = original.to_dict()
        restored = Task.from_dict(data)

        assert restored.task_id == original.task_id
        assert restored.data == original.data
        assert restored.priority == original.priority
        assert restored.status == original.status


class TestTaskStatus:
    """Test TaskStatus enum."""

    def test_is_terminal(self):
        """Test terminal status check."""
        assert TaskStatus.SUCCESS.is_terminal() is True
        assert TaskStatus.FAILED.is_terminal() is True
        assert TaskStatus.CANCELLED.is_terminal() is True
        assert TaskStatus.PENDING.is_terminal() is False
        assert TaskStatus.RUNNING.is_terminal() is False
        assert TaskStatus.SCHEDULED.is_terminal() is False


class TestTaskPriority:
    """Test TaskPriority enum."""

    def test_priority_values(self):
        """Test priority values."""
        assert TaskPriority.CRITICAL.value == 0
        assert TaskPriority.HIGH.value == 1
        assert TaskPriority.NORMAL.value == 2
        assert TaskPriority.LOW.value == 3