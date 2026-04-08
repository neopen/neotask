"""
@FileName: task.py
@Description: 数据模型
@Author: HiPeng
@Time: 2026/4/1 18:23
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
import json


class TaskStatus(Enum):
    """Task status enumeration."""

    PENDING = "pending"
    # PROCESSING = "processing"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SCHEDULED = "scheduled"

    def is_terminal(self) -> bool:
        """Check if status is terminal (no further changes)."""
        return self in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED)


class TaskPriority(Enum):
    """Task priority enumeration."""

    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class Task:
    """Task entity representing a unit of work."""

    task_id: str
    data: Dict[str, Any]
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.NORMAL
    node_id: str = ""
    retry_count: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "task_id": self.task_id,
            "data": json.dumps(self.data),
            "status": self.status.value,
            "priority": self.priority.value,
            "node_id": self.node_id,
            "retry_count": self.retry_count,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": json.dumps(self.result) if self.result else None,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """Create task from dictionary."""

        def parse_datetime(value: Optional[str]) -> Optional[datetime]:
            if not value:
                return None
            return datetime.fromisoformat(value)

        return cls(
            task_id=data["task_id"],
            data=json.loads(data["data"]),
            status=TaskStatus(data["status"]),
            priority=TaskPriority(int(data["priority"])),
            node_id=data.get("node_id", ""),
            retry_count=int(data.get("retry_count", 0)),
            created_at=parse_datetime(data["created_at"]) or datetime.now(timezone.utc),
            started_at=parse_datetime(data.get("started_at")),
            completed_at=parse_datetime(data.get("completed_at")),
            result=json.loads(data["result"]) if data.get("result") else None,
            error=data.get("error"),
        )

    def start(self, node_id: str) -> None:
        """Mark task as started."""
        self.status = TaskStatus.RUNNING
        self.node_id = node_id
        self.started_at = datetime.now(timezone.utc)

    def complete(self, result: Dict[str, Any]) -> None:
        """Mark task as completed successfully."""
        self.status = TaskStatus.SUCCESS
        self.result = result
        self.completed_at = datetime.now(timezone.utc)

    def fail(self, error: str) -> None:
        """Mark task as failed."""
        self.status = TaskStatus.FAILED
        self.error = error
        self.completed_at = datetime.now(timezone.utc)

    def cancel(self) -> None:
        """Mark task as cancelled."""
        self.status = TaskStatus.CANCELLED
        self.completed_at = datetime.now(timezone.utc)