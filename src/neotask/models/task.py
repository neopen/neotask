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

    @classmethod
    def from_value(cls, value: int) -> "TaskPriority":
        """Create from integer value."""
        for priority in cls:
            if priority.value == value:
                return priority
        return cls.NORMAL


@dataclass
class Task:
    """Task entity representing a unit of work."""

    task_id: str
    data: Dict[str, Any]
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.NORMAL
    progress: float = 0.0  # 任务进度 (0.0 - 1.0)
    progress_message: str = ""  # 进度消息
    node_id: str = ""
    retry_count: int = 0
    ttl: int = 3600
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage - 修复 None 值问题"""
        return {
            "task_id": self.task_id,
            "data": json.dumps(self.data) if self.data is not None else "{}",
            "status": self.status.value,
            "priority": self.priority.value,
            "progress": self.progress,
            "progress_message": self.progress_message or "",
            "node_id": self.node_id or "",
            "retry_count": self.retry_count,
            "ttl": self.ttl,
            "created_at": self.created_at.isoformat() if self.created_at else datetime.now(timezone.utc).isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else "",
            "completed_at": self.completed_at.isoformat() if self.completed_at else "",
            "result": json.dumps(self.result) if self.result is not None else "",
            "error": self.error or "",
        }

    def update_progress(self, progress: float, message: str = "") -> None:
        """更新任务进度"""
        self.progress = min(1.0, max(0.0, progress))
        if message:
            self.progress_message = message


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """Create task from dictionary."""
        def parse_datetime(value: Optional[str]) -> Optional[datetime]:
            if not value:
                return None
            try:
                return datetime.fromisoformat(value)
            except (ValueError, TypeError):
                return None

        def parse_json(value: Optional[str]) -> Any:
            if not value:
                return None
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return None

        return cls(
            task_id=data.get("task_id", ""),
            data=parse_json(data.get("data")) or {},
            status=TaskStatus(data.get("status", "pending")),
            priority=TaskPriority(int(data.get("priority", 2))),
            node_id=data.get("node_id", ""),
            retry_count=int(data.get("retry_count", 0)),
            ttl=int(data.get("ttl", 3600)),
            created_at=parse_datetime(data.get("created_at")) or datetime.now(timezone.utc),
            started_at=parse_datetime(data.get("started_at")),
            completed_at=parse_datetime(data.get("completed_at")),
            result=parse_json(data.get("result")),
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

    def is_terminal(self) -> bool:
        """Check if task is in terminal state."""
        return self.status.is_terminal()

    def is_retriable(self) -> bool:
        """Check if task can be retried."""
        return self.status == TaskStatus.FAILED and self.retry_count < 3


class TaskStats:
    """Task statistics container."""

    def __init__(
        self,
        total: int = 0,
        pending: int = 0,
        running: int = 0,
        completed: int = 0,
        failed: int = 0,
        cancelled: int = 0,
    ):
        self.total = total
        self.pending = pending
        self.running = running
        self.completed = completed
        self.failed = failed
        self.cancelled = cancelled

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total_completed = self.completed + self.failed
        if total_completed == 0:
            return 1.0
        return self.completed / total_completed