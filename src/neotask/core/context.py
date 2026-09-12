"""
@FileName: context.py
@Description: 执行上下文 - 任务执行的上下文信息
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from datetime import datetime
from contextvars import ContextVar
import uuid


@dataclass
class TaskContext:
    """任务执行上下文

    包含任务执行过程中的上下文信息，支持链路追踪。
    """
    task_id: str
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(cls, task_id: str, parent_context: Optional['TaskContext'] = None) -> 'TaskContext':
        """创建新的任务上下文"""
        trace_id = parent_context.trace_id if parent_context else str(uuid.uuid4())
        parent_span_id = parent_context.span_id if parent_context else None

        return cls(
            task_id=task_id,
            trace_id=trace_id,
            span_id=str(uuid.uuid4()),
            parent_span_id=parent_span_id
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "task_id": self.task_id,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "created_at": self.created_at.isoformat(),
            "metadata": self.metadata
        }

    def with_metadata(self, key: str, value: Any) -> 'TaskContext':
        """添加元数据"""
        self.metadata[key] = value
        return self


# ContextVar for propagating context across async boundaries
_current_context: ContextVar[Optional[TaskContext]] = ContextVar(
    "current_task_context", default=None
)


def get_current_context() -> Optional[TaskContext]:
    """获取当前任务上下文"""
    return _current_context.get()


def set_current_context(context: TaskContext) -> None:
    """设置当前任务上下文"""
    _current_context.set(context)