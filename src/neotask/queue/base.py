"""
@FileName: base.py
@Description: 队列抽象基类
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple, Any, Dict


@dataclass
class QueueItem:
    """队列项"""
    task_id: str
    priority: int
    enqueued_at: datetime
    scheduled_at: Optional[datetime] = None


class BaseQueue(ABC):
    """队列抽象基类

    设计模式：Strategy Pattern - 定义队列操作接口
    """

    @abstractmethod
    async def push(self, task_id: str, priority: int, delay: float = 0) -> bool:
        """入队

        Args:
            task_id: 任务ID
            priority: 优先级（数字越小优先级越高）
            delay: 延迟执行时间（秒）

        Returns:
            是否成功入队
        """
        pass

    @abstractmethod
    async def pop(self, count: int = 1) -> List[str]:
        """出队

        Args:
            count: 弹出数量

        Returns:
            任务ID列表
        """
        pass

    @abstractmethod
    async def pop_with_priority(self, count: int = 1) -> List[Tuple[str, int]]:
        """出队并返回优先级

        Args:
            count: 弹出数量

        Returns:
            (task_id, priority) 列表
        """
        pass

    @abstractmethod
    async def remove(self, task_id: str) -> bool:
        """移除任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功移除
        """
        pass

    @abstractmethod
    async def size(self) -> int:
        """获取队列大小"""
        pass

    @abstractmethod
    async def peek(self, count: int = 1) -> List[str]:
        """查看队首任务

        Args:
            count: 查看数量

        Returns:
            任务ID列表
        """
        pass

    @abstractmethod
    async def clear(self) -> None:
        """清空队列"""
        pass

    @abstractmethod
    async def contains(self, task_id: str) -> bool:
        """检查任务是否在队列中

        Args:
            task_id: 任务ID

        Returns:
            是否存在
        """
        pass


class QueueStats(ABC):
    """队列统计接口"""

    @abstractmethod
    async def get_stats(self) -> Dict[str, Any]:
        """获取队列统计信息"""
        pass