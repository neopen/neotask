"""
@FileName: factory.py
@Description: 队列工厂 - Factory Pattern
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

from typing import Optional
from neotask.queue.scheduler import QueueScheduler
from neotask.storage.base import QueueRepository


class QueueFactory:
    """队列工厂

    设计模式：Factory Pattern - 根据配置创建队列实例
    """

    @staticmethod
    def create(
        repository: Optional[QueueRepository] = None,
        max_size: int = 10000,
        queue_type: str = "scheduler"
    ) -> QueueScheduler:
        """创建队列实例

        Args:
            repository: 队列存储仓库
            max_size: 最大队列大小
            queue_type: 队列类型

        Returns:
            队列调度器实例
        """
        if queue_type == "scheduler":
            return QueueScheduler(repository, max_size)
        else:
            raise ValueError(f"Unknown queue type: {queue_type}")

    @staticmethod
    def create_memory(max_size: int = 10000) -> QueueScheduler:
        """创建内存队列"""
        return QueueScheduler(None, max_size)

    @staticmethod
    def create_persistent(
        repository: QueueRepository,
        max_size: int = 10000
    ) -> QueueScheduler:
        """创建持久化队列"""
        return QueueScheduler(repository, max_size)