"""
@FileName: base.py
@Description: 执行器接口
@Author: HiPeng
@Time: 2026/3/27 23:52
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class TaskExecutor(ABC):
    """Abstract task executor."""

    @abstractmethod
    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute task and return result."""
        pass
