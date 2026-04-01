"""
@FileName: base_executor.py
@Description: 执行器接口
@Author: HiPeng
@Time: 2026/3/27 23:52
"""
from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseExecutor(ABC):
    """任务执行器抽象基类

    用户需要继承此类并实现 execute 方法来定义具体的业务逻辑。
    """

    @abstractmethod
    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行任务逻辑

        Args:
            task_data: 任务数据字典

        Returns:
            Dict[str, Any]: 执行结果

        Raises:
            Exception: 执行过程中的任何异常
        """
        pass

    async def prepare(self) -> None:
        """执行器初始化前的准备工作

        子类可以重写此方法来执行一些初始化操作，
        如建立数据库连接、加载模型等。
        """
        pass

    async def cleanup(self) -> None:
        """执行器清理工作

        子类可以重写此方法来执行清理操作，
        如关闭数据库连接、释放资源等。
        """
        pass
