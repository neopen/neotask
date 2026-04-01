"""
@FileName: future.py
@Description: 
@Author: HiPeng
@Time: 2026/4/1 17:41
"""
import asyncio
from typing import Optional

from neotask.models import TaskResponse


class TaskFuture:
    """任务 Future 对象，用于异步等待任务结果"""

    def __init__(self, task_id: str):
        self.task_id = task_id
        self._future = asyncio.Future()
        self._result: Optional[TaskResponse] = None

    def set_result(self, result: TaskResponse) -> None:
        """设置任务结果"""
        self._result = result
        if not self._future.done():
            self._future.set_result(result)

    def set_exception(self, exception: Exception) -> None:
        """设置异常"""
        if not self._future.done():
            self._future.set_exception(exception)

    async def wait_for_result(self, timeout: Optional[float] = None) -> TaskResponse:
        """等待任务结果"""
        try:
            return await asyncio.wait_for(self._future, timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Task {self.task_id} timeout after {timeout} seconds")

    @property
    def result(self) -> Optional[TaskResponse]:
        """获取结果（如果已完成）"""
        return self._result

    @property
    def done(self) -> bool:
        """是否已完成"""
        return self._future.done()

    @property
    def cancelled(self) -> bool:
        """是否已取消"""
        return self._future.cancelled()

    def cancel(self) -> bool:
        """取消等待"""
        return self._future.cancel()
