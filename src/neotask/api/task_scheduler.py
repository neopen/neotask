"""
@FileName: task_scheduler.py
@Description: TaskScheduler - 定时任务入口，专注于延时/周期任务。
@Author: HiPeng
@Time: 2026/4/1 18:16
"""
from datetime import datetime
from typing import Optional, Any, Dict, Callable, Union

from neotask.api.task_pool import TaskPool
from neotask.executors.base import TaskExecutor
from neotask.models.config import SchedulerConfig
from neotask.models.task import TaskPriority


class TaskScheduler:
    """定时任务调度器 - 管理延时任务和周期任务。

    职责：
    - 延时执行（delay_seconds / execute_at）
    - 周期执行（interval / cron）
    - 定时任务的管理（取消、暂停、恢复）

    使用示例：
        >>> class MyExecutor(TaskExecutor): pass
        >>> scheduler = TaskScheduler(executor=MyExecutor())
        >>>
        >>> # 延时执行
        >>> scheduler.submit_delayed({"data": "value"}, delay_seconds=60)
        >>>
        >>> # 周期执行
        >>> scheduler.submit_interval({"data": "value"}, interval_seconds=300)
        >>>
        >>> # Cron 表达式
        >>> scheduler.submit_cron({"data": "value"}, "0 9 * * *")
    """

    def __init__(self, executor: TaskExecutor, config: Optional[SchedulerConfig] = None):
        self._config = config or SchedulerConfig()
        self._executor = executor

        # TODO: 实现定时任务存储和调度逻辑
        # 当前版本使用 TaskPool 作为底层，后续扩展定时能力

        # 临时：使用即时任务池作为底层
        self._pool = TaskPool(executor, config)

    # ========== 延时任务 ==========

    def submit_delayed(self, data: Dict[str, Any],
                       delay_seconds: int,
                       priority: Union[int, TaskPriority] = TaskPriority.NORMAL) -> str:
        """延时执行任务。

        Args:
            data: 任务数据
            delay_seconds: 延迟秒数
            priority: 优先级

        Returns:
            task_id
        """
        # TODO: 实现真正的延时任务
        # 当前版本直接执行
        return self._pool.submit(data, priority)

    def submit_at(self, data: Dict[str, Any],
                  execute_at: datetime,
                  priority: Union[int, TaskPriority] = TaskPriority.NORMAL) -> str:
        """指定时间点执行任务。

        Args:
            data: 任务数据
            execute_at: 执行时间点
            priority: 优先级

        Returns:
            task_id
        """
        delay_seconds = max(0.0, (execute_at - datetime.now()).total_seconds())
        return self.submit_delayed(data, int(delay_seconds), priority)

    # ========== 周期任务 ==========

    def submit_interval(self, data: Dict[str, Any],
                        interval_seconds: int,
                        priority: Union[int, TaskPriority] = TaskPriority.NORMAL) -> str:
        """按固定间隔周期执行任务。

        Args:
            data: 任务数据
            interval_seconds: 执行间隔（秒）
            priority: 优先级

        Returns:
            task_id
        """
        # TODO: 实现周期任务
        raise NotImplementedError("Periodic tasks will be available in v0.2.0")

    def submit_cron(self, data: Dict[str, Any],
                    cron_expr: str,
                    priority: Union[int, TaskPriority] = TaskPriority.NORMAL) -> str:
        """按 Cron 表达式周期执行任务。

        Args:
            data: 任务数据
            cron_expr: Cron 表达式，如 "0 9 * * *" 表示每天9点
            priority: 优先级

        Returns:
            task_id
        """
        # TODO: 实现 Cron 周期任务
        raise NotImplementedError("Cron tasks will be available in v0.2.0")

    # ========== 定时任务管理 ==========

    def cancel_periodic(self, task_id: str) -> bool:
        """取消周期任务。"""
        # TODO: 实现周期任务取消
        raise NotImplementedError("Periodic task cancellation will be available in v0.2.0")

    def pause_periodic(self, task_id: str) -> bool:
        """暂停周期任务。"""
        # TODO: 实现周期任务暂停
        raise NotImplementedError("Periodic task pause will be available in v0.2.0")

    def resume_periodic(self, task_id: str) -> bool:
        """恢复周期任务。"""
        # TODO: 实现周期任务恢复
        raise NotImplementedError("Periodic task resume will be available in v0.2.0")

    # ========== 委托给 TaskPool 的方法 ==========

    def wait_for_result(self, task_id: str, timeout: float = 300) -> Dict[str, Any]:
        """等待任务完成。"""
        return self._pool.wait_for_result(task_id, timeout)

    async def wait_for_result_async(self, task_id: str, timeout: float = 300) -> Dict[str, Any]:
        """异步等待任务完成。"""
        return await self._pool.wait_for_result_async(task_id, timeout)

    def get_status(self, task_id: str) -> Optional[str]:
        """获取任务状态。"""
        return self._pool.get_status(task_id)

    def get_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果。"""
        return self._pool.get_result(task_id)

    def cancel(self, task_id: str) -> bool:
        """取消任务。"""
        return self._pool.cancel(task_id)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息。"""
        return self._pool.get_stats()

    def shutdown(self) -> None:
        """关闭调度器。"""
        self._pool.shutdown()

    # ========== 事件回调 ==========

    def on_submitted(self, handler: Callable) -> None:
        self._pool.on_submitted(handler)

    def on_started(self, handler: Callable) -> None:
        self._pool.on_started(handler)

    def on_completed(self, handler: Callable) -> None:
        self._pool.on_completed(handler)

    def on_failed(self, handler: Callable) -> None:
        self._pool.on_failed(handler)
