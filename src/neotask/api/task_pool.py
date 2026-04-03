"""
@FileName: task_pool.py
@Description: Function Call接口（TaskPool - 即时任务入口，专注于立即执行的任务。）
@Author: HiPeng
@Time: 2026/4/1 18:27
"""
import uuid
from datetime import datetime, timezone

from neotask.core.future import FutureManager
from neotask.core.task_manager import TaskManager
from neotask.core.worker import WorkerPool
from neotask.executors.base import TaskExecutor
from neotask.monitor.event_bus import EventBus


class TaskPool:
    """即时任务池。"""

    def __init__(self, executor: TaskExecutor, **kwargs):
        # 创建统一任务管理器
        self._manager = TaskManager(
            storage_type=kwargs.get("storage_type", "memory"),
            sqlite_path=kwargs.get("sqlite_path", "neotask.db"),
            redis_url=kwargs.get("redis_url"),
            node_id=kwargs.get("node_id"),
            lock_type=kwargs.get("lock_type", "memory"),
        )

        self._executor = executor
        self._future_manager = FutureManager()
        self._event_bus = EventBus()

        # 创建 Worker 池
        self._worker_pool = WorkerPool(
            manager=self._manager,
            executor=self._executor,
            future_manager=self._future_manager,
            event_bus=self._event_bus,
            max_concurrent=kwargs.get("max_concurrent", 10),
        )

        # 启动 Worker
        self._worker_pool.start()

    def submit(self, data: dict, priority: int = 2) -> str:
        """提交任务。"""
        task_id = self._manager.create_task(data, priority=priority)
        self._manager.enqueue(task_id, priority)
        return task_id

    def wait_for_result(self, task_id: str, timeout: float = 300) -> dict:
        """等待任务完成。"""
        future = self._future_manager.get(task_id)
        return future.wait(timeout)

    def get_status(self, task_id: str) -> str:
        """获取任务状态。"""
        task = self._manager.get_task(task_id)
        return task.status.value if task else "not_found"

    def get_result(self, task_id: str) -> dict:
        """获取任务结果。"""
        task = self._manager.get_task(task_id)
        if not task:
            return {"error": "task not found"}
        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "result": task.result,
            "error": task.error,
        }

    def cancel(self, task_id: str) -> bool:
        """取消任务。"""
        # 从队列移除
        self._manager.remove_from_queue(task_id)
        # 取消任务
        return self._manager.cancel_task(task_id)

    def get_stats(self) -> dict:
        """获取统计信息。"""
        stats = self._manager.get_stats()
        return {
            "queue_size": self._manager.get_queue_size(),
            "pending": stats.pending,
            "processing": stats.processing,
            "completed": stats.completed,
            "failed": stats.failed,
            "cancelled": stats.cancelled,
        }

    def shutdown(self):
        """关闭任务池。"""
        self._worker_pool.stop()
        self._manager.shutdown()