"""
@FileName: task_scheduler.py
@Description: TaskScheduler - 定时任务入口，专注于延时/周期任务。
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import random
import threading
from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List, Callable, Union

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.common.exceptions import TaskAlreadyExistsError
from neotask.models.config import SchedulerConfig
from neotask.models.schedule import PeriodicTask
from neotask.models.task import TaskPriority, TaskStatus


class TaskScheduler:
    """定时任务调度器"""

    def __init__(
            self,
            executor: Optional[Union[Callable]] = None,
            config: Optional[SchedulerConfig] = None
    ):
        self._config = config or SchedulerConfig()

        pool_config = TaskPoolConfig(
            storage_type=self._config.storage_type,
            sqlite_path=self._config.sqlite_path,
            redis_url=self._config.redis_url,
            worker_concurrency=self._config.worker_concurrency,
            max_retries=self._config.max_retries,
            retry_delay=self._config.retry_delay
        )

        self._pool = TaskPool(executor, pool_config)
        self._periodic_tasks: Dict[str, PeriodicTask] = {}
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None

    # ========== 生命周期 ==========

    def start(self) -> None:
        """启动调度器"""
        self._pool.start()

        if self._running:
            return

        self._running = True

        def run_scheduler():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._scheduler_loop())
            finally:
                loop.close()

        self._scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self._scheduler_thread.start()

    def shutdown(self, graceful: bool = True, timeout: float = 30) -> None:
        """关闭调度器"""
        self._running = False

        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=5)

        self._pool.shutdown(graceful, timeout)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    # ========== 延时任务 API ==========

    def submit_delayed(
            self,
            data: Dict[str, Any],
            delay_seconds: float,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            ttl: int = 3600
    ) -> str:
        return self._pool.submit(data, task_id, priority, delay_seconds, ttl)

    async def submit_delayed_async(
            self,
            data: Dict[str, Any],
            delay_seconds: float,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            ttl: int = 3600
    ) -> str:
        return await self._pool.submit_async(data, task_id, priority, delay_seconds, ttl)

    def submit_at(
            self,
            data: Dict[str, Any],
            execute_at: datetime,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            ttl: int = 3600
    ) -> str:
        delay_seconds = max(0.0, (execute_at - datetime.now()).total_seconds())
        return self.submit_delayed(data, delay_seconds, task_id, priority, ttl)

    # ========== 周期任务 API ==========

    def submit_interval(
            self,
            data: Dict[str, Any],
            interval_seconds: float,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            run_immediately: bool = True
    ) -> str:
        task_id = task_id or self._generate_task_id()
        priority_value = priority.value if isinstance(priority, TaskPriority) else priority

        now = datetime.now()
        next_run = now if run_immediately else now + timedelta(seconds=interval_seconds)

        periodic_task = PeriodicTask(
            task_id=task_id,
            data=data,
            interval_seconds=interval_seconds,
            priority=priority_value,
            next_run=next_run
        )

        self._periodic_tasks[task_id] = periodic_task

        if run_immediately:
            self._pool.submit(data, task_id, priority)

        return task_id

    def submit_cron(
            self,
            data: Dict[str, Any],
            cron_expr: str,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL
    ) -> str:
        next_run = self._parse_cron(cron_expr)
        task_id = task_id or self._generate_task_id()
        priority_value = priority.value if isinstance(priority, TaskPriority) else priority

        periodic_task = PeriodicTask(
            task_id=task_id,
            data=data,
            interval_seconds=0,
            priority=priority_value,
            cron_expr=cron_expr,
            next_run=next_run
        )

        self._periodic_tasks[task_id] = periodic_task
        return task_id

    def _parse_cron(self, cron_expr: str) -> datetime:
        parts = cron_expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expr}")
        return datetime.now() + timedelta(seconds=3)

    def cancel_periodic(self, task_id: str) -> bool:
        if task_id in self._periodic_tasks:
            del self._periodic_tasks[task_id]
            return True
        return False

    def pause_periodic(self, task_id: str) -> bool:
        if task_id in self._periodic_tasks:
            self._periodic_tasks[task_id].is_paused = True
            return True
        return False

    def resume_periodic(self, task_id: str) -> bool:
        if task_id in self._periodic_tasks:
            task = self._periodic_tasks[task_id]
            task.is_paused = False
            task.next_run = datetime.now()
            return True
        return False

    def get_periodic_tasks(self) -> List[Dict[str, Any]]:
        return [
            {
                "task_id": task.task_id,
                "interval_seconds": task.interval_seconds,
                "cron_expr": task.cron_expr,
                "priority": task.priority,
                "next_run": task.next_run.isoformat() if task.next_run else None,
                "last_run": task.last_run.isoformat() if task.last_run else None,
                "run_count": task.run_count,
                "is_paused": task.is_paused,
                "created_at": task.created_at.isoformat()
            }
            for task in self._periodic_tasks.values()
        ]

    # ========== 调度循环 ==========

    async def _reschedule_periodic_task(self, task_id: str, priority: int) -> bool:
        """重新调度周期任务 - 重置状态并重新入队"""
        try:
            # 通过 _pool._lifecycle 获取任务
            task = await self._pool._lifecycle.get_task(task_id)
            if task is None:
                return False

            # 重置任务状态为 PENDING
            task.status = TaskStatus.PENDING
            task.started_at = None
            task.completed_at = None
            task.result = None
            task.error = None

            # 保存更新后的任务
            await self._pool._task_repo.save(task)

            # 重新入队
            await self._pool._queue_scheduler.push(task_id, priority)
            return True
        except Exception as e:
            return False

    async def _scheduler_loop(self) -> None:
        """调度循环 - 处理周期任务"""
        while self._running:
            try:
                now = datetime.now()
                tasks_to_run = []

                # 收集需要执行的任务
                for task_id, periodic_task in list(self._periodic_tasks.items()):
                    if periodic_task.is_paused:
                        continue

                    if periodic_task.next_run and periodic_task.next_run <= now:
                        tasks_to_run.append((task_id, periodic_task))

                # 执行任务
                for task_id, periodic_task in tasks_to_run:
                    # 更新计数和上次执行时间
                    periodic_task.run_count += 1
                    periodic_task.last_run = now

                    # 计算下次执行时间
                    if periodic_task.cron_expr:
                        periodic_task.next_run = self._parse_cron(periodic_task.cron_expr)
                    else:
                        periodic_task.next_run = now + timedelta(
                            seconds=periodic_task.interval_seconds
                        )

                    # 重新调度任务（重置状态并重新入队）
                    success = await self._reschedule_periodic_task(task_id, periodic_task.priority)

                    if not success:
                        # 如果重新调度失败，尝试创建新任务
                        try:
                            self._pool.submit(periodic_task.data, task_id, periodic_task.priority)
                        except TaskAlreadyExistsError:
                            # 任务已存在，尝试使用 redispatch
                            await self._pool._dispatcher.redispatch(task_id)

                await asyncio.sleep(self._config.scan_interval)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self._config.scan_interval)

    # ========== 辅助方法 ==========

    def _generate_task_id(self) -> str:
        # return f"PRD{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6]}"
        return f"PRD{datetime.now().strftime('%Y%m%d%H%M%S')}{random.randint(1000, 9999)}"

    # ========== 委托给 TaskPool 的方法 ==========

    def submit(self, data: Dict[str, Any], task_id: Optional[str] = None,
               priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
               delay: float = 0, ttl: int = 3600) -> str:
        return self._pool.submit(data, task_id, priority, delay, ttl)

    async def submit_async(self, data: Dict[str, Any], task_id: Optional[str] = None,
                           priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
                           delay: float = 0, ttl: int = 3600) -> str:
        return await self._pool.submit_async(data, task_id, priority, delay, ttl)

    def wait_for_result(self, task_id: str, timeout: float = 300) -> Any:
        return self._pool.wait_for_result(task_id, timeout)

    async def wait_for_result_async(self, task_id: str, timeout: float = 300) -> Any:
        return await self._pool.wait_for_result_async(task_id, timeout)

    def get_status(self, task_id: str) -> Optional[str]:
        return self._pool.get_status(task_id)

    async def get_status_async(self, task_id: str) -> Optional[str]:
        return await self._pool.get_status_async(task_id)

    def get_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        return self._pool.get_result(task_id)

    async def get_result_async(self, task_id: str) -> Optional[Dict[str, Any]]:
        return await self._pool.get_result_async(task_id)

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        return self._pool.get_task(task_id)

    def task_exists(self, task_id: str) -> bool:
        return self._pool.task_exists(task_id)

    def cancel(self, task_id: str) -> bool:
        if task_id in self._periodic_tasks:
            return self.cancel_periodic(task_id)
        return self._pool.cancel(task_id)

    def delete(self, task_id: str) -> bool:
        return self._pool.delete(task_id)

    def retry(self, task_id: str, delay: float = 0) -> bool:
        return self._pool.retry(task_id, delay)

    def get_stats(self) -> Dict[str, Any]:
        stats = self._pool.get_stats()
        stats["periodic_tasks_count"] = len(self._periodic_tasks)
        return stats

    def get_queue_size(self) -> int:
        return self._pool.get_queue_size()

    def get_health_status(self) -> Dict[str, Any]:
        return self._pool.get_health_status()

    def pause(self) -> None:
        self._pool.pause()

    def resume(self) -> None:
        self._pool.resume()

    def clear_queue(self) -> None:
        self._pool.clear_queue()

    def on_created(self, handler: Callable) -> None:
        self._pool.on_created(handler)

    def on_started(self, handler: Callable) -> None:
        self._pool.on_started(handler)

    def on_completed(self, handler: Callable) -> None:
        self._pool.on_completed(handler)

    def on_failed(self, handler: Callable) -> None:
        self._pool.on_failed(handler)

    def on_cancelled(self, handler: Callable) -> None:
        self._pool.on_cancelled(handler)
