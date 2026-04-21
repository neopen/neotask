"""
@FileName: task_scheduler.py
@Description: TaskScheduler - 定时任务入口，专注于延时/周期任务。
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import random
import threading
import uuid
from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List, Callable, Union

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.models.config import SchedulerConfig
from neotask.models.schedule import PeriodicTask
from neotask.models.task import TaskPriority
from neotask.scheduler.cron_parser import CronParser
from neotask.scheduler.periodic import PeriodicTaskManager
from neotask.scheduler.time_wheel import TimeWheel


class TaskScheduler:
    """定时任务调度器

    负责延时任务和周期任务的调度管理。
    设计模式: Facade Pattern - 封装底层调度组件，提供统一接口。

    职责:
        - 管理TaskPool实例用于任务执行
        - 管理PeriodicTaskManager用于周期任务调度
        - 管理TimeWheel用于高性能延时任务
        - 提供延时任务、周期任务、Cron任务的统一API

    使用示例:
        >>> scheduler = TaskScheduler(executor=my_task)
        >>> scheduler.start()
        >>>
        >>> # 延时任务
        >>> task_id = scheduler.submit_delayed({"data": "value"}, delay_seconds=60)
        >>>
        >>> # 周期任务
        >>> task_id = scheduler.submit_interval({"data": "value"}, interval_seconds=300)
        >>>
        >>> # Cron任务
        >>> task_id = scheduler.submit_cron({"data": "value"}, "0 9 * * *")
        >>>
        >>> # 等待结果
        >>> result = scheduler.wait_for_result(task_id)
        >>>
        >>> scheduler.shutdown()
    """

    def __init__(
            self,
            executor: Optional[Union[Callable]] = None,
            config: Optional[SchedulerConfig] = None
    ):
        """初始化定时任务调度器

        Args:
            executor: 任务执行函数，可以是同步或异步函数
            config: 调度器配置，包含存储、Worker、重试等配置
        """
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

        self._periodic_manager: Optional[PeriodicTaskManager] = None
        self._time_wheel: Optional[TimeWheel] = None

        # 周期任务存储（当不使用PeriodicTaskManager时的后备存储）
        self._periodic_tasks: Dict[str, PeriodicTask] = {}

        self._running = False
        self._stop_event = threading.Event()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ========== 生命周期管理 ==========

    def start(self) -> None:
        """启动调度器

        启动顺序:
            1. 启动底层TaskPool
            2. 启动PeriodicTaskManager（如果启用）
            3. 启动TimeWheel（如果启用）
            4. 启动调度线程

        设计模式: Template Method Pattern - 定义启动流程
        """
        # 先启动 TaskPool（它会在独立线程中启动事件循环）
        self._pool.start()

        # 等待 TaskPool 的事件循环就绪
        import time
        timeout = 5
        start_time = time.time()
        while (not hasattr(self._pool, '_loop') or
               self._pool._loop is None or
               not isinstance(self._pool._loop, asyncio.AbstractEventLoop)) and \
                time.time() - start_time < timeout:
            time.sleep(0.01)

        if not hasattr(self._pool, '_loop') or self._pool._loop is None:
            raise RuntimeError("TaskPool event loop not available")

        if self._running:
            return

        self._running = True
        self._stop_event.clear()

        # 获取 TaskPool 的事件循环
        self._loop = self._pool._loop

        # 异步初始化组件（使用 TaskPool 的事件循环）
        if self._config.enable_periodic_manager:
            self._periodic_manager = PeriodicTaskManager(
                task_pool=self._pool,
                storage=None
            )
            # 使用 call_soon_threadsafe + Future 的方式
            future = asyncio.run_coroutine_threadsafe(
                self._periodic_manager.start(),
                self._loop
            )
            try:
                future.result(timeout=5)
            except Exception as e:
                print(f"Failed to start periodic manager: {e}")

        if self._config.enable_time_wheel:
            self._time_wheel = TimeWheel(
                slot_count=self._config.time_wheel_slots,
                tick_interval=self._config.time_wheel_tick
            )
            future = asyncio.run_coroutine_threadsafe(
                self._time_wheel.start(self._on_delayed_task_ready),
                self._loop
            )
            try:
                future.result(timeout=5)
            except Exception as e:
                print(f"Failed to start time wheel: {e}")

        # 启动调度线程
        self._start_scheduler_thread()

    def _get_or_create_event_loop(self) -> asyncio.AbstractEventLoop:
        """获取或创建事件循环

        Returns:
            事件循环实例

        Design Pattern: Singleton Pattern - 确保事件循环唯一
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop

    def _run_async(self, coro) -> Any:
        """运行异步协程

        根据当前是否在事件循环中，选择合适的执行方式

        由于 TaskPool 已经在独立线程中运行事件循环，
        这里直接使用 TaskPool 的事件循环执行协程。

        Args:
            coro: 协程对象

        Returns:
            协程执行结果
        """
        # 获取 TaskPool 的事件循环
        if hasattr(self._pool, '_loop') and self._pool._loop is not None:
            # 确保是真正的事件循环对象
            loop = self._pool._loop
            if isinstance(loop, asyncio.AbstractEventLoop):
                future = asyncio.run_coroutine_threadsafe(coro, loop)
                return future.result()

        # 降级方案：创建临时事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def _start_scheduler_thread(self) -> None:
        """启动调度器线程

        设计模式: Thread Pattern - 独立线程处理周期任务扫描
        """

        def run_scheduler():
            # 创建新的事件循环用于调度线程
            thread_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(thread_loop)
            thread_loop.run_until_complete(self._scheduler_loop())

        self._scheduler_thread = threading.Thread(
            target=run_scheduler,
            daemon=True,
            name="TaskScheduler"
        )
        self._scheduler_thread.start()

    async def _scheduler_loop(self) -> None:
        """调度循环

        职责:
            - 扫描并执行到期的周期任务
            - 更新任务统计信息

        设计模式: Scheduler Pattern - 定期扫描调度
        """
        while self._running and not self._stop_event.is_set():
            try:
                now = datetime.now()

                if self._periodic_manager:
                    # 使用PeriodicTaskManager获取任务列表
                    tasks = await self._periodic_manager.list_tasks()
                    for task_info in tasks:
                        if not task_info.get("is_paused", False):
                            next_run_str = task_info.get("next_run")
                            if next_run_str:
                                next_run_dt = datetime.fromisoformat(next_run_str)
                                if next_run_dt <= now:
                                    # 获取完整的任务实例
                                    instance = await self._periodic_manager.get_task_instance(task_info["task_id"])
                                    if instance:
                                        await self._periodic_manager._execute_periodic_task(
                                            task_info["task_id"],
                                            instance
                                        )
                else:
                    # 使用本地存储的周期任务
                    for task_id, periodic_task in list(self._periodic_tasks.items()):
                        if periodic_task.is_paused:
                            continue

                        if periodic_task.next_run and periodic_task.next_run <= now:
                            # 更新执行信息
                            periodic_task.run_count += 1
                            periodic_task.last_run = now

                            # 计算下次执行时间
                            if periodic_task.cron_expr and periodic_task.cron_obj:
                                periodic_task.next_run = periodic_task.cron_obj.next(after=now)
                            else:
                                periodic_task.next_run = now + timedelta(seconds=periodic_task.interval_seconds)

                            # 检查是否达到最大执行次数
                            if periodic_task.max_runs and periodic_task.run_count >= periodic_task.max_runs:
                                del self._periodic_tasks[task_id]
                                continue

                            # 执行任务
                            exec_task_id = f"EXEC_{task_id}_{periodic_task.run_count}_{uuid.uuid4().hex[:4]}"
                            self._pool.submit(
                                periodic_task.data,
                                exec_task_id,
                                periodic_task.priority
                            )

                await asyncio.sleep(self._config.scan_interval)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self._config.scan_interval)

    def shutdown(self, graceful: bool = True, timeout: float = 30) -> None:
        """关闭调度器

        Args:
            graceful: 是否优雅关闭，等待当前任务完成
            timeout: 优雅关闭超时时间（秒）

        设计模式: Template Method Pattern - 定义关闭流程
        """
        self._running = False
        self._stop_event.set()

        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=5)

        if self._periodic_manager:
            self._run_async(self._periodic_manager.stop())

        if self._time_wheel:
            self._run_async(self._time_wheel.stop())

        self._pool.shutdown(graceful, timeout)

    def __enter__(self):
        """上下文管理器入口

        设计模式: Context Manager Pattern - 支持with语句
        """
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.shutdown()

    def _ensure_running(self) -> None:
        """确保调度器正在运行

        设计模式: Lazy Initialization Pattern - 按需启动
        """
        if not self._running:
            self.start()

    # ========== 延时任务 API ==========

    def submit_delayed(
            self,
            data: Dict[str, Any],
            delay_seconds: float,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            ttl: int = 3600
    ) -> str:
        """提交延时任务

        Args:
            data: 任务数据
            delay_seconds: 延迟秒数
            task_id: 任务ID（可选，自动生成）
            priority: 优先级
            ttl: 任务超时时间（秒）

        Returns:
            任务ID

        设计模式: Delegation Pattern - 委托给TaskPool
        """
        self._ensure_running()
        return self._pool.submit(data, task_id, priority, delay_seconds, ttl)

    async def submit_delayed_async(
            self,
            data: Dict[str, Any],
            delay_seconds: float,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            ttl: int = 3600
    ) -> str:
        """提交延时任务（异步版本）

        Args:
            data: 任务数据
            delay_seconds: 延迟秒数
            task_id: 任务ID（可选，自动生成）
            priority: 优先级
            ttl: 任务超时时间（秒）

        Returns:
            任务ID

        设计模式: Delegation Pattern - 委托给TaskPool
        """
        self._ensure_running()
        return await self._pool.submit_async(data, task_id, priority, delay_seconds, ttl)

    def submit_at(
            self,
            data: Dict[str, Any],
            execute_at: datetime,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            ttl: int = 3600
    ) -> str:
        """在指定时间点执行任务

        Args:
            data: 任务数据
            execute_at: 执行时间点
            task_id: 任务ID（可选，自动生成）
            priority: 优先级
            ttl: 任务超时时间（秒）

        Returns:
            任务ID

        设计模式: Adapter Pattern - 将时间点转换为延迟秒数
        """
        delay_seconds = max(0.0, (execute_at - datetime.now()).total_seconds())
        return self.submit_delayed(data, delay_seconds, task_id, priority, ttl)

    async def submit_at_async(
            self,
            data: Dict[str, Any],
            execute_at: datetime,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            ttl: int = 3600
    ) -> str:
        """在指定时间点执行任务（异步版本）"""
        delay_seconds = max(0.0, (execute_at - datetime.now()).total_seconds())
        return await self.submit_delayed_async(data, delay_seconds, task_id, priority, ttl)

    async def _on_delayed_task_ready(
            self,
            task_id: str,
            priority: int,
            data: Optional[Dict] = None
    ) -> None:
        """时间轮任务到期回调

        Args:
            task_id: 任务ID
            priority: 优先级
            data: 任务数据

        设计模式: Callback Pattern - 时间轮回调通知
        """
        if data:
            await self._pool.submit_async(data, task_id, priority)
        else:
            await self._pool.submit_async({}, task_id, priority)

    # ========== 周期任务 API ==========

    def submit_interval(
            self,
            data: Dict[str, Any],
            interval_seconds: float,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            run_immediately: bool = True,
            max_runs: Optional[int] = None,
            name: str = ""
    ) -> str:
        """按固定间隔周期执行任务

        Args:
            data: 任务数据
            interval_seconds: 执行间隔（秒）
            task_id: 任务ID（可选，自动生成）
            priority: 优先级
            run_immediately: 是否立即执行第一次
            max_runs: 最大执行次数
            name: 任务名称

        Returns:
            周期任务ID

        设计模式: Factory Pattern - 创建周期任务
        """
        self._ensure_running()

        task_id = task_id or self._generate_task_id()
        priority_value = priority.value if isinstance(priority, TaskPriority) else priority

        if self._periodic_manager:
            return self._run_async(
                self._periodic_manager.create_interval(
                    interval_seconds=int(interval_seconds),
                    task_data=data,
                    name=name or task_id,
                    priority=priority_value,
                    max_runs=max_runs
                )
            )

        now = datetime.now()
        next_run = now if run_immediately else now + timedelta(seconds=interval_seconds)

        periodic_task = PeriodicTask(
            task_id=task_id,
            data=data,
            interval_seconds=interval_seconds,
            priority=priority_value,
            next_run=next_run,
            max_runs=max_runs
        )

        self._periodic_tasks[task_id] = periodic_task

        if run_immediately:
            exec_task_id = f"EXEC_{task_id}_0_{uuid.uuid4().hex[:4]}"
            self._pool.submit(data, exec_task_id, priority)

        return task_id

    def submit_cron(
            self,
            data: Dict[str, Any],
            cron_expr: str,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            max_runs: Optional[int] = None,
            name: str = ""
    ) -> str:
        """按Cron表达式周期执行任务

        Args:
            data: 任务数据
            cron_expr: Cron表达式，如 "0 9 * * *" 表示每天9点
            task_id: 任务ID（可选，自动生成）
            priority: 优先级
            max_runs: 最大执行次数
            name: 任务名称

        Returns:
            周期任务ID

        设计模式: Factory Pattern - 创建Cron任务
        """
        self._ensure_running()

        if not CronParser.is_valid(cron_expr):
            raise ValueError(f"Invalid cron expression: {cron_expr}")

        task_id = task_id or self._generate_task_id()
        priority_value = priority.value if isinstance(priority, TaskPriority) else priority

        if self._periodic_manager:
            return self._run_async(
                self._periodic_manager.create_cron(
                    cron_expr=cron_expr,
                    task_data=data,
                    name=name or task_id,
                    priority=priority_value,
                    max_runs=max_runs
                )
            )

        cron_obj = CronParser.parse(cron_expr)
        next_run = cron_obj.next()

        periodic_task = PeriodicTask(
            task_id=task_id,
            data=data,
            interval_seconds=0,
            priority=priority_value,
            cron_expr=cron_expr,
            cron_obj=cron_obj,
            next_run=next_run,
            max_runs=max_runs
        )

        self._periodic_tasks[task_id] = periodic_task

        return task_id

    def cancel_periodic(self, task_id: str) -> bool:
        """取消周期任务

        Args:
            task_id: 周期任务ID

        Returns:
            是否成功取消

        设计模式: Command Pattern - 取消任务命令
        """
        if self._periodic_manager:
            return self._run_async(self._periodic_manager.delete_task(task_id))

        if task_id in self._periodic_tasks:
            del self._periodic_tasks[task_id]
            return True

        return False

    def pause_periodic(self, task_id: str) -> bool:
        """暂停周期任务

        Args:
            task_id: 周期任务ID

        Returns:
            是否成功暂停
        """
        if self._periodic_manager:
            return self._run_async(self._periodic_manager.pause(task_id))

        if task_id in self._periodic_tasks:
            self._periodic_tasks[task_id].is_paused = True
            return True

        return False

    def resume_periodic(self, task_id: str) -> bool:
        """恢复周期任务

        Args:
            task_id: 周期任务ID

        Returns:
            是否成功恢复
        """
        if self._periodic_manager:
            return self._run_async(self._periodic_manager.resume(task_id))

        if task_id in self._periodic_tasks:
            task = self._periodic_tasks[task_id]
            task.is_paused = False
            task.next_run = datetime.now()
            return True

        return False

    def get_periodic_tasks(self) -> List[Dict[str, Any]]:
        """获取所有周期任务

        Returns:
            周期任务信息列表

        设计模式: Iterator Pattern - 遍历周期任务
        """
        if self._periodic_manager:
            return self._run_async(self._periodic_manager.list_tasks())

        return [
            {
                "task_id": task.task_id,
                "name": getattr(task, 'name', task.task_id),  # 添加 name 字段
                "interval_seconds": task.interval_seconds,
                "cron_expr": task.cron_expr,
                "priority": task.priority,
                "next_run": task.next_run.isoformat() if task.next_run else None,
                "last_run": task.last_run.isoformat() if task.last_run else None,
                "run_count": task.run_count,
                "is_paused": task.is_paused,
                "max_runs": task.max_runs,
                "created_at": task.created_at.isoformat()
            }
            for task in self._periodic_tasks.values()
        ]

    def get_periodic_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取单个周期任务详情

        Args:
            task_id: 周期任务ID

        Returns:
            周期任务信息，不存在则返回None
        """
        if self._periodic_manager:
            return self._run_async(self._periodic_manager.get_task(task_id))

        task = self._periodic_tasks.get(task_id)
        if not task:
            return None

        return {
            "task_id": task.task_id,
            "name": getattr(task, 'name', task.task_id),  # 添加 name 字段
            "interval_seconds": task.interval_seconds,
            "cron_expr": task.cron_expr,
            "priority": task.priority,
            "next_run": task.next_run.isoformat() if task.next_run else None,
            "last_run": task.last_run.isoformat() if task.last_run else None,
            "run_count": task.run_count,
            "is_paused": task.is_paused,
            "max_runs": task.max_runs,
            "created_at": task.created_at.isoformat()
        }

    # ========== 辅助方法 ==========

    def _generate_task_id(self) -> str:
        """生成周期任务ID

        Returns:
            唯一任务ID

        设计模式: Factory Pattern - 生成唯一标识
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        random_suffix = random.randint(100000, 999999)
        return f"PRD_{timestamp}_{random_suffix}"

    # ========== 委托给 TaskPool 的方法 ==========

    def submit(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            delay: float = 0,
            ttl: int = 3600
    ) -> str:
        """提交即时任务

        Args:
            data: 任务数据
            task_id: 任务ID（可选）
            priority: 优先级
            delay: 延迟执行时间（秒）
            ttl: 任务超时时间（秒）

        Returns:
            任务ID

        设计模式: Delegation Pattern - 委托给TaskPool
        """
        return self._pool.submit(data, task_id, priority, delay, ttl)

    async def submit_async(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            delay: float = 0,
            ttl: int = 3600
    ) -> str:
        """提交即时任务（异步版本）"""
        return await self._pool.submit_async(data, task_id, priority, delay, ttl)

    def wait_for_result(self, task_id: str, timeout: float = 300) -> Any:
        """等待任务完成并返回结果

        Args:
            task_id: 任务ID
            timeout: 超时时间（秒）

        Returns:
            任务执行结果

        Raises:
            TaskNotFoundError: 任务不存在
            TimeoutError: 等待超时
            Exception: 任务执行失败
        """
        return self._pool.wait_for_result(task_id, timeout)

    async def wait_for_result_async(self, task_id: str, timeout: float = 300) -> Any:
        """等待任务完成并返回结果（异步版本）"""
        return await self._pool.wait_for_result_async(task_id, timeout)

    def get_status(self, task_id: str) -> Optional[str]:
        """获取任务状态

        Args:
            task_id: 任务ID

        Returns:
            任务状态字符串，不存在返回None
        """
        return self._pool.get_status(task_id)

    async def get_status_async(self, task_id: str) -> Optional[str]:
        """获取任务状态（异步版本）"""
        return await self._pool.get_status_async(task_id)

    def get_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果

        Args:
            task_id: 任务ID

        Returns:
            任务结果字典，不存在返回None
        """
        return self._pool.get_result(task_id)

    async def get_result_async(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果（异步版本）"""
        return await self._pool.get_result_async(task_id)

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取完整任务信息

        Args:
            task_id: 任务ID

        Returns:
            任务信息字典，不存在返回None
        """
        return self._pool.get_task(task_id)

    def task_exists(self, task_id: str) -> bool:
        """检查任务是否存在

        Args:
            task_id: 任务ID

        Returns:
            是否存在
        """
        return self._pool.task_exists(task_id)

    def cancel(self, task_id: str) -> bool:
        """取消任务

        先尝试取消周期任务，否则取消普通任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功取消
        """
        if self._periodic_manager:
            task = self._run_async(self._periodic_manager.get_task(task_id))
            if task:
                return self.cancel_periodic(task_id)

        if task_id in self._periodic_tasks:
            return self.cancel_periodic(task_id)

        return self._pool.cancel(task_id)

    def delete(self, task_id: str) -> bool:
        """删除任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功删除
        """
        return self._pool.delete(task_id)

    def retry(self, task_id: str, delay: float = 0) -> bool:
        """重试失败的任务

        Args:
            task_id: 任务ID
            delay: 重试延迟（秒）

        Returns:
            是否成功重试
        """
        return self._pool.retry(task_id, delay)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息

        Returns:
            统计信息字典，包含队列大小、任务计数、周期任务数量等
        """
        stats = self._pool.get_stats()

        if self._periodic_manager:
            periodic_stats = self._run_async(self._periodic_manager.get_stats())
            stats["periodic"] = periodic_stats
        else:
            stats["periodic_tasks_count"] = len(self._periodic_tasks)

        if self._time_wheel:
            stats["time_wheel"] = self._time_wheel.get_stats()

        return stats

    def get_queue_size(self) -> int:
        """获取队列大小

        Returns:
            队列中的任务数量
        """
        return self._pool.get_queue_size()

    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态

        Returns:
            健康状态信息，包含存储、队列、系统状态
        """
        return self._pool.get_health_status()

    def pause(self) -> None:
        """暂停处理新任务"""
        self._pool.pause()

    def resume(self) -> None:
        """恢复处理新任务"""
        self._pool.resume()

    def clear_queue(self) -> None:
        """清空队列"""
        self._pool.clear_queue()

    def on_created(self, handler: Callable) -> None:
        """注册任务创建回调

        Args:
            handler: 回调函数，签名为 async def handler(event: TaskEvent)
        """
        self._pool.on_created(handler)

    def on_started(self, handler: Callable) -> None:
        """注册任务开始回调"""
        self._pool.on_started(handler)

    def on_completed(self, handler: Callable) -> None:
        """注册任务完成回调"""
        self._pool.on_completed(handler)

    def on_failed(self, handler: Callable) -> None:
        """注册任务失败回调"""
        self._pool.on_failed(handler)

    def on_cancelled(self, handler: Callable) -> None:
        """注册任务取消回调"""
        self._pool.on_cancelled(handler)
