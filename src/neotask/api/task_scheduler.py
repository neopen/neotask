"""
@FileName: task_scheduler.py
@Description: TaskScheduler - 定时任务入口，专注于延时/周期任务。
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List, Callable, Union

from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.models.task import TaskPriority


@dataclass
class PeriodicTask:
    """周期任务定义"""
    task_id: str
    data: Dict[str, Any]
    interval_seconds: float
    priority: int
    cron_expr: Optional[str] = None
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    run_count: int = 0
    is_paused: bool = False
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class SchedulerConfig:
    """调度器配置 - 专注于定时任务"""
    # 存储配置
    storage_type: str = "memory"
    sqlite_path: str = "neotask.db"
    redis_url: Optional[str] = None

    # Worker配置
    worker_concurrency: int = 10

    # 重试配置
    max_retries: int = 3
    retry_delay: float = 1.0

    # 持久化配置
    enable_persistence: bool = False

    # 调度配置
    scan_interval: float = 1.0  # 调度循环扫描间隔（秒）

    @classmethod
    def memory(cls) -> "SchedulerConfig":
        """创建内存存储配置"""
        return cls(storage_type="memory")

    @classmethod
    def sqlite(cls, path: str = "neotask.db") -> "SchedulerConfig":
        """创建SQLite存储配置"""
        return cls(storage_type="sqlite", sqlite_path=path)

    @classmethod
    def redis(cls, url: str) -> "SchedulerConfig":
        """创建Redis存储配置"""
        return cls(storage_type="redis", redis_url=url)


class TaskScheduler:
    """定时任务调度器

    专注于延时执行、周期执行和Cron表达式任务。

    设计模式：Facade Pattern - 封装 TaskPool 和调度逻辑

    使用示例：
        >>> scheduler = TaskScheduler()
        >>>
        >>> # 延时执行
        >>> task_id = scheduler.submit_delayed({"data": "value"}, delay_seconds=60)
        >>>
        >>> # 周期执行
        >>> task_id = scheduler.submit_interval({"data": "value"}, interval_seconds=300)
        >>>
        >>> # Cron表达式
        >>> task_id = scheduler.submit_cron({"data": "value"}, "0 9 * * *")
        >>>
        >>> # 取消周期任务
        >>> scheduler.cancel_periodic(task_id)
    """

    def __init__(
            self,
            executor: Optional[Union[Callable]] = None,
            config: Optional[SchedulerConfig] = None
    ):
        """初始化调度器

        Args:
            executor: 任务执行函数
            config: 调度器配置
        """
        self._config = config or SchedulerConfig()

        # 创建底层任务池配置
        pool_config = TaskPoolConfig(
            storage_type=self._config.storage_type,
            sqlite_path=self._config.sqlite_path,
            redis_url=self._config.redis_url,
            worker_concurrency=self._config.worker_concurrency,
            max_retries=self._config.max_retries,
            retry_delay=self._config.retry_delay
        )

        # 创建底层任务池（独立实例）
        self._pool = TaskPool(executor, pool_config)

        # 周期任务存储
        self._periodic_tasks: Dict[str, PeriodicTask] = {}
        self._scheduler_task: Optional[asyncio.Task] = None
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """获取事件循环"""
        if self._loop is None or self._loop.is_closed():
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop

    def _run_coroutine(self, coro):
        """运行协程"""
        loop = self._get_loop()
        if loop.is_running():
            return asyncio.create_task(coro)
        else:
            return loop.run_until_complete(coro)

    # ========== 生命周期 ==========

    def start(self) -> None:
        """启动调度器"""
        # 启动底层任务池
        self._pool.start()

        if self._running:
            return

        self._running = True
        loop = self._get_loop()

        # 加载持久化的周期任务
        if self._config.enable_persistence:
            self._load_periodic_tasks()

        # 启动调度循环
        self._scheduler_task = loop.create_task(self._scheduler_loop())

    def shutdown(self, graceful: bool = True, timeout: float = 30) -> None:
        """关闭调度器

        Args:
            graceful: 是否优雅关闭
            timeout: 超时时间
        """
        self._running = False

        if self._scheduler_task:
            self._scheduler_task.cancel()

        # 关闭底层任务池
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
        """延时执行任务

        Args:
            data: 任务数据
            delay_seconds: 延迟秒数
            task_id: 任务ID（可选）
            priority: 优先级
            ttl: 任务超时时间

        Returns:
            task_id
        """
        return self._pool.submit(data, task_id, priority, delay_seconds, ttl)

    async def submit_delayed_async(
            self,
            data: Dict[str, Any],
            delay_seconds: float,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            ttl: int = 3600
    ) -> str:
        """延时执行任务（异步）"""
        return await self._pool.submit_async(data, task_id, priority, delay_seconds, ttl)

    def submit_at(
            self,
            data: Dict[str, Any],
            execute_at: datetime,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            ttl: int = 3600
    ) -> str:
        """指定时间点执行任务

        Args:
            data: 任务数据
            execute_at: 执行时间点
            task_id: 任务ID（可选）
            priority: 优先级
            ttl: 任务超时时间

        Returns:
            task_id
        """
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
        """按固定间隔周期执行任务

        Args:
            data: 任务数据
            interval_seconds: 执行间隔（秒）
            task_id: 任务ID（可选）
            priority: 优先级
            run_immediately: 是否立即执行第一次

        Returns:
            task_id
        """
        task_id = task_id or self._generate_task_id()
        priority_value = priority.value if isinstance(priority, TaskPriority) else priority

        periodic_task = PeriodicTask(
            task_id=task_id,
            data=data,
            interval_seconds=interval_seconds,
            priority=priority_value,
            next_run=datetime.now() if run_immediately else datetime.now() + timedelta(seconds=interval_seconds)
        )

        self._periodic_tasks[task_id] = periodic_task

        # 立即执行第一次
        if run_immediately:
            self._pool.submit(data, task_id, priority)

        # 持久化
        if self._config.enable_persistence:
            self._save_periodic_task(periodic_task)

        return task_id

    def submit_cron(
            self,
            data: Dict[str, Any],
            cron_expr: str,
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL
    ) -> str:
        """按Cron表达式周期执行任务

        Args:
            data: 任务数据
            cron_expr: Cron表达式，如 "0 9 * * *" 表示每天9点
            task_id: 任务ID（可选）
            priority: 优先级

        Returns:
            task_id
        """
        # 解析Cron表达式获取下次执行时间
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

        # 持久化
        if self._config.enable_persistence:
            self._save_periodic_task(periodic_task)

        return task_id

    def _parse_cron(self, cron_expr: str) -> datetime:
        """解析Cron表达式，返回下次执行时间

        简化实现，支持基本格式。
        生产环境建议使用 croniter 库。

        Cron格式：分 时 日 月 周
        示例：
            "0 9 * * *"   - 每天9:00
            "*/5 * * * *" - 每5分钟
            "0 9 * * 1"   - 每周一9:00
        """
        parts = cron_expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expr}")

        minute, hour, day, month, weekday = parts

        now = datetime.now()
        next_run = now.replace(second=0, microsecond=0)

        # 简化实现：仅支持基本模式
        # TODO: 集成 croniter 库实现完整Cron支持

        # 处理分钟
        if minute == "*" or minute == "*/1":
            next_run = next_run + timedelta(minutes=1)
        elif minute.startswith("*/"):
            interval = int(minute[2:])
            next_run = next_run + timedelta(minutes=interval)
        else:
            next_run = next_run.replace(minute=int(minute))

        return next_run

    def cancel_periodic(self, task_id: str) -> bool:
        """取消周期任务"""
        if task_id in self._periodic_tasks:
            del self._periodic_tasks[task_id]

            if self._config.enable_persistence:
                self._delete_periodic_task(task_id)

            return True
        return False

    def pause_periodic(self, task_id: str) -> bool:
        """暂停周期任务"""
        if task_id in self._periodic_tasks:
            self._periodic_tasks[task_id].is_paused = True
            return True
        return False

    def resume_periodic(self, task_id: str) -> bool:
        """恢复周期任务"""
        if task_id in self._periodic_tasks:
            task = self._periodic_tasks[task_id]
            task.is_paused = False
            # 重置下次执行时间为当前时间（立即检查）
            task.next_run = datetime.now()
            return True
        return False

    def get_periodic_tasks(self) -> List[Dict[str, Any]]:
        """获取所有周期任务"""
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

    def get_periodic_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取单个周期任务"""
        task = self._periodic_tasks.get(task_id)
        if not task:
            return None
        return {
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

    # ========== 调度循环 ==========

    async def _scheduler_loop(self) -> None:
        """调度循环 - 处理周期任务"""
        while self._running:
            try:
                now = datetime.now()

                for task_id, periodic_task in list(self._periodic_tasks.items()):
                    if periodic_task.is_paused:
                        continue

                    if periodic_task.next_run and periodic_task.next_run <= now:
                        # 执行周期任务
                        await self._pool.submit_async(
                            periodic_task.data,
                            task_id,
                            periodic_task.priority
                        )

                        # 更新执行信息
                        periodic_task.last_run = now
                        periodic_task.run_count += 1

                        # 计算下次执行时间
                        if periodic_task.cron_expr:
                            periodic_task.next_run = self._parse_cron(periodic_task.cron_expr)
                        else:
                            periodic_task.next_run = now + timedelta(
                                seconds=periodic_task.interval_seconds
                            )

                        # 持久化更新
                        if self._config.enable_persistence:
                            self._save_periodic_task(periodic_task)

                await asyncio.sleep(self._config.scan_interval)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self._config.scan_interval * 5)

    # ========== 持久化（预留接口） ==========

    def _save_periodic_task(self, task: PeriodicTask) -> None:
        """保存周期任务到存储"""
        # TODO: 实现持久化存储
        pass

    def _load_periodic_tasks(self) -> None:
        """从存储加载周期任务"""
        # TODO: 实现加载持久化任务
        pass

    def _delete_periodic_task(self, task_id: str) -> None:
        """删除周期任务"""
        # TODO: 实现删除持久化任务
        pass

    def _generate_task_id(self) -> str:
        """生成周期任务ID"""
        return f"PRD_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"

    # ========== 即时任务 API（委托给 TaskPool） ==========

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
            priority: 优先级（0-3，数字越小优先级越高）
            delay: 延迟执行时间（秒）
            ttl: 任务超时时间（秒）

        Returns:
            task_id
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
        """提交即时任务（异步）"""
        return await self._pool.submit_async(data, task_id, priority, delay, ttl)

    # ========== 任务查询 API（委托） ==========

    def wait_for_result(self, task_id: str, timeout: float = 300) -> Any:
        """等待任务完成"""
        return self._pool.wait_for_result(task_id, timeout)

    async def wait_for_result_async(self, task_id: str, timeout: float = 300) -> Any:
        """等待任务完成（异步）"""
        return await self._pool.wait_for_result_async(task_id, timeout)

    def get_status(self, task_id: str) -> Optional[str]:
        """获取任务状态"""
        return self._pool.get_status(task_id)

    async def get_status_async(self, task_id: str) -> Optional[str]:
        """获取任务状态（异步）"""
        return await self._pool.get_status_async(task_id)

    def get_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果"""
        return self._pool.get_result(task_id)

    async def get_result_async(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果（异步）"""
        return await self._pool.get_result_async(task_id)

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取完整任务信息"""
        return self._pool.get_task(task_id)

    async def get_task_async(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取完整任务信息（异步）"""
        return await self._pool.get_task_async(task_id)

    def task_exists(self, task_id: str) -> bool:
        """检查任务是否存在"""
        return self._pool.task_exists(task_id)

    # ========== 任务管理 API（委托） ==========

    def cancel(self, task_id: str) -> bool:
        """取消任务（包括周期任务）"""
        # 先尝试取消周期任务
        if task_id in self._periodic_tasks:
            return self.cancel_periodic(task_id)
        # 否则取消普通任务
        return self._pool.cancel(task_id)

    async def cancel_async(self, task_id: str) -> bool:
        """取消任务（异步）"""
        if task_id in self._periodic_tasks:
            return self.cancel_periodic(task_id)
        return await self._pool.cancel_async(task_id)

    def delete(self, task_id: str) -> bool:
        """删除任务"""
        return self._pool.delete(task_id)

    def retry(self, task_id: str, delay: float = 0) -> bool:
        """重试失败的任务"""
        return self._pool.retry(task_id, delay)

    # ========== 统计信息 API（委托） ==========

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self._pool.get_stats()
        stats["periodic_tasks_count"] = len(self._periodic_tasks)
        return stats

    async def get_stats_async(self) -> Dict[str, Any]:
        """获取统计信息（异步）"""
        stats = await self._pool.get_stats_async()
        stats["periodic_tasks_count"] = len(self._periodic_tasks)
        return stats

    def get_queue_size(self) -> int:
        """获取队列大小"""
        return self._pool.get_queue_size()

    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        return self._pool.get_health_status()

    # ========== 队列管理 API（委托） ==========

    def pause(self) -> None:
        """暂停处理新任务"""
        self._pool.pause()

    def resume(self) -> None:
        """恢复处理新任务"""
        self._pool.resume()

    def clear_queue(self) -> None:
        """清空队列"""
        self._pool.clear_queue()

    # ========== 事件回调 API（委托） ==========

    def on_created(self, handler: Callable) -> None:
        """注册任务创建回调"""
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
