"""
@FileName: engine.py
@Description: 任务引擎 - 统一入口，协调各模块
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from neotask.core.lifecycle import TaskLifecycleManager
from neotask.core.dispatcher import TaskDispatcher
from neotask.core.context import TaskContext
from neotask.storage.base import TaskRepository, QueueRepository
from neotask.storage.factory import StorageFactory
from neotask.executor.factory import ExecutorFactory, ExecutorType
from neotask.executor.base import TaskExecutor
from neotask.queue.queue_scheduler import QueueScheduler
from neotask.worker.pool import WorkerPool
from neotask.worker.supervisor import WorkerSupervisor
from neotask.event.bus import EventBus
from neotask.lock.factory import LockFactory
from neotask.monitor.metrics import MetricsCollector
from neotask.common.exceptions import TaskError


@dataclass
class EngineConfig:
    """引擎配置"""
    # 存储配置
    storage_type: str = "memory"
    sqlite_path: str = "neotask.db"
    redis_url: Optional[str] = None

    # 执行器配置
    executor_func: Optional[Any] = None
    executor_type: ExecutorType = ExecutorType.AUTO
    max_workers: int = 10

    # Worker配置
    worker_concurrency: int = 10
    prefetch_size: int = 20

    # 队列配置
    queue_max_size: int = 10000

    # 锁配置
    lock_type: str = "memory"

    # 监控配置
    enable_metrics: bool = True
    metrics_interval: int = 60


class TaskEngine:
    """任务引擎 - 统一入口

    设计模式：Facade Pattern - 提供统一的接口
    设计模式：Builder Pattern - 支持链式配置

    使用示例：
        >>> engine = TaskEngine()
        >>> engine.configure(EngineConfig(storage_type="redis", redis_url="redis://..."))
        >>> await engine.start()
        >>> task_id = await engine.submit({"data": "value"})
        >>> result = await engine.wait(task_id)
        >>> await engine.stop()
    """

    def __init__(self, config: Optional[EngineConfig] = None):
        self.config = config or EngineConfig()
        self._initialized = False
        self._running = False

        # 核心组件
        self._lifecycle_manager: Optional[TaskLifecycleManager] = None
        self._dispatcher: Optional[TaskDispatcher] = None
        self._queue_scheduler: Optional[QueueScheduler] = None
        self._worker_pool: Optional[WorkerPool] = None
        self._worker_supervisor: Optional[WorkerSupervisor] = None

        # 基础设施
        self._task_repo: Optional[TaskRepository] = None
        self._queue_repo: Optional[QueueRepository] = None
        self._executor: Optional[TaskExecutor] = None
        self._event_bus: Optional[EventBus] = None
        self._metrics: Optional[MetricsCollector] = None

        self._background_tasks: List[asyncio.Task] = []

    def configure(self, config: EngineConfig) -> "TaskEngine":
        """配置引擎（支持链式调用）"""
        self.config = config
        self._initialized = False
        return self

    async def initialize(self) -> None:
        """初始化引擎组件"""
        if self._initialized:
            return

        # 1. 初始化存储
        await self._init_storage()

        # 2. 初始化事件总线
        self._event_bus = EventBus()

        # 3. 初始化执行器
        await self._init_executor()

        # 4. 初始化队列调度器
        self._queue_scheduler = QueueScheduler(
            queue_repo=self._queue_repo,
            max_size=self.config.queue_max_size
        )

        # 5. 初始化生命周期管理器
        self._lifecycle_manager = TaskLifecycleManager(
            task_repo=self._task_repo,
            event_bus=self._event_bus
        )

        # 6. 初始化分发器
        self._dispatcher = TaskDispatcher(
            lifecycle_manager=self._lifecycle_manager,
            queue_scheduler=self._queue_scheduler
        )

        # 7. 初始化Worker池
        self._worker_pool = WorkerPool(
            executor=self._executor,
            task_repo=self._task_repo,
            queue_scheduler=self._queue_scheduler,
            event_bus=self._event_bus,
            lifecycle_manager=self._lifecycle_manager,
            concurrency=self.config.worker_concurrency,
            prefetch_size=self.config.prefetch_size
        )

        # 8. 初始化Worker监督者
        self._worker_supervisor = WorkerSupervisor(
            worker_pool=self._worker_pool,
            health_check_interval=30
        )

        # 9. 初始化监控
        if self.config.enable_metrics:
            self._metrics = MetricsCollector(
                task_repo=self._task_repo,
                queue_scheduler=self._queue_scheduler,
                interval=self.config.metrics_interval
            )

        self._initialized = True

    async def _init_storage(self) -> None:
        """初始化存储"""
        from neotask.models.config import StorageConfig

        if self.config.storage_type == "memory":
            storage_config = StorageConfig.memory()
        elif self.config.storage_type == "sqlite":
            storage_config = StorageConfig.sqlite(self.config.sqlite_path)
        elif self.config.storage_type == "redis":
            storage_config = StorageConfig.redis(self.config.redis_url)
        else:
            raise ValueError(f"Unknown storage type: {self.config.storage_type}")

        self._task_repo, self._queue_repo = StorageFactory.create(storage_config)

    async def _init_executor(self) -> None:
        """初始化执行器"""
        if self.config.executor_func is None:
            # 默认执行器
            async def default_executor(data):
                return {"result": "executed", "data": data}
            self.config.executor_func = default_executor

        self._executor = ExecutorFactory.create(
            self.config.executor_func,
            executor_type=self.config.executor_type,
            max_workers=self.config.max_workers
        )

    async def start(self) -> None:
        """启动引擎"""
        if not self._initialized:
            await self.initialize()

        if self._running:
            return

        self._running = True

        # 启动Worker池
        await self._worker_pool.start()

        # 启动监督者
        await self._worker_supervisor.start()

        # 启动监控
        if self._metrics:
            await self._metrics.start()

        # 启动事件总线
        await self._event_bus.start()

    async def stop(self, graceful: bool = True) -> None:
        """停止引擎

        Args:
            graceful: 是否优雅停止（等待当前任务完成）
        """
        if not self._running:
            return

        self._running = False

        # 停止接收新任务
        await self._queue_scheduler.disable()

        if graceful:
            # 等待队列清空
            await self._queue_scheduler.wait_until_empty(timeout=300)

        # 停止Worker池
        await self._worker_pool.stop(graceful=graceful)

        # 停止监督者
        await self._worker_supervisor.stop()

        # 停止监控
        if self._metrics:
            await self._metrics.stop()

        # 停止事件总线
        await self._event_bus.stop()

        # 关闭存储连接
        if hasattr(self._task_repo, 'close'):
            await self._task_repo.close()
        if hasattr(self._queue_repo, 'close'):
            await self._queue_repo.close()

        # 取消后台任务
        for task in self._background_tasks:
            if not task.done():
                task.cancel()

    # ========== 任务操作 API ==========

    async def submit(
        self,
        data: Dict[str, Any],
        task_id: Optional[str] = None,
        priority: int = 5,
        delay: float = 0,
        ttl: int = 3600
    ) -> str:
        """提交任务

        Args:
            data: 任务数据
            task_id: 任务ID（可选）
            priority: 优先级（1-10，数字越小优先级越高）
            delay: 延迟执行时间（秒）
            ttl: 任务超时时间（秒）

        Returns:
            task_id
        """
        return await self._dispatcher.dispatch(
            data=data,
            task_id=task_id,
            priority=priority,
            delay=delay,
            ttl=ttl
        )

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务信息"""
        task = await self._lifecycle_manager.get_task(task_id)
        if task:
            return task.to_dict()
        return None

    async def get_status(self, task_id: str) -> Optional[str]:
        """获取任务状态"""
        task = await self._lifecycle_manager.get_task(task_id)
        return task.status.value if task else None

    async def get_result(self, task_id: str) -> Optional[Any]:
        """获取任务结果"""
        task = await self._lifecycle_manager.get_task(task_id)
        return task.result if task else None

    async def cancel(self, task_id: str) -> bool:
        """取消任务"""
        # 尝试从队列移除
        removed = await self._queue_scheduler.remove(task_id)
        if removed:
            return await self._lifecycle_manager.cancel(task_id)

        # 尝试取消正在执行的任务
        return await self._worker_pool.cancel_task(task_id)

    async def wait(
        self,
        task_id: str,
        timeout: float = 300
    ) -> Any:
        """等待任务完成并返回结果"""
        return await self._lifecycle_manager.wait(task_id, timeout)

    # ========== 批量操作 API ==========

    async def submit_batch(
        self,
        tasks: List[Dict[str, Any]],
        priority: int = 5
    ) -> List[str]:
        """批量提交任务"""
        task_ids = []
        for task_data in tasks:
            task_id = await self.submit(task_data, priority=priority)
            task_ids.append(task_id)
        return task_ids

    async def wait_all(
        self,
        task_ids: List[str],
        timeout: float = 300
    ) -> Dict[str, Any]:
        """等待所有任务完成"""
        results = {}
        for task_id in task_ids:
            try:
                result = await self.wait(task_id, timeout)
                results[task_id] = result
            except Exception as e:
                results[task_id] = {"error": str(e)}
        return results

    # ========== 管理 API ==========

    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = {
            "queue_size": await self._queue_scheduler.size(),
            "active_workers": self._worker_pool.active_count(),
            "running": self._running,
        }

        if self._metrics:
            stats["metrics"] = self._metrics.get_summary()

        return stats

    async def pause(self) -> None:
        """暂停处理新任务"""
        await self._queue_scheduler.pause()

    async def resume(self) -> None:
        """恢复处理新任务"""
        await self._queue_scheduler.resume()

    @property
    def is_running(self) -> bool:
        return self._running