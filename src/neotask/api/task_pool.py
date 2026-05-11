"""
@FileName: task_pool.py
@Description: Function Call接口 - 即时任务入口，专注于立即执行的任务。
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import inspect
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, List, Callable, Union

from neotask.common.logger import error, debug, info
from neotask.core.dispatcher import TaskDispatcher
from neotask.core.future import FutureManager
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.core.heartbeat import HeartbeatManager, HeartbeatConfig
from neotask.event.bus import EventBus, TaskEvent
from neotask.executor.base import TaskExecutor
from neotask.executor.factory import ExecutorFactory
from neotask.lock.factory import LockFactory
from neotask.models.config import StorageConfig, LockConfig, TaskPoolConfig
from neotask.models.task import TaskPriority
from neotask.monitor.health import SystemHealthChecker
from neotask.monitor.metrics import MetricsCollector
from neotask.monitor.reporter import ReporterManager, ConsoleReporter
from neotask.queue.dead_letter import DeadLetterQueue
from neotask.queue.queue_scheduler import QueueScheduler
from neotask.storage.factory import StorageFactory
from neotask.worker.pool import WorkerPool
from neotask.worker.supervisor import WorkerSupervisor
from neotask.worker.reclaimer import TaskReclaimer, ReclaimerConfig
from neotask.distributed.node import NodeManager
from neotask.distributed.coordinator import Coordinator, CoordinatorConfig


class TaskPool:
    """即时任务池

    专注于立即执行的任务，提供简洁的同步/异步API。

    设计模式：Facade Pattern - 封装底层复杂组件

    去中心化架构特性：
    - 所有节点对等，无主节点
    - 抢占式任务消费
    - 自动故障检测与恢复
    - 水平扩展能力
    """

    def __init__(
            self,
            executor: Union[TaskExecutor, Callable, None] = None,
            config: Optional[TaskPoolConfig] = None
    ):
        """初始化任务池"""
        self._config = config or TaskPoolConfig()

        # 初始化执行器
        self._executor = self._init_executor(executor)

        # 初始化存储
        self._task_repo, self._queue_repo = self._init_storage()

        # 初始化锁管理器
        self._lock_manager = self._init_lock()

        # 初始化事件总线
        self._event_bus = EventBus()

        # 初始化未来管理器
        self._future_manager = FutureManager()

        # 初始化队列调度器
        self._queue_scheduler = QueueScheduler(
            queue_repo=self._queue_repo,
            max_size=self._config.queue_max_size
        )

        # 初始化生命周期管理器
        self._lifecycle = TaskLifecycleManager(
            task_repo=self._task_repo,
            event_bus=self._event_bus
        )

        # 初始化分发器
        self._dispatcher = TaskDispatcher(
            lifecycle_manager=self._lifecycle,
            queue_scheduler=self._queue_scheduler,
            node_id=self._config.node_id
        )

        # 初始化Worker池
        self._worker_pool = WorkerPool(
            executor=self._executor,
            task_repo=self._task_repo,
            queue_scheduler=self._queue_scheduler,
            event_bus=self._event_bus,
            lock_manager=self._lock_manager,
            lifecycle_manager=self._lifecycle,
            concurrency=self._config.worker_concurrency,
            prefetch_size=self._config.prefetch_size,
            task_timeout=self._config.task_timeout,
            enable_prefetch=self._config.enable_prefetch
        )

        # 设置重试配置
        self._worker_pool.set_retry_config(
            max_retries=self._config.max_retries,
            retry_delay=self._config.retry_delay
        )

        # 初始化监督者
        self._supervisor = WorkerSupervisor(self._worker_pool)

        # ========== 分布式组件（去中心化）==========
        self._node_manager: Optional[NodeManager] = None
        self._heartbeat_manager: Optional[HeartbeatManager] = None
        self._reclaimer: Optional[TaskReclaimer] = None
        self._coordinator: Optional[Coordinator] = None

        # 仅在 Redis 模式下启用分布式组件
        if self._config.storage_type == "redis" and self._config.redis_url:
            self._init_distributed_components()

        # 初始化监控
        self._metrics = MetricsCollector() if self._config.enable_metrics else None
        self._health_checker = SystemHealthChecker(
            task_repo=self._task_repo,
            queue=self._queue_scheduler
        ) if self._config.enable_health_check else None
        self._reporter_manager = None

        # 设置事件处理器
        self._setup_event_handlers()

        # 运行状态
        self._running = False
        self._started = False

        # 线程安全的执行器
        self._executor_service = ThreadPoolExecutor(max_workers=1, thread_name_prefix="TaskPool")
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None

    def _init_distributed_components(self) -> None:
        """初始化分布式组件（去中心化架构）"""
        try:
            # 1. 节点管理器
            self._node_manager = NodeManager(
                redis_url=self._config.redis_url,
                node_id=self._config.node_id
            )

            # 2. 心跳管理器 - 直接传递 Redis URL
            heartbeat_config = HeartbeatConfig(
                interval=5,
                timeout=20,
                cleanup_interval=30
            )
            self._heartbeat_manager = HeartbeatManager(
                node_id=self._config.node_id,
                redis_url=self._config.redis_url,  # 直接传 URL
                task_repo=self._task_repo,
                config=heartbeat_config
            )

            # 3. 任务回收器
            reclaimer_config = ReclaimerConfig(
                interval=30,
                task_timeout=self._config.task_timeout or 300,
                max_retries=self._config.max_retries,
                enable_timeout_reclaim=True,
                enable_orphan_reclaim=True,
                enable_stale_lock_reclaim=True
            )
            self._reclaimer = TaskReclaimer(
                task_repo=self._task_repo,
                queue_scheduler=self._queue_scheduler,
                lock=self._lock_manager,
                event_bus=self._event_bus,
                config=reclaimer_config
            )

            # 4. 协调器
            coordinator_config = CoordinatorConfig(
                load_balance_strategy="round_robin",
                distribution_mode="shared"
            )
            self._coordinator = Coordinator(
                node_manager=self._node_manager,
                queue_scheduler=self._queue_scheduler,
                config=coordinator_config
            )

            # 5. 死信队列
            self._dead_letter = None
            if self._config.enable_dead_letter and self._config.storage_type == "redis":
                self._dead_letter = DeadLetterQueue(
                    redis_url=self._config.redis_url,
                    max_size=self._config.dead_letter_max_size,
                    ttl=self._config.dead_letter_ttl
                )

            info(f"Distributed components initialized for node: {self._config.node_id}")

        except Exception as e:
            error(f"Failed to initialize distributed components: {e}")

    def _init_executor(self, executor: Union[TaskExecutor, Callable, None]) -> TaskExecutor:
        """初始化执行器"""
        if executor is None:
            async def default_executor(data):
                return {"result": "executed", "data": data}
            return ExecutorFactory.create(default_executor)

        if isinstance(executor, TaskExecutor):
            return executor

        # 如果是同步函数，包装为异步函数
        if not inspect.iscoroutinefunction(executor):
            async def wrapped_executor(data):
                return executor(data)
            return ExecutorFactory.create(wrapped_executor)

        return ExecutorFactory.create(
            executor,
            executor_type=self._config.executor_type,
            max_workers=self._config.max_workers
        )

    def _init_storage(self):
        """初始化存储"""
        if self._config.storage_type == "memory":
            storage_config = StorageConfig.memory()
        elif self._config.storage_type == "sqlite":
            storage_config = StorageConfig.sqlite(self._config.sqlite_path)
        elif self._config.storage_type == "redis":
            storage_config = StorageConfig.redis(self._config.redis_url)
        else:
            raise ValueError(f"Unknown storage type: {self._config.storage_type}")

        return StorageFactory.create(storage_config)

    def _init_lock(self):
        """初始化锁管理器"""
        if self._config.lock_type == "memory":
            lock_config = LockConfig.memory()
        elif self._config.lock_type == "redis":
            lock_config = LockConfig.redis(self._config.redis_url, self._config.lock_timeout)
        else:
            raise ValueError(f"Unknown lock type: {self._config.lock_type}")

        return LockFactory.create(lock_config)

    def _setup_event_handlers(self):
        """设置事件处理器"""
        # 指标收集
        if self._metrics:
            @self._event_bus.subscribe_global
            async def metrics_handler(event: TaskEvent):
                if event.event_type == "task.created":
                    await self._metrics.record_task_submit(event.task_id)
                elif event.event_type == "task.started":
                    await self._metrics.record_task_start(event.task_id)
                elif event.event_type == "task.completed":
                    await self._metrics.record_task_complete(event.task_id)
                elif event.event_type == "task.failed":
                    await self._metrics.record_task_failed(event.task_id)
                elif event.event_type == "task.cancelled":
                    await self._metrics.record_task_cancelled(event.task_id)

        # 未来管理器完成
        @self._event_bus.subscribe("task.completed")
        async def complete_future_handler(event: TaskEvent):
            result = event.data.get("result") if isinstance(event.data, dict) else event.data
            await self._future_manager.complete(event.task_id, result=result)

        @self._event_bus.subscribe("task.failed")
        async def fail_future_handler(event: TaskEvent):
            err = event.data.get("error") if isinstance(event.data, dict) else str(event.data)
            await self._future_manager.complete(event.task_id, error=err)

        @self._event_bus.subscribe("task.cancelled")
        async def cancel_future_handler(event: TaskEvent):
            await self._future_manager.complete(event.task_id, error="Task cancelled")

        # 任务回收事件（用于监控）
        @self._event_bus.subscribe("task.reclaimed")
        async def reclaim_handler(event: TaskEvent):
            debug(f"Task reclaimed: {event.task_id}, reason: {event.data}")

    def _ensure_running(self):
        """确保服务正在运行"""
        if not self._running:
            self.start()

    def _run_coroutine(self, coro):
        """运行协程 - 线程安全版本"""
        if self._loop is None:
            raise RuntimeError("TaskPool not started. Call start() first.")

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    # ========== 生命周期管理 ==========

    def start(self) -> None:
        """启动任务池"""
        if self._running:
            return

        # 创建独立的事件循环线程
        def run_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            async def start_components():
                # 启动核心组件
                await self._event_bus.start()
                await self._queue_scheduler.start()
                await self._worker_pool.start()
                await self._supervisor.start()

                # 启动分布式组件（去中心化）
                if self._node_manager:
                    await self._node_manager.start()
                    info(f"Node manager started for {self._config.node_id}")

                if self._heartbeat_manager:
                    await self._heartbeat_manager.start()
                    debug("Heartbeat manager started")

                if self._reclaimer:
                    await self._reclaimer.start()
                    info("Task reclaimer started")

                # 启动监控上报
                if self._config.enable_reporter:
                    self._reporter_manager = ReporterManager(interval=60)
                    self._reporter_manager.add_reporter(ConsoleReporter())
                    self._reporter_manager.set_metrics_callback(self.get_stats)
                    await self._reporter_manager.start()

                info(f"TaskPool started with node_id: {self._config.node_id}")

            loop.run_until_complete(start_components())
            loop.run_forever()

        self._loop_thread = threading.Thread(target=run_loop, daemon=True)
        self._loop_thread.start()

        # 等待循环启动
        import time
        timeout = 5
        start_time = time.time()
        while self._loop is None and time.time() - start_time < timeout:
            time.sleep(0.01)

        if self._loop is None:
            raise RuntimeError("Failed to start event loop")

        self._running = True
        self._started = True

    def shutdown(self, graceful: bool = True, timeout: float = 30) -> None:
        """关闭任务池"""
        if not self._running:
            return

        self._running = False
        info("Shutting down TaskPool...")

        async def shutdown_components():
            # 停止接收新任务
            await self._queue_scheduler.disable()

            # 等待队列清空
            if graceful:
                await self._queue_scheduler.wait_until_empty(timeout)

            # 停止分布式组件
            if self._reclaimer:
                await self._reclaimer.stop()
            if self._heartbeat_manager:
                await self._heartbeat_manager.stop()
            if self._node_manager:
                await self._node_manager.stop()
            # 关闭死信队列
            if self._dead_letter:
                await self._dead_letter.close()

            # 停止核心组件
            await self._supervisor.stop()
            await self._worker_pool.stop(graceful, timeout)
            await self._queue_scheduler.stop()

            if self._reporter_manager:
                await self._reporter_manager.stop()

            await self._event_bus.stop()

            # 关闭存储连接 - 捕获异常避免循环冲突
            for repo in [self._task_repo, self._queue_repo]:
                if hasattr(repo, 'close'):
                    try:
                        await repo.close()
                    except RuntimeError as e:
                        if "different loop" in str(e):
                            # 忽略循环冲突错误
                            debug(f"Ignored loop conflict when closing {repo}")
                        else:
                            error(f"Error closing repo: {e}")
                    except Exception as e:
                        error(f"Error closing repo: {e}")

            info("TaskPool shutdown complete")

        # 执行关闭
        try:
            if self._loop and self._loop.is_running():
                # 尝试在当前循环中执行
                future = asyncio.run_coroutine_threadsafe(shutdown_components(), self._loop)
                try:
                    future.result(timeout=timeout + 5)
                except Exception as e:
                    error(f"Failed to stop components: {e}")
            else:
                # 创建临时循环
                asyncio.run(shutdown_components())
        except RuntimeError as e:
            if "different loop" in str(e):
                # 忽略循环冲突，尝试直接执行
                pass
            else:
                error(f"Shutdown error: {e}")

        self._started = False
        self._loop = None

    def __enter__(self):
        """同步上下文管理器入口"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """同步上下文管理器出口"""
        self.shutdown()

    # ========== 异步上下文管理器 ==========
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.start()
        # 等待事件循环启动
        import time
        timeout = 5
        start_time = time.time()
        while self._loop is None and time.time() - start_time < timeout:
            await asyncio.sleep(0.01)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        self.shutdown()
        # 等待清理完成
        await asyncio.sleep(0.2)

    # ========== 任务提交 API ==========

    def submit(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            delay: float = 0,
            ttl: int = 3600
    ) -> str:
        """提交任务（同步）"""
        self._ensure_running()
        result = self._run_coroutine(
            self._dispatcher.dispatch(data, task_id, priority, delay, ttl)
        )
        return result.task_id if hasattr(result, 'task_id') else result

    async def submit_async(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            delay: float = 0,
            ttl: int = 3600
    ) -> str:
        """提交任务（异步）"""
        self._ensure_running()
        result = await self._dispatcher.dispatch(data, task_id, priority, delay, ttl)
        return result.task_id if hasattr(result, 'task_id') else result

    def submit_batch(
            self,
            tasks: List[Dict[str, Any]],
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL
    ) -> List[str]:
        """批量提交任务（同步）"""
        self._ensure_running()
        task_ids = []
        for task_data in tasks:
            task_id = self.submit(task_data, priority=priority)
            task_ids.append(task_id)
        return task_ids

    async def submit_batch_async(
            self,
            tasks: List[Dict[str, Any]],
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL
    ) -> List[str]:
        """批量提交任务（异步）"""
        self._ensure_running()
        task_ids = []
        for task_data in tasks:
            task_id = await self.submit_async(task_data, priority=priority)
            task_ids.append(task_id)
        return task_ids

    # ========== 任务等待 API ==========

    def wait_for_result(self, task_id: str, timeout: float = 300) -> Any:
        """等待任务完成并返回结果（同步）"""
        return self._run_coroutine(self._lifecycle.wait_for_task(task_id, timeout))

    async def wait_for_result_async(self, task_id: str, timeout: float = 300) -> Any:
        """等待任务完成并返回结果（异步）"""
        return await self._lifecycle.wait_for_task(task_id, timeout)

    def wait_all(
            self,
            task_ids: List[str],
            timeout: float = 300
    ) -> Dict[str, Any]:
        """等待所有任务完成（同步）"""
        results = {}
        for task_id in task_ids:
            try:
                result = self.wait_for_result(task_id, timeout)
                results[task_id] = result
            except Exception as e:
                results[task_id] = {"error": str(e)}
        return results

    async def wait_all_async(
            self,
            task_ids: List[str],
            timeout: float = 300
    ) -> Dict[str, Any]:
        """等待所有任务完成（异步）"""
        results = {}
        for task_id in task_ids:
            try:
                result = await self.wait_for_result_async(task_id, timeout)
                results[task_id] = result
            except Exception as e:
                results[task_id] = {"error": str(e)}
        return results

    # ========== 任务查询 API ==========

    def get_status(self, task_id: str) -> Optional[str]:
        """获取任务状态（同步）"""
        task = self._run_coroutine(self._lifecycle.get_task(task_id))
        return task.status.value if task else None

    async def get_status_async(self, task_id: str) -> Optional[str]:
        """获取任务状态（异步）"""
        task = await self._lifecycle.get_task(task_id)
        return task.status.value if task else None

    def get_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果（同步）"""
        task = self._run_coroutine(self._lifecycle.get_task(task_id))
        if not task:
            return None
        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "result": task.result,
            "error": task.error,
            "created_at": task.created_at.isoformat() if task.created_at else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        }

    async def get_result_async(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果（异步）"""
        task = await self._lifecycle.get_task(task_id)
        if not task:
            return None
        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "result": task.result,
            "error": task.error,
            "created_at": task.created_at.isoformat() if task.created_at else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        }

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取完整任务信息（同步）"""
        task = self._run_coroutine(self._lifecycle.get_task(task_id))
        return task.to_dict() if task else None

    async def get_task_async(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取完整任务信息（异步）"""
        task = await self._lifecycle.get_task(task_id)
        return task.to_dict() if task else None

    def task_exists(self, task_id: str) -> bool:
        """检查任务是否存在（同步）"""
        return self._run_coroutine(self._task_repo.exists(task_id))

    async def task_exists_async(self, task_id: str) -> bool:
        """检查任务是否存在（异步）"""
        return await self._task_repo.exists(task_id)

    # ========== 任务管理 API ==========

    def cancel(self, task_id: str) -> bool:
        """取消任务（同步）"""
        self._ensure_running()
        self._run_coroutine(self._queue_scheduler.remove(task_id))
        return self._run_coroutine(self._lifecycle.cancel_task(task_id))

    async def cancel_async(self, task_id: str) -> bool:
        """取消任务（异步）"""
        await self._queue_scheduler.remove(task_id)
        return await self._lifecycle.cancel_task(task_id)

    def delete(self, task_id: str) -> bool:
        """删除任务（同步）"""
        return self._run_coroutine(self._lifecycle.delete_task(task_id))

    async def delete_async(self, task_id: str) -> bool:
        """删除任务（异步）"""
        return await self._lifecycle.delete_task(task_id)

    def retry(self, task_id: str, delay: float = 0) -> bool:
        """重试失败的任务（同步）"""
        self._ensure_running()
        return self._run_coroutine(self._dispatcher.redispatch(task_id, delay))

    async def retry_async(self, task_id: str, delay: float = 0) -> bool:
        """重试失败的任务（异步）"""
        return await self._dispatcher.redispatch(task_id, delay)

    # ========== 统计信息 API ==========

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息（同步）"""
        stats = self._run_coroutine(self._lifecycle.get_task_stats())
        queue_size = self._run_coroutine(self._queue_scheduler.size())

        result = {
            "queue_size": queue_size,
            "total": stats.total,
            "pending": stats.pending,
            "running": stats.running,
            "completed": stats.completed,
            "failed": stats.failed,
            "cancelled": stats.cancelled,
            "success_rate": stats.success_rate if stats.total > 0 else 1.0,
            "node_id": self._config.node_id,
        }

        if self._metrics:
            result["metrics"] = self._metrics.get_summary()

        # 添加回收器统计
        if self._reclaimer:
            result["reclaimer"] = self._reclaimer.get_stats()

        return result

    async def get_stats_async(self) -> Dict[str, Any]:
        """获取统计信息（异步）"""
        stats = await self._lifecycle.get_task_stats()
        queue_size = await self._queue_scheduler.size()

        result = {
            "queue_size": queue_size,
            "total": stats.total,
            "pending": stats.pending,
            "running": stats.running,
            "completed": stats.completed,
            "failed": stats.failed,
            "cancelled": stats.cancelled,
            "success_rate": stats.success_rate if stats.total > 0 else 1.0,
            "node_id": self._config.node_id,
        }

        if self._metrics:
            result["metrics"] = await self._metrics.get_summary_async()

        if self._reclaimer:
            result["reclaimer"] = self._reclaimer.get_stats()

        return result

    def get_worker_stats(self) -> Dict[int, Any]:
        """获取Worker统计信息"""
        return self._worker_pool.get_stats()

    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        health = {"status": "healthy", "node_id": self._config.node_id}

        if self._health_checker:
            health.update(self._health_checker.get_summary())

        if self._node_manager:
            try:
                active_nodes = self._run_coroutine(self._node_manager.get_active_nodes())
                health["active_nodes"] = len(active_nodes)
            except Exception:
                health["active_nodes"] = 0

        if self._heartbeat_manager:
            health["heartbeat"] = self._heartbeat_manager.get_stats()

        if self._reclaimer:
            health["reclaimer"] = self._reclaimer.get_stats()

        return health

    # ========== 队列管理 API ==========

    def get_queue_size(self) -> int:
        """获取队列大小（同步）"""
        return self._run_coroutine(self._queue_scheduler.size())

    async def get_queue_size_async(self) -> int:
        """获取队列大小（异步）"""
        return await self._queue_scheduler.size()

    def pause(self) -> None:
        """暂停处理新任务（同步）"""
        self._run_coroutine(self._queue_scheduler.pause())

    async def pause_async(self) -> None:
        """暂停处理新任务（异步）"""
        await self._queue_scheduler.pause()

    def resume(self) -> None:
        """恢复处理新任务（同步）"""
        self._run_coroutine(self._queue_scheduler.resume())

    async def resume_async(self) -> None:
        """恢复处理新任务（异步）"""
        await self._queue_scheduler.resume()

    def clear_queue(self) -> None:
        """清空队列（同步）"""
        self._run_coroutine(self._queue_scheduler.clear())

    async def clear_queue_async(self) -> None:
        """清空队列（异步）"""
        await self._queue_scheduler.clear()

    # ========== 锁管理 API ==========

    def acquire_lock(self, task_id: str, ttl: int = 30) -> bool:
        """获取任务锁（同步）"""
        lock_key = f"task:{task_id}"
        return self._run_coroutine(self._lock_manager.acquire(lock_key, ttl))

    async def acquire_lock_async(self, task_id: str, ttl: int = 30) -> bool:
        """获取任务锁（异步）"""
        lock_key = f"task:{task_id}"
        return await self._lock_manager.acquire(lock_key, ttl)

    def release_lock(self, task_id: str) -> bool:
        """释放任务锁（同步）"""
        lock_key = f"task:{task_id}"
        return self._run_coroutine(self._lock_manager.release(lock_key))

    async def release_lock_async(self, task_id: str) -> bool:
        """释放任务锁（异步）"""
        lock_key = f"task:{task_id}"
        return await self._lock_manager.release(lock_key)

    # ========== 节点管理 API（去中心化）==========

    def get_active_nodes(self) -> List[Dict[str, Any]]:
        """获取活跃节点列表"""
        if self._node_manager:
            nodes = self._run_coroutine(self._node_manager.get_active_nodes())
            return [{"node_id": n.node_id, "hostname": n.hostname, "pid": n.pid} for n in nodes]
        return []

    async def get_active_nodes_async(self) -> List[Dict[str, Any]]:
        """获取活跃节点列表（异步）"""
        if self._node_manager:
            nodes = await self._node_manager.get_active_nodes()
            return [{"node_id": n.node_id, "hostname": n.hostname, "pid": n.pid} for n in nodes]
        return []

    def is_leader(self) -> bool:
        """检查当前节点是否为领导者（去中心化架构中始终返回 False）"""
        # 去中心化架构无主节点
        return False

    # ========== 事件回调 API ==========

    def on_created(self, handler: Callable) -> None:
        """注册任务创建回调"""
        self._event_bus.subscribe("task.created", handler)

    def on_started(self, handler: Callable) -> None:
        """注册任务开始回调"""
        self._event_bus.subscribe("task.started", handler)

    def on_completed(self, handler: Callable) -> None:
        """注册任务完成回调"""
        self._event_bus.subscribe("task.completed", handler)

    def on_failed(self, handler: Callable) -> None:
        """注册任务失败回调"""
        self._event_bus.subscribe("task.failed", handler)

    def on_cancelled(self, handler: Callable) -> None:
        """注册任务取消回调"""
        self._event_bus.subscribe("task.cancelled", handler)

    def on_progress(self, handler: Callable) -> None:
        """注册任务进度回调"""
        self._event_bus.subscribe("task.progress", handler)

    def on_reclaimed(self, handler: Callable) -> None:
        """注册任务回收回调"""
        self._event_bus.subscribe("task.reclaimed", handler)