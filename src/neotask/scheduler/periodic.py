"""
@FileName: periodic.py
@Description: 周期任务管理器 - 管理周期任务的完整生命周期
@Author: HiPeng
@Time: 2026/4/21
"""

import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

from neotask.models.schedule import PeriodicTaskDefinition, PeriodicTaskInstance, PeriodicTaskStatus, MissedExecutionPolicy, PeriodicExecutionRecord
from neotask.scheduler.cron_parser import CronParser


class PeriodicTaskManager:
    """周期任务管理器

    负责周期任务的完整生命周期管理：
    - 创建/更新/删除周期任务
    - 暂停/恢复/停止
    - 执行调度
    - 执行历史记录
    - 状态监控

    设计模式：Manager Pattern - 统一管理周期任务

    使用示例：
        >>> manager = PeriodicTaskManager(task_pool)
        >>> await manager.start()
        >>>
        >>> # 创建固定间隔任务
        >>> task_id = await manager.create_interval(
        ...     name="heartbeat",
        ...     interval_seconds=300,
        ...     task_data={"action": "ping"}
        ... )
        >>>
        >>> # 创建Cron任务
        >>> task_id = await manager.create_cron(
        ...     name="daily_report",
        ...     cron_expr="0 9 * * *",
        ...     task_data={"action": "report"}
        ... )
        >>>
        >>> # 暂停/恢复
        >>> await manager.pause(task_id)
        >>> await manager.resume(task_id)
        >>>
        >>> # 获取统计
        >>> stats = await manager.get_stats()
    """

    def __init__(self, task_pool, storage=None):
        """初始化周期任务管理器

        Args:
            task_pool: TaskPool实例，用于执行任务
            storage: 持久化存储（可选）
        """
        self._task_pool = task_pool
        self._storage = storage

        # 周期任务存储
        self._tasks: Dict[str, PeriodicTaskInstance] = {}
        self._scheduler_task: Optional[asyncio.Task] = None
        self._running = False
        self._lock = asyncio.Lock()

        # 扫描间隔（秒）
        self._scan_interval = 1.0

        # 统计信息
        self._stats = {
            "total_tasks": 0,
            "active_tasks": 0,
            "paused_tasks": 0,
            "total_executions": 0,
            "last_scan": Optional[str]
        }

    async def start(self) -> None:
        """启动周期任务管理器"""
        if self._running:
            return

        self._running = True

        # 从存储加载周期任务
        await self._load_tasks()

        # 启动调度循环
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    async def stop(self, graceful: bool = True) -> None:
        """停止周期任务管理器

        Args:
            graceful: 是否优雅停止（等待当前执行完成）
        """
        self._running = False

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        # 保存任务状态
        if graceful:
            await self._save_tasks()

    # ========== 任务创建 API ==========

    async def create_interval(
            self,
            interval_seconds: int,
            task_data: Dict[str, Any],
            name: str = "",
            description: str = "",
            priority: int = 2,
            max_runs: Optional[int] = None,
            start_at: Optional[datetime] = None,
            end_at: Optional[datetime] = None,
            retry_count: int = 3,
            timeout: Optional[float] = None,
            tags: List[str] = None,
            task_id: Optional[str] = None
    ) -> str:
        """创建固定间隔周期任务

        Args:
            interval_seconds: 执行间隔（秒）
            task_data: 任务数据
            name: 任务名称
            description: 任务描述
            priority: 优先级
            max_runs: 最大执行次数
            start_at: 开始时间
            end_at: 结束时间
            retry_count: 重试次数
            timeout: 单次执行超时
            tags: 标签
            task_id: 任务ID（可选）

        Returns:
            周期任务ID
        """
        task_id = task_id or self._generate_task_id()

        definition = PeriodicTaskDefinition(
            task_id=task_id,
            name=name or task_id,
            description=description,
            interval_seconds=interval_seconds,
            priority=priority,
            task_data=task_data,
            max_runs=max_runs,
            start_at=start_at,
            end_at=end_at,
            retry_count=retry_count,
            timeout=timeout,
            tags=tags or [],
            ttl=timeout or 3600
        )

        # 计算下次执行时间
        next_run = self._calculate_next_run(definition)

        instance = PeriodicTaskInstance(
            task_id=task_id,
            definition=definition,
            next_run=next_run
        )

        async with self._lock:
            self._tasks[task_id] = instance
            self._update_stats()

        # 持久化
        await self._save_task(instance)

        return task_id

    async def create_cron(
            self,
            cron_expr: str,
            task_data: Dict[str, Any],
            name: str = "",
            description: str = "",
            priority: int = 2,
            max_runs: Optional[int] = None,
            start_at: Optional[datetime] = None,
            end_at: Optional[datetime] = None,
            retry_count: int = 3,
            timeout: Optional[float] = None,
            tags: List[str] = None,
            task_id: Optional[str] = None
    ) -> str:
        """创建Cron周期任务

        Args:
            cron_expr: Cron表达式
            task_data: 任务数据
            name: 任务名称
            description: 任务描述
            priority: 优先级
            max_runs: 最大执行次数
            start_at: 开始时间
            end_at: 结束时间
            retry_count: 重试次数
            timeout: 单次执行超时
            tags: 标签
            task_id: 任务ID（可选）

        Returns:
            周期任务ID
        """
        task_id = task_id or self._generate_task_id()

        # 解析Cron表达式
        cron_obj = CronParser.parse(cron_expr)

        definition = PeriodicTaskDefinition(
            task_id=task_id,
            name=name or task_id,
            description=description,
            cron_expr=cron_expr,
            cron_obj=cron_obj,
            priority=priority,
            task_data=task_data,
            max_runs=max_runs,
            start_at=start_at,
            end_at=end_at,
            retry_count=retry_count,
            timeout=timeout,
            tags=tags or [],
            ttl=timeout or 3600
        )

        # 计算下次执行时间
        next_run = self._calculate_next_run(definition)

        instance = PeriodicTaskInstance(
            task_id=task_id,
            definition=definition,
            next_run=next_run
        )

        async with self._lock:
            self._tasks[task_id] = instance
            self._update_stats()

        # 持久化
        await self._save_task(instance)

        return task_id

    # ========== 任务管理 API ==========

    async def update_task(
            self,
            task_id: str,
            **kwargs
    ) -> bool:
        """更新周期任务配置

        Args:
            task_id: 任务ID
            **kwargs: 要更新的字段

        Returns:
            是否成功
        """
        async with self._lock:
            if task_id not in self._tasks:
                return False

            instance = self._tasks[task_id]
            definition = instance.definition

            # 更新定义字段
            for key, value in kwargs.items():
                if hasattr(definition, key):
                    setattr(definition, key, value)

            # 特殊处理：更新Cron表达式时重新解析
            if "cron_expr" in kwargs:
                definition.cron_obj = CronParser.parse(definition.cron_expr)

            # 重新计算下次执行时间
            instance.next_run = self._calculate_next_run(definition)
            instance.updated_at = datetime.now()
            instance.version += 1

            # 持久化
            await self._save_task(instance)

        return True

    async def delete_task(self, task_id: str) -> bool:
        """删除周期任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功
        """
        async with self._lock:
            if task_id not in self._tasks:
                return False

            instance = self._tasks[task_id]
            instance.status = PeriodicTaskStatus.STOPPED
            del self._tasks[task_id]
            self._update_stats()

        # 持久化删除
        await self._delete_task(task_id)

        return True

    async def pause(self, task_id: str) -> bool:
        """暂停周期任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功
        """
        async with self._lock:
            if task_id not in self._tasks:
                return False

            instance = self._tasks[task_id]
            if instance.status == PeriodicTaskStatus.ACTIVE:
                instance.status = PeriodicTaskStatus.PAUSED
                instance.paused_at = datetime.now()
                instance.updated_at = datetime.now()
                await self._save_task(instance)
                self._update_stats()
                return True

        return False

    async def resume(self, task_id: str) -> bool:
        """恢复周期任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功
        """
        async with self._lock:
            if task_id not in self._tasks:
                return False

            instance = self._tasks[task_id]
            if instance.status == PeriodicTaskStatus.PAUSED:
                instance.status = PeriodicTaskStatus.ACTIVE
                instance.paused_at = None
                instance.updated_at = datetime.now()
                # 重新计算下次执行时间
                instance.next_run = self._calculate_next_run(instance.definition)
                await self._save_task(instance)
                self._update_stats()
                return True

        return False

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取周期任务信息

        Args:
            task_id: 任务ID

        Returns:
            任务信息字典
        """
        async with self._lock:
            instance = self._tasks.get(task_id)
            if not instance:
                return None
            return self._instance_to_dict(instance)

    async def get_task_instance(self, task_id: str) -> Optional[PeriodicTaskInstance]:
        """获取周期任务实例

        Args:
            task_id: 任务ID

        Returns:
            周期任务实例，不存在则返回None
        """
        async with self._lock:
            return self._tasks.get(task_id)

    async def list_tasks(
            self,
            status: Optional[PeriodicTaskStatus] = None,
            tags: Optional[List[str]] = None,
            limit: int = 100,
            offset: int = 0
    ) -> List[Dict[str, Any]]:
        """列出周期任务

        Args:
            status: 状态过滤
            tags: 标签过滤
            limit: 返回数量限制
            offset: 偏移量

        Returns:
            任务信息列表
        """
        async with self._lock:
            tasks = list(self._tasks.values())

            # 状态过滤
            if status:
                tasks = [t for t in tasks if t.status == status]

            # 标签过滤
            if tags:
                tasks = [t for t in tasks if any(tag in t.definition.tags for tag in tags)]

            # 排序（按下次执行时间）
            tasks.sort(key=lambda t: t.next_run or datetime.max)

            # 分页
            tasks = tasks[offset:offset + limit]

            return [self._instance_to_dict(t) for t in tasks]

    # ========== 调度循环 ==========

    async def _scheduler_loop(self) -> None:
        """调度循环"""
        while self._running:
            try:
                now = datetime.now()
                tasks_to_execute = []

                async with self._lock:
                    for task_id, instance in self._tasks.items():
                        # 检查状态
                        if instance.status != PeriodicTaskStatus.ACTIVE:
                            continue

                        # 检查是否达到最大执行次数
                        if (instance.definition.max_runs and
                                instance.run_count >= instance.definition.max_runs):
                            instance.status = PeriodicTaskStatus.COMPLETED
                            instance.stopped_at = now
                            continue

                        # 检查是否在时间范围内
                        if instance.definition.start_at and now < instance.definition.start_at:
                            continue
                        if instance.definition.end_at and now > instance.definition.end_at:
                            instance.status = PeriodicTaskStatus.COMPLETED
                            instance.stopped_at = now
                            continue

                        # 检查是否需要执行
                        if instance.next_run and instance.next_run <= now:
                            tasks_to_execute.append((task_id, instance))

                # 执行任务
                for task_id, instance in tasks_to_execute:
                    await self._execute_periodic_task(task_id, instance)

                # 更新统计
                self._stats["last_scan"] = now.isoformat()

                await asyncio.sleep(self._scan_interval)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self._scan_interval)

    async def _execute_periodic_task(self, task_id: str, instance: PeriodicTaskInstance) -> None:
        """执行周期任务

        Args:
            task_id: 周期任务ID
            instance: 周期任务实例
        """
        definition = instance.definition

        # 处理错过的执行
        if instance.next_run:
            missed_count = self._calculate_missed_count(instance)
            if missed_count > 1:
                await self._handle_missed_executions(instance, missed_count)

        # 更新执行记录
        execution_id = self._generate_execution_id()
        instance.run_count += 1
        instance.last_run = datetime.now()

        try:
            # 提交任务到TaskPool
            task_instance_id = await self._task_pool.submit_async(
                data=definition.task_data,
                priority=definition.priority,
                ttl=definition.ttl
            )

            # 记录执行
            record = PeriodicExecutionRecord(
                execution_id=execution_id,
                task_id=task_id,
                task_instance_id=task_instance_id,
                scheduled_time=instance.next_run or datetime.now(),
                start_time=datetime.now()
            )

            # 等待任务完成（可选）
            if definition.timeout:
                try:
                    result = await self._task_pool.wait_for_result_async(
                        task_instance_id,
                        timeout=definition.timeout
                    )
                    instance.success_count += 1
                    instance.last_success = datetime.now()
                    record.status = "success"
                    record.result = result
                except Exception as e:
                    instance.failed_count += 1
                    instance.last_error = str(e)
                    record.status = "failed"
                    record.error = str(e)
            else:
                # 异步执行，不等待结果
                record.status = "running"

            await self._save_execution_record(record)

        except Exception as e:
            instance.failed_count += 1
            instance.last_error = str(e)

        # 更新下次执行时间
        instance.next_run = self._calculate_next_run(definition, after=datetime.now())
        instance.updated_at = datetime.now()

        # 更新统计
        self._stats["total_executions"] += 1

        # 持久化
        await self._save_task(instance)

    def _calculate_missed_count(self, instance: PeriodicTaskInstance) -> int:
        """计算错过的执行次数"""
        if not instance.next_run:
            return 0

        now = datetime.now()
        if instance.definition.interval_seconds:
            # 固定间隔
            elapsed = (now - instance.next_run).total_seconds()
            return max(1, int(elapsed / instance.definition.interval_seconds))
        else:
            # Cron表达式
            # 简化处理，只返回1
            return 1

    async def _handle_missed_executions(
            self,
            instance: PeriodicTaskInstance,
            missed_count: int
    ) -> None:
        """处理错过的执行

        Args:
            instance: 周期任务实例
            missed_count: 错过次数
        """
        policy = instance.definition.missed_policy

        if policy == MissedExecutionPolicy.IGNORE:
            # 忽略，只更新下次执行时间
            pass

        elif policy == MissedExecutionPolicy.RUN_ONCE:
            # 只执行一次
            pass

        elif policy == MissedExecutionPolicy.CATCH_UP:
            # 追赶执行（最多5次，避免雪崩）
            catch_up_count = min(missed_count, 5)
            for _ in range(catch_up_count):
                await self._execute_periodic_task(instance.task_id, instance)

        elif policy == MissedExecutionPolicy.SKIP:
            # 跳过，只更新下次执行时间
            pass

    # ========== 辅助方法 ==========

    def _calculate_next_run(
            self,
            definition: PeriodicTaskDefinition,
            after: Optional[datetime] = None
    ) -> Optional[datetime]:
        """计算下次执行时间

        Args:
            definition: 任务定义
            after: 起始时间

        Returns:
            下次执行时间
        """
        now = after or datetime.now()

        if definition.interval_seconds:
            # 固定间隔
            if definition.start_at and now < definition.start_at:
                return definition.start_at
            return now + timedelta(seconds=definition.interval_seconds)

        elif definition.cron_obj:
            # Cron表达式
            start = definition.start_at or now
            next_time = definition.cron_obj.next(after=start)
            return next_time

        return None

    def _generate_task_id(self) -> str:
        """生成周期任务ID"""
        return f"periodic_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"

    def _generate_execution_id(self) -> str:
        """生成执行记录ID"""
        return f"exec_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"

    def _instance_to_dict(self, instance: PeriodicTaskInstance) -> Dict[str, Any]:
        """将实例转换为字典"""
        return {
            "task_id": instance.task_id,
            "name": instance.definition.name,
            "description": instance.definition.description,
            "status": instance.status.value,
            "is_paused": instance.status == PeriodicTaskStatus.PAUSED,
            "priority": instance.definition.priority,
            "interval_seconds": instance.definition.interval_seconds,
            "cron_expr": instance.definition.cron_expr,
            "run_count": instance.run_count,
            "success_count": instance.success_count,
            "failed_count": instance.failed_count,
            "last_run": instance.last_run.isoformat() if instance.last_run else None,
            "last_success": instance.last_success.isoformat() if instance.last_success else None,
            "next_run": instance.next_run.isoformat() if instance.next_run else None,
            "start_at": instance.definition.start_at.isoformat() if instance.definition.start_at else None,
            "end_at": instance.definition.end_at.isoformat() if instance.definition.end_at else None,
            "max_runs": instance.definition.max_runs,
            "tags": instance.definition.tags,
            "created_at": instance.created_at.isoformat(),
            "updated_at": instance.updated_at.isoformat(),
            "version": instance.version
        }

    def _update_stats(self) -> None:
        """更新统计信息"""
        self._stats["total_tasks"] = len(self._tasks)
        self._stats["active_tasks"] = sum(
            1 for t in self._tasks.values()
            if t.status == PeriodicTaskStatus.ACTIVE
        )
        self._stats["paused_tasks"] = sum(
            1 for t in self._tasks.values()
            if t.status == PeriodicTaskStatus.PAUSED
        )

    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        async with self._lock:
            return self._stats.copy()

    async def get_execution_history(
            self,
            task_id: str,
            limit: int = 50
    ) -> List[Dict[str, Any]]:
        """获取任务执行历史

        Args:
            task_id: 周期任务ID
            limit: 返回数量限制

        Returns:
            执行历史列表
        """
        # TODO: 从存储获取执行历史
        return []

    # ========== 持久化方法 ==========

    async def _load_tasks(self) -> None:
        """从存储加载周期任务"""
        if not self._storage:
            return

        try:
            # TODO: 实现从存储加载
            pass
        except Exception:
            pass

    async def _save_tasks(self) -> None:
        """保存所有周期任务到存储"""
        if not self._storage:
            return

        async with self._lock:
            for instance in self._tasks.values():
                await self._save_task(instance)

    async def _save_task(self, instance: PeriodicTaskInstance) -> None:
        """保存单个周期任务"""
        if not self._storage:
            return

        try:
            key = f"periodic_task:{instance.task_id}"
            data = self._instance_to_dict(instance)
            # TODO: 实现保存到存储
        except Exception:
            pass

    async def _delete_task(self, task_id: str) -> None:
        """删除周期任务存储"""
        if not self._storage:
            return

        try:
            key = f"periodic_task:{task_id}"
            # TODO: 实现从存储删除
        except Exception:
            pass

    async def _save_execution_record(self, record: PeriodicExecutionRecord) -> None:
        """保存执行记录"""
        if not self._storage:
            return

        try:
            key = f"periodic_execution:{record.execution_id}"
            # TODO: 实现保存执行记录
        except Exception:
            pass


# 便捷函数
def create_periodic_manager(task_pool, storage=None) -> PeriodicTaskManager:
    """创建周期任务管理器

    Args:
        task_pool: TaskPool实例
        storage: 持久化存储

    Returns:
        PeriodicTaskManager实例
    """
    return PeriodicTaskManager(task_pool, storage)
