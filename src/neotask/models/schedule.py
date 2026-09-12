"""
@FileName: schedule.py
@Description: 
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/9 15:25
"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Any, Dict, List

from neotask.scheduler import CronExpression


@dataclass
class DelayedTask:
    """延迟任务"""
    task_id: str
    execute_at: float  # Unix timestamp
    priority: int
    data: Optional[Dict] = None

    def __lt__(self, other):
        return self.execute_at < other.execute_at


@dataclass
class PeriodicTask:
    """周期任务定义"""
    task_id: str
    data: Dict[str, Any]
    interval_seconds: float
    priority: int
    cron_expr: Optional[str] = None
    cron_obj: Optional[CronExpression] = None
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    run_count: int = 0
    is_paused: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    max_runs: Optional[int] = None  # 最大执行次数，None表示无限


class PeriodicTaskStatus(str, Enum):
    """周期任务状态"""
    ACTIVE = "active"  # 活跃
    PAUSED = "paused"  # 暂停
    STOPPED = "stopped"  # 已停止
    COMPLETED = "completed"  # 已完成（达到最大执行次数）


class MissedExecutionPolicy(str, Enum):
    """错过执行策略"""
    IGNORE = "ignore"  # 忽略错过的执行
    RUN_ONCE = "run_once"  # 只执行一次（不追赶）
    CATCH_UP = "catch_up"  # 追赶所有错过的执行
    SKIP = "skip"  # 跳过错过的执行


@dataclass
class PeriodicTaskDefinition:
    """周期任务定义"""
    task_id: str
    name: str = ""
    description: str = ""

    # 调度配置
    interval_seconds: Optional[float] = None  # 固定间隔
    cron_expr: Optional[str] = None  # Cron表达式
    cron_obj: Optional[CronExpression] = None  # 解析后的Cron对象

    # 执行配置
    priority: int = 2  # 优先级 0-3
    task_data: Dict[str, Any] = field(default_factory=dict)
    ttl: int = 3600  # 任务超时时间

    # 生命周期配置
    max_runs: Optional[int] = None  # 最大执行次数
    start_at: Optional[datetime] = None  # 开始时间
    end_at: Optional[datetime] = None  # 结束时间

    # 重试配置
    retry_count: int = 3  # 失败重试次数
    retry_delay: float = 1.0  # 重试延迟

    # 其他配置
    timeout: Optional[float] = None  # 单次执行超时
    missed_policy: MissedExecutionPolicy = MissedExecutionPolicy.SKIP
    timezone: str = "UTC"  # 时区

    # 元数据
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str = ""
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PeriodicTaskInstance:
    """周期任务实例（运行时状态）"""
    task_id: str
    definition: PeriodicTaskDefinition
    status: PeriodicTaskStatus = PeriodicTaskStatus.ACTIVE

    # 执行统计
    run_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    last_run: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_error: Optional[str] = None
    next_run: Optional[datetime] = None

    # 时间信息
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    paused_at: Optional[datetime] = None
    stopped_at: Optional[datetime] = None

    # 版本控制
    version: int = 1


@dataclass
class PeriodicExecutionRecord:
    """周期任务执行记录"""
    execution_id: str
    task_id: str
    task_instance_id: str  # 具体执行的任务ID
    scheduled_time: datetime
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: str = "pending"  # pending, running, success, failed
    result: Optional[Dict] = None
    error: Optional[str] = None
    retry_count: int = 0

