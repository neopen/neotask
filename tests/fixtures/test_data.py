"""
@FileName: test_data.py
@Description: 测试数据定义
@Author: HiPeng
@Time: 2026/4/21
"""

from typing import Dict, Any, List
from datetime import datetime, timedelta, timezone

from neotask.models.task import Task, TaskStatus, TaskPriority


class TestData:
    """测试数据生成器"""

    @staticmethod
    def sample_task_data() -> Dict[str, Any]:
        """获取示例任务数据"""
        return {
            "simple": {"key": "value", "test": True},
            "complex": {
                "user_id": 12345,
                "action": "process",
                "metadata": {
                    "source": "api",
                    "timestamp": datetime.now().isoformat()
                }
            },
            "nested": {
                "level1": {
                    "level2": {
                        "level3": "deep_value"
                    }
                }
            },
            "with_list": {
                "items": [1, 2, 3, 4, 5],
                "names": ["a", "b", "c"]
            }
        }

    @staticmethod
    def create_task(
        task_id: str = None,
        data: Dict[str, Any] = None,
        status: TaskStatus = TaskStatus.PENDING,
        priority: TaskPriority = TaskPriority.NORMAL,
        node_id: str = "",
        retry_count: int = 0,
        ttl: int = 3600
    ) -> Task:
        """创建测试任务"""
        from neotask.models.task import Task
        import uuid

        return Task(
            task_id=task_id or f"test_task_{uuid.uuid4().hex[:8]}",
            data=data or {"test": "data"},
            status=status,
            priority=priority,
            node_id=node_id,
            retry_count=retry_count,
            ttl=ttl
        )

    @staticmethod
    def create_tasks(count: int, **kwargs) -> List[Task]:
        """批量创建测试任务"""
        return [TestData.create_task(**kwargs) for _ in range(count)]

    @staticmethod
    def create_tasks_with_priorities() -> List[Task]:
        """创建不同优先级的任务"""
        return [
            TestData.create_task(priority=TaskPriority.CRITICAL, data={"priority": "critical"}),
            TestData.create_task(priority=TaskPriority.HIGH, data={"priority": "high"}),
            TestData.create_task(priority=TaskPriority.NORMAL, data={"priority": "normal"}),
            TestData.create_task(priority=TaskPriority.LOW, data={"priority": "low"}),
        ]

    @staticmethod
    def create_tasks_with_statuses() -> List[Task]:
        """创建不同状态的任务"""
        return [
            TestData.create_task(status=TaskStatus.PENDING, data={"status": "pending"}),
            TestData.create_task(status=TaskStatus.RUNNING, data={"status": "running"}),
            TestData.create_task(status=TaskStatus.SUCCESS, data={"status": "success"}),
            TestData.create_task(status=TaskStatus.FAILED, data={"status": "failed"}),
            TestData.create_task(status=TaskStatus.CANCELLED, data={"status": "cancelled"}),
        ]

    @staticmethod
    def create_expired_task(ttl: int = 1) -> Task:
        """创建已过期的任务"""
        task = TestData.create_task(ttl=ttl)
        task.created_at = datetime.now(timezone.utc) - timedelta(seconds=ttl + 10)
        return task


# 预定义的测试数据常量
SAMPLE_TASK_IDS = [
    "task_001",
    "task_002",
    "task_003",
    "task_004",
    "task_005",
]

SAMPLE_DATA_SIMPLE = {"key": "value", "number": 42}
SAMPLE_DATA_NESTED = {"user": {"id": 1, "name": "test"}, "items": [1, 2, 3]}
SAMPLE_DATA_LARGE = {f"key_{i}": f"value_{i}" for i in range(100)}