"""
@FileName: test_periodic_manager.py
@Description: 周期任务管理器单元测试
@Author: HiPeng
@Time: 2026/4/21
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock

from neotask.scheduler.periodic import PeriodicTaskManager
from neotask.models.schedule import PeriodicTaskStatus, MissedExecutionPolicy


class TestPeriodicTaskManager:
    """周期任务管理器测试"""

    @pytest.fixture
    async def mock_task_pool(self):
        """模拟 TaskPool"""
        mock = Mock()
        mock.submit_async = AsyncMock(return_value="task_instance_123")
        mock.wait_for_result_async = AsyncMock(return_value={"result": "success"})
        return mock

    @pytest.fixture
    async def periodic_manager(self, mock_task_pool):
        """创建周期任务管理器"""
        manager = PeriodicTaskManager(mock_task_pool, storage=None)
        manager._scan_interval = 0.05  # 加快扫描速度
        await manager.start()
        yield manager
        await manager.stop()

    async def test_create_interval_task(self, periodic_manager):
        """测试创建固定间隔任务"""
        task_id = await periodic_manager.create_interval(
            interval_seconds=60,
            task_data={"action": "test"},
            name="test_interval",
            priority=1,
            max_runs=10
        )

        assert task_id is not None
        assert task_id in periodic_manager._tasks

        task = await periodic_manager.get_task(task_id)
        assert task is not None
        assert task["name"] == "test_interval"
        assert task["interval_seconds"] == 60
        assert task["max_runs"] == 10

    async def test_create_cron_task(self, periodic_manager):
        """测试创建Cron任务"""
        task_id = await periodic_manager.create_cron(
            cron_expr="0 9 * * *",
            task_data={"action": "daily"},
            name="test_cron",
            priority=2
        )

        assert task_id is not None
        assert task_id in periodic_manager._tasks

        task = await periodic_manager.get_task(task_id)
        assert task is not None
        assert task["cron_expr"] == "0 9 * * *"

    async def test_pause_and_resume_task(self, periodic_manager):
        """测试暂停和恢复任务"""
        task_id = await periodic_manager.create_interval(
            interval_seconds=60,
            task_data={"action": "test"}
        )

        # 暂停
        result = await periodic_manager.pause(task_id)
        assert result is True

        task = await periodic_manager.get_task(task_id)
        assert task["status"] == PeriodicTaskStatus.PAUSED.value

        # 恢复
        result = await periodic_manager.resume(task_id)
        assert result is True

        task = await periodic_manager.get_task(task_id)
        assert task["status"] == PeriodicTaskStatus.ACTIVE.value

    async def test_delete_task(self, periodic_manager):
        """测试删除任务"""
        task_id = await periodic_manager.create_interval(
            interval_seconds=60,
            task_data={"action": "test"}
        )

        assert task_id in periodic_manager._tasks

        result = await periodic_manager.delete_task(task_id)
        assert result is True
        assert task_id not in periodic_manager._tasks

    async def test_update_task(self, periodic_manager):
        """测试更新任务"""
        task_id = await periodic_manager.create_interval(
            interval_seconds=60,
            task_data={"action": "test"},
            name="original_name"
        )

        # 更新
        result = await periodic_manager.update_task(
            task_id,
            name="updated_name",
            priority=3
        )
        assert result is True

        task = await periodic_manager.get_task(task_id)
        assert task["name"] == "updated_name"
        assert task["priority"] == 3

    async def test_list_tasks(self, periodic_manager):
        """测试列出任务"""
        # 创建多个任务
        await periodic_manager.create_interval(
            interval_seconds=60,
            task_data={"action": "task1"},
            tags=["tag_a"]
        )
        await periodic_manager.create_interval(
            interval_seconds=120,
            task_data={"action": "task2"},
            tags=["tag_b"]
        )
        await periodic_manager.create_cron(
            cron_expr="0 9 * * *",
            task_data={"action": "task3"},
            tags=["tag_a"]
        )

        # 列出所有任务
        all_tasks = await periodic_manager.list_tasks()
        assert len(all_tasks) == 3

        # 按标签过滤
        tagged_tasks = await periodic_manager.list_tasks(tags=["tag_a"])
        assert len(tagged_tasks) == 2

        # 分页
        paged_tasks = await periodic_manager.list_tasks(limit=2, offset=0)
        assert len(paged_tasks) == 2

    async def test_get_stats(self, periodic_manager):
        """测试获取统计信息"""
        await periodic_manager.create_interval(
            interval_seconds=60,
            task_data={"action": "task1"}
        )
        await periodic_manager.create_interval(
            interval_seconds=120,
            task_data={"action": "task2"}
        )

        stats = await periodic_manager.get_stats()
        assert stats["total_tasks"] == 2
        assert stats["active_tasks"] == 2
        assert stats["paused_tasks"] == 0

    async def test_max_runs_limit(self, periodic_manager, mock_task_pool):
        """测试最大执行次数限制"""
        task_id = await periodic_manager.create_interval(
            interval_seconds=0.05,  # 短间隔用于测试
            task_data={"action": "test"},
            max_runs=3
        )

        # 等待多次执行
        await asyncio.sleep(0.25)

        # 检查执行次数
        task = await periodic_manager.get_task(task_id)
        assert task["run_count"] <= 3

        # 检查是否已完成
        task = await periodic_manager.get_task(task_id)
        if task["run_count"] == 3:
            assert task["status"] == PeriodicTaskStatus.COMPLETED.value

    async def test_execute_periodic_task(self, periodic_manager, mock_task_pool):
        """测试执行周期任务"""
        task_id = await periodic_manager.create_interval(
            interval_seconds=0.05,
            task_data={"action": "test"},
            timeout=1.0
        )

        # 等待执行
        await asyncio.sleep(0.1)

        # 验证任务被提交
        mock_task_pool.submit_async.assert_called()
        assert mock_task_pool.submit_async.call_count >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
