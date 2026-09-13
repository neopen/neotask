"""
@FileName: test_periodic_manager.py
@Description: 周期任务管理器单元测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
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

        # COMPLETED 由下一次扫描写入，这里轮询等待而不是立即断言（避免时序抖动）
        if task["run_count"] == 3:
            deadline = asyncio.get_running_loop().time() + 1.0
            while asyncio.get_running_loop().time() < deadline:
                task = await periodic_manager.get_task(task_id)
                if task["status"] == PeriodicTaskStatus.COMPLETED.value:
                    break
                await asyncio.sleep(0.02)
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


class TestMissedExecutionPolicy:
    """错过执行策略与 run_immediately 测试

    这些测试不启动调度循环，直接调用 `_execute_periodic_task`，
    从而对 next_run 的推进与提交次数做确定性断言。
    """

    @pytest.fixture
    def manager(self):
        """不启动调度循环的管理器"""
        mock = Mock()
        mock.submit_async = AsyncMock(return_value="task_instance_123")
        mock.wait_for_result_async = AsyncMock(return_value={"result": "success"})
        return PeriodicTaskManager(mock, storage=None)

    @staticmethod
    async def _make_overdue(manager, policy, seconds_ago=600, **kwargs):
        """创建一个 next_run 已过期 seconds_ago 秒的任务"""
        task_id = await manager.create_interval(
            interval_seconds=60,
            task_data={"action": "test"},
            missed_policy=policy,
            **kwargs
        )
        instance = await manager.get_task_instance(task_id)
        instance.run_count = 0
        instance.next_run = datetime.now() - timedelta(seconds=seconds_ago)
        return task_id, instance

    async def test_run_immediately_default(self, manager):
        """默认 run_immediately=True 时 next_run 立即到期"""
        task_id = await manager.create_interval(
            interval_seconds=60,
            task_data={"action": "test"}
        )

        instance = await manager.get_task_instance(task_id)
        assert instance.next_run <= datetime.now()

    async def test_run_immediately_false_waits_one_interval(self, manager):
        """run_immediately=False 时首次执行要等一个间隔"""
        task_id = await manager.create_interval(
            interval_seconds=60,
            task_data={"action": "test"},
            run_immediately=False
        )

        instance = await manager.get_task_instance(task_id)
        delay = (instance.next_run - datetime.now()).total_seconds()
        assert 55 < delay <= 60

    async def test_future_start_at_not_overridden(self, manager):
        """start_at 在未来时 run_immediately 不覆盖它"""
        start_at = datetime.now() + timedelta(hours=2)
        task_id = await manager.create_interval(
            interval_seconds=60,
            task_data={"action": "test"},
            start_at=start_at
        )

        instance = await manager.get_task_instance(task_id)
        assert abs((instance.next_run - start_at).total_seconds()) < 1

    @pytest.mark.parametrize("policy", [MissedExecutionPolicy.SKIP, MissedExecutionPolicy.IGNORE])
    async def test_skip_and_ignore_advance_without_running(self, manager, policy):
        """SKIP 与 IGNORE 同义：丢弃错过的槽位且本轮不提交"""
        task_id, instance = await self._make_overdue(manager, policy)
        before = datetime.now()

        await manager._execute_periodic_task(task_id, instance)

        assert manager._task_pool.submit_async.call_count == 0
        assert instance.run_count == 0
        assert instance.next_run > before

    async def test_run_once_submits_exactly_once(self, manager):
        """RUN_ONCE 只补一次，不追赶所有错过的槽位"""
        task_id, instance = await self._make_overdue(manager, MissedExecutionPolicy.RUN_ONCE)

        await manager._execute_periodic_task(task_id, instance)

        assert manager._task_pool.submit_async.call_count == 1
        assert instance.run_count == 1

    async def test_catch_up_is_capped(self, manager):
        """CATCH_UP 补跑错过的槽位，但单轮不超过 MAX_CATCH_UP"""
        task_id, instance = await self._make_overdue(
            manager, MissedExecutionPolicy.CATCH_UP, seconds_ago=600
        )

        await manager._execute_periodic_task(task_id, instance)

        assert manager._task_pool.submit_async.call_count == manager.MAX_CATCH_UP
        assert instance.run_count == manager.MAX_CATCH_UP

    async def test_catch_up_respects_max_runs(self, manager):
        """CATCH_UP 补跑过程不超过 max_runs"""
        task_id, instance = await self._make_overdue(
            manager, MissedExecutionPolicy.CATCH_UP, seconds_ago=600, max_runs=2
        )

        await manager._execute_periodic_task(task_id, instance)

        assert manager._task_pool.submit_async.call_count == 2
        assert instance.run_count == 2

    async def test_normal_jitter_is_not_treated_as_missed(self, manager):
        """扫描抖动落在容忍窗口内时不算错过，仍正常提交一次"""
        task_id, instance = await self._make_overdue(
            manager, MissedExecutionPolicy.SKIP, seconds_ago=0.5
        )

        await manager._execute_periodic_task(task_id, instance)

        assert manager._task_pool.submit_async.call_count == 1
        assert instance.run_count == 1

    async def test_cron_missed_slots_counted(self, manager):
        """Cron 任务也能统计错过的槽位并补跑"""
        task_id = await manager.create_cron(
            cron_expr="* * * * *",
            task_data={"action": "test"},
            missed_policy=MissedExecutionPolicy.CATCH_UP
        )
        instance = await manager.get_task_instance(task_id)
        instance.run_count = 0
        instance.next_run = datetime.now() - timedelta(seconds=240)

        missed = manager._count_missed_slots(instance, datetime.now())
        await manager._execute_periodic_task(task_id, instance)

        assert missed >= 2
        assert manager._task_pool.submit_async.call_count == missed + 1

    async def test_cron_default_policy_skips(self, manager):
        """Cron 默认策略为 SKIP，错过的槽位直接丢弃"""
        task_id = await manager.create_cron(
            cron_expr="* * * * *",
            task_data={"action": "test"}
        )
        instance = await manager.get_task_instance(task_id)
        assert instance.definition.missed_policy == MissedExecutionPolicy.SKIP

        instance.next_run = datetime.now() - timedelta(seconds=240)
        await manager._execute_periodic_task(task_id, instance)

        assert manager._task_pool.submit_async.call_count == 0

    async def test_policy_accepts_string(self, manager):
        """字符串形式的策略会被归一化为枚举"""
        task_id = await manager.create_interval(
            interval_seconds=60,
            task_data={"action": "test"},
            missed_policy="CATCH_UP"
        )
        instance = await manager.get_task_instance(task_id)
        assert instance.definition.missed_policy == MissedExecutionPolicy.CATCH_UP

    async def test_invalid_policy_rejected(self, manager):
        """非法策略字符串会被拒绝"""
        with pytest.raises(ValueError):
            await manager.create_interval(
                interval_seconds=60,
                task_data={"action": "test"},
                missed_policy="explode"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
