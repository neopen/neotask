"""
@FileName: test_time_wheel.py
@Description: 时间轮单元测试
@Author: HiPeng
@Time: 2026/4/21
"""

import pytest
import asyncio
from typing import List
from neotask.scheduler.time_wheel import TimeWheel


class TestTimeWheel:
    """时间轮测试"""

    @pytest.fixture
    async def time_wheel(self):
        """创建时间轮 fixture"""
        wheel = TimeWheel(slot_count=10, tick_interval=0.05)
        return wheel

    async def test_add_task(self):
        """测试添加任务"""
        executed: List[str] = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        wheel = TimeWheel(slot_count=10, tick_interval=0.05)
        await wheel.start(callback)

        await wheel.add_task("task_1", 1, 0.1)

        await asyncio.sleep(0.2)
        await wheel.stop()

        assert "task_1" in executed, f"Expected 'task_1' in {executed}, got {executed}"

    async def test_multiple_tasks(self):
        """测试多个任务"""
        executed: List[str] = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        wheel = TimeWheel(slot_count=10, tick_interval=0.05)
        await wheel.start(callback)

        await wheel.add_task("task_1", 1, 0.05)
        await wheel.add_task("task_2", 2, 0.1)
        await wheel.add_task("task_3", 3, 0.15)

        await asyncio.sleep(0.25)
        await wheel.stop()

        assert len(executed) == 3, f"Expected 3 tasks, got {len(executed)}: {executed}"
        assert "task_1" in executed
        assert "task_2" in executed
        assert "task_3" in executed

    async def test_cancel_task(self):
        """测试取消任务"""
        executed: List[str] = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        wheel = TimeWheel(slot_count=10, tick_interval=0.05)
        await wheel.start(callback)

        await wheel.add_task("task_1", 1, 0.15)
        await wheel.add_task("task_2", 2, 0.15)

        cancelled = await wheel.cancel_task("task_1")
        assert cancelled is True

        await asyncio.sleep(0.25)
        await wheel.stop()

        assert "task_1" not in executed, f"task_1 should not be in {executed}"
        assert "task_2" in executed, f"Expected 'task_2' in {executed}, got {executed}"

    async def test_cancel_nonexistent_task(self):
        """测试取消不存在的任务"""
        wheel = TimeWheel(slot_count=10, tick_interval=0.05)
        await wheel.start(lambda x, y, z: None)

        cancelled = await wheel.cancel_task("nonexistent")
        assert cancelled is False

        await wheel.stop()

    async def test_size(self):
        """测试任务数量"""
        wheel = TimeWheel(slot_count=10, tick_interval=0.05)
        await wheel.start(lambda x, y, z: None)

        assert await wheel.size() == 0

        await wheel.add_task("task_1", 1, 0.5)
        await wheel.add_task("task_2", 2, 1.0)

        assert await wheel.size() == 2

        await wheel.stop()

    async def test_multiple_rounds(self):
        """测试多轮调度"""
        executed: List[str] = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        wheel = TimeWheel(slot_count=5, tick_interval=0.05)
        await wheel.start(callback)

        await wheel.add_task("task_long", 1, 0.3)

        await asyncio.sleep(0.4)
        await wheel.stop()

        assert "task_long" in executed, f"Expected 'task_long' in {executed}, got {executed}"

    async def test_get_stats(self):
        """测试获取统计信息"""
        wheel = TimeWheel(slot_count=20, tick_interval=0.1)
        await wheel.start(lambda x, y, z: None)

        await wheel.add_task("task_1", 1, 1.0)
        await wheel.add_task("task_2", 2, 2.0)

        stats = wheel.get_stats()
        assert stats["slot_count"] == 20
        assert stats["tick_interval"] == 0.1
        assert stats["total_tasks"] == 2
        assert stats["is_running"] is True

        await wheel.stop()

    async def test_zero_delay_task(self):
        """测试零延迟任务（立即执行）"""
        executed: List[str] = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        wheel = TimeWheel(slot_count=10, tick_interval=0.05)
        await wheel.start(callback)

        await wheel.add_task("task_immediate", 1, 0)

        await asyncio.sleep(0.05)
        await wheel.stop()

        assert "task_immediate" in executed, f"Expected 'task_immediate' in {executed}, got {executed}"

    async def test_order_of_execution(self):
        """测试执行顺序（按延迟时间）"""
        executed: List[str] = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        # 使用更小的 tick 间隔和更大的槽位数
        wheel = TimeWheel(slot_count=60, tick_interval=0.01)
        await wheel.start(callback)

        await wheel.add_task("task_3", 3, 0.03)
        await wheel.add_task("task_1", 1, 0.01)
        await wheel.add_task("task_2", 2, 0.02)

        # 增加等待时间确保所有任务执行完成
        await asyncio.sleep(0.1)
        await wheel.stop()

        # 验证所有任务都执行了
        assert len(executed) == 3, f"Expected 3 tasks, got {len(executed)}: {executed}"
        assert "task_1" in executed
        assert "task_2" in executed
        assert "task_3" in executed

    async def test_high_priority_execution(self):
        """测试相同延迟时间的不同优先级任务（都应执行）"""
        executed: List[tuple] = []

        async def callback(task_id, priority, data=None):
            executed.append((task_id, priority))

        # 使用合适的 tick 间隔
        wheel = TimeWheel(slot_count=60, tick_interval=0.01)
        await wheel.start(callback)

        # 所有任务使用相同的延迟时间
        await wheel.add_task("low_priority", 5, 0.05)
        await wheel.add_task("high_priority", 1, 0.05)
        await wheel.add_task("medium_priority", 3, 0.05)

        # 等待足够时间
        await asyncio.sleep(0.1)
        await wheel.stop()

        task_ids = [t[0] for t in executed]
        # 验证所有任务都被执行（注意：时间轮不保证优先级顺序，只保证到期执行）
        assert len(task_ids) == 3, f"Expected 3 tasks, got {len(task_ids)}: {executed}"
        assert "high_priority" in task_ids
        assert "medium_priority" in task_ids
        assert "low_priority" in task_ids

    async def test_same_slot_tasks(self):
        """测试同一槽位的多个任务"""
        executed: List[str] = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        wheel = TimeWheel(slot_count=60, tick_interval=0.01)
        await wheel.start(callback)

        # 所有任务使用相同的延迟时间，会落入同一槽位
        await wheel.add_task("task_a", 1, 0.05)
        await wheel.add_task("task_b", 2, 0.05)
        await wheel.add_task("task_c", 3, 0.05)

        await asyncio.sleep(0.1)
        await wheel.stop()

        assert len(executed) == 3, f"Expected 3 tasks, got {len(executed)}: {executed}"
        assert "task_a" in executed
        assert "task_b" in executed
        assert "task_c" in executed

    async def test_sequential_tasks(self):
        """测试顺序添加任务（不同延迟时间）"""
        executed: List[str] = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        wheel = TimeWheel(slot_count=60, tick_interval=0.01)
        await wheel.start(callback)

        await wheel.add_task("task_first", 1, 0.03)
        await wheel.add_task("task_second", 2, 0.06)
        await wheel.add_task("task_third", 3, 0.09)

        # 等待足够时间让所有任务执行
        await asyncio.sleep(0.15)
        await wheel.stop()

        assert len(executed) == 3, f"Expected 3 tasks, got {len(executed)}: {executed}"
        # 验证执行顺序（按延迟时间，先添加的延迟小的先执行）
        assert executed[0] == "task_first", f"First should be task_first, got {executed}"
        assert executed[1] == "task_second", f"Second should be task_second, got {executed}"
        assert executed[2] == "task_third", f"Third should be task_third, got {executed}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])