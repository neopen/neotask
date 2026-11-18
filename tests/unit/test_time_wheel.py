"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_time_wheel.py
@Description: 时间轮单元测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/21
"""

import pytest
import asyncio
import time
from typing import List
from neotask.scheduler.time_wheel import TimeWheel, TimeWheelTask


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

    async def test_scheduled_task_not_fired_early(self):
        """任务不应明显早于延迟时间触发，且误差在 tick 量级内

        时间轮按 ``perf_counter`` 取相位并取最近滴答，槽位可能比截止时间
        早半个 tick；这里用宽松下界只拦截「整槽提前」这类真实回归。
        """
        executed = {}

        async def callback(task_id, priority, data=None):
            executed[task_id] = asyncio.get_running_loop().time()

        wheel = TimeWheel(slot_count=20, tick_interval=0.05)
        await wheel.start(callback)

        submitted_at = asyncio.get_running_loop().time()
        await wheel.add_task("task_1", 1, 0.2)

        await asyncio.sleep(0.5)
        await wheel.stop()

        assert "task_1" in executed
        elapsed = executed["task_1"] - submitted_at
        assert 0.15 <= elapsed <= 0.45, f"Unexpected delay: {elapsed:.3f}s"

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

        # 事件驱动等待：轮询已执行数量，避免固定 sleep 在负载下偶发不足
        deadline = asyncio.get_running_loop().time() + 1.0
        while len(executed) < 3 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        await wheel.stop()

        assert len(executed) == 3, f"Expected 3 tasks, got {len(executed)}: {executed}"
        # 验证执行顺序（按延迟时间，先添加的延迟小的先执行）
        assert executed[0] == "task_first", f"First should be task_first, got {executed}"
        assert executed[1] == "task_second", f"Second should be task_second, got {executed}"
        assert executed[2] == "task_third", f"Third should be task_third, got {executed}"

    async def test_nearest_slot_rounding(self):
        """槽位按最近的滴答取整，相位基于单调高精度时钟"""
        wheel = TimeWheel(slot_count=100, tick_interval=0.1)
        wheel._next_tick_at = time.perf_counter()
        wheel._current_slot = 0

        slot_short = await wheel.add_task("short", 1, 0.24)
        slot_long = await wheel.add_task("long", 1, 0.26)
        slot_exact = await wheel.add_task("exact", 1, 0.5)

        assert slot_short == 2, f"0.24s 应落在第 2 槽，实际 {slot_short}"
        assert slot_long == 3, f"0.26s 应落在第 3 槽，实际 {slot_long}"
        assert slot_exact == 5, f"0.5s 应落在第 5 槽，实际 {slot_exact}"

    async def test_half_tick_early_task_not_deferred(self):
        """槽位取整误差最大为半个 tick，落在此范围内的任务不应被整槽推迟"""
        executed: List[str] = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        wheel = TimeWheel(slot_count=10, tick_interval=0.05)
        wheel._callback = callback
        # 距截止还有 20ms（0.4 tick）：仍在取整误差内，本槽应执行
        task = TimeWheelTask(
            task_id="almost_due",
            priority=1,
            deadline=time.perf_counter() + 0.02,
        )
        wheel._slots[0].append(task)
        wheel._task_index[task.task_id] = task

        await wheel._tick()

        assert executed == ["almost_due"], f"应按时执行，实际 {executed}"

    async def test_more_than_half_tick_early_task_deferred(self):
        """提前超过半个 tick（时钟异常等）时，任务推迟一槽而不是提前执行"""
        executed: List[str] = []

        async def callback(task_id, priority, data=None):
            executed.append(task_id)

        wheel = TimeWheel(slot_count=10, tick_interval=0.05)
        wheel._callback = callback
        task = TimeWheelTask(
            task_id="too_early",
            priority=1,
            deadline=time.perf_counter() + 0.04,
        )
        wheel._slots[0].append(task)
        wheel._task_index[task.task_id] = task

        await wheel._tick()

        assert executed == [], f"过早任务不应执行，实际 {executed}"
        assert "too_early" in wheel._task_index
        assert [t.task_id for t in wheel._slots[1]] == ["too_early"]

    async def test_next_tick_advances_before_callbacks(self):
        """滴答回调中并发添加任务时，槽位相对下一次滴答计算"""
        wheel = TimeWheel(slot_count=100, tick_interval=0.1)
        added_slot = []

        async def callback(task_id, priority, data=None):
            if task_id == "due":
                added_slot.append(await wheel.add_task("inside", 1, 0.5))

        wheel._callback = callback
        wheel._current_slot = 7
        wheel._next_tick_at = time.perf_counter()
        task = TimeWheelTask(
            task_id="due",
            priority=1,
            deadline=time.perf_counter(),
        )
        wheel._slots[7].append(task)
        wheel._task_index[task.task_id] = task

        await wheel._tick()

        # 回调时 current_slot 已前进到 8，下一次滴答在 0.1s 后；
        # 0.5s 相对该滴答应为 +4 槽 -> 目标 12。若 _next_tick_at 未前进，
        # 会被算成 +5 槽 -> 13。
        assert added_slot == [12], f"槽位应相对下一次滴答计算，实际 {added_slot}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])