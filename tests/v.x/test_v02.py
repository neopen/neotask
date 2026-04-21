# test_v02.py
"""v0.2 功能快速验证脚本"""

import asyncio
import os
import tempfile


async def test_priority_queue():
    """测试优先级队列"""
    from neotask.queue.priority_queue import PriorityQueue

    q = PriorityQueue()

    # 入队
    await q.push("low", 3)
    await q.push("high", 1)
    await q.push("critical", 0)
    await q.push("normal", 2)

    # 出队验证优先级
    tasks = await q.pop(4)
    assert tasks == ["critical", "high", "normal", "low"]

    print("✅ 优先级队列测试通过")


async def test_memory_storage():
    """测试内存存储"""
    from neotask.storage.memory import MemoryTaskRepository
    from neotask.models.task import Task, TaskStatus

    repo = MemoryTaskRepository()

    task = Task(
        task_id="test_001",
        data={"key": "value"},
        status=TaskStatus.PENDING
    )

    await repo.save(task)
    retrieved = await repo.get("test_001")
    assert retrieved is not None
    assert retrieved.task_id == "test_001"

    await repo.update_status("test_001", TaskStatus.RUNNING)
    updated = await repo.get("test_001")
    assert updated.status == TaskStatus.RUNNING

    print("✅ 内存存储测试通过")


async def test_sqlite_storage():
    """测试 SQLite 存储"""
    from neotask.storage.sqlite import SQLiteTaskRepository
    from neotask.models.task import Task, TaskStatus

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        repo = SQLiteTaskRepository(db_path)

        task = Task(
            task_id="test_002",
            data={"key": "value"},
            status=TaskStatus.PENDING
        )

        await repo.save(task)
        retrieved = await repo.get("test_002")
        assert retrieved is not None

        # 验证持久化
        repo2 = SQLiteTaskRepository(db_path)
        retrieved2 = await repo2.get("test_002")
        assert retrieved2 is not None

        print("✅ SQLite 存储测试通过")
    finally:
        os.unlink(db_path)


async def test_event_bus():
    """测试事件总线"""
    from neotask.event.bus import EventBus, TaskEvent

    bus = EventBus()
    await bus.start()

    received = []

    async def handler(event):
        received.append(event.event_type)

    bus.subscribe("test.event", handler)

    await bus.emit(TaskEvent("test.event", "task_001"))
    await asyncio.sleep(0.1)

    assert "test.event" in received

    await bus.stop()
    print("✅ 事件总线测试通过")


async def test_metrics():
    """测试指标收集"""
    from neotask.monitor.metrics import MetricsCollector

    collector = MetricsCollector()

    await collector.record_task_submit("task_001")
    await asyncio.sleep(0.05)

    await collector.record_task_start("task_001")
    await asyncio.sleep(0.05)

    await collector.record_task_complete("task_001")
    await asyncio.sleep(0.05)

    summary = collector.get_summary()
    assert summary["total_submitted"] == 1
    assert summary["total_completed"] == 1
    assert summary["success_rate"] == 1.0

    print("✅ 指标收集测试通过")


async def test_task_pool():
    """测试 TaskPool 完整流程"""
    from neotask.api.task_pool import TaskPool, TaskPoolConfig

    async def executor(data):
        return {"result": f"processed_{data.get('id', 'unknown')}"}

    config = TaskPoolConfig(storage_type="memory")
    pool = TaskPool(executor, config)
    pool.start()

    try:
        # 提交任务
        task_id = await pool.submit_async({"id": 1, "data": "test"})
        assert task_id is not None

        # 等待结果
        result = await pool.wait_for_result_async(task_id, timeout=5)
        assert result["result"] == "processed_1"

        # 获取统计
        stats = await pool.get_stats_async()
        assert stats["total"] >= 1
        assert stats["completed"] >= 1

        print("✅ TaskPool 集成测试通过")

    finally:
        pool.shutdown()


async def main():
    print("\n=== NeoTask v0.2 功能验证 ===\n")

    tests = [
        ("优先级队列", test_priority_queue),
        ("内存存储", test_memory_storage),
        ("SQLite存储", test_sqlite_storage),
        ("事件总线", test_event_bus),
        ("指标收集", test_metrics),
        ("TaskPool集成", test_task_pool),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            await test_func()
            passed += 1
        except Exception as e:
            print(f"❌ {name}测试失败: {e}")
            failed += 1

    print(f"\n{'=' * 40}")
    print(f"测试结果: {passed} 通过, {failed} 失败")

    if failed == 0:
        print("\n✅ v0.2 所有功能验证通过！")
    else:
        print("\n❌ 部分测试失败，请检查")


if __name__ == "__main__":
    asyncio.run(main())
