"""
@FileName: test_sqlite_compat.py
@Description: sqlite_compat 跨事件循环连接回归测试 - 防止 aiosqlite 连接与单一 loop 耦合。
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

import asyncio

import pytest

from neotask.utils.sqlite_compat import create_connection


def _use_connection_in_standalone_loop(conn) -> None:
    """在一个全新的（非测试）事件循环中建表并写入数据。"""

    async def _run() -> None:
        await conn.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
        await conn.execute("INSERT OR REPLACE INTO kv VALUES (?, ?)", ("k", "from-loop-a"))
        await conn.commit()

    asyncio.run(_run())


@pytest.mark.asyncio
async def test_connection_reusable_across_event_loops(tmp_path):
    """同一连接先在独立 loop 中建连，再回到测试 loop 使用，不应报跨 loop 错误。"""
    conn = create_connection(str(tmp_path / "crossloop.db"))
    try:
        # 模拟 TaskPool 内部守护 loop：连接在另一个 loop 中首次使用
        await asyncio.get_running_loop().run_in_executor(
            None, _use_connection_in_standalone_loop, conn
        )

        # 切回当前测试 loop，继续使用同一个连接
        cursor = await conn.execute("SELECT v FROM kv WHERE k = ?", ("k",))
        row = await cursor.fetchone()
        assert row[0] == "from-loop-a"

        cursor = await conn.execute("SELECT k FROM kv")
        assert [r[0] for r in await cursor.fetchall()] == ["k"]

        cursor = await conn.execute("DELETE FROM kv WHERE k = ?", ("k",))
        assert cursor.rowcount == 1
        await conn.commit()

        # 关闭后再次使用应自动重建连接
        await conn.close()
        cursor = await conn.execute("SELECT COUNT(*) FROM kv")
        assert (await cursor.fetchone())[0] == 0
    finally:
        await conn.close()


def _init_repository_in_standalone_loop(repo) -> None:
    asyncio.run(repo._ensure_init())


@pytest.mark.asyncio
async def test_task_repository_across_event_loops(tmp_path):
    """SQLiteTaskRepository 在后台 loop 初始化后，可在调用方 loop 正常读写。"""
    from neotask.models.task import Task, TaskPriority
    from neotask.storage.sqlite import SQLiteTaskRepository

    repo = SQLiteTaskRepository(str(tmp_path / "tasks.db"))
    try:
        await asyncio.get_running_loop().run_in_executor(
            None, _init_repository_in_standalone_loop, repo
        )

        task = Task(task_id="cross-loop-1", data={"x": 1}, priority=TaskPriority.NORMAL)
        await repo.save(task)

        retrieved = await repo.get("cross-loop-1")
        assert retrieved is not None
        assert retrieved.task_id == "cross-loop-1"

        await repo.delete("cross-loop-1")
        assert await repo.get("cross-loop-1") is None
    finally:
        await repo.close()


def _init_periodic_store_in_standalone_loop(store) -> None:
    asyncio.run(store._ensure_init())


@pytest.mark.asyncio
async def test_periodic_store_across_event_loops(tmp_path):
    """SQLitePeriodicStore 同样支持跨 loop 读写。"""
    from neotask.storage.periodic import SQLitePeriodicStore

    store = SQLitePeriodicStore(str(tmp_path / "periodic.db"))
    try:
        await asyncio.get_running_loop().run_in_executor(
            None, _init_periodic_store_in_standalone_loop, store
        )

        await store.save_task("periodic-1", {"task_id": "periodic-1", "cron": "* * * * *"})
        loaded = await store.load_task("periodic-1")
        assert loaded is not None
        assert loaded["cron"] == "* * * * *"

        await store.save_execution("exec-1", {"task_id": "periodic-1", "status": "success"})
        executions = await store.load_executions("periodic-1")
        assert len(executions) == 1

        await store.delete_task("periodic-1")
        assert await store.load_task("periodic-1") is None
    finally:
        await store.close()
