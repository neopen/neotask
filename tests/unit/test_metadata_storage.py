"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_metadata_storage.py
@Description: 三个存储后端的父子查询与成本聚合测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio
import os
import tempfile
from datetime import timedelta
from uuid import uuid4

import pytest

from neotask.models.task import Task, TaskPriority
from neotask.storage.memory import MemoryTaskRepository
from neotask.storage.redis import RedisTaskRepository
from neotask.storage.sqlite import SQLiteTaskRepository


def _task(task_id, *, parent=None, offset_seconds=0, tokens_in=0,
          tokens_out=0, api_calls=0, amount=0.0, currency=None):
    """构造带成本/父子信息的任务，offset_seconds 控制创建时间先后"""
    task = Task(
        task_id=task_id,
        data={},
        priority=TaskPriority.NORMAL,
        parent_task_id=parent,
        estimated_cost=amount,
    )
    if tokens_in or tokens_out or api_calls or currency:
        task.cost_tracking = {
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "api_calls": api_calls,
        }
        if currency:
            task.cost_tracking["currency"] = currency
    if offset_seconds:
        task.created_at = task.created_at + timedelta(seconds=offset_seconds)
    return task


# ========== Memory ==========

class TestMemoryMetadata:
    @pytest.fixture
    def repo(self):
        return MemoryTaskRepository()

    async def test_list_by_parent_filters_and_orders(self, repo):
        await repo.save(_task("m-child-late", parent="m-parent", offset_seconds=2))
        await repo.save(_task("m-child-early", parent="m-parent", offset_seconds=1))
        await repo.save(_task("m-other", parent="m-other-parent"))

        children = await repo.list_by_parent("m-parent")

        assert [t.task_id for t in children] == ["m-child-early", "m-child-late"]

    async def test_list_by_parent_pagination(self, repo):
        for i in range(5):
            await repo.save(_task(f"m-page-{i}", parent="m-page-parent",
                                  offset_seconds=i))

        page = await repo.list_by_parent("m-page-parent", limit=2, offset=2)

        assert [t.task_id for t in page] == ["m-page-2", "m-page-3"]

    async def test_list_by_parent_returns_copy(self, repo):
        """返回的是副本，修改它不会污染仓储内的任务"""
        await repo.save(_task("m-copy", parent="m-copy-parent"))

        children = await repo.list_by_parent("m-copy-parent")
        children[0].estimated_cost = 99.0

        assert (await repo.get("m-copy")).estimated_cost == 0.0

    async def test_cost_summary_selected_tasks(self, repo):
        await repo.save(_task("m-cost-1", tokens_in=100, tokens_out=10,
                              api_calls=1, amount=0.5, currency="USD"))
        await repo.save(_task("m-cost-2", tokens_in=50, tokens_out=5,
                              amount=0.25, currency="USD"))
        await repo.save(_task("m-cost-none"))

        summary = await repo.cost_summary(["m-cost-1", "m-cost-2", "m-missing"])

        assert summary["task_count"] == 2
        assert summary["tasks_with_cost"] == 2
        assert summary["tokens_in"] == 150
        assert summary["tokens_out"] == 15
        assert summary["api_calls"] == 1
        assert summary["estimated_cost"] == pytest.approx(0.75)
        assert summary["currency"] == "USD"

    async def test_cost_summary_all_marks_mixed_currency(self, repo):
        await repo.save(_task("m-all-1", amount=1.0, currency="USD"))
        await repo.save(_task("m-all-2", amount=2.0, currency="EUR"))
        await repo.save(_task("m-all-3"))

        summary = await repo.cost_summary()

        assert summary["task_count"] == 3
        assert summary["tasks_with_cost"] == 2
        assert summary["estimated_cost"] == pytest.approx(3.0)
        assert summary["currency"] == "MIXED"


# ========== SQLite ==========

class TestSQLiteMetadata:
    @pytest.fixture
    async def repo(self):
        db_path = None
        repo = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
                db_path = f.name
            repo = SQLiteTaskRepository(db_path)
            await repo._ensure_init()
            yield repo
        finally:
            if repo:
                await repo.close()
            if db_path and os.path.exists(db_path):
                await asyncio.sleep(0.1)
                try:
                    os.unlink(db_path)
                except PermissionError:
                    pass

    async def test_cost_tracking_json_roundtrip(self, repo):
        """cost_tracking 以 JSON 文本落库，读取后还原成 dict"""
        await repo.save(_task("s-cost", tokens_in=7, amount=0.3, currency="USD"))

        restored = await repo.get("s-cost")

        assert restored.estimated_cost == pytest.approx(0.3)
        assert restored.cost_tracking == {
            "tokens_in": 7, "tokens_out": 0, "api_calls": 0, "currency": "USD",
        }

    async def test_save_fields_updates_cost_columns(self, repo):
        """save_fields 只更新成本列（高频追加成本路径）"""
        task = _task("s-fields")
        await repo.save(task)

        task.estimated_cost = 0.9
        task.cost_tracking = {"tokens_in": 20, "currency": "USD"}
        assert await repo.save_fields(task, ("estimated_cost", "cost_tracking"))

        restored = await repo.get("s-fields")
        assert restored.estimated_cost == pytest.approx(0.9)
        assert restored.cost_tracking == {"tokens_in": 20, "currency": "USD"}

    async def test_list_by_parent_uses_index(self, repo):
        await repo.save(_task("s-child-late", parent="s-parent", offset_seconds=2))
        await repo.save(_task("s-child-early", parent="s-parent", offset_seconds=1))
        await repo.save(_task("s-unrelated"))

        children = await repo.list_by_parent("s-parent")
        assert [t.task_id for t in children] == ["s-child-early", "s-child-late"]

        indexes = await repo._conn.execute("PRAGMA index_list(tasks)")
        try:
            names = {row[1] for row in await indexes.fetchall()}
        finally:
            await indexes.close()
        assert "idx_tasks_parent" in names

    async def test_cost_summary_aggregates_in_sql(self, repo):
        await repo.save(_task("s-sum-1", tokens_in=100, tokens_out=10,
                              api_calls=2, amount=0.5, currency="USD"))
        await repo.save(_task("s-sum-2", tokens_in=50, tokens_out=5,
                              amount=0.25, currency="USD"))
        await repo.save(_task("s-sum-none"))

        summary = await repo.cost_summary()

        assert summary["task_count"] == 3
        assert summary["tasks_with_cost"] == 2
        assert summary["tokens_in"] == 150
        assert summary["tokens_out"] == 15
        assert summary["api_calls"] == 2
        assert summary["estimated_cost"] == pytest.approx(0.75)
        assert summary["currency"] == "USD"

    async def test_cost_summary_task_ids_subset(self, repo):
        await repo.save(_task("s-sub-1", tokens_in=10, amount=1.0, currency="USD"))
        await repo.save(_task("s-sub-2", tokens_in=20, amount=2.0, currency="USD"))

        summary = await repo.cost_summary(["s-sub-2", "s-missing"])

        assert summary["task_count"] == 1
        assert summary["tokens_in"] == 20
        assert summary["estimated_cost"] == pytest.approx(2.0)


# ========== Redis（需要 REDIS_TEST_URL） ==========

@pytest.mark.skipif(
    not os.environ.get("REDIS_TEST_URL"),
    reason="REDIS_TEST_URL environment variable not set",
)
class TestRedisMetadata:
    @pytest.fixture
    async def redis_env(self):
        """返回 (repo, track)：track(*task_ids) 登记测试期间创建的键，退出时清理"""
        repo = RedisTaskRepository(os.environ["REDIS_TEST_URL"])
        created = []

        def track(*task_ids):
            created.extend(task_ids)

        try:
            yield repo, track
        finally:
            for task_id in created:
                await repo.delete(task_id)
            await repo.close()

    async def test_list_by_parent_and_cleanup(self, redis_env):
        repo, track = redis_env
        prefix = f"meta-{uuid4().hex[:8]}"
        parent = f"{prefix}-parent"
        child_late = f"{prefix}-late"
        child_early = f"{prefix}-early"
        track(child_late, child_early)

        await repo.save(_task(child_late, parent=parent, offset_seconds=2))
        await repo.save(_task(child_early, parent=parent, offset_seconds=1))

        children = await repo.list_by_parent(parent)
        assert [t.task_id for t in children] == [child_early, child_late]

        # 删除子任务后父集合中的残留索引应被清理
        await repo.delete(child_early)
        assert [t.task_id for t in await repo.list_by_parent(parent)] == [child_late]

    async def test_cost_summary_selected_tasks(self, redis_env):
        repo, track = redis_env
        prefix = f"meta-{uuid4().hex[:8]}"
        ids = [f"{prefix}-1", f"{prefix}-2"]
        track(*ids)

        await repo.save(_task(ids[0], tokens_in=10, amount=1.0, currency="USD"))
        await repo.save(_task(ids[1], tokens_in=20, amount=2.0, currency="USD"))

        summary = await repo.cost_summary(ids)

        assert summary["task_count"] == 2
        assert summary["tasks_with_cost"] == 2
        assert summary["tokens_in"] == 30
        assert summary["estimated_cost"] == pytest.approx(3.0)
        assert summary["currency"] == "USD"
