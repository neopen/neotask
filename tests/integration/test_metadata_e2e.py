"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_metadata_e2e.py
@Description: 成本采集与父子关联的端到端测试（TaskPool / TaskScheduler / SQLite 重启）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio
import os
from uuid import uuid4

import pytest

from neotask import TaskPool, TaskPoolConfig, report_cost
from neotask.api.task_scheduler import TaskScheduler
from neotask.common.constants import EventType
from neotask.models.config import SchedulerConfig
from neotask.storage.redis import RedisTaskRepository


async def metadata_executor(data: dict) -> dict:
    """按 data.report 决定是否上报成本"""
    await asyncio.sleep(0.1)
    if data.get("report"):
        report_cost(
            tokens_in=data.get("in", 0),
            tokens_out=data.get("out", 0),
            api_calls=data.get("calls", 1),
            estimated_cost=data.get("cost", 0.0),
            currency=data.get("currency", "USD"),
        )
    return {"echo": data.get("id")}


@pytest.fixture
def pool():
    task_pool = TaskPool(
        executor=metadata_executor,
        config=TaskPoolConfig(worker_concurrency=2, max_retries=0),
    )
    task_pool.start()
    try:
        yield task_pool
    finally:
        task_pool.shutdown()


class TestCostEndToEnd:
    async def test_executor_cost_flows_to_result_summary_and_event(self, pool):
        completed = []
        pool.on_completed(lambda event: completed.append(event))

        task_id = pool.submit({"id": "cost-1", "report": True, "in": 100, "out": 40})
        pool.wait_for_result(task_id, timeout=10)

        info = pool.get_result(task_id)
        assert info["estimated_cost"] == pytest.approx(0.0)
        assert info["cost_tracking"]["tokens_in"] == 100
        assert info["cost_tracking"]["tokens_out"] == 40
        assert info["cost_tracking"]["api_calls"] == 1
        assert info["queue_wait_ms"] is not None
        assert info["duration_ms"] is not None

        summary = pool.get_cost_summary()
        assert summary["task_count"] == 1
        assert summary["tasks_with_cost"] == 1
        assert summary["tokens_in"] == 100
        assert summary["currency"] == "USD"

        await asyncio.sleep(0.2)
        assert len(completed) == 1
        assert completed[0].data == {
            "result": {"echo": "cost-1"},
            "cost": {
                "tokens_in": 100,
                "tokens_out": 40,
                "api_calls": 1,
                "estimated_cost": 0.0,
                "currency": "USD",
            },
        }

    async def test_completed_event_keeps_raw_result_without_cost(self, pool):
        completed = []
        pool.on_completed(lambda event: completed.append(event))

        task_id = pool.submit({"id": "no-cost"})
        pool.wait_for_result(task_id, timeout=10)
        await asyncio.sleep(0.2)

        assert len(completed) == 1
        assert completed[0].data == {"echo": "no-cost"}
        assert pool.get_result(task_id)["estimated_cost"] == 0.0

    async def test_external_report_after_terminal(self, pool):
        cost_events = []
        pool.event_bus.subscribe(EventType.COST_REPORTED, lambda event: cost_events.append(event))

        task_id = pool.submit({"id": "late-cost"})
        pool.wait_for_result(task_id, timeout=10)

        assert pool.report_cost(
            task_id, tokens_in=5, api_calls=2, estimated_cost=0.1, currency="USD"
        ) is True
        assert pool.report_cost("missing-task", tokens_in=1) is False

        info = pool.get_result(task_id)
        assert info["estimated_cost"] == pytest.approx(0.1)
        assert info["cost_tracking"] == {
            "tokens_in": 5, "tokens_out": 0, "api_calls": 2, "currency": "USD",
        }

        subset = pool.get_cost_summary(task_ids=[task_id, "missing-task"])
        assert subset["task_count"] == 1
        assert subset["estimated_cost"] == pytest.approx(0.1)

        await asyncio.sleep(0.2)
        assert len(cost_events) == 1
        assert cost_events[0].data["tokens_in"] == 5


class TestParentChildEndToEnd:
    async def test_parent_child_lookup(self, pool):
        parent = pool.submit({"id": "parent"}, task_id="e2e-parent")
        children = [
            pool.submit({"id": f"child-{i}"}, parent_task_id=parent)
            for i in range(3)
        ]
        pool.wait_all(children, timeout=10)

        assert pool.get_children(parent) == children
        assert pool.get_parent(children[1]) == parent
        assert pool.get_parent(parent) is None
        assert pool.get_children("missing-parent") == []
        assert pool.get_result(children[0])["parent_task_id"] == parent

    async def test_scheduler_passthrough(self):
        scheduler = TaskScheduler(
            executor=metadata_executor,
            config=SchedulerConfig(
                storage_type="memory",
                worker_concurrency=2,
                max_retries=0,
            ),
        )
        scheduler.start()
        try:
            parent = scheduler.submit({"id": "sched-parent"}, task_id="sched-parent")
            child = scheduler.submit(
                {"id": "sched-child", "report": True, "in": 1, "out": 1, "cost": 0.05},
                parent_task_id=parent,
            )
            scheduler.wait_for_result(child, timeout=10)

            assert scheduler.get_children(parent) == [child]
            assert scheduler.get_parent(child) == parent

            assert scheduler.report_cost(
                child, tokens_in=9, estimated_cost=0.01, currency="USD"
            ) is True
            info = scheduler.get_result(child)
            assert info["cost_tracking"]["tokens_in"] == 10
            summary = scheduler.get_cost_summary(task_ids=[child])
            assert summary["estimated_cost"] == pytest.approx(0.06)
        finally:
            scheduler.shutdown()


class TestSQLitePersistence:
    async def test_metadata_survives_restart(self, tmp_path):
        db_path = str(tmp_path / "metadata.db")

        def make_pool() -> TaskPool:
            task_pool = TaskPool(
                executor=metadata_executor,
                config=TaskPoolConfig(
                    storage_type="sqlite",
                    sqlite_path=db_path,
                    worker_concurrency=2,
                    max_retries=0,
                ),
            )
            task_pool.start()
            return task_pool

        first = make_pool()
        try:
            parent = first.submit({"id": "persist-parent"}, task_id="persist-parent")
            child = first.submit(
                {"id": "persist-child", "report": True, "in": 70, "out": 30, "cost": 0.3},
                parent_task_id=parent,
            )
            first.wait_for_result(child, timeout=10)
        finally:
            first.shutdown()

        second = make_pool()
        try:
            info = second.get_result(child)
            assert info["parent_task_id"] == parent
            assert info["estimated_cost"] == pytest.approx(0.3)
            assert info["cost_tracking"] == {
                "tokens_in": 70, "tokens_out": 30, "api_calls": 1, "currency": "USD",
            }
            assert info["queue_wait_ms"] is not None
            assert info["duration_ms"] is not None

            assert second.get_children(parent) == [child]

            summary = second.get_cost_summary(task_ids=[child])
            assert summary["estimated_cost"] == pytest.approx(0.3)
            assert summary["tokens_in"] == 70
        finally:
            second.shutdown()
            await asyncio.sleep(0.1)


@pytest.mark.skipif(
    not os.environ.get("REDIS_TEST_URL"),
    reason="REDIS_TEST_URL environment variable not set",
)
class TestRedisEndToEnd:
    async def test_cost_and_parent_flow_through_redis(self):
        url = os.environ["REDIS_TEST_URL"]
        prefix = f"e2e-{uuid4().hex[:8]}"
        parent_id = f"{prefix}-parent"
        child_id = None

        pool = TaskPool(
            executor=metadata_executor,
            config=TaskPoolConfig(
                storage_type="redis",
                redis_url=url,
                worker_concurrency=2,
                max_retries=0,
            ),
        )
        pool.start()
        try:
            parent = pool.submit({"id": "redis-parent"}, task_id=parent_id)
            child_id = pool.submit(
                {"id": "redis-child", "report": True, "in": 3, "out": 4, "cost": 0.2},
                parent_task_id=parent,
            )
            pool.wait_for_result(child_id, timeout=10)

            assert pool.get_children(parent) == [child_id]
            info = pool.get_result(child_id)
            assert info["parent_task_id"] == parent
            assert info["estimated_cost"] == pytest.approx(0.2)
            assert info["cost_tracking"]["currency"] == "USD"
        finally:
            pool.shutdown()
            repo = RedisTaskRepository(url)
            try:
                for task_id in (parent_id, child_id):
                    if task_id:
                        await repo.delete(task_id)
            finally:
                await repo.close()
