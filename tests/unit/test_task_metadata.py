"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_task_metadata.py
@Description: Task 模型元数据字段测试（成本、耗时、父子关联）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import json
from datetime import datetime, timedelta, timezone

from neotask.models.task import Task, TaskStatus


class TestMetadataDefaults:
    """新增字段的默认值与序列化"""

    def test_defaults(self):
        """不传新字段时全部取默认值，既有构造调用不受影响"""
        task = Task(task_id="meta-1", data={})

        assert task.checkpoint_version == 0
        assert task.parent_task_id is None
        assert task.estimated_cost == 0.0
        assert task.cost_tracking == {}
        assert task.queue_wait_ms is None
        assert task.duration_ms is None

    def test_to_dict_serializes_cost_tracking(self):
        """cost_tracking 落库为 JSON 文本；空明细写空串"""
        task = Task(
            task_id="meta-2",
            data={},
            parent_task_id="parent-1",
            estimated_cost=0.25,
            cost_tracking={"tokens_in": 10, "currency": "USD"},
        )
        data = task.to_dict()

        assert data["parent_task_id"] == "parent-1"
        assert data["estimated_cost"] == 0.25
        assert json.loads(data["cost_tracking"]) == {"tokens_in": 10, "currency": "USD"}

        empty = Task(task_id="meta-3", data={}).to_dict()
        assert empty["cost_tracking"] == ""
        assert empty["parent_task_id"] == ""

    def test_roundtrip(self):
        """to_dict → from_dict 后新字段完整保留"""
        task = Task(
            task_id="meta-4",
            data={"k": "v"},
            checkpoint_version=3,
            parent_task_id="parent-2",
            estimated_cost=1.5,
            cost_tracking={"tokens_in": 50, "tokens_out": 20, "api_calls": 2, "currency": "EUR"},
            queue_wait_ms=12.5,
            duration_ms=340.0,
        )

        restored = Task.from_dict(task.to_dict())

        assert restored.checkpoint_version == 3
        assert restored.parent_task_id == "parent-2"
        assert restored.estimated_cost == 1.5
        assert restored.cost_tracking == {
            "tokens_in": 50, "tokens_out": 20, "api_calls": 2, "currency": "EUR",
        }
        assert restored.queue_wait_ms == 12.5
        assert restored.duration_ms == 340.0

    def test_from_dict_legacy_payload(self):
        """1.0.x 的旧字典没有新键，反序列化应取默认值"""
        restored = Task.from_dict({
            "task_id": "legacy-1",
            "data": "{}",
            "status": "success",
            "priority": 2,
            "created_at": "2026-11-10T10:00:00+00:00",
        })

        assert restored.checkpoint_version == 0
        assert restored.parent_task_id is None
        assert restored.estimated_cost == 0.0
        assert restored.cost_tracking == {}
        assert restored.queue_wait_ms is None
        assert restored.duration_ms is None

    def test_from_dict_empty_strings(self):
        """JSON 列写空串（to_dict 的缺省输出）反序列化为默认值"""
        restored = Task.from_dict({
            "task_id": "legacy-2",
            "data": "{}",
            "parent_task_id": "",
            "cost_tracking": "",
            "queue_wait_ms": None,
            "duration_ms": None,
        })

        assert restored.parent_task_id is None
        assert restored.cost_tracking == {}
        assert restored.queue_wait_ms is None
        assert restored.duration_ms is None


class TestTimingFields:
    """start/complete/fail/cancel 自动记录耗时"""

    def test_start_records_queue_wait(self):
        task = Task(
            task_id="timing-1",
            data={},
            created_at=datetime.now(timezone.utc) - timedelta(milliseconds=250),
        )

        task.start("node-1")

        assert task.queue_wait_ms is not None
        assert task.queue_wait_ms >= 240
        assert task.duration_ms is None

    def test_complete_records_duration(self):
        task = Task(task_id="timing-2", data={})
        task.start("node-1")
        task.started_at = datetime.now(timezone.utc) - timedelta(milliseconds=500)

        task.complete({"ok": True})

        assert task.duration_ms is not None
        assert 490 <= task.duration_ms <= 2000

    def test_fail_records_duration(self):
        task = Task(task_id="timing-3", data={})
        task.start("node-1")
        task.started_at = datetime.now(timezone.utc) - timedelta(milliseconds=300)

        task.fail("boom")

        assert task.duration_ms is not None
        assert 290 <= task.duration_ms <= 2000

    def test_cancel_records_duration(self):
        task = Task(task_id="timing-4", data={})
        task.start("node-1")
        task.started_at = datetime.now(timezone.utc) - timedelta(milliseconds=200)

        task.cancel()

        assert task.duration_ms is not None
        assert 190 <= task.duration_ms <= 2000

    def test_complete_without_start_leaves_duration_none(self):
        """未经过 start 的任务无法推导执行时长"""
        task = Task(task_id="timing-5", data={})

        task.complete({"ok": True})

        assert task.status == TaskStatus.SUCCESS
        assert task.queue_wait_ms is None
        assert task.duration_ms is None

    def test_reset_for_retry_clears_timing(self):
        task = Task(task_id="timing-6", data={})
        task.start("node-1")
        task.fail("boom")
        assert task.duration_ms is not None

        task.reset_for_retry()

        assert task.status == TaskStatus.PENDING
        assert task.queue_wait_ms is None
        assert task.duration_ms is None
        assert task.started_at is None
        assert task.completed_at is None
