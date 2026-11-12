"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_business_failure.py
@Description: 业务级失败上报（v1.1.0 第 4 项）单元测试与持久化验证
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026-11-12
"""

import asyncio
import os
import time

import pytest

from neotask import TaskPool, TaskPoolConfig, report_failure
from neotask.common.constants import EventType
from neotask.core.failure import BusinessFailure
from neotask.models.task import Task, TaskStatus
from neotask.storage.redis import RedisTaskRepository
from neotask.storage.sqlite import SQLiteTaskRepository


def wait_until(pool, task_id, timeout=5.0):
    """轮询等待任务进入终态，返回状态字符串"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = pool.get_status(task_id)
        if status in ("failed", "success", "cancelled"):
            return status
        time.sleep(0.02)
    raise AssertionError(f"task {task_id} still {pool.get_status(task_id)!r} after {timeout}s")


class TestBusinessFailure:
    """失败登记簿本身的行为"""

    def test_first_record_wins(self):
        """后续登记不覆盖首条原因"""
        recorder = BusinessFailure()
        recorder.record("qa_failed", "score=10")
        recorder.record("risk_rejected", "blacklist")

        assert recorder.reason == "qa_failed"
        assert recorder.message == "qa_failed: score=10"

    def test_empty_reason_falls_back(self):
        """空原因仍要保证 error 非空"""
        recorder = BusinessFailure()
        assert recorder.triggered is False
        assert recorder.message == ""

        recorder.record("")
        assert recorder.reason == "business_failure"
        assert recorder.message == "business_failure"

    def test_message_without_detail(self):
        recorder = BusinessFailure()
        recorder.record("qa_failed")
        assert recorder.message == "qa_failed"

    def test_report_failure_outside_worker_returns_false(self):
        """不在 Worker 执行上下文中调用时返回 False，不抛异常"""
        assert report_failure("qa_failed") is False


class TestReportFailureInExecutor:
    """执行器内 report_failure 的收尾语义"""

    def test_business_failure_marks_failed_without_retry(self):
        """业务失败定稿 FAILED，且完全不消耗自动重试预算"""
        invocations = []

        async def executor(data):
            invocations.append(data)
            assert report_failure("qa_failed", "score=10") is True
            return {"score": data["score"]}

        config = TaskPoolConfig(max_retries=3, retry_delay=0.01)
        with TaskPool(executor=executor, config=config) as pool:
            events = []
            pool.event_bus.subscribe(
                EventType.FAILED,
                lambda event: events.append(event)
            )

            task_id = pool.submit({"score": 10})
            assert wait_until(pool, task_id) == "failed"
            time.sleep(0.2)  # 给事件投递留时间

            task = pool.get_task(task_id)
            assert task["status"] == "failed"
            assert task["failure_reason"] == "qa_failed"
            assert task["error"] == "qa_failed: score=10"
            assert task["retry_count"] == 0
            # 执行器只被调用一次，证明没有重试
            assert len(invocations) == 1

            assert events, "task.failed 事件应已投递"
            assert events[-1].data["reason"] == "qa_failed"
            assert events[-1].timestamp > 0

    def test_business_failure_visible_in_result(self):
        """get_result 暴露 failure_reason，调用方无需读完整任务记录"""

        async def executor(data):
            report_failure("risk_rejected")
            return {"ok": True}

        with TaskPool(executor=executor) as pool:
            task_id = pool.submit({"user": 1})
            wait_until(pool, task_id)
            result = pool.get_result(task_id)

        assert result["status"] == "failed"
        assert result["failure_reason"] == "risk_rejected"
        assert result["error"] == "risk_rejected"

    def test_thread_executor_can_report_failure(self):
        """线程执行器中的同步任务同样能登记原因

        ThreadExecutor 显式复制 contextvars，因此同步函数里也能拿到登记器。
        """
        import threading

        def sync_executor(data):
            # 同步函数运行在线程池线程中
            assert threading.current_thread() is not threading.main_thread()
            assert report_failure("qa_failed", "thread") is True
            return {"done": True}

        config = TaskPoolConfig(executor_type="thread", worker_concurrency=1)
        with TaskPool(executor=sync_executor, config=config) as pool:
            task_id = pool.submit({"n": 1})
            assert wait_until(pool, task_id) == "failed"
            assert pool.get_task(task_id)["failure_reason"] == "qa_failed"

    def test_success_path_leaves_reason_empty(self):
        """未登记失败的任务保持原有语义"""

        async def executor(data):
            return {"ok": True}

        with TaskPool(executor=executor) as pool:
            task_id = pool.submit({})
            assert wait_until(pool, task_id) == "success"
            assert pool.get_task(task_id)["failure_reason"] == ""


class TestExplicitReportFailure:
    """pool.report_failure 显式形态"""

    def test_explicit_form_fails_running_task(self):
        started = asyncio.Event()

        async def executor(data):
            started.set()
            await asyncio.sleep(30)
            return {"never": True}

        with TaskPool(executor=executor) as pool:
            task_id = pool.submit({})
            # 用业务上报把长任务判失败，而不是 cancel
            deadline = time.time() + 5
            while not started.is_set() and time.time() < deadline:
                time.sleep(0.02)
            assert pool.get_status(task_id) == "running"

            assert pool.report_failure(task_id, "cancelled_by_user", "explicit") is True
            assert wait_until(pool, task_id) == "failed"
            task = pool.get_task(task_id)
            assert task["failure_reason"] == "cancelled_by_user"
            assert task["error"] == "cancelled_by_user: explicit"

    def test_explicit_form_rejects_terminal_task(self):
        """已结束的任务不能被再次定为失败"""

        async def executor(data):
            return {"ok": True}

        with TaskPool(executor=executor) as pool:
            task_id = pool.submit({})
            assert wait_until(pool, task_id) == "success"
            assert pool.report_failure(task_id, "qa_failed") is False


class TestFailureReasonPersistence:
    """failure_reason 在各后端下的落库行为"""

    @pytest.mark.parametrize("storage_type", ["memory", "sqlite"])
    async def test_roundtrip(self, storage_type, task_repo_factory):
        repo = task_repo_factory(storage_type)
        task = Task(task_id="bf-1", data={"a": 1})
        await repo.save(task)

        task.fail("qa_failed: score=10", failure_reason="qa_failed")
        assert await repo.save_fields(
            task,
            ("status", "error", "failure_reason", "completed_at")
        )

        loaded = await repo.get("bf-1")
        assert loaded.status is TaskStatus.FAILED
        assert loaded.failure_reason == "qa_failed"

    async def test_survives_reopen(self, tmp_path):
        """重新打开同一个库文件后仍可读到原因（SQLite 持久化）"""
        db_path = str(tmp_path / "bf.db")
        repo = SQLiteTaskRepository(db_path)
        task = Task(task_id="bf-2", data={})
        await repo.save(task)
        task.fail("qa_failed", failure_reason="qa_failed")
        await repo.save_fields(task, ("status", "error", "failure_reason"))
        await repo.close()

        reopened = SQLiteTaskRepository(db_path)
        try:
            loaded = await reopened.get("bf-2")
            assert loaded.failure_reason == "qa_failed"
        finally:
            await reopened.close()


@pytest.mark.skipif(
    not os.environ.get("REDIS_TEST_URL"),
    reason="REDIS_TEST_URL environment variable not set"
)
class TestFailureReasonRedisPersistence:
    """Redis 后端持久化 failure_reason

    Redis 走默认的整条重写（``Task.to_dict``），新字段无需改存储层即可落库；
    此用例锁定该约定，避免日后有人改成按列更新时丢掉原因字段。
    """

    async def test_roundtrip(self):
        repo = RedisTaskRepository(os.environ["REDIS_TEST_URL"])
        try:
            task = Task(task_id="bf-redis", data={})
            await repo.save(task)

            task.fail("qa_failed: score=10", failure_reason="qa_failed")
            assert await repo.save_fields(
                task,
                ("status", "error", "failure_reason")
            )

            loaded = await repo.get("bf-redis")
            assert loaded.status is TaskStatus.FAILED
            assert loaded.failure_reason == "qa_failed"
        finally:
            await repo.delete("bf-redis")
            await repo.close()


class TestLegacySchemaUpgrade:
    """1.0.x 老库在 1.1.0 下启动期自动补列（设计文档决策 4）"""

    #: v1.0.8 的 tasks 表结构，不含 failure_reason
    LEGACY_DDL = """
        CREATE TABLE tasks (
            task_id TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            status TEXT NOT NULL,
            priority INTEGER NOT NULL,
            node_id TEXT NOT NULL,
            retry_count INTEGER NOT NULL,
            ttl INTEGER DEFAULT 3600,
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            result TEXT,
            error TEXT,
            progress REAL DEFAULT 0.0,
            progress_message TEXT DEFAULT ''
        )
    """

    async def test_old_database_gets_new_column(self, tmp_path):
        import aiosqlite

        db_path = str(tmp_path / "v108.db")
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(self.LEGACY_DDL)
            await conn.execute(
                "INSERT INTO tasks (task_id, data, status, priority, node_id,"
                " retry_count, created_at) VALUES ('legacy-1', '{}', 'pending',"
                " 2, 'node-a', 0, '2026-01-01T00:00:00+00:00')"
            )
            await conn.commit()

        repo = SQLiteTaskRepository(db_path)
        try:
            # 首次读取触发 _ensure_init，缺列应被自动补齐
            loaded = await repo.get("legacy-1")
            assert loaded.task_id == "legacy-1"
            assert loaded.failure_reason is None

            loaded.fail("qa_failed: legacy", failure_reason="qa_failed")
            assert await repo.save_fields(
                loaded,
                ("status", "error", "failure_reason", "completed_at")
            )

            again = await repo.get("legacy-1")
            assert again.failure_reason == "qa_failed"
        finally:
            await repo.close()

    async def test_alter_is_idempotent(self, tmp_path):
        """重复初始化不重复 ALTER（列已存在时跳过）"""
        repo = SQLiteTaskRepository(str(tmp_path / "idem.db"))
        try:
            task = Task(task_id="bf-3", data={})
            await repo.save(task)
            # 再走一遍建表 + 补列路径
            await repo._init_db(repo._conn)
            loaded = await repo.get("bf-3")
            assert loaded.failure_reason is None
        finally:
            await repo.close()
