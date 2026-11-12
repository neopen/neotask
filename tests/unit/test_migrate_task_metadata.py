"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_migrate_task_metadata.py
@Description: 迁移脚本与 SQLite 启动期补列测试（1.0.x 库升级到 1.1.0）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio
import importlib.util
import sqlite3
from pathlib import Path

import pytest

from neotask.models.task import Task, TaskPriority
from neotask.storage.sqlite import SQLiteTaskRepository

_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "migrate_task_metadata.py"
_NEW_COLUMNS = {
    "failure_reason", "checkpoint_version", "parent_task_id",
    "estimated_cost", "cost_tracking", "queue_wait_ms", "duration_ms",
}


def _load_script():
    spec = importlib.util.spec_from_file_location("migrate_task_metadata", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migrate_script = _load_script()

_LEGACY_SCHEMA = """
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
);
CREATE INDEX idx_tasks_status ON tasks(status);
"""


@pytest.fixture
def legacy_db(tmp_path):
    """构造 1.0.8 结构的任务库（无 1.1.0 新列）"""
    db_path = tmp_path / "legacy_108.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_LEGACY_SCHEMA)
    rows = [
        # 完整时间戳（naive）
        ("T1", "success", "2026-11-10T10:00:00", "2026-11-10T10:00:00.200000",
         "2026-11-10T10:00:01.200000"),
        # 全部带 Z（UTC）
        ("T2", "failed", "2026-11-10T09:00:00Z", "2026-11-10T09:00:01Z",
         "2026-11-10T09:00:03Z"),
        # 从未执行
        ("T3", "pending", "2026-11-10T11:00:00", None, None),
        # 执行中
        ("T4", "running", "2026-11-10T12:00:00", "2026-11-10T12:00:00.500000", None),
        # created 与 started 时区不可比：queue_wait 跳过，duration 可算
        ("T5", "success", "2026-11-10T08:00:00", "2026-11-10T08:00:01+08:00",
         "2026-11-10T08:00:03+08:00"),
    ]
    conn.executemany(
        "INSERT INTO tasks (task_id, data, status, priority, node_id, retry_count, "
        "created_at, started_at, completed_at) VALUES (?, '{}', ?, 2, 'node', 0, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()
    return db_path


def _columns(db_path):
    conn = sqlite3.connect(str(db_path))
    try:
        return [row[1] for row in conn.execute("PRAGMA table_info(tasks)")]
    finally:
        conn.close()


def _indexes(db_path):
    conn = sqlite3.connect(str(db_path))
    try:
        return {row[1] for row in conn.execute("PRAGMA index_list(tasks)")}
    finally:
        conn.close()


class TestMigrateTaskMetadata:
    def test_dry_run_reports_without_writing(self, legacy_db):
        report = migrate_script.migrate_task_metadata(str(legacy_db), dry_run=True)

        assert report["dry_run"] is True
        assert report["total_tasks"] == 5
        assert set(report["added_columns"]) == _NEW_COLUMNS
        assert report["backfilled_queue_wait"] == 3   # T1/T2/T4
        assert report["backfilled_duration"] == 3     # T1/T2/T5
        assert report["parent_index_exists"] is False

        # 预演不落任何结构变更
        assert _NEW_COLUMNS.isdisjoint(_columns(legacy_db))
        assert "idx_tasks_parent" not in _indexes(legacy_db)

    def test_apply_adds_columns_backfills_and_indexes(self, legacy_db):
        report = migrate_script.migrate_task_metadata(str(legacy_db))

        assert set(report["added_columns"]) == _NEW_COLUMNS
        assert _NEW_COLUMNS <= set(_columns(legacy_db))
        assert "idx_tasks_parent" in _indexes(legacy_db)

        conn = sqlite3.connect(str(legacy_db))
        conn.row_factory = sqlite3.Row
        try:
            rows = {row["task_id"]: row for row in conn.execute("SELECT * FROM tasks")}
        finally:
            conn.close()

        assert rows["T1"]["queue_wait_ms"] == pytest.approx(200.0)
        assert rows["T1"]["duration_ms"] == pytest.approx(1000.0)
        assert rows["T2"]["queue_wait_ms"] == pytest.approx(1000.0)
        assert rows["T2"]["duration_ms"] == pytest.approx(2000.0)
        assert rows["T3"]["queue_wait_ms"] is None
        assert rows["T3"]["duration_ms"] is None
        assert rows["T4"]["queue_wait_ms"] == pytest.approx(500.0)
        assert rows["T4"]["duration_ms"] is None
        assert rows["T5"]["queue_wait_ms"] is None
        assert rows["T5"]["duration_ms"] == pytest.approx(2000.0)

        # 新列默认值
        assert rows["T1"]["checkpoint_version"] == 0
        assert rows["T1"]["estimated_cost"] == 0.0
        assert rows["T1"]["parent_task_id"] is None
        assert rows["T1"]["cost_tracking"] is None

    def test_rerun_is_idempotent_and_keeps_existing_values(self, legacy_db):
        migrate_script.migrate_task_metadata(str(legacy_db))

        conn = sqlite3.connect(str(legacy_db))
        conn.execute("UPDATE tasks SET duration_ms = 123.0 WHERE task_id = 'T1'")
        conn.commit()
        conn.close()

        report = migrate_script.migrate_task_metadata(str(legacy_db))

        assert report["added_columns"] == []
        assert report["parent_index_exists"] is True
        assert report["backfilled_queue_wait"] == 0
        assert report["backfilled_duration"] == 0

        conn = sqlite3.connect(str(legacy_db))
        try:
            assert conn.execute(
                "SELECT duration_ms FROM tasks WHERE task_id = 'T1'"
            ).fetchone()[0] == pytest.approx(123.0)
        finally:
            conn.close()

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            migrate_script.migrate_task_metadata(str(tmp_path / "nope.db"))

    def test_database_without_tasks_table_raises(self, tmp_path):
        db_path = tmp_path / "empty.db"
        sqlite3.connect(str(db_path)).close()

        with pytest.raises(ValueError, match="tasks"):
            migrate_script.migrate_task_metadata(str(db_path))

    def test_invalid_batch_size_raises(self, legacy_db):
        with pytest.raises(ValueError):
            migrate_script.migrate_task_metadata(str(legacy_db), batch_size=0)

    def test_cli_defaults_to_dry_run(self, legacy_db):
        assert migrate_script.main(["--db", str(legacy_db)]) == 0
        assert _NEW_COLUMNS.isdisjoint(_columns(legacy_db))

    def test_cli_apply_writes(self, legacy_db):
        assert migrate_script.main(["--db", str(legacy_db), "--apply"]) == 0
        assert _NEW_COLUMNS <= set(_columns(legacy_db))


class TestStartupAlter:
    """未跑迁移脚本时，SQLiteTaskRepository 启动期也能补齐新列"""

    @pytest.fixture
    async def legacy_repo(self, legacy_db):
        repo = SQLiteTaskRepository(str(legacy_db))
        try:
            await repo._ensure_init()
            yield repo
        finally:
            await repo.close()
            await asyncio.sleep(0.1)

    async def test_open_legacy_db(self, legacy_repo):
        task = await legacy_repo.get("T1")

        assert task is not None
        assert task.checkpoint_version == 0
        assert task.parent_task_id is None
        assert task.estimated_cost == 0.0
        assert task.cost_tracking == {}
        assert task.queue_wait_ms is None  # 启动期只补列，不回填耗时

    async def test_new_task_with_metadata_writes(self, legacy_repo):
        """补列后 21 列 INSERT/UPDATE 可用（含 cost_tracking JSON）"""
        task = Task(
            task_id="T-new",
            data={},
            priority=TaskPriority.NORMAL,
            parent_task_id="T1",
            estimated_cost=0.4,
            cost_tracking={"tokens_in": 12, "currency": "USD"},
        )
        await legacy_repo.save(task)

        restored = await legacy_repo.get("T-new")
        assert restored.parent_task_id == "T1"
        assert restored.estimated_cost == pytest.approx(0.4)
        assert restored.cost_tracking == {"tokens_in": 12, "currency": "USD"}

        children = await legacy_repo.list_by_parent("T1")
        assert [t.task_id for t in children] == ["T-new"]
