"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: migrate_task_metadata.py
@Description: 任务元数据迁移工具 — 把 1.0.x 的 SQLite 任务库升级到 1.1.0 结构
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12

背景
----
1.1.0 给 ``tasks`` 表新增了 ``failure_reason`` / ``checkpoint_version`` /
``parent_task_id`` / ``estimated_cost`` / ``cost_tracking`` / ``queue_wait_ms`` /
``duration_ms`` 七列。新库由 ``SQLiteTaskRepository._init_db()`` 启动时自动补列，
但以下两件事启动期 ALTER 不做，需要本脚本停机执行：

1. **历史耗时回填**：``queue_wait_ms`` / ``duration_ms`` 在 1.0.x 任务上为 NULL，
   可由 ``started_at - created_at`` / ``completed_at - started_at`` 推导；
2. **索引重建**：确认 ``idx_tasks_parent`` 存在（``get_children()`` 的主查询路径）。

``_NEW_TASK_COLUMNS`` 直接复用 ``neotask.storage.sqlite`` 的常量，新列只在一处声明，
避免脚本与运行时建表逻辑漂移。

用法
----
    # 1) 预演（只统计，不修改库）
    python scripts/migrate_task_metadata.py --db data/tasks.db

    # 2) 执行迁移（补列 + 回填 + 建索引）
    python scripts/migrate_task_metadata.py --db data/tasks.db --apply

注意
----
1. 务必在**停机维护窗口**执行，且确认没有进程正在使用该库文件。
2. 幂等：已存在的列跳过，已有值的 ``queue_wait_ms`` / ``duration_ms`` 不覆盖。
3. 只回填能推导出的部分；``started_at`` 为空（从未执行）的任务保持 NULL。
4. 时间戳按 ISO 8601 解析，末尾 ``Z`` 视为 UTC；起止时间相差 UTC/本地时区
   （naive 与 aware 混用）的行会跳过，交人工核对。
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from neotask.storage.sqlite import _NEW_TASK_COLUMNS

#: 父子查询使用的索引，与 SQLiteTaskRepository._init_db() 保持一致
PARENT_INDEX = "idx_tasks_parent"

#: 单次 UPDATE 的最大行数，避免一页内 executemany 参数过多
_UPDATE_CHUNK = 500


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    """解析 NeoTask 落库的 ISO 时间戳，兼容末尾 ``Z``

    ``datetime.fromisoformat`` 在 Python 3.11 之前不认 ``Z``，先替换成 ``+00:00``。
    """
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _elapsed_ms(start: Optional[datetime], end: Optional[datetime]) -> Optional[float]:
    """计算毫秒差；任一端缺失或时区信息不一致时返回 None"""
    if start is None or end is None:
        return None
    if (start.tzinfo is None) != (end.tzinfo is None):
        return None
    try:
        return max(0.0, (end - start).total_seconds() * 1000.0)
    except TypeError:
        return None


def _table_columns(conn: sqlite3.Connection) -> List[str]:
    """读取 tasks 表的列名；表不存在时返回空列表"""
    return [row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()]


def _index_names(conn: sqlite3.Connection) -> List[str]:
    """读取 tasks 表上的索引名"""
    rows = conn.execute("PRAGMA index_list(tasks)").fetchall()
    # PRAGMA index_list 的列：seq, name, unique, origin, partial
    return [row[1] for row in rows]


def _collect_backfill(
        conn: sqlite3.Connection,
        has_queue_column: bool,
        has_duration_column: bool,
        batch_size: int,
        dry_run: bool,
) -> Tuple[int, int]:
    """按 rowid 键集分页回填 ``queue_wait_ms`` / ``duration_ms``

    返回 (实际回填 queue_wait_ms 行数, 实际回填 duration_ms 行数)。
    列已存在且非 NULL 的行视为已回填，计数与写入都跳过，因此重复执行返回 0。
    ``dry_run`` 时只统计不写。``has_*_column`` 按调用时的真实列状态传入：
    仍为 False 时整列可回填，为 True 时只处理 NULL 行。
    """
    select_columns = ["rowid", "task_id", "created_at", "started_at", "completed_at"]
    queue_idx: Optional[int] = None
    duration_idx: Optional[int] = None
    if has_queue_column:
        queue_idx = len(select_columns)
        select_columns.append("queue_wait_ms")
    if has_duration_column:
        duration_idx = len(select_columns)
        select_columns.append("duration_ms")
    select_sql = (
        f"SELECT {', '.join(select_columns)} FROM tasks "
        "WHERE rowid > ? ORDER BY rowid LIMIT ?"
    )

    queue_updates: List[Tuple[float, str]] = []
    duration_updates: List[Tuple[float, str]] = []
    queue_count = 0
    duration_count = 0
    last_rowid = 0

    while True:
        rows = conn.execute(select_sql, (last_rowid, batch_size)).fetchall()
        if not rows:
            break

        for row in rows:
            last_rowid = row[0]
            task_id = row[1]
            created = _parse_iso(row[2])
            started = _parse_iso(row[3])
            completed = _parse_iso(row[4])

            if queue_idx is None or row[queue_idx] is None:
                queue_wait = _elapsed_ms(created, started)
                if queue_wait is not None:
                    queue_updates.append((queue_wait, task_id))
            if duration_idx is None or row[duration_idx] is None:
                duration = _elapsed_ms(started, completed)
                if duration is not None:
                    duration_updates.append((duration, task_id))

        if not dry_run:
            # 只写 NULL 值：1.1.0 运行期已写入的耗时不会被历史推导覆盖
            for start in range(0, len(queue_updates), _UPDATE_CHUNK):
                conn.executemany(
                    "UPDATE tasks SET queue_wait_ms = ? "
                    "WHERE task_id = ? AND queue_wait_ms IS NULL",
                    queue_updates[start:start + _UPDATE_CHUNK],
                )
            for start in range(0, len(duration_updates), _UPDATE_CHUNK):
                conn.executemany(
                    "UPDATE tasks SET duration_ms = ? "
                    "WHERE task_id = ? AND duration_ms IS NULL",
                    duration_updates[start:start + _UPDATE_CHUNK],
                )
            conn.commit()

        queue_count += len(queue_updates)
        duration_count += len(duration_updates)
        queue_updates.clear()
        duration_updates.clear()

    return queue_count, duration_count


def migrate_task_metadata(
        db_path: str,
        *,
        dry_run: bool = False,
        batch_size: int = 500,
) -> Dict[str, Any]:
    """把 1.0.x 的 SQLite 任务库升级到 1.1.0 结构（一次性停机操作）

    执行内容：
    - 用 ``PRAGMA table_info(tasks)`` 差集比对，补齐 ``_NEW_TASK_COLUMNS`` 中缺失的列
    - 回填可由时间戳推导的 ``queue_wait_ms`` / ``duration_ms``（只写 NULL 行）
    - 创建父子查询索引 ``idx_tasks_parent``

    语义与约束：
    - 库文件必须已存在，且包含 ``tasks`` 表（不存在时抛异常而非新建空库）
    - ``dry_run=True`` 只统计不写入，用于执行前预演
    - 幂等：重复执行不会重复加列，也不会覆盖已有耗时

    Args:
        db_path: SQLite 数据库文件路径
        dry_run: 是否只预演（True 时不修改库）
        batch_size: 分页读取的每页行数

    Returns:
        迁移报告 dict：执行模式/任务总数/新增列/回填行数/索引状态
    """
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"数据库文件不存在: {path}")
    if batch_size < 1:
        raise ValueError("batch_size 必须为正整数")

    conn = sqlite3.connect(str(path), timeout=30)
    try:
        present = _table_columns(conn)
        if not present:
            raise ValueError("库中不存在 tasks 表，请确认路径是否指向 NeoTask 任务库")

        missing = [(name, decl) for name, decl in _NEW_TASK_COLUMNS if name not in present]
        index_exists = PARENT_INDEX in _index_names(conn)
        total_tasks = int(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])

        has_queue_column = "queue_wait_ms" in present
        has_duration_column = "duration_ms" in present

        if not dry_run:
            with conn:
                for name, decl in missing:
                    # 列名来自模块常量，无外部输入，拼接 DDL 不存在注入面
                    conn.execute(f"ALTER TABLE tasks ADD COLUMN {name} {decl}")

        queue_backfilled, duration_backfilled = _collect_backfill(
            conn, has_queue_column, has_duration_column, batch_size, dry_run
        )

        if not dry_run and not index_exists:
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {PARENT_INDEX} ON tasks(parent_task_id)"
            )
            conn.commit()

        return {
            "db_path": str(path),
            "dry_run": dry_run,
            "total_tasks": total_tasks,
            "added_columns": [name for name, _ in missing],
            "parent_index_exists": index_exists,
            "backfilled_queue_wait": queue_backfilled,
            "backfilled_duration": duration_backfilled,
        }
    finally:
        conn.close()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="把 1.0.x 的 SQLite 任务库升级到 1.1.0 结构（补列 + 回填耗时 + 建索引）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "示例：\n"
            "  python scripts/migrate_task_metadata.py --db data/tasks.db\n"
            "  python scripts/migrate_task_metadata.py --db data/tasks.db --apply\n"
        ),
    )
    parser.add_argument("--db", required=True, help="NeoTask 的 SQLite 数据库文件路径")
    parser.add_argument("--apply", action="store_true", help="真正执行迁移（默认仅预演）")
    parser.add_argument("--batch-size", type=int, default=500, help="分页读取的批次大小")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if args.batch_size < 1:
        print("错误：--batch-size 必须为正整数", file=sys.stderr)
        return 2

    mode = "执行迁移" if args.apply else "预演（不会修改库）"
    print(f"数据库  : {args.db}")
    print(f"模式    : {mode}")
    print("-" * 78)
    print("提示：请确认使用该库的任务池已停机，且没有其他进程持有该文件。")

    started = time.time()
    try:
        report = migrate_task_metadata(
            args.db,
            dry_run=not args.apply,
            batch_size=args.batch_size,
        )
    except (FileNotFoundError, ValueError, sqlite3.Error) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 2

    elapsed = time.time() - started
    action = "将新增" if report["dry_run"] else "已新增"
    print("-" * 78)
    print(f"任务总数    : {report['total_tasks']}")
    print(f"{action}列    : {report['added_columns'] or '无（已是 1.1.0 结构）'}")
    print(
        f"回填耗时    : queue_wait_ms {report['backfilled_queue_wait']} 行，"
        f"duration_ms {report['backfilled_duration']} 行"
    )
    if report["parent_index_exists"]:
        print(f"父子索引    : {PARENT_INDEX} 已存在")
    else:
        print(f"父子索引    : {PARENT_INDEX} {'将创建' if report['dry_run'] else '已创建'}")
    print(f"耗时        : {elapsed:.2f}s")

    if report["dry_run"]:
        print("\n预演完成，未做任何修改。确认无误后加 --apply 执行迁移。")
    else:
        print("\n迁移完成。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
