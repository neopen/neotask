"""
@FileName: migrate_memory_to_sqlite.py
@Description: 存储迁移工具 — 把 memory 任务池的任务/队列/延迟/周期任务迁移到 SQLite
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/10
"""

import asyncio
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from neotask import TaskPool, TaskScheduler, SchedulerConfig
from neotask.models.config import StorageConfig
from neotask.models.task import TaskStatus
from neotask.storage import StorageFactory, SQLitePeriodicStore


def migrate_memory_to_sqlite(
        pool: TaskPool,
        sqlite_path: str,
        *,
        scheduler: Optional[TaskScheduler] = None,
        requeue_running: bool = True,
        batch_size: int = 500,
) -> Dict[str, Any]:
    """把 memory 任务池的全部数据迁移到 SQLite（一次性操作，源池须已停机）

    迁移内容：
    - 任务表：全部状态（pending/running/success/failed/cancelled/scheduled）
    - 队列：PENDING 任务按原优先级重建到 SQLite 队列
    - 延迟任务：不直接入队，以 ``delayed_tasks`` 返回，由
      :func:`reschedule_delayed_tasks` 在新池上按剩余延迟重新调度
    - 周期任务：传入 scheduler 时，迁移任务定义与执行记录

    语义与约束：
    - 调用前必须 ``pool.shutdown(graceful=False)``（或 scheduler.shutdown）；
      graceful=True 会把队列里的任务执行完再停机，积压任务就迁不到了
    - 已存在的 task_id 跳过不覆盖；但目标池启动后不要重跑，否则会把源快照
      中的 PENDING 任务重新入队导致重复执行
    - RUNNING 任务默认重置为 PENDING 重新入队（at-least-once，执行器需幂等）
    - 当前架构延迟队列不落盘：迁移后的延迟任务只在新进程存活期间有效

    Args:
        pool: 已停机的源任务池（storage_type="memory"）
        sqlite_path: 目标 SQLite 数据库路径
        scheduler: 源调度器（有周期任务时传入）
        requeue_running: 是否把 RUNNING 任务重置为 PENDING 入队
        batch_size: 分页读取与批量写入的分块大小

    Returns:
        迁移报告 dict（任务数/状态分布/入队数/延迟任务/周期任务数）
    """
    if getattr(pool, "_running", False):
        raise RuntimeError("源任务池仍在运行，请先 shutdown(graceful=False) 再迁移")
    if getattr(pool._task_repo, "is_persistent", False):
        raise ValueError("源存储不是 memory，本脚本仅支持 memory → sqlite")
    return asyncio.run(_migrate_async(pool, sqlite_path, scheduler, requeue_running, batch_size))


async def _migrate_async(
        pool: TaskPool,
        sqlite_path: str,
        scheduler: Optional[TaskScheduler],
        requeue_running: bool,
        batch_size: int,
) -> Dict[str, Any]:
    task_repo, queue_repo = StorageFactory.create(StorageConfig.sqlite(str(sqlite_path)))
    try:
        # 枚举源全部任务（按状态分页；停机后无并发，快照稳定）
        tasks = []
        for status in TaskStatus:
            offset = 0
            while True:
                page = await pool._task_repo.list_by_status(status, limit=batch_size, offset=offset)
                tasks.extend(page)
                if len(page) < batch_size:
                    break
                offset += len(page)

        # execute_at 没有公开读取接口，只能访问延迟队列内部字典（停机后无并发）
        delayed = {
            item.task_id: (item.priority, item.execute_at)
            for item in pool._queue_scheduler._delayed_queue._tasks.values()
        }

        running_reset = 0
        for task in tasks:
            if task.status == TaskStatus.RUNNING and requeue_running:
                task.status = TaskStatus.PENDING
                task.started_at = None
                task.node_id = ""
                running_reset += 1

        copied = 0
        for start in range(0, len(tasks), batch_size):
            copied += len(await task_repo.save_batch(tasks[start:start + batch_size]))

        # 队列重建：PENDING 且不在延迟堆中的任务立即入队，延迟任务走重调度
        pending = [
            (task.task_id, task.priority.value)
            for task in tasks
            if task.status == TaskStatus.PENDING and task.task_id not in delayed
        ]
        enqueued = 0
        for start in range(0, len(pending), batch_size):
            enqueued += await queue_repo.push_batch(pending[start:start + batch_size])

        periodic_count, execution_count = await _migrate_periodic(scheduler, sqlite_path)

        status_counts: Dict[str, int] = {}
        for task in tasks:
            status_counts[task.status.value] = status_counts.get(task.status.value, 0) + 1

        return {
            "sqlite_path": str(sqlite_path),
            "total_tasks": len(tasks),
            "copied_tasks": copied,
            "skipped_existing": len(tasks) - copied,
            "status_counts": status_counts,
            "enqueued": enqueued,
            "running_reset": running_reset,
            "delayed_tasks": [
                {"task_id": task_id, "priority": priority, "execute_at": execute_at}
                for task_id, (priority, execute_at) in delayed.items()
            ],
            "periodic_tasks": periodic_count,
            "periodic_executions": execution_count,
        }
    finally:
        await task_repo.close()
        await queue_repo.close()


async def _migrate_periodic(
        scheduler: Optional[TaskScheduler], sqlite_path: str
) -> Tuple[int, int]:
    """迁移周期任务定义与执行记录，返回 (定义数, 执行记录数)"""
    manager = getattr(scheduler, "_periodic_manager", None) if scheduler is not None else None
    if manager is None:
        return 0, 0

    store = SQLitePeriodicStore(str(sqlite_path))
    try:
        # 从运行态实例序列化（而非旧存储），保留最新的 next_run/run_count
        instances = list(manager._tasks.values())
        for instance in instances:
            await store.save_task(instance.task_id, manager._serialize_instance(instance))

        executions = 0
        if manager._storage is not None:
            for instance in instances:
                records = await manager._storage.load_executions(instance.task_id, limit=1000)
                for record in records:
                    await store.save_execution(record["execution_id"], record)
                    executions += 1
        return len(instances), executions
    finally:
        await store.close()


def reschedule_delayed_tasks(pool: TaskPool, delayed_tasks: List[Dict[str, Any]]) -> int:
    """把迁移报告中的延迟任务按剩余延迟重新调度到新池（新池须已 start）

    剩余延迟按当前时间重算，已到期的按 0.1s 立即触发。
    注意：延迟队列在当前架构中只存内存，新进程重启后不会自动恢复。
    """
    async def _schedule() -> int:
        scheduled = 0
        now = time.time()
        for item in delayed_tasks:
            delay = max(0.1, item["execute_at"] - now)
            if await pool._queue_scheduler.schedule_delayed(item["task_id"], item["priority"], delay):
                scheduled += 1
        return scheduled

    return asyncio.run(_schedule())


# ========== 以下为演示：构造 memory 数据 → 迁移 → 用 SQLite 新池验证 ==========


async def process_data(data: dict) -> dict:
    """模拟业务任务：fail=True 时抛错"""
    await asyncio.sleep(0.1)
    if data.get("fail"):
        raise ValueError("模拟业务失败")
    return {"ok": True, "name": data.get("name", "")}


def wait_status(scheduler: TaskScheduler, task_id: str, expected: str, timeout: float = 10.0) -> None:
    """轮询等待任务进入指定状态"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if scheduler.get_status(task_id) == expected:
            return
        time.sleep(0.1)
    raise TimeoutError(f"任务 {task_id} 未在 {timeout}s 内进入 {expected}")


def main():
    # 每次运行使用独立库文件，避免和上次运行的数据混在一起
    db_path = Path(tempfile.gettempdir()) / f"neotask_migrate_{int(time.time())}.db"
    print("=== memory → SQLite 迁移示例 ===")
    print(f"目标数据库: {db_path}\n")

    # ---------- 1. 源：memory 调度器，制造各种状态的数据 ----------
    source = TaskScheduler(
        executor=process_data,
        config=SchedulerConfig(
            storage_type="memory",
            enable_persistence=True,
            scan_interval=0.5,
            worker_concurrency=2,
            max_retries=0,
        ),
    )
    source.start()

    ok_ids: List[str] = []
    backlog_ids: List[str] = []
    try:
        # 周期任务：立即执行一次，产生执行记录（间隔 1h，演示期间不会再触发）
        periodic_id = source.submit_interval(
            {"name": "周期报表"}, interval_seconds=3600,
            run_immediately=True, name="migration-demo",
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            task = source.get_periodic_task(periodic_id)
            if task and task["run_count"] >= 1:
                break
            time.sleep(0.1)
        print(f"[源] 周期任务已执行 1 次: {periodic_id}")

        # 成功 / 失败任务
        for name in ("任务A", "任务B"):
            tid = source.submit({"name": name})
            source.wait_for_result(tid, timeout=10)
            ok_ids.append(tid)
        failed_id = source.submit({"name": "失败任务", "fail": True})
        wait_status(source, failed_id, "failed")
        print("[源] 成功 2 条、失败 1 条")

        # 暂停出队后提交积压（停留在队列中不执行），再加一条 5s 延迟任务
        source.pause()
        for name, priority in (("积压-紧急", 0), ("积压-高", 1), ("积压-低", 3)):
            backlog_ids.append(source.submit({"name": name}, priority=priority))
        delayed_id = source.submit_delayed({"name": "延迟任务"}, delay_seconds=5)
        print(f"[源] 积压 {len(backlog_ids)} 条、延迟 1 条（5s）、周期 1 条")
    finally:
        # graceful=False：不执行积压直接停机；预取器残留会回灌共享队列
        source.shutdown(graceful=False)

    # ---------- 2. 迁移 ----------
    report = migrate_memory_to_sqlite(source._pool, str(db_path), scheduler=source)
    print("\n[迁移报告]")
    print(f"  任务总数: {report['total_tasks']}（新增 {report['copied_tasks']}，"
          f"跳过已存在 {report['skipped_existing']}）")
    print(f"  状态分布: {report['status_counts']}")
    print(f"  重建队列: {report['enqueued']} 条")
    print(f"  延迟任务: {len(report['delayed_tasks'])} 条（待在新池重调度）")
    print(f"  周期任务: {report['periodic_tasks']} 条定义 + "
          f"{report['periodic_executions']} 条执行记录")

    # ---------- 3. 目标：SQLite 新池打开同一个库，验证并继续消费 ----------
    target = TaskScheduler(
        executor=process_data,
        config=SchedulerConfig(
            storage_type="sqlite",
            sqlite_path=str(db_path),
            enable_persistence=True,
            scan_interval=0.5,
            worker_concurrency=2,
            max_retries=0,
        ),
    )
    target.start()
    try:
        print("\n[目标池验证]")
        # 历史任务状态与结果完整
        for tid in ok_ids:
            assert target.get_status(tid) == "success", f"历史任务 {tid} 状态丢失"
        assert target.get_status(failed_id) == "failed", "失败任务状态丢失"
        print(f"  历史任务: 成功 {len(ok_ids)} 条、失败 1 条，状态完整")
        print(f"  示例结果: {target.get_result(ok_ids[0])['result']}")

        # 积压任务被新池按优先级继续消费
        for tid in backlog_ids:
            target.wait_for_result(tid, timeout=10)
        print(f"  积压任务: {len(backlog_ids)} 条已被新池执行完成")

        # 延迟任务按剩余延迟重调度，并等待其真正触发
        scheduled = reschedule_delayed_tasks(target._pool, report["delayed_tasks"])
        target.wait_for_result(delayed_id, timeout=15)
        print(f"  延迟任务: 重调度 {scheduled} 条，已按剩余延迟触发完成")

        # 周期任务定义与执行记录已恢复（next_run 保留，约 1h 后才会再触发）
        periodic = target.get_periodic_task(periodic_id)
        assert periodic is not None and periodic["run_count"] >= 1, "周期任务未恢复"
        history = asyncio.run(target._periodic_manager.get_execution_history(periodic_id))
        print(f"  周期任务: {periodic['name']} 已恢复，run_count={periodic['run_count']}，"
              f"执行记录 {len(history)} 条，next_run={periodic['next_run']}")
    finally:
        target.shutdown()

    print("\n说明: 迁移为一次性停机操作；目标池启动后不要重复运行迁移，")
    print("      否则源快照中的 PENDING 任务会被重新入队导致重复执行。")
    print("      延迟队列在当前架构中不落盘，新进程重启后延迟任务需自行恢复。")


if __name__ == "__main__":
    main()