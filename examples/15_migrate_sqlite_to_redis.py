"""
@FileName: 15_migrate_sqlite_to_redis.py
@Description: 存储迁移示例 — 把 SQLite 任务池的任务/队列/延迟/周期任务迁移到 Redis
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/10
"""

import asyncio
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from neotask import TaskPool, TaskScheduler, SchedulerConfig
from neotask.models.config import StorageConfig
from neotask.models.task import TaskStatus
from neotask.storage import (
    StorageFactory,
    RedisPeriodicStore,
    SQLitePeriodicStore,
    SQLiteTaskRepository,
)

#: 演示默认使用本地空库 db2；可用 REDIS_URL 指向自己的实例
DEFAULT_REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/2")


def migrate_sqlite_to_redis(
        pool: TaskPool,
        redis_url: str,
        *,
        scheduler: Optional[TaskScheduler] = None,
        requeue_running: bool = True,
        batch_size: int = 500,
) -> Dict[str, Any]:
    """把 SQLite 任务池的全部数据迁移到 Redis（一次性操作）

    迁移内容：
    - 任务数据：全部状态写入 ``neotask:task:<id>`` 并维护 ``neotask:status:<s>`` 索引
    - 队列：PENDING 任务按原优先级写入 ``neotask:queue:priority`` zset
    - 延迟任务：写入 ``neotask:queue:delayed`` zset（score = 绝对到期时间）
    - 周期任务：传入 scheduler 时，迁移任务定义与执行记录

    关于源池的两种用法：
    - 在线迁移（推荐）：SQLite 池与本函数在同一进程，先
      ``shutdown(graceful=False)`` 再调用，可同时迁移内存延迟队列中的任务
    - 离线迁移：进程已退出时，传入一个尚未 start 的 SQLite 任务池
      （``TaskPool(config=TaskPoolConfig(storage_type="sqlite", ...))``）。
      此时延迟队列已随进程消失，只能恢复任务表与重建队列

    语义与约束：
    - 已存在的 task_id 跳过不覆盖；目标池启动后不要重跑，否则会把源快照
      中的 PENDING 任务重新入队导致重复执行
    - RUNNING 任务默认重置为 PENDING 重新入队（at-least-once，执行器需幂等）
    - Redis 的 ``queue:delayed`` 不保存优先级，到期转入优先级队列时统一按
      NORMAL（存储层既有行为，非迁移丢失）
    - 写入的是 ``neotask:`` 根命名空间，建议目标使用独立实例或空库

    Args:
        pool: 已 shutdown 或尚未 start 的 SQLite 任务池
        redis_url: 目标 Redis URL（可带 db，如 redis://127.0.0.1:6379/2）
        scheduler: 源调度器（有周期任务时传入）
        requeue_running: 是否把 RUNNING 任务重置为 PENDING 入队
        batch_size: 分页读取与批量写入的分块大小

    Returns:
        迁移报告 dict（任务数/状态分布/入队数/延迟任务/周期任务数）
    """
    if getattr(pool, "_running", False):
        raise RuntimeError("源任务池仍在运行，请先 shutdown(graceful=False) 再迁移")
    if not isinstance(pool._task_repo, SQLiteTaskRepository):
        raise ValueError("源存储不是 sqlite，本脚本仅支持 sqlite → redis")
    return asyncio.run(_migrate_async(pool, redis_url, scheduler, requeue_running, batch_size))


async def _migrate_async(
        pool: TaskPool,
        redis_url: str,
        scheduler: Optional[TaskScheduler],
        requeue_running: bool,
        batch_size: int,
) -> Dict[str, Any]:
    task_repo, queue_repo = StorageFactory.create(StorageConfig.redis(redis_url))
    try:
        # 枚举源全部任务（SQL 分页；停机后无并发变更，快照稳定）
        tasks = []
        try:
            for status in TaskStatus:
                offset = 0
                while True:
                    page = await pool._task_repo.list_by_status(status, limit=batch_size, offset=offset)
                    tasks.extend(page)
                    if len(page) < batch_size:
                        break
                    offset += len(page)
        finally:
            # 停机时已关闭过一次；上面的读取会惰性重连，这里再关掉以释放库文件
            await pool._task_repo.close()

        # SQLite 后端的延迟队列同样只存内存：只能从存活进程的停机快照中读取
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

        # 队列重建：PENDING 且不在延迟堆中的任务立即入队，延迟任务写入延迟 zset
        pending = [
            (task.task_id, task.priority.value)
            for task in tasks
            if task.status == TaskStatus.PENDING and task.task_id not in delayed
        ]
        enqueued = 0
        for start in range(0, len(pending), batch_size):
            enqueued += await queue_repo.push_batch(pending[start:start + batch_size])

        delayed_written = 0
        now = time.time()
        for task_id, (priority, execute_at) in delayed.items():
            await queue_repo.push(
                task_id, priority, delay=max(0.1, execute_at - now)
            )
            delayed_written += 1

        periodic_count, execution_count = await _migrate_periodic(scheduler, redis_url)

        status_counts: Dict[str, int] = {}
        for task in tasks:
            status_counts[task.status.value] = status_counts.get(task.status.value, 0) + 1

        return {
            "redis_url": redis_url,
            "total_tasks": len(tasks),
            "copied_tasks": copied,
            "skipped_existing": len(tasks) - copied,
            "status_counts": status_counts,
            "enqueued": enqueued,
            "delayed_tasks": delayed_written,
            "running_reset": running_reset,
            "periodic_tasks": periodic_count,
            "periodic_executions": execution_count,
        }
    finally:
        await task_repo.close()
        await queue_repo.close()


async def _migrate_periodic(
        scheduler: Optional[TaskScheduler], redis_url: str
) -> Tuple[int, int]:
    """迁移周期任务定义与执行记录，返回 (定义数, 执行记录数)

    源存储是 SQLitePeriodicStore 时，调度器停机已关闭连接；
    其 ``load_executions`` 会惰性重连，无需特殊处理。
    """
    manager = getattr(scheduler, "_periodic_manager", None) if scheduler is not None else None
    if manager is None:
        return 0, 0

    store = RedisPeriodicStore(redis_url)
    try:
        # 从运行态实例序列化（而非旧存储），保留最新的 next_run/run_count
        instances = list(manager._tasks.values())
        for instance in instances:
            await store.save_task(instance.task_id, manager._serialize_instance(instance))

        executions = 0
        if manager._storage is not None:
            try:
                for instance in instances:
                    records = await manager._storage.load_executions(instance.task_id, limit=1000)
                    for record in records:
                        await store.save_execution(record["execution_id"], record)
                        executions += 1
            finally:
                # 调度器停机时已关闭过一次；load_executions 会惰性重连，再关掉释放库文件
                if isinstance(manager._storage, SQLitePeriodicStore):
                    await manager._storage.close()
        return len(instances), executions
    finally:
        await store.close()


# ========== 以下为演示：SQLite 池产生数据 → 迁移 → 用 Redis 新池验证 ==========


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


async def _count_keys(redis_url: str, pattern: str) -> int:
    import redis.asyncio as aioredis

    client = aioredis.from_url(redis_url, decode_responses=True)
    try:
        count = 0
        async for _ in client.scan_iter(match=pattern, count=500):
            count += 1
        return count
    finally:
        await client.aclose()


async def _cleanup_keys(redis_url: str, pattern: str) -> int:
    """删除匹配键（仅用于演示库清理）"""
    import redis.asyncio as aioredis

    client = aioredis.from_url(redis_url, decode_responses=True)
    removed = 0
    try:
        batch: List[str] = []
        async for key in client.scan_iter(match=pattern, count=500):
            batch.append(key)
            if len(batch) >= 500:
                removed += await client.delete(*batch)
                batch.clear()
        if batch:
            removed += await client.delete(*batch)
        return removed
    finally:
        await client.aclose()


def main():
    redis_url = DEFAULT_REDIS_URL
    db_path = Path(tempfile.gettempdir()) / f"neotask_migrate_{int(time.time())}.db"
    print("=== SQLite → Redis 迁移示例 ===")
    print(f"源数据库:   {db_path}")
    print(f"目标 Redis: {redis_url}")

    # 演示只在空库上自动清理；非空库不动现有数据，避免误删
    db_was_empty = asyncio.run(_count_keys(redis_url, "*")) == 0
    print(f"目标库当前为空: {db_was_empty}\n")

    # ---------- 1. 源：SQLite 调度器，制造各种状态的数据 ----------
    source = TaskScheduler(
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
    source.start()

    ok_ids: List[str] = []
    backlog_ids: List[str] = []
    try:
        # 周期任务：立即执行一次产生执行记录（间隔 1h，演示期间不会再触发）
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
        # graceful=False：不执行积压直接停机，保留延迟队列供迁移读取
        source.shutdown(graceful=False)

    # ---------- 2. 迁移 ----------
    report = migrate_sqlite_to_redis(source._pool, redis_url, scheduler=source)
    print("\n[迁移报告]")
    print(f"  任务总数: {report['total_tasks']}（新增 {report['copied_tasks']}，"
          f"跳过已存在 {report['skipped_existing']}）")
    print(f"  状态分布: {report['status_counts']}")
    print(f"  重建队列: {report['enqueued']} 条")
    print(f"  延迟 zset: {report['delayed_tasks']} 条（落盘，跨重启有效）")
    print(f"  周期任务: {report['periodic_tasks']} 条定义 + "
          f"{report['periodic_executions']} 条执行记录")

    # ---------- 3. 目标：Redis 新池，验证并继续消费 ----------
    target = TaskScheduler(
        executor=process_data,
        config=SchedulerConfig(
            storage_type="redis",
            redis_url=redis_url,
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

        # 延迟任务在 Redis zset 中到期后自动转入优先级队列并执行
        target.wait_for_result(delayed_id, timeout=15)
        print("  延迟任务: 已在 Redis 延迟 zset 到期后触发完成")

        # 周期任务定义与执行记录已恢复
        periodic = target.get_periodic_task(periodic_id)
        assert periodic is not None and periodic["run_count"] >= 1, "周期任务未恢复"
        history = asyncio.run(target._periodic_manager.get_execution_history(periodic_id))
        print(f"  周期任务: {periodic['name']} 已恢复，run_count={periodic['run_count']}，"
              f"执行记录 {len(history)} 条，next_run={periodic['next_run']}")
    finally:
        target.shutdown()

    # ---------- 4. 清理演示数据 ----------
    for suffix in ("", "-wal", "-shm"):
        Path(str(db_path) + suffix).unlink(missing_ok=True)
    print("\n[清理] 已删除演示用的 SQLite 文件")
    if db_was_empty:
        removed = asyncio.run(_cleanup_keys(redis_url, "neotask:*"))
        print(f"[清理] 已删除演示写入的 {removed} 个 neotask:* 键")
    else:
        print("[清理] 目标库原本非空，为安全起见未删除任何键；"
              "请自行清理本次演示数据或换一个空库")

    print("\n说明: 在线迁移（同一进程内停机后立即迁移）才能带走内存延迟队列；")
    print("      进程已退出时只能迁移任务表并重建队列，延迟任务已丢失。")
    print("      目标池启动后不要重复运行迁移，否则 PENDING 任务会被重新入队。")


if __name__ == "__main__":
    main()