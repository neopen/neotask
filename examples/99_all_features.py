"""
@FileName: 99_all_features.py
@Description: 完整功能演示 - 展示即时任务、优先级、批量、事件、进度、统计与定时调度
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/9
"""

import asyncio
import time

from neotask import (
    SchedulerConfig,
    TaskPool,
    TaskPoolConfig,
    TaskPriority,
    TaskScheduler,
    report_progress,
)
from neotask.scheduler.cron_parser import CronParser


# 记录实际执行顺序，验证优先级出队
EXECUTED_ORDER: list = []
# 记录各类事件触发次数
EVENT_COUNTS: dict = {}


async def demo_executor(data: dict) -> dict:
    """通用演示执行器：按 kind 走不同分支"""
    kind = data.get("kind", "normal")

    if kind == "fail":
        raise ValueError("模拟任务失败")

    if kind == "progress":
        for progress, message in ((0.2, "初始化"), (0.5, "处理中"), (0.9, "收尾")):
            await report_progress(progress, message)
            await asyncio.sleep(0.05)
        return {"name": data["name"], "progress": "done"}

    if "order_tag" in data:
        EXECUTED_ORDER.append(data["order_tag"])

    await asyncio.sleep(data.get("duration", 0.2))
    return {"name": data["name"], "success": True, "timestamp": time.time()}


def make_event_handler(event_name: str):
    """生成事件计数器（事件处理器必须是 async 函数）"""

    async def handler(event):
        EVENT_COUNTS[event_name] = EVENT_COUNTS.get(event_name, 0) + 1

    return handler


def print_section(title: str):
    """打印章节标题"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def demo_taskpool():
    """演示 TaskPool：基本提交、优先级、批量、事件、进度、失败与统计"""
    print_section("TaskPool - 即时任务池")

    with TaskPool(
        executor=demo_executor,
        config=TaskPoolConfig(
            worker_concurrency=1,     # 单 Worker，便于观察优先级顺序
            enable_prefetch=False,    # 关闭预取，保证严格按优先级出队
            max_retries=0
        )
    ) as pool:
        # 注册事件
        pool.on_created(make_event_handler("created"))
        pool.on_started(make_event_handler("started"))
        pool.on_completed(make_event_handler("completed"))
        pool.on_failed(make_event_handler("failed"))
        pool.on_progress(make_event_handler("progress"))

        # 1. 基本提交
        print("\n1. 基本提交:")
        task_id = pool.submit({"name": "basic_task"})
        print(f"   结果: {pool.wait_for_result(task_id, timeout=10)}")

        # 2. 优先级：先暂停出队，全部入队后再按优先级执行
        print("\n2. 优先级队列:")
        pool.pause()
        priority_tasks = [
            ("LOW", TaskPriority.LOW),
            ("NORMAL", TaskPriority.NORMAL),
            ("HIGH", TaskPriority.HIGH),
            ("CRITICAL", TaskPriority.CRITICAL),
        ]
        priority_ids = []
        for name, priority in priority_tasks:
            priority_ids.append(
                pool.submit({"name": name, "order_tag": name}, priority=priority)
            )
        print(f"   暂停期间队列长度: {pool.get_queue_size()}")
        pool.resume()
        pool.wait_all(priority_ids, timeout=10)
        print(f"   执行顺序: {' → '.join(EXECUTED_ORDER)}")

        # 3. 批量提交
        print("\n3. 批量提交:")
        batch_ids = pool.submit_batch(
            [{"name": f"batch_{i}", "duration": 0.1} for i in range(5)]
        )
        results = pool.wait_all(batch_ids, timeout=10)
        completed = sum(1 for r in results.values() if isinstance(r, dict) and "error" not in r)
        print(f"   提交 {len(batch_ids)} 个，完成 {completed} 个")

        # 4. 进度上报（触发 task.progress 事件）
        print("\n4. 进度上报:")
        progress_id = pool.submit({"kind": "progress", "name": "progress_task"})
        pool.wait_for_result(progress_id, timeout=10)
        print(f"   进度事件次数: {EVENT_COUNTS.get('progress', 0)}")

        # 5. 失败任务（触发 task.failed 事件）
        print("\n5. 失败任务:")
        fail_id = pool.submit({"kind": "fail", "name": "fail_task"})
        try:
            pool.wait_for_result(fail_id, timeout=10)
        except Exception as e:
            print(f"   捕获异常: {e}")

        time.sleep(0.2)  # 等待事件回调写入

        # 6. 统计与事件总览
        stats = pool.get_stats()
        print("\n6. 统计与事件:")
        print(f"   任务: 总={stats['total']}, 完成={stats['completed']}, "
              f"失败={stats['failed']}, 成功率={stats['success_rate']:.1%}")
        print(f"   事件: {EVENT_COUNTS}")
        print(f"   健康状态: {pool.get_health_status()['status']}")


def demo_taskscheduler():
    """演示 TaskScheduler：延时、周期（max_runs）与 Cron"""
    print_section("TaskScheduler - 定时任务调度器")

    scheduler = TaskScheduler(
        executor=demo_executor,
        config=SchedulerConfig(storage_type="memory", scan_interval=0.5)
    )
    scheduler.start()

    try:
        # 1. 延时任务
        print("\n1. 延时任务 (2 秒后):")
        start = time.perf_counter()
        delayed_id = scheduler.submit_delayed(
            {"name": "delayed_task", "kind": "normal"},
            delay_seconds=2
        )
        print(f"   结果: {scheduler.wait_for_result(delayed_id, timeout=10)}")
        print(f"   实际耗时: {time.perf_counter() - start:.2f}s")

        # 2. 周期任务（最多执行 2 次）
        print("\n2. 周期任务 (每 1 秒，最多 2 次):")
        periodic_id = scheduler.submit_interval(
            {"name": "periodic_task", "kind": "normal"},
            interval_seconds=1,
            run_immediately=True,
            max_runs=2,
            name="周期任务"
        )
        time.sleep(2.5)
        detail = scheduler.get_periodic_task(periodic_id)
        if detail:
            print(f"   run_count={detail['run_count']}, max_runs={detail['max_runs']}")
        scheduler.cancel_periodic(periodic_id)

        # 3. Cron 任务（最小粒度为分钟，这里只注册并查看下次执行时间）
        print("\n3. Cron 任务:")
        cron_expr = "0 9 * * *"
        cron_id = scheduler.submit_cron(
            {"name": "cron_task", "kind": "normal"},
            cron_expr,
            name="每日 9 点"
        )
        detail = scheduler.get_periodic_task(cron_id)
        print(f"   表达式: {cron_expr} -> {CronParser.describe(cron_expr)}")
        print(f"   下次执行: {detail['next_run'] if detail else 'N/A'}")
        scheduler.cancel_periodic(cron_id)

        # 4. 统计
        stats = scheduler.get_stats()
        print("\n4. 调度器统计:")
        print(f"   任务: 总={stats['total']}, 完成={stats['completed']}, "
              f"队列={stats['queue_size']}")
        print(f"   剩余周期任务: {len(scheduler.get_periodic_tasks())}")

    finally:
        scheduler.shutdown()


def main():
    print("\n" + "=" * 70)
    print("  NeoTask 完整功能演示")
    print("=" * 70)

    demo_taskpool()
    demo_taskscheduler()

    print("\n" + "=" * 70)
    print("  演示完成")
    print("=" * 70)


if __name__ == "__main__":
    main()
