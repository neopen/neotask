"""
@FileName: 99_all_features.py
@Description: 完整功能演示 - 展示所有主要特性
@Author: HiPeng
@Time: 2026/4/9
"""

import asyncio
import time
from neotask import TaskPool, TaskScheduler, TaskPoolConfig, SchedulerConfig, TaskPriority


async def demo_executor(data: dict) -> dict:
    """通用演示执行器"""
    name = data.get("name", "unknown")
    duration = data.get("duration", 0.3)

    print(f"  [执行] {name} (耗时 {duration}s)")
    await asyncio.sleep(duration)

    return {
        "name": name,
        "duration": duration,
        "success": True,
        "timestamp": time.time()
    }


def print_section(title: str):
    """打印章节标题"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


async def demo_taskpool():
    """演示 TaskPool 功能"""
    print_section("TaskPool - 即时任务池")

    with TaskPool(
        executor=demo_executor,
        config=TaskPoolConfig(worker_concurrency=5)
    ) as pool:
        # 1. 基本提交
        print("\n1. 基本提交:")
        task_id = pool.submit({"name": "basic_task"})
        result = pool.wait_for_result(task_id)
        print(f"   结果: {result}")

        # 2. 优先级
        print("\n2. 优先级队列:")
        tasks = [
            ("低优先级", TaskPriority.LOW),
            ("高优先级", TaskPriority.HIGH),
            ("紧急", TaskPriority.CRITICAL),
        ]
        for name, priority in tasks:
            task_id = pool.submit({"name": name}, priority=priority)
            print(f"   提交: {name} (优先级={priority.value})")

        # 等待优先级任务完成
        time.sleep(1.5)

        # 3. 批量提交
        print("\n3. 批量提交:")
        batch_data = [{"name": f"batch_{i}"} for i in range(5)]
        task_ids = pool.submit_batch(batch_data)
        print(f"   提交了 {len(task_ids)} 个任务")

        # 4. 等待所有
        results = pool.wait_all(task_ids)
        print(f"   完成: {len([r for r in results.values() if r])}")

        # 5. 统计
        stats = pool.get_stats()
        print(f"\n统计: 总={stats['total']}, 完成={stats['completed']}")


async def demo_taskscheduler():
    """演示 TaskScheduler 功能"""
    print_section("TaskScheduler - 定时任务调度器")

    scheduler = TaskScheduler(
        executor=demo_executor,
        config=SchedulerConfig.memory()
    )
    scheduler.start()

    try:
        # 1. 延时任务
        print("\n1. 延时任务 (3秒后):")
        task_id = scheduler.submit_delayed(
            {"name": "delayed_task"},
            delay_seconds=3
        )
        print(f"   任务ID: {task_id}")

        # 2. 周期任务
        print("\n2. 周期任务 (每2秒):")
        periodic_id = scheduler.submit_interval(
            {"name": "periodic_task"},
            interval_seconds=2,
            run_immediately=True
        )
        print(f"   任务ID: {periodic_id}")

        # 3. 等待观察
        print("\n等待 6 秒观察执行...")
        for i in range(6):
            try:
                stats = scheduler.get_stats()
                periodic_count = stats.get("periodic_tasks_count", 0)
                print(f"\r   第{i+1}秒: 队列={stats['queue_size']}, "
                      f"周期任务数={periodic_count}", end="", flush=True)
            except Exception:
                print(f"\r   第{i+1}秒: 观察中...", end="", flush=True)
            time.sleep(1)

        print()  # 换行

        # 4. 取消周期任务
        print(f"\n取消周期任务: {periodic_id}")
        scheduler.cancel_periodic(periodic_id)

        # 5. 获取统计
        try:
            stats = scheduler.get_stats()
            print(f"\n最终统计: 总任务={stats['total']}, 完成={stats['completed']}")
        except Exception:
            print("\n最终统计: 获取失败")

    finally:
        scheduler.shutdown()


async def main():
    print("\n" + "=" * 70)
    print("  NeoTask 完整功能演示")
    print("=" * 70)

    await demo_taskpool()
    await demo_taskscheduler()

    print("\n" + "=" * 70)
    print("  演示完成")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())