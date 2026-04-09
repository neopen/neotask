"""
@FileName: 07_cron_tasks.py
@Description: Cron 定时任务示例 - 演示 Cron 表达式定时任务
@Author: HiPeng
@Time: 2026/4/9
"""

import asyncio
from neotask import TaskScheduler, SchedulerConfig


async def report_task(data: dict) -> dict:
    """报表生成任务"""
    print(f"[定时任务] 生成报表: {data['report_type']}")
    return {"generated": True, "type": data["report_type"]}


async def main():
    scheduler = TaskScheduler(
        executor=report_task,
        config=SchedulerConfig.memory()
    )

    try:
        print("=== Cron 定时任务示例 ===\n")

        # Cron 表达式格式: 分 时 日 月 周
        # * 表示任意值，*/n 表示每 n 单位

        # 1. 每分钟执行一次
        print("1. 每分钟执行一次:")
        task_id = scheduler.submit_cron(
            {"report_type": "minute_report"},
            cron_expr="*/1 * * * *"
        )
        print(f"   任务ID: {task_id}")

        # 2. 每5秒执行一次（测试用，实际Cron最小单位是分钟）
        print("\n2. 测试任务（5秒后执行）:")
        # 注意：Cron 最小单位是分钟，这里用延时任务模拟
        task_id = scheduler.submit_delayed(
            {"report_type": "test_report"},
            delay_seconds=5
        )
        print(f"   任务ID: {task_id}")

        # 查看所有周期任务
        print("\n已注册的周期任务:")
        for task in scheduler.get_periodic_tasks():
            print(f"  - {task['task_id']}: interval={task['interval_seconds']}s, "
                  f"cron={task['cron_expr']}, run_count={task['run_count']}")

        print("\n等待 6 秒观察执行...")
        await asyncio.sleep(6)

        # 取消所有周期任务
        for task in scheduler.get_periodic_tasks():
            scheduler.cancel_periodic(task["task_id"])

        print("\n所有周期任务已取消")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())