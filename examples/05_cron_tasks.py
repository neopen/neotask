"""
@FileName: 05_cron_tasks.py
@Description: Cron 定时任务示例 - 演示 Cron 表达式定时任务
@Author: HiPeng
@Time: 2026/4/9
"""

import asyncio
from datetime import datetime

from neotask import TaskScheduler, SchedulerConfig


async def report_task(data: dict) -> dict:
    """报表生成任务"""
    print(f"[定时任务] 生成报表({datetime.now()}): {data['report_type']}")
    return {"generated": True, "type": data["report_type"]}


async def main():
    scheduler = TaskScheduler(
        executor=report_task,
        config=SchedulerConfig.memory()
    )

    try:
        print("=== Cron 定时任务示例 ===\n")

        # 方式1：使用 delay 参数实现延时任务
        print("1. 延时任务（3秒后执行一次）:")
        task_id = scheduler.submit_delayed(
            {"report_type": "delayed_report"},
            delay_seconds=3
        )
        print(f"   任务ID: {task_id}")

        # 方式2：使用 submit_at 指定时间点执行
        from datetime import timedelta
        execute_at = datetime.now() + timedelta(seconds=5)
        print(f"\n2. 定时任务（{execute_at.strftime('%H:%M:%S')} 执行）:")
        task_id2 = scheduler.submit_at(
            {"report_type": "scheduled_report"},
            execute_at=execute_at
        )
        print(f"   任务ID: {task_id2}")

        # 方式3：使用周期任务（推荐）
        print("\n3. 周期任务（每3秒执行）:")
        interval_id = scheduler.submit_interval(
            {"report_type": "interval_report"},
            interval_seconds=3,
            run_immediately=True
        )
        print(f"   任务ID: {interval_id}")

        print("\n等待 12 秒观察执行...")
        await asyncio.sleep(12)

        # 取消周期任务
        scheduler.cancel_periodic(interval_id)
        print("\n周期任务已取消")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())