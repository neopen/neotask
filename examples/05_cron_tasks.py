"""
@FileName: 05_cron_tasks.py
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

        # 注意：当前 Cron 解析器简化实现，返回 3 秒后执行
        # 生产环境建议安装 croniter 库实现完整 Cron 支持
        print("1. Cron 任务（简化版，3秒后执行）:")
        task_id = scheduler.submit_cron(
            {"report_type": "cron_report"},
            cron_expr="*/1 * * * *"
        )
        print(f"   任务ID: {task_id}")

        # 2. 周期任务（推荐用于快速测试）
        print("\n2. 周期任务（每3秒执行）:")
        interval_id = scheduler.submit_interval(
            {"report_type": "interval_report"},
            interval_seconds=3,
            run_immediately=True
        )
        print(f"   任务ID: {interval_id}")

        # 查看所有周期任务
        print("\n已注册的周期任务:")
        for task in scheduler.get_periodic_tasks():
            print(f"  - {task['task_id']}: interval={task['interval_seconds']}s, "
                  f"cron={task['cron_expr']}, run_count={task['run_count']}")

        print("\n等待 10 秒观察执行...")
        await asyncio.sleep(10)

        # 取消所有周期任务
        for task in scheduler.get_periodic_tasks():
            scheduler.cancel_periodic(task["task_id"])

        print("\n所有周期任务已取消")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())