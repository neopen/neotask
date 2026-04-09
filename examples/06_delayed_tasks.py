"""
@FileName: 06_delayed_tasks.py
@Description: 延时任务示例 - 演示延迟执行和定时任务
@Author: HiPeng
@Time: 2026/4/9
"""

import time
from datetime import datetime, timedelta
from neotask import TaskScheduler, SchedulerConfig


async def notification_task(data: dict) -> dict:
    """通知任务"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 发送通知: {data['message']}")
    return {"sent": True, "message": data["message"]}


def main():
    scheduler = TaskScheduler(
        executor=notification_task,
        config=SchedulerConfig.memory()
    )

    try:
        print("=== 延时任务示例 ===\n")

        # 1. 延时执行（3秒后）
        print("1. 延时 3 秒执行:")
        task_id = scheduler.submit_delayed(
            {"message": "3秒后发送的通知"},
            delay_seconds=3
        )
        print(f"   任务ID: {task_id}, 将在3秒后执行")

        # 2. 指定时间点执行
        execute_at = datetime.now() + timedelta(seconds=5)
        print(f"\n2. 指定时间点执行: {execute_at.strftime('%H:%M:%S')}")
        task_id = scheduler.submit_at(
            {"message": "定时发送的通知"},
            execute_at=execute_at
        )
        print(f"   任务ID: {task_id}")

        # 3. 周期执行（每2秒）
        print("\n3. 周期执行（每2秒）:")
        task_id = scheduler.submit_interval(
            {"message": "心跳通知"},
            interval_seconds=2,
            run_immediately=True
        )
        print(f"   任务ID: {task_id}, 将每2秒执行一次")

        # 等待观察执行效果
        print("\n等待 10 秒观察执行效果...")
        time.sleep(10)

        # 取消周期任务
        scheduler.cancel_periodic(task_id)
        print(f"\n已取消周期任务: {task_id}")

        # 查看统计
        stats = scheduler.get_stats()
        print(f"\n统计: 总任务={stats['total']}, 成功={stats['completed']}")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()