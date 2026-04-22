"""
@FileName: 08_periodic.py
@Description: 周期任务管理示例 - 暂停、恢复、取消周期任务
@Author: HiPeng
@Time: 2026/4/9
"""

import time

from neotask import TaskScheduler, SchedulerConfig


async def heartbeat_task(data: dict) -> dict:
    print(f"[{data['name']}] 心跳信号 - {time.strftime('%H:%M:%S')}")
    return {"status": "ok", "name": data["name"]}


def main():
    scheduler = TaskScheduler(
        executor=heartbeat_task,
        config=SchedulerConfig.memory()
    )

    try:
        print("=== 周期任务管理示例 ===\n")

        task1 = scheduler.submit_interval(
            {"name": "心跳A"},
            interval_seconds=1,
            run_immediately=True
        )
        print(f"创建周期任务 A: {task1} (每1秒)")

        task2 = scheduler.submit_interval(
            {"name": "心跳B"},
            interval_seconds=2,
            run_immediately=True
        )
        print(f"创建周期任务 B: {task2} (每2秒)")

        print("\n等待 5 秒观察执行...")
        time.sleep(5)

        # 使用公开 API 获取统计信息
        tasks = scheduler.get_periodic_tasks()
        for task in tasks:
            if task['task_id'] == task1:
                print(f"\n[INFO] 任务 A run_count = {task['run_count']}")
            if task['task_id'] == task2:
                print(f"[INFO] 任务 B run_count = {task['run_count']}")

        print(f"\n暂停任务 A: {task1}")
        scheduler.pause_periodic(task1)
        time.sleep(3)

        print(f"恢复任务 A: {task1}")
        scheduler.resume_periodic(task1)
        time.sleep(3)

        print("\n当前周期任务状态:")
        for task in scheduler.get_periodic_tasks():
            print(f"  - {task['task_id']}: "
                  f"run_count={task['run_count']}, "
                  f"paused={task['is_paused']}")

        print(f"\n取消任务 B: {task2}")
        scheduler.cancel_periodic(task2)
        time.sleep(2)

        print("\n最终周期任务:")
        for task in scheduler.get_periodic_tasks():
            print(f"  - {task['task_id']}: run_count={task['run_count']}")

    finally:
        scheduler.shutdown()

if __name__ == "__main__":
    main()
