"""
@FileName: 08_periodic_management.py
@Description: 周期任务管理示例 - 暂停、恢复、取消周期任务
@Author: HiPeng
@Time: 2026/4/9
"""

import time
from neotask import TaskScheduler, SchedulerConfig


async def heartbeat_task(data: dict) -> dict:
    """心跳任务"""
    print(f"[{data['name']}] 心跳信号 - {time.strftime('%H:%M:%S')}")
    return {"status": "ok", "name": data["name"]}


def main():
    scheduler = TaskScheduler(
        executor=heartbeat_task,
        config=SchedulerConfig.memory()
    )

    try:
        print("=== 周期任务管理示例 ===\n")

        # 创建多个周期任务
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

        # 暂停任务 A
        print(f"\n暂停任务 A: {task1}")
        scheduler.pause_periodic(task1)
        time.sleep(3)

        # 恢复任务 A
        print(f"恢复任务 A: {task1}")
        scheduler.resume_periodic(task1)
        time.sleep(3)

        # 查看周期任务状态
        print("\n当前周期任务状态:")
        for task in scheduler.get_periodic_tasks():
            print(f"  - {task['task_id']}: "
                  f"run_count={task['run_count']}, "
                  f"paused={task['is_paused']}")

        # 取消任务 B
        print(f"\n取消任务 B: {task2}")
        scheduler.cancel_periodic(task2)
        time.sleep(2)

        # 最终状态
        print("\n最终周期任务:")
        for task in scheduler.get_periodic_tasks():
            print(f"  - {task['task_id']}: run_count={task['run_count']}")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()