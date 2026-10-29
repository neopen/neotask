"""
@FileName: 08_periodic.py
@Description: 周期任务管理示例 - 暂停、恢复、取消与最大执行次数
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/9
"""

import time

from neotask import TaskScheduler, SchedulerConfig


async def heartbeat_task(data: dict) -> dict:
    """周期执行的心跳任务"""
    print(f"  [{time.strftime('%H:%M:%S')}] {data['name']} 心跳")
    return {"status": "ok", "name": data["name"]}


def print_periodic(scheduler: TaskScheduler, title: str) -> None:
    """打印当前所有周期任务的运行状态"""
    print(f"\n{title}")
    for task in scheduler.get_periodic_tasks():
        print(
            f"  - {task['name']}({task['task_id']}): "
            f"status={task['status']}, run_count={task['run_count']}, "
            f"next_run={task['next_run']}"
        )


def main():
    scheduler = TaskScheduler(
        executor=heartbeat_task,
        # scan_interval 越短，周期任务的触发精度越高（这里设为 0.5s）
        config=SchedulerConfig(storage_type="memory", scan_interval=0.5)
    )
    scheduler.start()

    try:
        print("=== 周期任务管理示例 ===\n")

        # 1. 创建两个周期任务，B 只执行 2 次（max_runs）
        task_a = scheduler.submit_interval(
            {"name": "心跳A"},
            interval_seconds=1,
            name="心跳A(每1秒)",
            run_immediately=True
        )
        print(f"创建周期任务 A: {task_a} (每 1 秒, 无限次)")

        task_b = scheduler.submit_interval(
            {"name": "心跳B"},
            interval_seconds=2,
            name="心跳B(每2秒)",
            run_immediately=True,
            max_runs=2
        )
        print(f"创建周期任务 B: {task_b} (每 2 秒, 最多 2 次)")

        # 2. 观察执行
        print("\n等待 3 秒观察执行...")
        time.sleep(3)

        for task_id in (task_a, task_b):
            detail = scheduler.get_periodic_task(task_id)
            if detail:
                print(f"  {task_id}: status={detail['status']}, "
                      f"run_count={detail['run_count']}, "
                      f"max_runs={detail['max_runs']}")

        # 3. 暂停 / 恢复任务 A
        print(f"\n暂停任务 A: {scheduler.pause_periodic(task_a)}")
        time.sleep(2)

        print(f"恢复任务 A: {scheduler.resume_periodic(task_a)}")
        time.sleep(2)

        # 4. 查看状态，然后取消 A
        print_periodic(scheduler, "当前周期任务状态:")
        print(f"\n取消任务 A: {scheduler.cancel_periodic(task_a)}")

        # 5. 最终状态
        print_periodic(scheduler, "取消后的周期任务:")
        # 注意：达到 max_runs 的任务只是 status=completed，仍保留在列表中以便查询
        print(
            f"\n周期任务记录数: {len(scheduler.get_periodic_tasks())}"
            f"（已完成的 B 不会自动移除）"
        )

        stats = scheduler.get_stats()
        print(f"任务统计: 总={stats['total']}, 完成={stats['completed']}")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
