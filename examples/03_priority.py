"""
@FileName: 03_priority.py
@Description: 优先级队列示例 - 演示不同优先级任务的执行顺序
@Author: HiPeng
@Time: 2026/4/2 17:24
"""

import time
from neotask import TaskPool, TaskPriority


async def priority_task(data: dict) -> dict:
    """带优先级的任务"""
    priority_name = data.get("priority_name", "unknown")
    print(f"[{priority_name}] 执行中: {data['msg']}")
    time.sleep(0.1)
    return {"executed": True, "priority": priority_name}


def main():
    pool = TaskPool(executor=priority_task)

    try:
        # 提交不同优先级的任务
        # 优先级值越小，优先级越高
        tasks = [
            ("紧急任务", TaskPriority.CRITICAL, 0),
            ("高优先级任务", TaskPriority.HIGH, 1),
            ("普通任务 1", TaskPriority.NORMAL, 2),
            ("普通任务 2", TaskPriority.NORMAL, 2),
            ("低优先级任务", TaskPriority.LOW, 3),
        ]

        print("提交任务（高优先级会先执行）:")
        for msg, priority, _ in tasks:
            task_id = pool.submit(
                {"msg": msg, "priority_name": priority.name},
                priority=priority
            )
            print(f"  已提交: {msg} (优先级={priority.name})")

        # 等待任务完成
        time.sleep(2)
        print("\n所有任务执行完成")

        # 查看统计
        stats = pool.get_stats()
        print(f"\n统计信息: 已完成={stats['completed']}, 队列={stats['queue_size']}")

    finally:
        pool.shutdown()


if __name__ == "__main__":
    main()