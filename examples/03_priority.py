"""
@FileName: 03_priority.py
@Description: 优先级队列示例 - 演示不同优先级任务的执行顺序
@Author: HiPeng
@Time: 2026/4/2 17:24
"""

import time
from neotask import TaskPool, TaskPoolConfig, TaskPriority


async def priority_task(data: dict) -> dict:
    """带优先级的任务"""
    priority_name = data.get("priority_name", "unknown")
    task_id = data.get("task_id", "?")

    print(f"[{priority_name}] 开始执行: 任务 {task_id}")
    time.sleep(0.5)  # 增加执行时间，便于观察顺序
    print(f"[{priority_name}] 完成: 任务 {task_id}")

    return {"executed": True, "priority": priority_name}


def main():
    # 限制并发数为 1，强制串行执行，这样才能看到优先级效果
    pool = TaskPool(
        executor=priority_task,
        config=TaskPoolConfig(
            worker_concurrency=1,  # 关键：单 Worker，强制串行
            max_retries=0
        )
    )

    try:
        print("=== 优先级队列演示 ===")
        print("Worker 并发数: 1 (串行执行)")
        print("优先级值越小，优先级越高\n")

        # 提交不同优先级的任务
        # 优先级值: CRITICAL=0, HIGH=1, NORMAL=2, LOW=3
        tasks = [
            ("普通任务 1", TaskPriority.NORMAL, 1),
            ("紧急任务", TaskPriority.CRITICAL, 2),
            ("高优先级任务", TaskPriority.HIGH, 3),
            ("普通任务 2", TaskPriority.NORMAL, 4),
            ("低优先级任务", TaskPriority.LOW, 5),
        ]

        print("提交任务（按提交顺序，但队列会按优先级重排）:")
        for msg, priority, tid in tasks:
            task_id = pool.submit(
                {"priority_name": priority.name, "task_id": tid},
                priority=priority
            )
            print(f"  已提交: {msg} (优先级={priority.name}, 值={priority.value})")

        print("\n等待任务执行...\n")

        # 等待所有任务完成
        time.sleep(5)

        print("\n预期执行顺序: 紧急任务 → 高优先级 → 普通任务 → 低优先级")
        print("实际执行顺序请观察上面的输出")

        # 查看统计
        stats = pool.get_stats()
        print(f"\n统计信息: 总任务={stats['total']}, 完成={stats['completed']}")

    finally:
        pool.shutdown()


if __name__ == "__main__":
    main()