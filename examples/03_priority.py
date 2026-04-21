"""
@FileName: 03_priority.py
@Description: 优先级队列示例 - 演示不同优先级任务的执行顺序
@Author: HiPeng
@Time: 2026/4/2 17:24
"""
import asyncio

from neotask import TaskPool, TaskPoolConfig, TaskPriority


async def priority_task(data: dict) -> dict:
    """带优先级的任务"""
    priority_name = data.get("priority_name", "unknown")
    task_id = data.get("task_id", "?")
    duration = data.get("duration", 0.3)

    print(f"[{priority_name}] 执行: 任务 {task_id}")
    await asyncio.sleep(duration)
    print(f"[{priority_name}] 完成: 任务 {task_id}")

    return {"executed": True, "priority": priority_name, "task_id": task_id}


def main():
    config = TaskPoolConfig(
        storage_type="memory",
        worker_concurrency=1,  # 单 Worker，强制串行
        max_retries=0,
        enable_metrics=True,
        queue_max_size=100
    )

    print("=== 优先级队列演示 ===")
    print("优先级值越小，优先级越高")
    print("队列会按优先级排序，高优先级任务先执行")
    print(f"Worker 并发数: {config.worker_concurrency}\n")

    with TaskPool(executor=priority_task, config=config) as pool:
        tasks = [
            ("普通任务 1", TaskPriority.NORMAL, 1),
            ("紧急任务", TaskPriority.CRITICAL, 2),
            ("高优先级任务", TaskPriority.HIGH, 3),
            ("普通任务 2", TaskPriority.NORMAL, 4),
            ("低优先级任务", TaskPriority.LOW, 5),
        ]

        print("提交任务（添加延迟确保所有任务先入队）:")
        task_ids = []
        for msg, priority, tid in tasks:
            task_id = pool.submit(
                {"priority_name": priority.name, "task_id": tid, "duration": 0.3},
                priority=priority,
                delay=0.05  # 关键：短暂延迟，让所有任务先进入队列
            )
            task_ids.append(task_id)
            print(f"  已提交: {msg} (优先级={priority.name}, 值={priority.value})")

        print("\n等待任务执行...\n")

        for task_id in task_ids:
            try:
                pool.wait_for_result(task_id, timeout=10)
            except Exception as e:
                print(f"  任务 {task_id} 失败: {e}")

        print("\n预期执行顺序: 紧急任务 → 高优先级 → 普通任务 → 低优先级")
        print("实际执行顺序请观察上面的输出")

        stats = pool.get_stats()
        print(f"\n统计信息: 总任务={stats['total']}, 完成={stats['completed']}, 失败={stats['failed']}")


if __name__ == "__main__":
    main()
