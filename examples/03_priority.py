"""
@FileName: 03_priority.py
@Description: 优先级队列示例 - 演示不同优先级任务的执行顺序
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 17:24
"""

import time

from neotask import TaskPool, TaskPoolConfig, TaskPriority


# 记录实际执行顺序（单 Worker + 关闭预取，执行顺序即可代表出队顺序）
EXECUTED: list = []


def priority_task(data: dict) -> dict:
    """带优先级的任务"""
    priority_name = data["priority_name"]
    EXECUTED.append(priority_name)

    print(f"  [执行] {priority_name}")
    time.sleep(0.2)  # 模拟耗时操作
    return {"executed": True, "priority": priority_name}


def main():
    config = TaskPoolConfig(
        storage_type="memory",
        worker_concurrency=1,     # 单 Worker，强制串行执行
        enable_prefetch=False,    # 关闭预取，保证严格按优先级出队
        max_retries=0,
        queue_max_size=100
    )

    print("=== 优先级队列演示 ===")
    print("优先级值越小优先级越高：CRITICAL(0) > HIGH(1) > NORMAL(2) > LOW(3)")
    print(f"Worker 并发数: {config.worker_concurrency}，预取: {config.enable_prefetch}\n")

    with TaskPool(executor=priority_task, config=config) as pool:
        # 先暂停出队：保证 4 个任务全部入队后再按优先级出队，
        # 否则 Worker 可能在提交过程中就把先入队的低优先级任务取走
        pool.pause()

        submitted = [
            ("LOW", TaskPriority.LOW),
            ("NORMAL", TaskPriority.NORMAL),
            ("HIGH", TaskPriority.HIGH),
            ("CRITICAL", TaskPriority.CRITICAL),
        ]
        task_ids = []
        for name, priority in submitted:
            task_id = pool.submit({"priority_name": name}, priority=priority)
            task_ids.append(task_id)
            print(f"  已提交: {name} (值={priority.value})")

        print(f"\n暂停期间队列长度: {pool.get_queue_size()}（任务只入队不出队）")
        pool.resume()
        print("恢复出队，等待执行...\n")

        results = pool.wait_all(task_ids, timeout=10)
        failed = [tid for tid, r in results.items() if isinstance(r, dict) and "error" in r]

        print(f"\n实际执行顺序: {' → '.join(EXECUTED)}")
        expected = ["CRITICAL", "HIGH", "NORMAL", "LOW"]
        print(f"预期执行顺序: {' → '.join(expected)}")
        print(f"顺序是否符合预期: {'是' if EXECUTED == expected else '否'}")

        stats = pool.get_stats()
        print(
            f"\n统计信息: 总任务={stats['total']}, 完成={stats['completed']}, "
            f"失败={len(failed)}"
        )


if __name__ == "__main__":
    main()
