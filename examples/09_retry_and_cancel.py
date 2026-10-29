"""
@FileName: 09_retry_and_cancel.py
@Description: 重试和取消示例 - 自动重试、手动重试与排队任务取消
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/9
"""

import time

from neotask import TaskPool, TaskPoolConfig


def auto_retry_demo():
    """1. 自动重试：失败后按 retry_delay * retry_count 退避重新入队"""
    print("======= 1. 自动重试（max_retries=2）=======")
    attempts = {"count": 0}

    def flaky_task(data: dict) -> dict:
        attempts["count"] += 1
        print(f"  第 {attempts['count']} 次执行")
        if attempts["count"] < 3:
            raise ValueError(f"模拟第 {attempts['count']} 次失败")
        return {"status": "success", "attempts": attempts["count"]}

    # max_retries=2 表示失败后最多再重试 2 次，即总共最多执行 3 次
    with TaskPool(
        executor=flaky_task,
        config=TaskPoolConfig(
            max_retries=2,
            retry_delay=0.3,   # 第 n 次重试前等待 0.3 * n 秒
            worker_concurrency=1
        )
    ) as pool:
        task_id = pool.submit({"id": 1})
        result = pool.wait_for_result(task_id, timeout=20)
        stats = pool.get_stats()

        print(f"  最终结果: {result}")
        print(f"  实际执行次数: {attempts['count']}（1 次原始 + {attempts['count'] - 1} 次重试）")
        print(f"  统计: 完成={stats['completed']}, 失败={stats['failed']}")


def manual_retry_demo():
    """2. 手动重试：对已失败任务调用 retry()，状态会重置为 PENDING 后再执行"""
    print("\n======= 2. 手动重试（retry）=======")
    state = {"fail": True, "count": 0}

    def once_failing_task(data: dict) -> dict:
        state["count"] += 1
        print(f"  第 {state['count']} 次执行")
        if state["fail"]:
            raise ValueError("模拟失败")
        return {"status": "success", "attempts": state["count"]}

    with TaskPool(
        executor=once_failing_task,
        config=TaskPoolConfig(max_retries=0)  # 关闭自动重试，只观察手动重试
    ) as pool:
        task_id = pool.submit({"id": 2})
        try:
            pool.wait_for_result(task_id, timeout=10)
        except Exception as e:
            print(f"  任务失败: {e}")

        print(f"  失败后状态: {pool.get_status(task_id)}")

        # 修复业务问题后手动重试：内部会把 failed/cancelled 任务重置为 PENDING
        state["fail"] = False
        retried = pool.retry(task_id)
        print(f"  retry() 返回: {retried}")

        result = pool.wait_for_result(task_id, timeout=10)
        print(f"  重试结果: {result}")
        print(f"  最终状态: {pool.get_status(task_id)}")


def cancel_demo():
    """3. 取消排队中的任务：暂停出队后提交，取消不会被 Worker 执行"""
    print("\n======= 3. 取消任务 =======")

    async def slow_task(data: dict) -> dict:
        import asyncio

        await asyncio.sleep(1)
        return {"id": data["id"]}

    with TaskPool(
        executor=slow_task,
        config=TaskPoolConfig(worker_concurrency=2, max_retries=0)
    ) as pool:
        # 暂停出队，保证任务都停在队列里，再逐个取消
        pool.pause()
        task_ids = []
        for i in range(5):
            task_ids.append(pool.submit({"id": i}))
        print(f"  已提交 5 个任务，暂停期间队列长度: {pool.get_queue_size()}")

        for task_id in task_ids:
            print(f"  取消 {task_id}: {'成功' if pool.cancel(task_id) else '失败'}")

        pool.resume()
        time.sleep(0.5)

        statuses = {pool.get_status(task_id) for task_id in task_ids}
        print(f"  取消后的状态集合: {statuses}")
        stats = pool.get_stats()
        print(f"  统计: 取消={stats['cancelled']}, 完成={stats['completed']}")

    print("  说明: 取消仅对未开始执行的任务是“干净”的；对正在运行的任务只会标记为")
    print("        CANCELLED，执行中的协程不会被强制中断，业务侧应在任务内自行检查中断信号。")


def main():
    auto_retry_demo()
    manual_retry_demo()
    cancel_demo()


if __name__ == "__main__":
    main()
