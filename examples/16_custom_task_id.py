"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: 16_custom_task_id.py
@Description: 外部指定 task_id 与幂等重提交 — 业务方自持 ID、存在性检查与重复提交处理
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio

from neotask import TaskAlreadyExistsError, TaskPool


async def process_order(data: dict) -> dict:
    """处理订单任务"""
    await asyncio.sleep(0.1)
    return {"status": "success", "order_id": data["order_id"]}


def submit_idempotent(pool: TaskPool, task_id: str, data: dict) -> str:
    """幂等提交：已存在则复用，不重复入队

    Args:
        pool: 任务池
        task_id: 业务方自持的任务 ID
        data: 任务数据

    Returns:
        实际生效的 task_id
    """
    if pool.task_exists(task_id):
        print(f"  任务 {task_id} 已存在，跳过重复提交")
        return task_id
    return pool.submit(data, task_id=task_id)


def main():
    with TaskPool(executor=process_order) as pool:
        # 1. 外部指定 task_id
        print("1. 外部指定 task_id")
        order_id = "ORDER-20261112-0001"
        task_id = pool.submit({"order_id": order_id}, task_id=f"ord-{order_id}")
        print(f"  提交返回: {task_id}")
        print(f"  与业务 ID 一致: {task_id == f'ord-{order_id}'}")

        # 2. 存在性检查
        print("\n2. 存在性检查")
        print(f"  task_exists('{task_id}'): {pool.task_exists(task_id)}")
        print(f"  task_exists('ord-not-submitted'): {pool.task_exists('ord-not-submitted')}")

        # 3. 重复提交会显式失败（避免同一业务 ID 被静默排队两次）
        print("\n3. 重复提交同一 task_id")
        try:
            pool.submit({"order_id": order_id}, task_id=task_id)
        except TaskAlreadyExistsError as exc:
            print(f"  已按预期拒绝: {exc}")

        # 4. 幂等重提交：先查后提，整段流程重试也不会重复入队
        print("\n4. 幂等重提交（先查后提）")
        retry_id = f"ord-{order_id}"
        for attempt in range(1, 3):
            print(f"  第 {attempt} 次调用 submit_idempotent:")
            submit_idempotent(pool, retry_id, {"order_id": order_id})

        other_id = submit_idempotent(pool, "ord-ORDER-20261112-0002", {"order_id": "ORDER-20261112-0002"})
        print(f"  新业务 ID 正常提交: {other_id}")

        # 5. 等待结果并确认按外部 ID 可查
        print("\n5. 按外部 ID 取结果")
        result = pool.wait_for_result(task_id, timeout=10)
        print(f"  {task_id} 结果: {result}")

        info = pool.get_task("ord-ORDER-20261112-0002")
        print(f"  另一任务状态: {info['status'] if info else '未找到'}")

        stats = pool.get_stats()
        print(f"\n统计: 总数={stats['total']}, 成功={stats['completed']}, 失败={stats['failed']}")


if __name__ == "__main__":
    main()
