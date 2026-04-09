"""
@FileName: 10_task_query.py
@Description: 任务查询示例 - 查询任务状态、结果和历史
@Author: HiPeng
@Time: 2026/4/9
"""

import time
from neotask import TaskPool


async def sample_task(data: dict) -> dict:
    """示例任务"""
    time.sleep(0.2)
    return {"result": data["value"] * 2, "id": data["id"]}


def main():
    pool = TaskPool(executor=sample_task)

    try:
        print("=== 任务查询示例 ===\n")

        # 提交多个任务
        task_ids = []
        for i in range(5):
            task_id = pool.submit({"id": i, "value": i})
            task_ids.append(task_id)
            print(f"提交任务 {i}: {task_id}")

        # 等待任务完成
        time.sleep(2)

        # 查询每个任务
        print("\n任务详情:")
        for task_id in task_ids:
            # 获取状态
            status = pool.get_status(task_id)
            # 获取结果
            result = pool.get_result(task_id)
            # 获取完整信息
            task_info = pool.get_task(task_id)

            print(f"\n任务: {task_id}")
            print(f"  状态: {status}")
            print(f"  结果: {result['result'] if result else 'N/A'}")
            print(f"  创建时间: {task_info.get('created_at') if task_info else 'N/A'}")

        # 检查任务是否存在
        print(f"\n任务存在性检查:")
        print(f"  {task_ids[0]} 存在: {pool.task_exists(task_ids[0])}")
        print(f"  nonexistent 存在: {pool.task_exists('nonexistent')}")

        # 获取统计
        stats = pool.get_stats()
        print(f"\n全局统计:")
        print(f"  总任务: {stats['total']}")
        print(f"  待处理: {stats['pending']}")
        print(f"  运行中: {stats['running']}")
        print(f"  已完成: {stats['completed']}")
        print(f"  失败: {stats['failed']}")

    finally:
        pool.shutdown()


if __name__ == "__main__":
    main()