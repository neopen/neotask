"""
@FileName: 10_task_query.py
@Description: 任务查询示例 - 查询状态、结果、完整信息、统计与健康状态
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/9
"""

import asyncio

from neotask import TaskPool, TaskPoolConfig


async def sample_task(data: dict) -> dict:
    """示例任务"""
    await asyncio.sleep(0.2)
    return {"result": data["value"] * 2, "id": data["id"]}


def main():
    print("=== 任务查询示例 ===\n")

    with TaskPool(executor=sample_task, config=TaskPoolConfig(worker_concurrency=3)) as pool:
        # 1. 提交多个任务，并一次性等待全部完成
        task_ids = []
        for i in range(5):
            task_id = pool.submit({"id": i, "value": i})
            task_ids.append(task_id)
            print(f"提交任务 {i}: {task_id}")

        results = pool.wait_all(task_ids, timeout=10)
        print(f"\n全部完成: {len(results)} 个\n")

        # 2. 查询每个任务
        print("任务详情:")
        for task_id in task_ids:
            status = pool.get_status(task_id)                 # 仅状态字符串
            result = pool.get_result(task_id)                  # 状态 + 结果 + 时间戳
            task_info = pool.get_task(task_id)                 # 完整信息（含 data/优先级/重试次数）

            print(f"\n任务: {task_id}")
            print(f"  状态: {status}")
            print(f"  结果: {result['result'] if result else 'N/A'}")
            print(f"  创建时间: {task_info['created_at']}")
            print(f"  完成时间: {task_info['completed_at']}")
            print(f"  重试次数: {task_info['retry_count']}")

        # 3. 任务存在性检查
        print("\n任务存在性检查:")
        print(f"  {task_ids[0]} 存在: {pool.task_exists(task_ids[0])}")
        print(f"  nonexistent 存在: {pool.task_exists('nonexistent')}")

        # 4. 全局统计
        stats = pool.get_stats()
        print("\n全局统计:")
        print(f"  总任务: {stats['total']}")
        print(f"  待处理: {stats['pending']}")
        print(f"  运行中: {stats['running']}")
        print(f"  已完成: {stats['completed']}")
        print(f"  失败: {stats['failed']}")
        print(f"  取消: {stats['cancelled']}")
        print(f"  成功率: {stats['success_rate']:.1%}")
        print(f"  队列大小: {stats['queue_size']}")

        # 5. 健康状态（默认 5 秒内复用上次检查结果，避免探针高频调用打爆 CPU）
        health = pool.get_health_status()
        print("\n健康状态:")
        print(f"  状态: {health['status']}")
        print(f"  节点ID: {health['node_id']}")


if __name__ == "__main__":
    main()
