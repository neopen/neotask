"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: 19_parent_child.py
@Description: 父子任务关联 — parent_task_id 提交、get_children / get_parent 查询与汇总
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio

from neotask import TaskPool, TaskPoolConfig


async def run_shard(data: dict) -> dict:
    """处理一个分片：返回分片名与行数"""
    await asyncio.sleep(0.1)
    return {"shard": data["shard"], "rows": data["rows"]}


def main():
    with TaskPool(
            executor=run_shard,
            config=TaskPoolConfig(worker_concurrency=3, max_retries=0),
    ) as pool:
        # 1. 先建父任务（外部指定 ID，便于子任务直接引用）
        print("1. 提交父任务")
        parent_id = pool.submit({"shard": "parent", "rows": 0}, task_id="report-001")
        print(f"  父任务: {parent_id}")

        # 2. 子任务携带 parent_task_id
        print("\n2. 提交子任务（parent_task_id 关联）")
        child_ids = [
            pool.submit({"shard": f"shard-{i}", "rows": rows},
                        parent_task_id=parent_id)
            for i, rows in enumerate((120, 80, 200), start=1)
        ]
        for task_id in child_ids:
            print(f"  子任务: {task_id}")

        pool.wait_all(child_ids, timeout=10)

        # 3. 按父查子：创建时间升序
        print("\n3. 查询父子关系")
        children = pool.get_children(parent_id)
        print(f"  get_children({parent_id}) -> {len(children)} 条")
        print(f"  与提交顺序一致: {children == child_ids}")

        # 4. 按子查父
        print("\n4. 反向查询")
        for task_id in child_ids:
            print(f"  {task_id} 的父任务: {pool.get_parent(task_id)}")
        print(f"  父任务自身无父级: {pool.get_parent(parent_id)}")

        # 5. 汇总子任务结果（父任务的完成判定由编排层自行决定）
        print("\n5. 汇总子任务结果")
        total_rows = 0
        for task_id in children:
            info = pool.get_result(task_id)
            total_rows += info["result"]["rows"]
            print(f"  {info['result']['shard']}: {info['result']['rows']} 行")
        print(f"  合计: {total_rows} 行")

        # 6. 每个任务都有耗时/排队时长字段
        print("\n6. 任务耗时字段")
        for task_id in child_ids[:1]:
            info = pool.get_result(task_id)
            print(f"  queue_wait_ms={info['queue_wait_ms']}, "
                  f"duration_ms={info['duration_ms']}")


if __name__ == "__main__":
    main()
