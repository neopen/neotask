"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: 21_blackboard.py
@Description: 本地上下文 — Memory 后端 context_set/context_get/context_delete 与 TTL
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio

from neotask import TaskPool, TaskPoolConfig, UnsupportedFeatureError


def main():
    holder = {}

    async def read_shared(data: dict) -> dict:
        """从共享上下文读取前置任务写入的配置"""
        await asyncio.sleep(0.05)
        return {"config": holder["pool"].context_get("job-config")}

    with TaskPool(executor=read_shared, config=TaskPoolConfig()) as pool:
        holder["pool"] = pool

        print("1. 写入共享上下文")
        pool.context_set("job-config", {"batch_size": 100, "retry": 2})
        print(f"  context_get -> {pool.context_get('job-config')}")

        print("\n2. 任务读取同一份上下文")
        task_id = pool.submit({})
        print(f"  任务结果: {pool.wait_for_result(task_id, timeout=10)}")

        print("\n3. 带 TTL 的键")
        pool.context_set("temporary", "5 秒后过期", ttl=5)
        print(f"  立即读取: {pool.context_get('temporary')}")

        print("\n4. 删除键")
        print(f"  context_delete -> {pool.context_delete('job-config')}")
        print(f"  删除后读取: {pool.context_get('job-config')}")

    print("\n5. 非 Memory 后端会显式报错（不静默降级）")
    # 不启动 SQLite 实例，仅验证装配后的后端判定
    sqlite_pool = TaskPool(
        executor=read_shared,
        config=TaskPoolConfig.sqlite(":memory:"),
    )
    try:
        sqlite_pool.context_set("k", 1)
    except UnsupportedFeatureError as exc:
        print(f"  {exc}")


if __name__ == "__main__":
    main()
