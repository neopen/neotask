"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: 20_checkpoint.py
@Description: 断点续跑 — save_checkpoint/load_checkpoint、重试恢复、task.checkpointed 事件
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio

from neotask import (
    CHECKPOINT_DATA_KEY,
    EventType,
    TaskPool,
    TaskPoolConfig,
    load_checkpoint,
    save_checkpoint,
)

# 模拟分页抓取：每页处理后保存 checkpoint，失败重试时从上次页码继续
PAGES = 5


async def crawl(data: dict) -> dict:
    """逐页处理；第 3 页第一次执行时故意抛错"""
    previous = data.get(CHECKPOINT_DATA_KEY) or await load_checkpoint() or {}
    page = previous.get("page", 0)

    while page < PAGES:
        page += 1
        if page == 3 and not previous.get("failed_once"):
            await save_checkpoint({"page": page - 1, "failed_once": True})
            raise RuntimeError("模拟网络中断")
        await asyncio.sleep(0.02)
        await save_checkpoint({"page": page})
        print(f"  已处理第 {page} 页")

    return {"pages": page}


def main():
    config = TaskPoolConfig(max_retries=0)  # 关闭自动重试，手动演示恢复
    with TaskPool(executor=crawl, config=config) as pool:
        pool.event_bus.subscribe(
            EventType.CHECKPOINTED,
            lambda event: print(
                f"  [事件] task.checkpointed {event.task_id} version={event.data['version']}"
            ),
        )

        print("1. 首次执行（第 3 页失败）")
        task_id = pool.submit({"job": "crawl"}, task_id="crawl-001")
        try:
            pool.wait_for_result(task_id, timeout=10)
        except Exception as exc:
            print(f"  执行失败: {exc}")

        info = pool.get_task(task_id)
        print(f"  checkpoint_version={info['checkpoint_version']}")
        print(f"  最近状态: {pool.load_checkpoint(task_id)}")

        print("\n2. 手动重试：自动带上最近 checkpoint 续跑")
        pool.retry(task_id)
        result = pool.wait_for_result(task_id, timeout=10)
        print(f"  最终结果: {result}")

        info = pool.get_task(task_id)
        print(f"  checkpoint_version={info['checkpoint_version']}")

        print("\n3. 显式形态（进程执行器等拿不到上下文的场景）")
        version = pool.save_checkpoint(task_id, {"cursor": 42})
        print(f"  pool.save_checkpoint -> version={version}")
        print(f"  pool.load_checkpoint -> {pool.load_checkpoint(task_id)}")


if __name__ == "__main__":
    main()
