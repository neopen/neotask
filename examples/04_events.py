"""
@FileName: 04_events.py
@Description: 事件回调示例 - 监听任务生命周期事件
@Author: HiPeng
@Time: 2026/4/2 17:24
"""

import asyncio
from neotask import TaskPool, TaskPoolConfig


async def event_task(data: dict) -> dict:
    """示例任务"""
    print(f"  执行任务 {data['id']}...")
    await asyncio.sleep(0.3)
    return {"status": "done", "id": data["id"]}


# 事件处理函数（必须是 async）
async def on_created(event):
    print(f"📝 任务创建: {event.task_id}")


async def on_started(event):
    print(f"▶️  任务开始: {event.task_id}")


async def on_completed(event):
    print(f"✅ 任务完成: {event.task_id}, 结果: {event.data}")


async def on_failed(event):
    print(f"❌ 任务失败: {event.task_id}, 错误: {event.data}")


async def on_cancelled(event):
    print(f"⏹️  任务取消: {event.task_id}")


async def main():
    pool = TaskPool(
        executor=event_task,
        config=TaskPoolConfig(worker_concurrency=3)
    )

    try:
        # 注册事件处理器
        pool.on_created(on_created)
        pool.on_started(on_started)
        pool.on_completed(on_completed)
        pool.on_failed(on_failed)
        pool.on_cancelled(on_cancelled)

        print("事件监听已注册\n")

        # 提交任务
        task_ids = []
        for i in range(3):
            task_id = await pool.submit_async({"id": i})
            task_ids.append(task_id)
            print(f"  提交任务: {task_id}")

        # 等待所有任务完成
        for task_id in task_ids:
            result = await pool.wait_for_result_async(task_id)
            print(f"  结果: {result}")

        print("\n所有任务完成")

    finally:
        pool.shutdown()


if __name__ == "__main__":
    asyncio.run(main())