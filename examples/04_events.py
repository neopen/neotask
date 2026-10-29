"""
@FileName: 04_events.py
@Description: 事件回调示例 - 监听任务生命周期事件（含进度、失败、取消）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 17:24
"""

import asyncio
import time

from neotask import TaskPool, TaskPoolConfig, report_progress


async def event_task(data: dict) -> dict:
    """根据 kind 决定成功还是失败，用于演示不同事件"""
    if data["kind"] == "fail":
        await asyncio.sleep(0.1)
        raise ValueError("模拟任务失败")

    # 上报进度（异步任务内调用，事件会广播给 on_progress 处理器）
    await report_progress(0.3, "准备中")
    await asyncio.sleep(0.1)
    await report_progress(0.8, "处理中")
    await asyncio.sleep(0.1)
    return {"status": "done", "id": data["id"]}


# 事件处理函数（必须是 async 函数）
async def on_created(event):
    print(f"  [created  ] 任务创建: {event.task_id}")


async def on_started(event):
    print(f"  [started  ] 任务开始: {event.task_id}")


async def on_progress(event):
    progress = float(event.data.get("progress") or 0.0)
    print(f"  [progress ] 任务进度: {event.task_id} {progress:.0%} {event.data.get('message', '')}")


async def on_completed(event):
    print(f"  [completed] 任务完成: {event.task_id}, 结果: {event.data}")


async def on_failed(event):
    print(f"  [failed   ] 任务失败: {event.task_id}, 错误: {event.data}")


async def on_cancelled(event):
    print(f"  [cancelled] 任务取消: {event.task_id}")


def main():
    print("=== 事件回调示例 ===\n")

    with TaskPool(
        executor=event_task,
        config=TaskPoolConfig(worker_concurrency=2, max_retries=0)
    ) as pool:
        # 1. 注册 6 类生命周期事件（处理器必须是 async）
        pool.on_created(on_created)
        pool.on_started(on_started)
        pool.on_progress(on_progress)
        pool.on_completed(on_completed)
        pool.on_failed(on_failed)
        pool.on_cancelled(on_cancelled)
        print("事件监听已注册\n")

        # 2. 成功任务：created → started → progress → completed
        print("提交成功任务...")
        ok_id = pool.submit({"kind": "ok", "id": 1}, task_id="EVT-OK")
        ok_result = pool.wait_for_result(ok_id, timeout=10)
        print(f"  结果: {ok_result}\n")

        # 3. 失败任务：created → started → failed（wait_for_result 会抛异常）
        print("提交失败任务...")
        bad_id = pool.submit({"kind": "fail", "id": 2}, task_id="EVT-FAIL")
        try:
            pool.wait_for_result(bad_id, timeout=10)
        except Exception as e:
            print(f"  捕获到失败: {e}\n")

        # 4. 取消任务：暂停出队后提交并取消，只会有 created → cancelled
        print("提交排队任务并取消（不会产生 started）...")
        pool.pause()
        queued_id = pool.submit({"kind": "ok", "id": 3}, task_id="EVT-CANCEL")
        cancelled = pool.cancel(queued_id)
        pool.resume()
        print(f"  取消结果: {cancelled}, 最终状态: {pool.get_status(queued_id)}\n")

        # 等待事件回调全部落到终端
        time.sleep(0.3)

        # 5. 统计与健康状态
        stats = pool.get_stats()
        print(
            f"统计: 总={stats['total']}, 完成={stats['completed']}, "
            f"失败={stats['failed']}, 取消={stats['cancelled']}"
        )
        health = pool.get_health_status()
        print(f"健康状态: {health['status']}")


if __name__ == "__main__":
    main()
