"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: 23_human_in_the_loop.py
@Description: 人工介入 — wait_for_signal/pool.signal、WAITING 状态、等待超时
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/25
"""

import asyncio
import time

from neotask import (
    CHECKPOINT_DATA_KEY,
    EventType,
    TaskPool,
    TaskPoolConfig,
    TaskStatus,
    save_checkpoint,
    wait_for_signal,
)


async def approval_job(data: dict) -> dict:
    """先生成待审内容并保存 checkpoint，再等待人工审批"""
    previous = data.get(CHECKPOINT_DATA_KEY) or {}
    if previous.get("draft"):
        draft = previous["draft"]
        print(f"  [执行] 从 checkpoint 恢复草稿: {draft}")
    else:
        await asyncio.sleep(0.02)
        draft = "季度营销方案 v1"
        await save_checkpoint({"draft": draft})
        print(f"  [执行] 生成草稿: {draft}")

    # 挂起后 worker 槽位立即释放；唤醒时 Handler 从头重放，checkpoint 保证不重复生成
    decision = await wait_for_signal("approval", timeout=None)
    return {"draft": draft, "decision": decision}


def demo_signal_wakeup() -> None:
    print("1. 等待外部信号 → 唤醒后重放")
    with TaskPool(executor=approval_job, config=TaskPoolConfig.memory()) as pool:
        for event_type in (EventType.WAITING, EventType.SIGNAL_RECEIVED):
            pool.event_bus.subscribe(
                event_type,
                lambda event: print(f"  [事件] {event.event_type} {event.data}"),
            )

        task_id = pool.submit({"job": "marketing"}, task_id="approval-001")

        deadline = time.time() + 10
        while time.time() < deadline:
            if pool.get_task(task_id)["status"] == TaskStatus.WAITING.value:
                break
            time.sleep(0.01)

        info = pool.get_task(task_id)
        print(f"  状态: {info['status']}")
        print(f"  等待统计: {pool.get_stats()['waiting']}")

        # 模拟人工审批；只有 waiting 状态能唤醒，重复 signal 返回 False
        ok = pool.signal(task_id, {"approved": True, "by": "alice"})
        print(f"  pool.signal -> {ok}")
        result = pool.wait_for_result(task_id, timeout=10)
        print(f"  最终结果: {result}")
        print(f"  重复 signal -> {pool.signal(task_id, 'again')}")


def demo_waiting_timeout() -> None:
    print("\n2. 等待超时 → failed（failure_reason=waiting_timeout）")
    config = TaskPoolConfig.memory()
    config.waiting_timeout = 0.2
    config.waiting_scan_interval = 0  # 关闭后台扫描，手动触发便于演示
    with TaskPool(executor=approval_job, config=config) as pool:
        task_id = pool.submit({"job": "marketing"}, task_id="approval-002")

        deadline = time.time() + 10
        while time.time() < deadline:
            if pool.get_task(task_id)["status"] == "waiting":
                break
            time.sleep(0.01)

        time.sleep(0.3)
        print(f"  cleanup_expired_waiting -> {pool.cleanup_expired_waiting()}")

        info = pool.get_task(task_id)
        print(f"  状态: {info['status']}, failure_reason={info['failure_reason']}")


def main() -> None:
    demo_signal_wakeup()
    demo_waiting_timeout()
    print("\n3. 等待中的任务可跨进程重启后唤醒（SQLite/Redis 持久化 WAITING）")


if __name__ == "__main__":
    main()
