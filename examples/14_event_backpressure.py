"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: 14_event_backpressure.py
@Description: EventBus 背压示例 - 有界队列丢弃策略、慢回调监控与隔离
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/11
"""

import asyncio
import time

from neotask import EventBus, TaskEvent


async def main():
    print("=== EventBus 背压与慢回调监控示例 ===\n")

    # 慢回调模拟耗时订阅（如推送远程告警系统），快回调只记录时间戳
    fast_done = asyncio.Event()
    slow_done = asyncio.Event()
    fast_at = []

    async def slow_handler(event: TaskEvent) -> None:
        await asyncio.sleep(0.3)
        slow_done.set()

    async def fast_handler(event: TaskEvent) -> None:
        fast_at.append(time.perf_counter())
        fast_done.set()

    bus = EventBus(
        max_queue_size=5,             # 待处理事件队列上限
        overflow_policy="drop",       # 队列满时丢弃新事件并计数告警
        slow_handler_threshold=0.05,  # 回调耗时超过 0.05s 即告警并计数
    )
    bus.subscribe("task.completed", slow_handler)
    bus.subscribe("task.completed", fast_handler)

    async with bus:
        # 1. 慢回调隔离：同一事件的多个订阅者并发执行
        start = time.perf_counter()
        await bus.emit(TaskEvent(event_type="task.completed", task_id="TSK-DEMO-1"))

        await asyncio.wait_for(fast_done.wait(), timeout=1.0)
        fast_latency = fast_at[0] - start
        await asyncio.wait_for(slow_done.wait(), timeout=2.0)
        slow_latency = time.perf_counter() - start

        print("1. 慢回调隔离（同一事件并发分发）")
        print(f"   快回调完成: +{fast_latency:.3f}s")
        print(f"   慢回调完成: +{slow_latency:.3f}s（模拟耗时 0.3s）")
        print("   -> 慢回调不会阻塞同一事件的其它订阅者")

        # 2. 有界队列背压：突发发布 20 个事件，队列上限只有 5
        #    消费速度跟不上时，超出上限的事件被丢弃而不是无限堆积
        for i in range(20):
            await bus.emit(TaskEvent(event_type="task.completed", task_id=f"TSK-BURST-{i}"))

        stats = bus.get_stats()
        print(f"\n2. 突发发布 20 个事件（队列上限 {stats['max_queue_size']}）")
        print(f"   队列排队: {stats['queue_size']}，已丢弃: {stats['dropped_events']}")
        print("   （日志中的黄色 warning 即背压/丢弃告警，按次数限频输出）")

        # 等待所有事件处理完毕：每个事件不是被处理就是被丢弃
        total_emitted = 21
        for _ in range(200):
            stats = bus.get_stats()
            if stats["processed_events"] + stats["dropped_events"] >= total_emitted:
                break
            await asyncio.sleep(0.05)

        # 3. 监控指标
        stats = bus.get_stats()
        print("\n3. 监控指标 get_stats()")
        print(f"   processed_events   = {stats['processed_events']}（已处理）")
        print(f"   dropped_events     = {stats['dropped_events']}（背压丢弃）")
        print(f"   slow_handler_calls = {stats['slow_handler_calls']}"
              f"（超过 {stats['slow_handler_threshold']}s 阈值的回调次数）")
        print(f"   queue_size         = {stats['queue_size']}（当前排队）")

    print("\n生产建议: 为 EventBus 设置 max_queue_size 保护发布方，")
    print("          监控 dropped_events 与 slow_handler_calls 定位慢订阅者。")


if __name__ == "__main__":
    asyncio.run(main())
