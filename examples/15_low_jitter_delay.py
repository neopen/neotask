"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: 15_low_jitter_delay.py
@Description: 低抖动延时任务示例 - 事件驱动唤醒与时间轮调度的触发精度对比
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/11
"""

import time

from neotask import SchedulerConfig, TaskScheduler


async def delayed_job(data: dict) -> dict:
    """延时任务执行体：原样返回标记"""
    return {"tag": data["tag"]}


def run_round(label: str, config: SchedulerConfig, delays: list) -> None:
    """提交一组延时任务并测量每个任务的实际触发开销"""
    scheduler = TaskScheduler(executor=delayed_job, config=config)
    # 先启动，避免把启动耗时算进延时误差
    scheduler.start()

    try:
        print(f"\n=== {label} ===")
        base = time.perf_counter()
        submitted = [(d, scheduler.submit_delayed({"tag": f"delay-{d}"}, d)) for d in delays]

        for d, task_id in submitted:
            scheduler.wait_for_result(task_id, timeout=10)
            overhead_ms = (time.perf_counter() - base - d) * 1000
            print(f"  目标 {d:.1f}s -> 触发开销 {overhead_ms:+7.1f}ms")

        stats = scheduler.get_stats()
        if "time_wheel" in stats:
            wheel = stats["time_wheel"]
            print(f"  时间轮: 槽位={wheel['slot_count']}, tick={wheel['tick_interval']}s")
    finally:
        scheduler.shutdown()


def main():
    print("=== 低抖动延时任务示例 ===")
    print("任务入队/到期会通过事件立即唤醒等待中的 Worker，")
    print("不再依赖空队列退避睡眠，触发开销通常在几十毫秒内。")

    delays = [0.2, 0.5, 1.0]

    # 1. 默认模式：延迟队列按截止时间提升 + 入队事件唤醒
    run_round(
        "默认模式（延迟队列 + 事件唤醒）",
        SchedulerConfig(storage_type="memory"),
        delays,
    )

    # 2. 时间轮模式：更细的 tick 带来更低的调度抖动，
    #    适合大量延时任务的高性能场景（O(1) 入轮/出轮）
    run_round(
        "时间轮模式（tick=0.05s）",
        SchedulerConfig(
            storage_type="memory",
            enable_time_wheel=True,
            time_wheel_slots=64,
            time_wheel_tick=0.05,
        ),
        delays,
    )

    print("\n说明: 开销为「实际完成时刻 - 目标延时」，含执行器调度耗时，")
    print("      个别任务出现几毫秒负值属于正常的时钟/调度边界误差。")


if __name__ == "__main__":
    main()
