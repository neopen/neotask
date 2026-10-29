"""
@FileName: 06_delayed_tasks.py
@Description: 延时任务示例 - 延迟执行、指定时间执行与取消
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/9
"""

import time
from datetime import datetime, timedelta

from neotask import TaskScheduler, SchedulerConfig


# 记录真正被执行过的任务标记，用于确认被取消的延时任务没有执行
EXECUTED: list = []


async def notification_task(data: dict) -> dict:
    """通知任务"""
    EXECUTED.append(data["tag"])
    print(f"  [{datetime.now().strftime('%H:%M:%S')}] 发送通知: {data['message']}")
    return {"sent": True, "tag": data["tag"]}


def main():
    scheduler = TaskScheduler(
        executor=notification_task,
        config=SchedulerConfig(storage_type="memory")
    )
    # 先启动，避免把启动耗时算进延时误差
    scheduler.start()

    try:
        print("=== 延时任务示例 ===\n")
        base = time.perf_counter()

        # 1. 延迟执行（1.5 秒后）
        d1 = scheduler.submit_delayed({"tag": "delayed", "message": "1.5 秒后发送"}, 1.5)
        print(f"1. 延时 1.5 秒: {d1}")

        # 2. 指定时间点执行（3 秒后的某个时刻）
        execute_at = datetime.now() + timedelta(seconds=3)
        d2 = scheduler.submit_at(
            {"tag": "at_time", "message": f"定时于 {execute_at.strftime('%H:%M:%S')} 发送"},
            execute_at
        )
        print(f"2. 指定时间点 {execute_at.strftime('%H:%M:%S')}: {d2}")

        # 3. 提交后立即取消（任务在延迟队列中，不会被执行）
        d3 = scheduler.submit_delayed({"tag": "cancelled", "message": "这条通知不该出现"}, 2.0)
        cancelled = scheduler.cancel(d3)
        print(f"3. 延时 2 秒后立即取消: {d3} -> 取消{'成功' if cancelled else '失败'}"
              f"，状态={scheduler.get_status(d3)}")

        # 4. 等待两个有效延时任务完成，记录实际触发时间
        scheduler.wait_for_result(d1, timeout=10)
        elapsed1 = time.perf_counter() - base
        scheduler.wait_for_result(d2, timeout=10)
        elapsed2 = time.perf_counter() - base

        print(f"\n实测: 目标 1.5s -> 实际 {elapsed1:.2f}s；目标 3.0s -> 实际 {elapsed2:.2f}s")
        print("延时精度说明: 延迟队列每 0.1s 扫描一次，Worker 空队列预取有 ≤0.5s 退避，")
        print("             因此实际触发通常比目标时间晚 0.1~0.8s（可缩短 scan_interval 优化）。")

        print(f"\n实际执行过的任务: {EXECUTED}")
        print(f"被取消的任务是否执行: {'是（异常）' if 'cancelled' in EXECUTED else '否（符合预期）'}")

        # 5. 统计
        stats = scheduler.get_stats()
        print(f"\n统计: 总任务={stats['total']}, 成功={stats['completed']}, "
              f"取消={stats['cancelled']}")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
