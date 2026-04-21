"""
@FileName: debug_time_wheel.py
@Description: 
@Author: HiPeng
@Time: 2026/4/21 21:47
"""
"""
调试时间轮
"""

import asyncio
from neotask.scheduler.time_wheel import TimeWheel


async def debug_time_wheel():
    """调试时间轮"""
    executed = []

    async def callback(task_id, priority, data=None):
        print(f"[CALLBACK] task_id={task_id}, priority={priority}")
        executed.append(task_id)

    print("1. 创建时间轮...")
    wheel = TimeWheel(slot_count=10, tick_interval=0.05)

    print("2. 启动时间轮...")
    await wheel.start(callback)
    print(f"   _running={wheel._running}")
    print(f"   _ticker_task={wheel._ticker_task}")

    print("3. 添加任务...")
    result = await wheel.add_task("task_1", 1, 0.1)
    print(f"   add_task result: {result}")

    print("4. 等待任务执行...")
    for i in range(10):
        await asyncio.sleep(0.05)
        print(f"   tick {i + 1}: size={await wheel.size()}, executed={executed}")

    print("5. 停止时间轮...")
    await wheel.stop()

    print(f"6. 最终结果: executed={executed}")

    assert "task_1" in executed, f"Task was not executed! executed={executed}"


if __name__ == "__main__":
    asyncio.run(debug_time_wheel())