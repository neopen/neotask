"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: 17_business_failure.py
@Description: 业务失败上报 — report_failure 登记原因、跳过自动重试、事件携带 reason
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026-11-12
"""

import asyncio
import time

from neotask import EventType, TaskPool, TaskPoolConfig, report_failure


async def review_content(data: dict) -> dict:
    """审核任务：审核不通过属于业务失败，不是程序异常

    业务失败用 report_failure 登记原因后正常返回，框架按 FAILED 收尾且不重试；
    只有真正的异常（超时、依赖不可用）才交给 max_retries 处理。
    """
    await asyncio.sleep(0.1)
    score = data["score"]
    if score < 60:
        report_failure("qa_failed", f"score={score}")
    return {"score": score, "approved": score >= 60}


async def build_package(data: dict) -> dict:
    """模拟一个较慢的任务，便于从外部按业务判定失败"""
    await asyncio.sleep(data.get("duration", 2))
    return {"artifact": data["name"]}


def wait_terminal(pool: TaskPool, task_id: str, timeout: float = 10) -> str:
    """轮询等待任务进入终态

    显式上报把正在执行的任务判为失败后，执行器协程还没结束，
    因此这里按状态等待，而不是等结果 Future。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = pool.get_status(task_id)
        if status in ("failed", "success", "cancelled"):
            return status
        time.sleep(0.02)
    raise TimeoutError(f"task {task_id} not finished in {timeout}s")


def main():
    config = TaskPoolConfig(max_retries=3, retry_delay=0.05)

    with TaskPool(executor=review_content, config=config) as pool:
        failed_events = []
        pool.event_bus.subscribe(
            EventType.FAILED,
            lambda event: failed_events.append(event)
        )

        # 1. 业务失败：登记原因，不抛异常
        print("1. 执行器内上报业务失败")
        passed_id = pool.submit({"score": 92})
        rejected_id = pool.submit({"score": 10})
        pool.wait_all([passed_id, rejected_id], timeout=10)
        print(f"  {passed_id}: {pool.get_status(passed_id)}")
        print(f"  {rejected_id}: {pool.get_status(rejected_id)}")

        # 2. 原因可在结果与任务记录中查到
        print("\n2. 失败原因可查")
        result = pool.get_result(rejected_id)
        print(f"  status: {result['status']}")
        print(f"  failure_reason: {result['failure_reason']!r}")
        print(f"  error: {result['error']!r}")

        # 3. 业务失败不消耗重试预算：执行器只被调用一次
        print("\n3. 业务失败不触发自动重试")
        task = pool.get_task(rejected_id)
        print(f"  max_retries={config.max_retries}, 实际 retry_count={task['retry_count']}")

        # 4. 事件同样携带 reason，监控侧无需解析 error 文本
        print("\n4. task.failed 事件携带原因")
        time.sleep(0.2)  # 事件投递是异步的，留出派发时间
        for event in failed_events:
            print(f"  {event.task_id}: reason={event.data['reason']!r}")

        print(f"\n统计: 总数={pool.get_stats()['total']}, "
              f"成功={pool.get_stats()['completed']}, 失败={pool.get_stats()['failed']}")

    # 5. 显式形态：执行器之外（如审批接口）按 task_id 判定正在运行的任务失败
    print("\n5. 显式上报正在运行任务的失败原因")
    with TaskPool(executor=build_package) as pool:
        long_id = pool.submit({"name": "app.zip", "duration": 2})

        # 等到任务真正进入 RUNNING 再上报
        for _ in range(100):
            if pool.get_status(long_id) == "running":
                break
            time.sleep(0.02)

        accepted = pool.report_failure(long_id, "cancelled_by_reviewer", "package rejected")
        print(f"  report_failure 返回: {accepted}")

        print(f"  终态: {wait_terminal(pool, long_id)}")
        task = pool.get_task(long_id)
        print(f"  status={task['status']}, failure_reason={task['failure_reason']!r}")
        print(f"  error={task['error']!r}")

        # 已终结的任务不能再被定为失败
        print(f"  重复上报已终结任务: {pool.report_failure(long_id, 'other')}")

        print("  注：上报只改任务状态，执行器协程仍在跑；它结束后回写结果会被"
              "终态守卫忽略，随后日志中的 WARNING 即为此处")


if __name__ == "__main__":
    main()
