"""
@FileName: 05_cron_tasks.py
@Description: Cron 定时任务示例 - 表达式解析、注册、查看下次执行时间与取消
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/9
"""

from neotask import TaskScheduler, SchedulerConfig
from neotask.scheduler.cron_parser import PREDEFINED_CRONS, CronParser


async def cron_task(data: dict) -> dict:
    """Cron 触发的任务"""
    return {"generated": True, "type": data["report_type"]}


def describe_and_preview() -> None:
    """1. 表达式解析：中文描述 + 接下来 3 次执行时间"""
    print("1. Cron 表达式解析与预览:")

    expressions = ["0 9 * * *", "*/5 * * * *", "0 0 * * 0"]
    for expr in expressions:
        print(f"   {expr:<14} {CronParser.describe(expr)}")
        next_runs = CronParser.parse(expr).get_next_n(3)
        print(f"   接下来 3 次: {', '.join(dt.strftime('%m-%d %H:%M') for dt in next_runs)}")

    print("\n   预定义表达式:")
    for name, expr in PREDEFINED_CRONS.items():
        print(f"   {name:<10} -> {expr}")


def register_cron_tasks(scheduler: TaskScheduler) -> None:
    """3. 注册 Cron 任务，并查看调度器记录的信息"""
    print("\n3. 注册 Cron 任务:")

    daily_id = scheduler.submit_cron(
        {"report_type": "daily_report"},
        "0 9 * * *",
        task_id="CRON-DAILY",
        name="每日 9 点报表"
    )
    print(f"   任务ID: {daily_id}")

    hourly_id = scheduler.submit_cron(
        {"report_type": "hourly_summary"},
        "@hourly",
        task_id="CRON-HOURLY",
        name="每小时汇总"
    )
    print(f"   任务ID: {hourly_id}")

    # 查看已注册的 Cron 任务：cron_expr / next_run 均由调度器计算
    for task in scheduler.get_periodic_tasks():
        print(
            f"   [周期任务] {task['name']}({task['task_id']}) "
            f"cron={task['cron_expr']} 下次执行={task['next_run']}"
        )

    # 单个任务详情
    detail = scheduler.get_periodic_task(daily_id)
    if detail:
        print(f"   任务详情: name={detail['name']}, max_runs={detail['max_runs']}")


def validate_expression() -> None:
    """2. 非法表达式会被拒绝"""
    print("\n2. 非法 Cron 表达式校验:")
    try:
        CronParser.parse("0 9 * *")  # 只有 4 个字段
        print("   未按预期抛出异常")
    except Exception as e:
        print(f"   捕获到异常: {e}")


def main():
    scheduler = TaskScheduler(
        executor=cron_task,
        config=SchedulerConfig(storage_type="memory", scan_interval=0.5)
    )
    scheduler.start()

    try:
        print("=== Cron 定时任务示例 ===\n")
        describe_and_preview()
        validate_expression()
        register_cron_tasks(scheduler)

        print(
            "\n注意: Cron 最小粒度是分钟，注册后不会立刻执行；"
            "\n      当前为 memory 存储，重启后周期任务会丢失，生产环境请用 sqlite/redis。"
        )

        print("\n4. 取消已注册的 Cron 任务:")
        for task_id in ("CRON-DAILY", "CRON-HOURLY"):
            print(f"   {task_id} 取消结果: {scheduler.cancel_periodic(task_id)}")

        print(f"\n剩余周期任务数: {len(scheduler.get_periodic_tasks())}")

    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
