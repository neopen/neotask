"""
@FileName: 11_sqlite_persistence.py
@Description: SQLite 持久化示例 - 进程重启后仍可查询任务状态和结果
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

import tempfile
import time
from pathlib import Path

from neotask import TaskPool, TaskPoolConfig


async def compute_task(data: dict) -> dict:
    """计算两个数之和"""
    return {"sum": data["a"] + data["b"]}


def main():
    # 每次运行使用独立库文件，避免和上次运行的数据混在一起
    db_path = Path(tempfile.gettempdir()) / f"neotask_example_{int(time.time())}.db"
    print("=== SQLite 持久化示例 ===")
    print(f"数据库文件: {db_path}\n")

    storage_config = TaskPoolConfig(
        storage_type="sqlite",
        sqlite_path=str(db_path)
    )

    # ---------- 实例 A：执行任务并落库 ----------
    print("[实例 A] 提交并执行任务")
    with TaskPool(executor=compute_task, config=storage_config) as pool:
        task_id = pool.submit({"a": 3, "b": 4}, task_id="SQLITE-DEMO")
        result = pool.wait_for_result(task_id, timeout=10)
        stats = pool.get_stats()
        print(f"  结果: {result}")
        print(f"  状态: {pool.get_status(task_id)}")
        print(f"  统计: 总数={stats['total']}, 完成={stats['completed']}")

    print("\n实例 A 已关闭（模拟进程退出）\n")

    # ---------- 实例 B：重新打开同一个库，读取历史任务 ----------
    print("[实例 B] 用新的任务池打开同一个数据库")
    with TaskPool(executor=compute_task, config=storage_config) as pool:
        status = pool.get_status("SQLITE-DEMO")
        result_info = pool.get_result("SQLITE-DEMO")
        stats = pool.get_stats()

        print(f"  任务仍存在: {pool.task_exists('SQLITE-DEMO')}")
        print(f"  历史状态: {status}")
        print(f"  历史结果: {result_info['result'] if result_info else None}")

        info = pool.get_task("SQLITE-DEMO")
        print(f"  创建时间: {info['created_at']}")
        print(f"  完成时间: {info['completed_at']}")
        print(f"  重试次数: {info['retry_count']}")
        print(f"  统计: 总数={stats['total']}（来自 SQLite 中的历史数据）")

    print("\n说明: 未执行的任务在重启后也会保留，可继续被新的 Worker 消费；")
    print("      周期任务持久化则需要 SchedulerConfig(storage_type='sqlite')。")


if __name__ == "__main__":
    main()
