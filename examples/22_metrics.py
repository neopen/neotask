"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: 22_metrics.py
@Description: 轻量监控 — get_metrics 快照与 on_metrics 周期推送
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/20
"""

import threading
import time

from neotask import TaskPool, TaskPoolConfig


def main():
    attempts = {"flaky": 0}

    def work(data):
        if data.get("flaky"):
            attempts["flaky"] += 1
            if attempts["flaky"] == 1:
                raise RuntimeError("transient failure")
        time.sleep(0.005)
        return {"value": data["value"] * 2}

    config = TaskPoolConfig(
        worker_concurrency=2,
        max_retries=1,
        retry_delay=0.3,
    )

    with TaskPool(executor=work, config=config) as pool:
        print("1. 空闲快照")
        snapshot = pool.get_metrics()
        print(
            f"   backend={snapshot['storage_backend']} "
            f"workers={snapshot['worker_concurrency']} "
            f"completed={snapshot['completed']}"
        )

        print("\n2. 暂停出队后查看队列深度与最早等待时长")
        pool.pause()
        task_ids = [
            pool.submit({"value": i}, priority=i % 4) for i in range(4)
        ]
        time.sleep(0.05)
        snapshot = pool.get_metrics()
        print(f"   queue_depth_by_priority={snapshot['queue_depth_by_priority']}")
        print(
            "   oldest_pending_age_seconds="
            f"{snapshot['oldest_pending_age_seconds']:.3f}"
        )
        pool.resume()
        for task_id in task_ids:
            pool.wait_for_result(task_id, timeout=10)

        print("\n3. 周期推送：每 0.2 秒收到一份快照（首次立即推送）")
        received = []
        stop = threading.Event()

        def on_snapshot(data):
            received.append(data)
            print(
                f"   pending={data['pending']} running={data['running']} "
                f"completed={data['completed']} retry={data['retry_queue_size']}"
            )
            if len(received) >= 3:
                stop.set()

        subscription = pool.on_metrics(on_snapshot, interval=0.2)
        stop.wait(5)
        subscription.cancel()
        time.sleep(0.1)
        print(f"   共收到 {len(received)} 份快照，订阅已取消: {not subscription.active}")

        print("\n4. 失败自动重试时 retry_queue_size 会在延迟窗口内计数")
        flaky_id = pool.submit({"value": 7, "flaky": True})
        deadline = time.time() + 3
        retry_seen = 0
        while time.time() < deadline:
            retry_seen = pool.get_metrics()["retry_queue_size"]
            if retry_seen:
                break
            time.sleep(0.01)
        print(f"   等待重试: retry_queue_size={retry_seen}")

        print(f"   重试后结果: {pool.wait_for_result(flaky_id, timeout=10)}")
        snapshot = pool.get_metrics()
        print(
            f"   完成后: retry_queue_size={snapshot['retry_queue_size']} "
            f"completed={snapshot['completed']} failed={snapshot['failed']}"
        )

        print("\n5. 快照字段可直接作为监控上报的 JSON 载荷")
        print(f"   {snapshot}")


if __name__ == "__main__":
    main()
