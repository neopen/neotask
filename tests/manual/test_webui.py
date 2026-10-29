"""
@FileName: test_webui.py
@Description: Web UI 手动测试 - 内嵌 FastAPI 服务与统计接口
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/2 19:05
"""
"""Web UI manual test."""

import asyncio
import json
import time
import urllib.request

import pytest

from neotask import TaskScheduler, TaskExecutor, SchedulerConfig


class DemoExecutor(TaskExecutor):
    """Demo executor for Web UI testing."""

    async def execute(self, task_data: dict) -> dict:
        task_type = task_data.get("type", "normal")
        duration = task_data.get("duration", 1)

        print(f"Running {task_type} task for {duration}s...")
        await asyncio.sleep(duration)

        return {
            "type": task_type,
            "duration": duration,
            "completed_at": time.time()
        }


def _wait_for_stats(url: str, timeout: float = 5.0):
    """轮询统计接口，直到服务就绪"""
    deadline = time.time() + timeout
    last_error = None

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                return response.status, json.loads(response.read().decode())
        except Exception as exc:  # 服务尚未监听或依赖缺失
            last_error = exc
            time.sleep(0.1)

    raise AssertionError(f"Web UI not ready at {url}: {last_error}")


def test_webui_embedded():
    """Test embedded Web UI."""
    pytest.importorskip("fastapi")
    pytest.importorskip("uvicorn")

    from neotask.web.server import WebUIServer

    print("\n" + "=" * 60)
    print("Test: Embedded Web UI")
    print("=" * 60)

    scheduler = TaskScheduler(
        executor=DemoExecutor(),
        config=SchedulerConfig.memory()
    )
    scheduler.start()

    server = WebUIServer(scheduler, host="127.0.0.1", port=8080, auto_open=False)

    try:
        server.start()
        print("Web UI: http://127.0.0.1:8080")

        print("\nSubmitting demo tasks...")
        tasks = [
            {"type": "quick", "duration": 0.5},
            {"type": "normal", "duration": 1},
            {"type": "background", "duration": 0.8},
        ]

        task_ids = []
        for task_data in tasks:
            task_id = scheduler.submit(task_data)
            task_ids.append(task_id)
            print(f"Submitted: {task_data['type']} -> {task_id}")

        # 等待任务完成，确保统计接口有数据可读
        for task_id in task_ids:
            scheduler.wait_for_result(task_id, timeout=10)

        status, stats = _wait_for_stats("http://127.0.0.1:8080/api/stats")
        assert status == 200
        assert isinstance(stats, dict)
        print(f"Stats response: {stats}")

        print("\n✓ Web UI test passed")

    finally:
        server.stop()
        scheduler.shutdown()


if __name__ == "__main__":
    test_webui_embedded()
