"""
@FileName: test_progress.py
@Description: 任务进度上报集成测试 - 验证 on_progress 订阅侧能被真正触发
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/28
"""

import asyncio
import time

from neotask import TaskPool, TaskPoolConfig, report_progress, report_progress_sync
from neotask.executor import ThreadExecutor


def _wait_until(predicate, timeout: float = 3.0) -> bool:
    """轮询等待条件成立；事件总线是异步队列，需要给一点处理时间"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_async_progress_is_published_and_persisted():
    """异步 executor 调用 report_progress 后应触发事件并写回任务进度"""
    events = []

    async def executor(data):
        await report_progress(0.4, "loading")
        await asyncio.sleep(0.2)
        return {"ok": True}

    pool = TaskPool(
        executor,
        TaskPoolConfig(storage_type="memory", worker_concurrency=1)
    )
    pool.on_progress(events.append)
    pool.start()

    try:
        task_id = pool.submit({})

        # 执行中的进度应能通过 get_task 读到（来自 lifecycle 缓存/存储）
        assert _wait_until(
            lambda: (pool.get_task(task_id) or {}).get("progress") == 0.4
        ), "任务执行期间未观察到 40% 进度"

        assert pool.wait_for_result(task_id, timeout=5) == {"ok": True}

        # 事件经 EventBus 异步队列分发，轮询等待
        assert _wait_until(lambda: len(events) >= 1), "未收到 task.progress 事件"
        assert events[0].data["progress"] == 0.4
        assert events[0].data["message"] == "loading"

        # 成功后进度收敛为 100%
        task_info = pool.get_task(task_id)
        assert task_info["status"] == "success"
        assert task_info["progress"] == 1.0
    finally:
        pool.shutdown()


def test_thread_executor_sync_progress_is_published():
    """线程中执行的同步任务可通过 report_progress_sync 上报进度"""
    events = []

    def executor(data):
        assert report_progress_sync(0.5, "half") is True
        return {"ok": True}

    pool = TaskPool(
        ThreadExecutor(executor, max_workers=1),
        TaskPoolConfig(storage_type="memory", worker_concurrency=1)
    )
    pool.on_progress(events.append)
    pool.start()

    try:
        task_id = pool.submit({})
        assert pool.wait_for_result(task_id, timeout=5) == {"ok": True}

        assert _wait_until(lambda: len(events) >= 1), "未收到线程任务的进度事件"
        assert events[0].data["progress"] == 0.5
        assert events[0].data["message"] == "half"
    finally:
        pool.shutdown()


async def test_report_progress_outside_task_is_noop():
    """不在 Worker 执行上下文中时应安全返回 False"""
    assert await report_progress(0.5, "outside") is False
    assert report_progress_sync(0.5, "outside") is False
