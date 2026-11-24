"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: test_engine.py
@Description: TaskEngine 单元测试 - checkpoint 仓储注入路径（显式注入、
             自动创建、关闭）与任务执行
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/15
"""

from neotask import save_checkpoint
from neotask.core.engine import EngineConfig, TaskEngine
from neotask.storage.memory import MemoryCheckpointRepository


def test_engine_available_from_package_root():
    """文档承诺的 from neotask import TaskEngine, EngineConfig 必须可用"""
    from neotask import EngineConfig as RootEngineConfig
    from neotask import TaskEngine as RootTaskEngine

    assert RootTaskEngine is TaskEngine
    assert RootEngineConfig is EngineConfig


class _ClosingRepo(MemoryCheckpointRepository):
    def __init__(self):
        super().__init__()
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class TestTaskEngineCheckpointRepo:
    async def test_injected_repo_is_used_by_engine(self):
        repo = MemoryCheckpointRepository()
        seen = {}

        async def work(data):
            seen["saved"] = await save_checkpoint({"step": 1})
            return {"ok": True}

        engine = TaskEngine(EngineConfig(executor_func=work), checkpoint_repo=repo)
        await engine.start()
        try:
            assert engine._checkpoint_repo is repo
            assert engine._lifecycle_manager.checkpoint_enabled

            task_id = await engine.submit({"job": "inject"}, task_id="TSK_ENGINE_INJECT")
            assert await engine.wait(task_id, timeout=10) == {"ok": True}

            assert seen["saved"] is True
            record = await repo.load_latest(task_id)
            assert record is not None
            assert record.version == 1
            assert record.state == {"step": 1}
        finally:
            await engine.stop()

    async def test_enabled_config_auto_creates_repo(self):
        engine = TaskEngine(EngineConfig())
        await engine.start()
        try:
            assert isinstance(engine._checkpoint_repo, MemoryCheckpointRepository)
            assert engine._lifecycle_manager.checkpoint_enabled
        finally:
            await engine.stop()

    async def test_disabled_config_leaves_repo_empty(self):
        async def work(data):
            assert await save_checkpoint({"step": 1}) is False
            return {"ok": True}

        engine = TaskEngine(EngineConfig(executor_func=work, enable_checkpoint=False))
        await engine.start()
        try:
            assert engine._checkpoint_repo is None
            assert not engine._lifecycle_manager.checkpoint_enabled

            task_id = await engine.submit({"job": "disabled"})
            assert await engine.wait(task_id, timeout=10) == {"ok": True}
        finally:
            await engine.stop()

    async def test_injected_repo_closed_on_stop(self):
        repo = _ClosingRepo()
        engine = TaskEngine(EngineConfig(), checkpoint_repo=repo)
        await engine.start()
        await engine.stop()

        assert repo.closed is True
