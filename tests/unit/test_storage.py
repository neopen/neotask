"""
@FileName: test_storage.py
@Description: 存储模块完整单元测试
@Author: HiPeng
@Time: 2026/4/15
"""

import os
import tempfile

import pytest

from neotask.models.config import StorageConfig
from neotask.models.task import Task, TaskStatus, TaskPriority
from neotask.storage.factory import StorageFactory
from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository
from neotask.storage.redis import RedisTaskRepository, RedisQueueRepository
from neotask.storage.sqlite import SQLiteTaskRepository, SQLiteQueueRepository


# ========== 内存存储测试 ==========

class TestMemoryStorage:
    """测试内存存储"""

    @pytest.mark.asyncio
    async def test_task_repository_crud(self):
        """测试任务存储 CRUD"""
        repo = MemoryTaskRepository()

        task = Task(
            task_id="test-1",
            data={"key": "value"},
            priority=TaskPriority.HIGH
        )

        # Create
        await repo.save(task)

        # Read
        retrieved = await repo.get("test-1")
        assert retrieved is not None
        assert retrieved.task_id == "test-1"
        assert retrieved.data["key"] == "value"

        # Update
        task.status = TaskStatus.RUNNING
        await repo.save(task)
        updated = await repo.get("test-1")
        assert updated.status == TaskStatus.RUNNING

        # Delete
        await repo.delete("test-1")
        assert await repo.get("test-1") is None

    @pytest.mark.asyncio
    async def test_task_exists(self):
        """测试任务存在性检查"""
        repo = MemoryTaskRepository()

        task = Task(task_id="test-exists", data={})
        await repo.save(task)

        assert await repo.exists("test-exists") is True
        assert await repo.exists("non-existent") is False

    @pytest.mark.asyncio
    async def test_list_by_status(self):
        """测试按状态列出任务"""
        repo = MemoryTaskRepository()

        tasks = [
            Task(task_id="pending-1", data={}, status=TaskStatus.PENDING),
            Task(task_id="pending-2", data={}, status=TaskStatus.PENDING),
            Task(task_id="running-1", data={}, status=TaskStatus.RUNNING),
            Task(task_id="success-1", data={}, status=TaskStatus.SUCCESS),
        ]

        for task in tasks:
            await repo.save(task)

        pending = await repo.list_by_status(TaskStatus.PENDING)
        assert len(pending) == 2

        running = await repo.list_by_status(TaskStatus.RUNNING)
        assert len(running) == 1

    @pytest.mark.asyncio
    async def test_update_status(self):
        """测试更新状态"""
        repo = MemoryTaskRepository()

        task = Task(task_id="test-status", data={}, status=TaskStatus.PENDING)
        await repo.save(task)

        await repo.update_status("test-status", TaskStatus.RUNNING)

        updated = await repo.get("test-status")
        assert updated.status == TaskStatus.RUNNING

    @pytest.mark.asyncio
    async def test_queue_repository(self):
        """测试队列存储"""
        repo = MemoryQueueRepository()

        await repo.push("task-1", 5)
        await repo.push("task-2", 1)
        await repo.push("task-3", 3)

        assert await repo.size() == 3

        popped = await repo.pop(2)
        assert popped == ["task-2", "task-3"]

        assert await repo.size() == 1

        await repo.clear()
        assert await repo.size() == 0


# ========== SQLite 存储测试 ==========

class TestSQLiteStorage:
    """测试 SQLite 存储"""

    @pytest.fixture
    async def sqlite_task_repo(self):
        """创建 SQLite 任务存储"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        repo = SQLiteTaskRepository(db_path)
        await repo._ensure_init()
        yield repo
        os.unlink(db_path)

    @pytest.fixture
    async def sqlite_queue_repo(self):
        """创建 SQLite 队列存储"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        repo = SQLiteQueueRepository(db_path)
        yield repo
        os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_task_crud(self, sqlite_task_repo):
        """测试任务 CRUD"""
        repo = sqlite_task_repo

        task = Task(
            task_id="sqlite-test-1",
            data={"test": "data"},
            priority=TaskPriority.NORMAL
        )

        await repo.save(task)
        retrieved = await repo.get("sqlite-test-1")
        assert retrieved is not None
        assert retrieved.task_id == "sqlite-test-1"

        await repo.delete("sqlite-test-1")
        assert await repo.get("sqlite-test-1") is None

    @pytest.mark.asyncio
    async def test_task_exists(self, sqlite_task_repo):
        """测试任务存在性"""
        repo = sqlite_task_repo

        task = Task(task_id="exists-test", data={})
        await repo.save(task)

        assert await repo.exists("exists-test") is True
        assert await repo.exists("non-existent") is False

    @pytest.mark.asyncio
    async def test_list_by_status(self, sqlite_task_repo):
        """测试按状态列出"""
        repo = sqlite_task_repo

        tasks = [
            Task(task_id="s-pending-1", data={}, status=TaskStatus.PENDING),
            Task(task_id="s-pending-2", data={}, status=TaskStatus.PENDING),
            Task(task_id="s-running-1", data={}, status=TaskStatus.RUNNING),
        ]

        for task in tasks:
            await repo.save(task)

        pending = await repo.list_by_status(TaskStatus.PENDING)
        assert len(pending) == 2

    @pytest.mark.asyncio
    async def test_queue_crud(self, sqlite_queue_repo):
        """测试队列 CRUD"""
        repo = sqlite_queue_repo

        await repo.push("q-task-1", 5)
        await repo.push("q-task-2", 1)

        assert await repo.size() == 2

        popped = await repo.pop(2)
        assert set(popped) == {"q-task-1", "q-task-2"}

        assert await repo.size() == 0

    @pytest.mark.asyncio
    async def test_queue_remove(self, sqlite_queue_repo):
        """测试队列移除"""
        repo = sqlite_queue_repo

        await repo.push("remove-test-1", 5)
        await repo.push("remove-test-2", 1)

        removed = await repo.remove("remove-test-1")
        assert removed is True

        assert await repo.size() == 1


# ========== Redis 存储测试（需要 Redis 服务） ==========

@pytest.mark.skipif(
    not os.environ.get("REDIS_TEST_URL"),
    reason="REDIS_TEST_URL environment variable not set"
)
class TestRedisStorage:
    """测试 Redis 存储（需要 Redis 服务）"""

    @pytest.fixture
    async def redis_task_repo(self):
        """创建 Redis 任务存储"""
        redis_url = os.environ["REDIS_TEST_URL"]
        repo = RedisTaskRepository(redis_url)
        yield repo
        await repo.close()

    @pytest.fixture
    async def redis_queue_repo(self):
        """创建 Redis 队列存储"""
        redis_url = os.environ["REDIS_TEST_URL"]
        repo = RedisQueueRepository(redis_url)
        yield repo
        await repo.close()

    @pytest.mark.asyncio
    async def test_task_crud(self, redis_task_repo):
        """测试任务 CRUD"""
        repo = redis_task_repo

        task = Task(
            task_id="redis-test-1",
            data={"test": "redis"},
            priority=TaskPriority.CRITICAL
        )

        await repo.save(task)
        retrieved = await repo.get("redis-test-1")
        assert retrieved is not None
        assert retrieved.task_id == "redis-test-1"

        await repo.delete("redis-test-1")
        assert await repo.get("redis-test-1") is None

    @pytest.mark.asyncio
    async def test_task_exists(self, redis_task_repo):
        """测试任务存在性"""
        repo = redis_task_repo

        task = Task(task_id="redis-exists", data={})
        await repo.save(task)

        assert await repo.exists("redis-exists") is True

    @pytest.mark.asyncio
    async def test_queue_crud(self, redis_queue_repo):
        """测试队列 CRUD"""
        repo = redis_queue_repo

        await repo.push("redis-q-1", 5)
        await repo.push("redis-q-2", 1)

        assert await repo.size() == 2

        popped = await repo.pop(2)
        assert set(popped) == {"redis-q-1", "redis-q-2"}

        assert await repo.size() == 0


# ========== 存储工厂测试 ==========

class TestStorageFactory:
    """测试存储工厂"""

    def test_create_memory(self):
        """测试创建内存存储"""
        config = StorageConfig.memory()
        task_repo, queue_repo = StorageFactory.create(config)

        assert task_repo is not None
        assert queue_repo is not None
        assert isinstance(task_repo, MemoryTaskRepository)
        assert isinstance(queue_repo, MemoryQueueRepository)

    def test_create_sqlite(self):
        """测试创建 SQLite 存储"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        config = StorageConfig.sqlite(db_path)
        task_repo, queue_repo = StorageFactory.create(config)

        assert task_repo is not None
        assert queue_repo is not None

        os.unlink(db_path)

    def test_create_redis_without_url(self):
        """测试创建 Redis 存储但未提供 URL"""
        config = StorageConfig.redis(None)  # type: ignore
        with pytest.raises(ValueError):
            StorageFactory.create(config)

    @pytest.mark.skipif(
        not os.environ.get("REDIS_TEST_URL"),
        reason="REDIS_TEST_URL not set"
    )
    def test_create_redis(self):
        """测试创建 Redis 存储"""
        config = StorageConfig.redis(os.environ["REDIS_TEST_URL"])
        task_repo, queue_repo = StorageFactory.create(config)

        assert task_repo is not None
        assert queue_repo is not None
        assert isinstance(task_repo, RedisTaskRepository)
        assert isinstance(queue_repo, RedisQueueRepository)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
