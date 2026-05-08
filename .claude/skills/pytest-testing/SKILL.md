---
name: pytest-testing
description: NeoTask 项目测试运行与编写指南 — pytest 异步测试、fixtures、标记、覆盖率
---

# NeoTask 测试技能

## 概述

本技能指导在 NeoTask 项目中运行和编写测试。项目使用 pytest，配置 `asyncio_mode = auto`，无需手动管理事件循环。

## 运行测试

```bash
# 所有测试
pytest tests/ -v

# 仅单元测试
pytest tests/unit/ -v

# 仅集成测试
pytest tests/integration/ -v

# 单个文件
pytest tests/unit/test_task.py -v

# 按标记过滤
pytest tests/ -m unit -v
pytest tests/ -m integration -v
pytest tests/ -m "not slow" -v

# 覆盖率
pytest --cov=src/neotask tests/
pytest --cov=src/neotask --cov-report=html tests/

# 并行（需 pytest-xdist）
pytest tests/ -n auto
```

## 测试结构

```
tests/
├── conftest.py          # 全局 fixtures 导入 + pytest 配置
├── pytest.ini           # asyncio_mode=auto, 标记定义
├── fixtures/            # 共享 fixtures
│   ├── task.py          # sample_task, sample_tasks
│   ├── storage.py       # memory_task_repo, sqlite_task_repo, storage_type
│   ├── queue.py         # priority_queue, delayed_queue, queue_scheduler
│   ├── executor.py      # mock_executor
│   ├── event.py         # event_bus, test_event
│   ├── event_loop.py    # event_loop, event_loop_policy
│   ├── redis.py         # skip_if_no_redis, redis_url
│   ├── monitor.py       # metrics_collector
│   ├── distributed.py   # node_manager, coordinator
│   └── helpers.py       # close_loop_after_test
├── unit/                # 单元测试
├── integration/         # 集成测试
├── benchmark/           # 性能基准
├── chaos/               # 异常/混乱测试
└── distributed/         # 分布式测试
```

## 可用标记

| 标记 | 用途 |
|---|---|
| `@pytest.mark.asyncio` | 异步测试（asyncio_mode=auto 时可选） |
| `@pytest.mark.unit` | 单元测试 |
| `@pytest.mark.integration` | 集成测试 |
| `@pytest.mark.slow` | 慢速测试 |
| `@pytest.mark.benchmark` | 性能基准 |
| `@pytest.mark.chaos` | 异常场景 |
| `@pytest.mark.distributed` | 分布式功能 |
| `@pytest.mark.lock` | 分布式锁 |
| `@pytest.mark.heartbeat` | 节点心跳 |
| `@pytest.mark.sharding` | 任务分片 |

## 关键 Fixtures

### 存储

| Fixture | 说明 |
|---|---|
| `memory_task_repo` | 内存 TaskRepository |
| `memory_queue_repo` | 内存 QueueRepository |
| `sqlite_task_repo` | SQLite TaskRepository（临时文件，自动清理） |
| `sqlite_queue_repo` | SQLite QueueRepository（临时文件，自动清理） |
| `storage_type` | 参数化：`"memory"` 和 `"sqlite"` 各运行一次 |

### 队列

| Fixture | 说明 |
|---|---|
| `priority_queue` | 空 PriorityQueue |
| `delayed_queue` | 空 DelayedQueue |
| `queue_scheduler` | QueueScheduler（内存后端） |

### 任务

| Fixture | 说明 |
|---|---|
| `sample_task` | 单个 Task 实例 |
| `sample_tasks` | 10 个 Task 实例列表 |
| `sample_task_data` | 20 条 (task_id, priority, delay) 元组 |

### 执行器

| Fixture | 说明 |
|---|---|
| `mock_executor` | 返回 `{"result": "success", "data": data}` 的异步函数 |

### 其他

| Fixture | 说明 |
|---|---|
| `event_loop` | 每个测试函数独立的 event loop，自动清理 |
| `skip_if_no_redis` | Redis 不可用时跳过测试 |

## 编写新测试

### 同步单元测试

```python
"""Unit tests for X."""
import pytest
from neotask.models.task import Task, TaskStatus


class TestTaskModel:
    """Test Task model."""

    def test_create_task(self):
        task = Task(task_id="test-001", data={"key": "value"})
        assert task.task_id == "test-001"
        assert task.status == TaskStatus.PENDING

    def test_edge_case(self):
        task = Task(task_id="", data=None)
        assert task.task_id == ""
        assert task.data is None
```

### 异步单元测试

```python
"""Async unit tests."""
import pytest
from neotask.queue.priority_queue import PriorityQueue


class TestPriorityQueue:
    @pytest.mark.asyncio
    async def test_push_pop(self):
        queue = PriorityQueue()
        await queue.push("task-1", priority=0)
        item = await queue.pop()
        assert item.task_id == "task-1"

    @pytest.mark.asyncio
    async def test_empty_queue(self):
        queue = PriorityQueue()
        item = await queue.pop()
        assert item is None
```

### 使用已有 Fixture 的测试

```python
"""Test with storage fixture."""
import pytest


class TestWithFixtures:
    def test_memory_storage(self, memory_task_repo):
        """使用 memory_task_repo fixture"""
        assert memory_task_repo is not None

    @pytest.mark.asyncio
    async def test_sqlite_storage(self, sqlite_task_repo):
        """使用 sqlite_task_repo fixture"""
        task = Task(task_id="t1", data={})
        await sqlite_task_repo.save(task)
        result = await sqlite_task_repo.get("t1")
        assert result.task_id == "t1"
```

### 参数化测试

```python
import pytest


class TestQueue:
    @pytest.mark.parametrize("priority", [0, 1, 2, 3])
    def test_priority_values(self, priority):
        from neotask.models.task import TaskPriority
        assert TaskPriority(priority) is not None

    def test_storage_types(self, storage_type):
        """自动在 memory 和 sqlite 上各运行一次"""
        from neotask.storage.factory import StorageFactory
        config = type("Config", (), {"storage_type": storage_type})()
        repo = StorageFactory.create_task_repository(config)
        assert repo is not None
```

## 文件头规范

新测试文件使用以下头部：

```python
"""
@FileName: test_xxx.py
@Description: 简要描述
@Author: HiPeng
@Time: YYYY/M/D
"""
```

## 禁止事项

- 不要在测试中硬编码绝对路径
- 不要直接操作生产数据库
- 不要在测试间共享可变状态
- 避免 `time.sleep()`，使用 `asyncio.sleep()` 或 `await` fixture

## 向用户询问

当用户请求编写测试时，依次澄清：

1. **测试范围**：单元 / 集成 / 基准？
2. **目标模块**：测试哪个模块或类？
3. **存储后端**：memory / sqlite / redis？
4. **关键场景**：正常路径 / 边界条件 / 错误路径？
