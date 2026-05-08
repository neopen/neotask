"""
@FileName: distributed.py
@Description: 分布式相关 fixtures
@Author: HiPeng
@Time: 2026/4/29
"""

import os
import socket
import uuid
import pytest


@pytest.fixture
async def node_manager(distributed_redis_url):
    """节点管理器 fixture"""
    from neotask.distributed.node import NodeManager

    node_id = f"test_node_{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    manager = NodeManager(distributed_redis_url, node_id)

    try:
        await manager.start()
        yield manager
    finally:
        await manager.stop()


@pytest.fixture
async def multi_nodes(distributed_redis_url):
    """多节点 fixture"""
    from neotask.distributed.node import NodeManager

    nodes = []
    for i in range(3):
        node_id = f"multi_node_{i}_{uuid.uuid4().hex[:8]}"
        manager = NodeManager(distributed_redis_url, node_id)
        await manager.start()
        nodes.append(manager)

    yield nodes

    for node in nodes:
        await node.stop()


@pytest.fixture
def consistent_hash_sharder():
    """一致性哈希分片器 fixture"""
    from neotask.distributed.sharding import ConsistentHashSharder

    nodes = ["node1", "node2", "node3", "node4"]
    return ConsistentHashSharder(nodes, virtual_nodes=100)


@pytest.fixture
def modulo_sharder():
    """取模分片器 fixture"""
    from neotask.distributed.sharding import ModuloSharder

    return ModuloSharder(4)


@pytest.fixture
def range_sharder():
    """范围分片器 fixture"""
    from neotask.distributed.sharding import RangeSharder

    ranges = [("a", "m"), ("n", "z")]
    return RangeSharder(ranges)


@pytest.fixture
async def redis_lock(distributed_redis_url):
    """Redis 分布式锁 fixture"""
    from neotask.lock.redis import RedisLock

    lock = RedisLock(distributed_redis_url)
    yield lock
    await lock.close()


@pytest.fixture
def redis_config(distributed_redis_url):
    """Redis TaskPool 配置 fixture"""
    from neotask.api.task_pool import TaskPoolConfig
    return TaskPoolConfig(
        storage_type="redis",
        redis_url=distributed_redis_url,
        worker_concurrency=2,
        task_timeout=30
    )