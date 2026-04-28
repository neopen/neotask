"""
@FileName: test_node_manager.py
@Description: 节点管理器测试
@Author: HiPeng
@Time: 2026/4/28
"""

import asyncio
import time

import pytest

from neotask.distributed.node import NodeManager, NodeStatus
from tests.conftest import TEST_REDIS_URL


class TestNodeManager:
    """节点管理器测试"""

    @pytest.mark.asyncio
    async def test_register_node(self, redis_client, test_node_id):
        """测试节点注册"""
        manager = NodeManager(TEST_REDIS_URL, node_id=test_node_id)

        try:
            # 注册节点
            result = await manager.register()
            assert result is True

            # 等待一小段时间确保数据写入
            await asyncio.sleep(0.1)

            # 验证节点已注册
            key = f"{manager.NODE_KEY_PREFIX}{test_node_id}"
            data = await redis_client.hgetall(key)

            assert data is not None
            assert data.get("node_id") == test_node_id
            assert data.get("status") == "active"

        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_heartbeat(self, redis_client, test_node_id):
        """测试心跳上报"""
        manager = NodeManager(TEST_REDIS_URL, node_id=test_node_id)

        try:
            await manager.register()
            await asyncio.sleep(0.1)

            # 发送心跳
            await manager.heartbeat()
            await asyncio.sleep(0.1)

            # 验证心跳时间
            key = f"{manager.NODE_KEY_PREFIX}{test_node_id}"
            last_heartbeat = await redis_client.hget(key, "last_heartbeat")
            assert last_heartbeat is not None
            assert float(last_heartbeat) <= time.time()

        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_heartbeat_loop(self, test_node_id):
        """测试心跳循环"""
        manager = NodeManager(TEST_REDIS_URL, node_id=test_node_id)
        manager.HEARTBEAT_INTERVAL = 0.5

        try:
            await manager.start()

            # 等待几次心跳
            await asyncio.sleep(1.5)

            # 验证心跳持续更新
            key = f"{manager.NODE_KEY_PREFIX}{test_node_id}"
            last_heartbeat_1 = await manager._client.hget(key, "last_heartbeat")

            await asyncio.sleep(0.6)

            last_heartbeat_2 = await manager._client.hget(key, "last_heartbeat")
            assert float(last_heartbeat_2) > float(last_heartbeat_1)

        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_get_active_nodes(self, test_node_id):
        """测试获取活跃节点列表"""
        manager1 = NodeManager(TEST_REDIS_URL, node_id=f"{test_node_id}_1")
        manager2 = NodeManager(TEST_REDIS_URL, node_id=f"{test_node_id}_2")

        try:
            await manager1.start()
            await manager2.start()

            # 等待注册完成
            await asyncio.sleep(0.5)

            # 获取活跃节点
            active_nodes = await manager1.get_active_nodes()
            assert len(active_nodes) >= 2

            node_ids = [n.node_id for n in active_nodes]
            assert f"{test_node_id}_1" in node_ids
            assert f"{test_node_id}_2" in node_ids

        finally:
            await manager1.stop()
            await manager2.stop()

    @pytest.mark.asyncio
    async def test_is_node_alive(self, test_node_id):
        """测试节点存活检查"""
        manager = NodeManager(TEST_REDIS_URL, node_id=test_node_id)

        try:
            await manager.start()
            await asyncio.sleep(0.5)

            # 检查自己的节点
            assert await manager.is_node_alive(test_node_id) is True

            # 检查不存在的节点
            assert await manager.is_node_alive("non_existent_node") is False

        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_node_timeout(self, test_node_id):
        """测试节点超时检测"""
        manager = NodeManager(TEST_REDIS_URL, node_id=test_node_id)
        manager.HEARTBEAT_TIMEOUT = 1.0

        try:
            await manager.register()

            # 不发送心跳，等待超时
            await asyncio.sleep(1.5)

            # 节点应被视为不活跃
            is_alive = await manager.is_node_alive(test_node_id)
            assert is_alive is False

        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_unregister(self, redis_client, test_node_id):
        """测试节点注销"""
        manager = NodeManager(TEST_REDIS_URL, node_id=test_node_id)

        try:
            await manager.register()
            await asyncio.sleep(0.1)

            await manager.unregister()
            await asyncio.sleep(0.1)

            # 验证状态已变更
            key = f"{manager.NODE_KEY_PREFIX}{test_node_id}"
            status = await redis_client.hget(key, "status")
            assert status == NodeStatus.STOPPED.value

        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_node_info(self, test_node_id):
        """测试节点信息获取"""
        manager = NodeManager(TEST_REDIS_URL, node_id=test_node_id)

        try:
            await manager.start()
            await asyncio.sleep(0.1)

            info = manager.node_info
            assert info.node_id == test_node_id
            assert info.hostname is not None
            assert info.pid > 0
            assert info.status == NodeStatus.ACTIVE

        finally:
            await manager.stop()
