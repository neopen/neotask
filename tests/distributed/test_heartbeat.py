"""
@FileName: test_heartbeat.py
@Description: 节点心跳和故障检测测试
@Author: HiPeng
@Time: 2026/4/29
"""

import asyncio
import pytest
import time


# ========== 1. 基础节点管理测试 ==========

@pytest.mark.distributed
class TestNodeManagerBasic:
    """节点管理器基础测试"""

    @pytest.mark.asyncio
    async def test_node_registration(self, node_manager):
        """测试节点注册"""
        active_nodes = await node_manager.get_active_nodes()
        node_ids = [n.node_id for n in active_nodes]

        assert node_manager.node_id in node_ids
        print(f"\n节点注册成功: {node_manager.node_id}")

    @pytest.mark.asyncio
    async def test_node_info(self, node_manager):
        """测试节点信息"""
        info = node_manager.node_info
        assert info.node_id == node_manager.node_id
        assert info.hostname is not None
        assert info.pid > 0
        print(f"\n节点信息: id={info.node_id}, host={info.hostname}, pid={info.pid}")

    @pytest.mark.asyncio
    async def test_heartbeat_update(self, node_manager):
        """测试心跳更新"""
        await asyncio.sleep(2)

        active_nodes = await node_manager.get_active_nodes()
        node_info = next((n for n in active_nodes if n.node_id == node_manager.node_id), None)

        assert node_info is not None
        assert time.time() - node_info.last_heartbeat < 10
        print(f"\n心跳正常: last_heartbeat={node_info.last_heartbeat}")

    @pytest.mark.asyncio
    async def test_node_unregistration(self, node_manager):
        """测试节点注销"""
        node_id = node_manager.node_id
        await node_manager.stop()
        await asyncio.sleep(1)

        is_alive = await node_manager.is_node_alive(node_id)
        assert not is_alive
        print(f"\n节点注销成功: {node_id}")


# ========== 2. 多节点测试 ==========

@pytest.mark.distributed
class TestMultiNode:
    """多节点测试"""

    @pytest.mark.asyncio
    async def test_multiple_nodes_registration(self, multi_nodes):
        """测试多个节点注册"""
        active_nodes = await multi_nodes[0].get_active_nodes()
        node_ids = [n.node_id for n in active_nodes]

        for node in multi_nodes:
            assert node.node_id in node_ids

        print(f"\n多节点测试: 活跃节点数={len(active_nodes)}")
        assert len(active_nodes) >= 3

    @pytest.mark.asyncio
    async def test_node_detection(self, multi_nodes):
        """测试节点相互发现"""
        for node in multi_nodes:
            active_nodes = await node.get_active_nodes()
            assert len(active_nodes) >= 3
            assert node.node_id in [n.node_id for n in active_nodes]

        print(f"\n节点发现正常")

    @pytest.mark.asyncio
    async def test_node_failure_detection(self, multi_nodes):
        """测试节点故障检测"""
        failed_node = multi_nodes[1]
        failed_node_id = failed_node.node_id
        await failed_node.stop()

        await asyncio.sleep(5)

        active_nodes = await multi_nodes[0].get_active_nodes()
        node_ids = [n.node_id for n in active_nodes]

        assert failed_node_id not in node_ids
        print(f"\n故障检测成功: 节点 {failed_node_id} 已移除")

    @pytest.mark.asyncio
    async def test_node_rejoin(self, multi_nodes):
        """测试节点重新加入"""
        rejoined_node = multi_nodes[1]
        rejoined_node_id = rejoined_node.node_id
        await rejoined_node.stop()

        await asyncio.sleep(5)
        await rejoined_node.start()
        await asyncio.sleep(2)

        active_nodes = await multi_nodes[0].get_active_nodes()
        node_ids = [n.node_id for n in active_nodes]

        assert rejoined_node_id in node_ids
        print(f"\n节点重新加入成功: {rejoined_node_id}")


# ========== 3. 心跳超时测试 ==========

@pytest.mark.distributed
class TestHeartbeatTimeout:
    """心跳超时测试"""

    @pytest.mark.asyncio
    async def test_heartbeat_timeout_detection(self, distributed_redis_url):
        """测试心跳超时检测"""
        from neotask.distributed.node import NodeManager
        import uuid

        node1 = NodeManager(distributed_redis_url, f"heartbeat_node_1_{uuid.uuid4().hex[:8]}")
        node2 = NodeManager(distributed_redis_url, f"heartbeat_node_2_{uuid.uuid4().hex[:8]}")

        try:
            await node1.start()
            await node2.start()

            active = await node1.get_active_nodes()
            assert len(active) >= 2

            await node2.stop()
            await asyncio.sleep(22)

            active = await node1.get_active_nodes()
            node_ids = [n.node_id for n in active]

            assert node2.node_id not in node_ids
            print(f"\n心跳超时检测成功")

        finally:
            await node1.stop()
            await node2.stop()


# ========== 4. 节点状态测试 ==========

@pytest.mark.distributed
class TestNodeStatus:
    """节点状态测试"""

    @pytest.mark.asyncio
    async def test_node_status_transitions(self, distributed_redis_url):
        """测试节点状态转换"""
        from neotask.distributed.node import NodeManager, NodeStatus
        import uuid

        node = NodeManager(distributed_redis_url, f"status_test_node_{uuid.uuid4().hex[:8]}")

        try:
            await node.start()
            assert node.node_info.status == NodeStatus.ACTIVE
            print(f"\n节点启动状态: {node.node_info.status}")

            await node.stop()
            is_alive = await node.is_node_alive(node.node_id)
            assert not is_alive
            print(f"节点停止后不再活跃")

        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_node_metadata(self, distributed_redis_url):
        """测试节点元数据"""
        from neotask.distributed.node import NodeManager
        import uuid

        metadata = {
            "version": "0.4.0",
            "role": "worker",
            "concurrency": 10
        }

        node = NodeManager(distributed_redis_url, f"metadata_test_node_{uuid.uuid4().hex[:8]}")

        try:
            await node.register(metadata=metadata)
            active_nodes = await node.get_active_nodes()
            node_info = next((n for n in active_nodes if n.node_id == node.node_id), None)

            assert node_info is not None
            print(f"\n节点元数据设置成功")

        finally:
            await node.stop()


# ========== 5. 并发节点操作测试 ==========

@pytest.mark.distributed
class TestConcurrentNodeOps:
    """并发节点操作测试"""

    @pytest.mark.asyncio
    async def test_concurrent_registration(self, distributed_redis_url):
        """测试并发注册"""
        from neotask.distributed.node import NodeManager
        import uuid

        nodes = []

        async def create_node(i: int):
            node_id = f"concurrent_node_{i}_{uuid.uuid4().hex[:8]}"
            node = NodeManager(distributed_redis_url, node_id)
            await node.start()
            nodes.append(node)
            return node

        tasks = [create_node(i) for i in range(5)]
        await asyncio.gather(*tasks)

        try:
            if nodes:
                first_node = nodes[0]
                active_nodes = await first_node.get_active_nodes()
                print(f"\n并发注册测试: 活跃节点数={len(active_nodes)}")
                assert len(active_nodes) >= 5
        finally:
            for node in nodes:
                await node.stop()

    @pytest.mark.asyncio
    async def test_concurrent_heartbeat(self, distributed_redis_url):
        """测试并发心跳"""
        from neotask.distributed.node import NodeManager
        import uuid

        node = NodeManager(distributed_redis_url, f"heartbeat_concurrent_{uuid.uuid4().hex[:8]}")

        try:
            await node.start()

            async def send_heartbeat():
                await node.heartbeat()

            tasks = [send_heartbeat() for _ in range(20)]
            await asyncio.gather(*tasks)

            is_alive = await node.is_node_alive(node.node_id)
            assert is_alive
            print(f"\n并发心跳测试成功: 20次并发心跳")

        finally:
            await node.stop()


# ========== 主测试入口 ==========

def run_heartbeat_tests():
    """运行所有心跳测试"""
    import pytest
    args = [
        __file__,
        "-v",
        "-m", "distributed",
        "--maxfail=3"
    ]
    pytest.main(args)


if __name__ == "__main__":
    run_heartbeat_tests()