"""
@FileName: test_elector.py
@Description: 选举器测试
@Author: HiPeng
@Time: 2026/4/28
"""
import pytest
import asyncio

from neotask.distributed.elector import Elector
from tests.conftest import TEST_REDIS_URL


class TestElector:
    """选举器测试"""

    @pytest.fixture
    async def elector(self, test_node_id):
        """选举器 fixture"""
        elector = Elector(TEST_REDIS_URL, test_node_id)
        yield elector
        await elector.close()

    @pytest.mark.asyncio
    async def test_elect_leader(self, elector):
        """测试选举领导者"""
        result = await elector.elect(ttl=10)
        assert result is True
        assert elector.is_leader is True

    @pytest.mark.asyncio
    async def test_only_one_leader(self, test_node_id):
        """测试只有一个领导者"""
        elector1 = Elector(TEST_REDIS_URL, f"{test_node_id}_1")
        elector2 = Elector(TEST_REDIS_URL, f"{test_node_id}_2")

        # 顺序选举，避免并发问题
        result1 = await elector1.elect(ttl=10)
        result2 = await elector2.elect(ttl=10)

        # 只有一个成功
        assert result1 is True
        assert result2 is False

        # 验证只有一个领导者
        leader = await elector1.get_leader()
        assert leader is not None
        assert leader.node_id == f"{test_node_id}_1"

        await elector1.close()
        await elector2.close()

    @pytest.mark.asyncio
    async def test_get_leader(self, elector):
        """测试获取领导者信息"""
        await elector.elect(ttl=10)

        leader = await elector.get_leader()
        assert leader is not None
        assert leader.node_id == elector._node_id
        assert leader.term == 1
        assert leader.expires_at > leader.elected_at

    @pytest.mark.asyncio
    async def test_renew_leadership(self, elector):
        """测试领导权续期"""
        await elector.elect(ttl=3)

        # 获取初始过期时间
        leader_before = await elector.get_leader()
        await asyncio.sleep(1)

        # 续期
        result = await elector.renew(ttl=2)
        assert result is True

        # 验证过期时间已更新
        leader_after = await elector.get_leader()
        assert leader_after.expires_at > leader_before.expires_at

    @pytest.mark.asyncio
    async def test_resign(self, elector):
        """测试放弃领导权"""
        await elector.elect(ttl=10)
        assert elector.is_leader is True

        result = await elector.resign()
        assert result is True
        assert elector.is_leader is False

        # 验证领导者已不存在
        leader = await elector.get_leader()
        assert leader is None

    @pytest.mark.asyncio
    async def test_leader_auto_expire(self, elector):
        """测试领导权自动过期"""
        await elector.elect(ttl=1)
        assert elector.is_leader is True

        # 等待过期
        await asyncio.sleep(1.5)

        # 验证领导者已不存在
        leader = await elector.get_leader()
        assert leader is None
        assert elector.is_leader is False

    @pytest.mark.asyncio
    async def test_elect_after_leader_expired(self, test_node_id):
        """测试领导者过期后重新选举"""
        elector1 = Elector(TEST_REDIS_URL, f"{test_node_id}_1")
        elector2 = Elector(TEST_REDIS_URL, f"{test_node_id}_2")

        # 第一个成为领导者
        result1 = await elector1.elect(ttl=1)
        assert result1 is True
        assert elector1.is_leader is True

        # 等待领导者过期
        await asyncio.sleep(1.5)

        # 验证第一个不再是领导者
        assert elector1.is_leader is False

        # 第二个成为新领导者
        result2 = await elector2.elect(ttl=10)
        assert result2 is True
        assert elector2.is_leader is True

        # 验证新领导者
        leader = await elector2.get_leader()
        assert leader.node_id == f"{test_node_id}_2"

        await elector1.close()
        await elector2.close()

    @pytest.mark.asyncio
    async def test_wait_for_election(self, test_node_id):
        """测试等待选举"""
        elector = Elector(TEST_REDIS_URL, test_node_id)

        # 直接等待选举
        result = await elector.wait_for_election(timeout=5)
        assert result is True
        assert elector.is_leader is True

        await elector.close()

    @pytest.mark.asyncio
    async def test_wait_for_election_leader_exists(self, test_node_id):
        """测试领导者已存在时等待选举"""
        elector1 = Elector(TEST_REDIS_URL, f"{test_node_id}_1")
        elector2 = Elector(TEST_REDIS_URL, f"{test_node_id}_2")

        # 第一个成为领导者
        result1 = await elector1.elect(ttl=5)
        assert result1 is True

        # 第二个等待选举（应该返回 False，因为领导者已存在）
        result2 = await elector2.wait_for_election(timeout=1)
        assert result2 is False
        assert elector2.is_leader is False

        await elector1.close()
        await elector2.close()