"""
@FileName: elector.py
@Description: 主节点选举 - 基于Redis的分布式选举
@Author: HiPeng
@Time: 2026/4/28
"""

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Optional

import redis.asyncio as redis
from redis.asyncio import ConnectionPool


@dataclass
class LeaderInfo:
    """领导者信息"""
    node_id: str
    term: int
    elected_at: float
    expires_at: float


class Elector:
    """主节点选举器

    基于Redis的分布式选举，支持：
    - 主节点选举
    - 领导权续期
    - 主从切换
    """

    LEADER_KEY = "neotask:leader"

    def __init__(self, redis_url: str, node_id: str):
        """初始化选举器

        Args:
            redis_url: Redis连接URL
            node_id: 当前节点ID
        """
        self._redis_url = redis_url
        self._node_id = node_id
        self._client: Optional[redis.Redis] = None
        self._is_leader = False
        self._current_term = 0
        self._owner_id = None
        self._renew_task: Optional[asyncio.Task] = None

    async def _get_client(self) -> redis.Redis:
        """获取Redis客户端"""
        if self._client is None:
            pool = ConnectionPool.from_url(self._redis_url, decode_responses=True)
            self._client = redis.Redis(connection_pool=pool)
        return self._client

    def _generate_owner_id(self) -> str:
        """生成唯一持有者ID"""
        return f"{self._node_id}:{uuid.uuid4().hex[:8]}"

    async def elect(self, ttl: int = 30) -> bool:
        """尝试成为领导者

        Args:
            ttl: 领导权TTL（秒）

        Returns:
            是否成为领导者
        """
        client = await self._get_client()
        self._current_term += 1
        self._owner_id = self._generate_owner_id()

        value = f"{self._node_id}:{self._current_term}:{self._owner_id}"

        # 使用 SET NX 原子操作
        result = await client.set(
            self.LEADER_KEY,
            value,
            nx=True,
            ex=ttl
        )

        if result:
            self._is_leader = True
            self._start_renew_task(ttl)
            return True

        # 检查是否是同一个节点（可能是网络抖动导致）
        current_value = await client.get(self.LEADER_KEY)
        if current_value and current_value.startswith(f"{self._node_id}:"):
            # 同一个节点，续期
            await client.expire(self.LEADER_KEY, ttl)
            self._is_leader = True
            self._start_renew_task(ttl)
            return True

        self._is_leader = False
        return False

    def _start_renew_task(self, ttl: int) -> None:
        """启动续期任务"""
        if self._renew_task and not self._renew_task.done():
            self._renew_task.cancel()

        self._renew_task = asyncio.create_task(self._renew_loop(ttl))

    async def renew(self, ttl: int = 30) -> bool:
        """续期领导权

        Args:
            ttl: 新的TTL

        Returns:
            是否续期成功
        """
        if not self._is_leader:
            return False

        client = await self._get_client()

        # 先检查键是否存在
        exists = await client.exists(self.LEADER_KEY)
        if not exists:
            self._is_leader = False
            return False

        current = await client.get(self.LEADER_KEY)
        if not current:
            self._is_leader = False
            return False

        expected = f"{self._node_id}:{self._current_term}:{self._owner_id}"

        if current == expected:
            # 续期成功
            await client.expire(self.LEADER_KEY, ttl)
            return True
        elif current.startswith(f"{self._node_id}:"):
            # 节点匹配但 term/owner 不匹配，更新
            await client.set(self.LEADER_KEY, expected, ex=ttl)
            return True
        else:
            # 领导权已丢失
            self._is_leader = False
            return False

    async def resign(self) -> bool:
        """放弃领导权"""
        if not self._is_leader:
            return False

        client = await self._get_client()

        # 先检查键是否存在
        exists = await client.exists(self.LEADER_KEY)
        if exists:
            current = await client.get(self.LEADER_KEY)
            expected = f"{self._node_id}:{self._current_term}:{self._owner_id}"
            if current == expected:
                await client.delete(self.LEADER_KEY)

        self._is_leader = False

        if self._renew_task:
            self._renew_task.cancel()
            self._renew_task = None

        return True

    async def get_leader(self) -> Optional[LeaderInfo]:
        """获取当前领导者信息"""
        client = await self._get_client()

        # 先检查键是否存在
        exists = await client.exists(self.LEADER_KEY)
        if not exists:
            return None

        value = await client.get(self.LEADER_KEY)
        if not value:
            return None

        # 检查 TTL，如果 <= 0 说明已过期
        ttl = await client.ttl(self.LEADER_KEY)
        if ttl <= 0:
            # 键已过期，清理
            await client.delete(self.LEADER_KEY)
            return None

        parts = value.split(":")
        node_id = parts[0]
        term = int(parts[1]) if len(parts) > 1 else 0

        now = time.time()

        return LeaderInfo(
            node_id=node_id,
            term=term,
            elected_at=now - ttl,
            expires_at=now + ttl
        )

    async def wait_for_election(self, timeout: float = 30) -> bool:
        """等待成为领导者

        Args:
            timeout: 超时时间

        Returns:
            是否成为领导者
        """
        start = time.time()
        while time.time() - start < timeout:
            # 先检查是否已经是领导者
            leader = await self.get_leader()
            if leader and leader.node_id == self._node_id:
                self._is_leader = True
                return True

            # 尝试选举
            if await self.elect():
                return True

            await asyncio.sleep(0.5)
        return False

    async def _renew_loop(self, ttl: int) -> None:
        """续期循环"""
        # 每 TTL/2 秒续期一次，确保不会过期
        interval = max(ttl / 2, 1)

        while self._is_leader:
            await asyncio.sleep(interval)

            if not self._is_leader:
                break

            success = await self.renew(ttl)

            if not success:
                self._is_leader = False
                break

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    async def close(self) -> None:
        """关闭连接"""
        if self._renew_task:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass

        if self._client:
            await self._client.close()
            self._client = None