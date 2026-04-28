"""
@FileName: node.py
@Description: 节点管理 - 节点注册、心跳、健康检查
@Author: HiPeng
@Time: 2026/4/28
"""

import asyncio
import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, List, Any

import redis.asyncio as redis
from redis.asyncio import ConnectionPool


class NodeStatus(Enum):
    """节点状态"""
    ACTIVE = "active"  # 活跃
    STOPPED = "stopped"  # 已停止
    FAILED = "failed"  # 故障


@dataclass
class NodeInfo:
    """节点信息"""
    node_id: str
    hostname: str
    pid: int
    status: NodeStatus = NodeStatus.ACTIVE
    started_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class NodeManager:
    """节点管理器

    负责：
    - 节点注册/注销
    - 心跳上报
    - 节点健康检查
    - 活跃节点列表查询

    设计模式：Singleton Pattern - 每个进程只有一个节点实例
    """

    NODE_KEY_PREFIX = "neotask:node:"
    NODES_SET_KEY = "neotask:nodes"
    HEARTBEAT_INTERVAL = 5.0
    HEARTBEAT_TIMEOUT = 20.0

    def __init__(self, redis_url: str, node_id: Optional[str] = None):
        """初始化节点管理器

        Args:
            redis_url: Redis连接URL
            node_id: 节点ID（默认自动生成）
        """
        self._redis_url = redis_url
        self._node_id = node_id or self._generate_node_id()
        self._client: Optional[redis.Redis] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._health_check_task: Optional[asyncio.Task] = None
        self._running = False
        self._node_info = NodeInfo(
            node_id=self._node_id,
            hostname=socket.gethostname(),
            pid=os.getpid()
        )

    async def _get_client(self) -> redis.Redis:
        """获取Redis客户端"""
        if self._client is None:
            pool = ConnectionPool.from_url(self._redis_url, decode_responses=True)
            self._client = redis.Redis(connection_pool=pool)
        return self._client

    def _generate_node_id(self) -> str:
        """生成节点ID"""
        hostname = socket.gethostname()
        return f"{hostname}_{os.getpid()}_{uuid.uuid4().hex[:8]}"

    async def register(self, metadata: Optional[Dict] = None) -> bool:
        """注册节点

        Args:
            metadata: 节点元数据

        Returns:
            是否注册成功
        """
        client = await self._get_client()
        key = f"{self.NODE_KEY_PREFIX}{self._node_id}"

        if metadata:
            self._node_info.metadata.update(metadata)

        # 修复：使用 HSET 的正确语法（逐个字段设置）
        await client.hset(key, "node_id", self._node_id)
        await client.hset(key, "hostname", self._node_info.hostname)
        await client.hset(key, "pid", str(self._node_info.pid))
        await client.hset(key, "status", NodeStatus.ACTIVE.value)
        await client.hset(key, "started_at", str(self._node_info.started_at))
        await client.hset(key, "last_heartbeat", str(self._node_info.last_heartbeat))

        # 处理 metadata
        import json
        await client.hset(key, "metadata", json.dumps(self._node_info.metadata))

        await client.expire(key, int(self.HEARTBEAT_TIMEOUT * 2))

        # 添加到节点集合
        await client.sadd(self.NODES_SET_KEY, self._node_id)

        return True

    async def unregister(self) -> bool:
        """注销节点"""
        client = await self._get_client()
        key = f"{self.NODE_KEY_PREFIX}{self._node_id}"

        await client.hset(key, "status", NodeStatus.STOPPED.value)
        await client.expire(key, 60)

        return True

    async def heartbeat(self) -> None:
        """上报心跳"""
        client = await self._get_client()
        key = f"{self.NODE_KEY_PREFIX}{self._node_id}"

        await client.hset(key, "last_heartbeat", str(time.time()))
        await client.expire(key, int(self.HEARTBEAT_TIMEOUT * 2))

    async def get_active_nodes(self) -> List[NodeInfo]:
        """获取活跃节点列表"""
        client = await self._get_client()
        node_ids = await client.smembers(self.NODES_SET_KEY)

        active_nodes = []
        now = time.time()

        for node_id in node_ids:
            key = f"{self.NODE_KEY_PREFIX}{node_id}"
            data = await client.hgetall(key)

            if not data:
                continue

            last_heartbeat = float(data.get("last_heartbeat", 0))
            status = data.get("status", "")

            if status == NodeStatus.ACTIVE.value and (now - last_heartbeat) < self.HEARTBEAT_TIMEOUT:
                active_nodes.append(NodeInfo(
                    node_id=node_id,
                    hostname=data.get("hostname", ""),
                    pid=int(data.get("pid", 0)),
                    status=NodeStatus.ACTIVE,
                    started_at=float(data.get("started_at", 0)),
                    last_heartbeat=last_heartbeat
                ))

        return active_nodes

    async def is_node_alive(self, node_id: str) -> bool:
        """检查节点是否存活"""
        client = await self._get_client()
        key = f"{self.NODE_KEY_PREFIX}{node_id}"

        data = await client.hgetall(key)
        if not data:
            return False

        last_heartbeat = float(data.get("last_heartbeat", 0))
        status = data.get("status", "")

        return status == NodeStatus.ACTIVE.value and (time.time() - last_heartbeat) < self.HEARTBEAT_TIMEOUT

    async def start(self) -> None:
        """启动节点管理器"""
        if self._running:
            return

        self._running = True
        await self.register()

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._health_check_task = asyncio.create_task(self._health_check_loop())

    async def stop(self) -> None:
        """停止节点管理器"""
        self._running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        await self.unregister()

        if self._client:
            await self._client.close()

    async def _heartbeat_loop(self) -> None:
        """心跳循环"""
        while self._running:
            try:
                await self.heartbeat()
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1)

    async def _health_check_loop(self) -> None:
        """健康检查循环"""
        while self._running:
            try:
                await asyncio.sleep(self.HEARTBEAT_TIMEOUT)
                # 检查其他节点
                active_nodes = await self.get_active_nodes()
                # TODO: 处理故障节点
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def node_info(self) -> NodeInfo:
        return self._node_info
