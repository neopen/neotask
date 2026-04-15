"""
@FileName: heartbeat.py
@Description: 节点心跳管理
@Author: HiPeng
@Time: 2026/4/15
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Set

from neotask.storage.base import TaskRepository


@dataclass
class HeartbeatConfig:
    """心跳配置"""
    interval: float = 5.0  # 心跳间隔（秒）
    timeout: float = 20.0  # 心跳超时（秒）
    cleanup_interval: float = 30.0  # 清理检查间隔


class HeartbeatManager:
    """节点心跳管理器

    负责：
    - 定期上报本节点心跳
    - 检测其他节点心跳
    - 发现并回收僵尸节点任务
    """

    def __init__(
            self,
            node_id: str,
            storage,  # Redis/SQLite 存储
            task_repo: TaskRepository,
            config: Optional[HeartbeatConfig] = None
    ):
        self._node_id = node_id
        self._storage = storage
        self._task_repo = task_repo
        self._config = config or HeartbeatConfig()

        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None

        # 节点注册
        self._active_nodes: Set[str] = set()

    async def start(self) -> None:
        """启动心跳管理器"""
        if self._running:
            return

        self._running = True

        # 注册节点
        await self._register_node()

        # 启动心跳上报
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # 启动节点监控
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def stop(self) -> None:
        """停止心跳管理器"""
        self._running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._monitor_task:
            self._monitor_task.cancel()

        # 注销节点
        await self._unregister_node()

    async def _register_node(self) -> None:
        """注册节点"""
        key = f"node:{self._node_id}"
        await self._storage.hset(key, mapping={
            "node_id": self._node_id,
            "status": "active",
            "started_at": time.time(),
            "last_heartbeat": time.time()
        })
        await self._storage.expire(key, int(self._config.timeout * 2))

    async def _unregister_node(self) -> None:
        """注销节点"""
        key = f"node:{self._node_id}"
        await self._storage.hset(key, "status", "stopped")
        await self._storage.expire(key, 60)

    async def _heartbeat_loop(self) -> None:
        """心跳上报循环"""
        while self._running:
            try:
                key = f"node:{self._node_id}"
                await self._storage.hset(key, "last_heartbeat", time.time())
                await self._storage.expire(key, int(self._config.timeout * 2))
                await asyncio.sleep(self._config.interval)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1)

    async def _monitor_loop(self) -> None:
        """节点监控循环"""
        while self._running:
            try:
                # 获取所有活跃节点
                keys = await self._storage.keys("node:*")
                now = time.time()

                for key in keys:
                    node_id = key.replace("node:", "")
                    if node_id == self._node_id:
                        continue

                    data = await self._storage.hgetall(key)
                    if not data:
                        continue

                    last_heartbeat = float(data.get("last_heartbeat", 0))
                    status = data.get("status", "")

                    if status == "active" and (now - last_heartbeat) > self._config.timeout:
                        # 节点可能宕机，触发任务回收
                        await self._reclaim_node_tasks(node_id)

                await asyncio.sleep(self._config.cleanup_interval)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    async def _reclaim_node_tasks(self, node_id: str) -> None:
        """回收节点任务

        将指定节点正在执行的任务重新放回队列
        """
        # 获取该节点正在执行的任务
        # TODO: 根据存储类型实现
        pass
