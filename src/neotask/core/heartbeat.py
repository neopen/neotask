"""
@FileName: heartbeat.py
@Description: 节点心跳管理
@Author: HiPeng
@Time: 2026/4/15
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Set, Dict, Any, List

from neotask.models.task import TaskStatus
from neotask.storage.base import TaskRepository


@dataclass
class HeartbeatConfig:
    """心跳配置"""
    interval: float = 5.0  # 心跳间隔（秒）
    timeout: float = 20.0  # 心跳超时（秒）
    cleanup_interval: float = 30.0  # 清理检查间隔
    max_reclaim_per_cycle: int = 100  # 每周期最大回收数量


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

        # 统计
        self._reclaimed_nodes: Set[str] = set()
        self._total_reclaimed_tasks = 0

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
        await self._storage.hset(key, "node_id", self._node_id)
        await self._storage.hset(key, "status", "active")
        await self._storage.hset(key, "started_at", str(time.time()))
        await self._storage.hset(key, "last_heartbeat", str(time.time()))
        await self._storage.expire(key, int(self._config.timeout * 2))

        # 加入节点集合
        await self._storage.sadd("neotask:active_nodes", self._node_id)

    async def _unregister_node(self) -> None:
        """注销节点"""
        key = f"node:{self._node_id}"
        await self._storage.hset(key, "status", "stopped")
        await self._storage.expire(key, 60)

        # 从节点集合移除
        await self._storage.srem("neotask:active_nodes", self._node_id)

    async def _heartbeat_loop(self) -> None:
        """心跳上报循环"""
        while self._running:
            try:
                key = f"node:{self._node_id}"
                await self._storage.hset(key, "last_heartbeat", str(time.time()))
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
                await asyncio.sleep(self._config.cleanup_interval)
                await self._check_and_reclaim_dead_nodes()
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    async def _check_and_reclaim_dead_nodes(self) -> None:
        """检查并回收死节点的任务"""
        # 获取所有活跃节点
        node_ids = await self._storage.smembers("neotask:active_nodes")
        now = time.time()

        for node_id in node_ids:
            if node_id == self._node_id:
                continue

            key = f"node:{node_id}"
            data = await self._storage.hgetall(key)

            if not data:
                continue

            last_heartbeat = float(data.get("last_heartbeat", 0))
            status = data.get("status", "")

            # 判断节点是否死亡
            is_dead = (
                    status != "stopped" and
                    (now - last_heartbeat) > self._config.timeout
            )

            if is_dead and node_id not in self._reclaimed_nodes:
                # 回收该节点的任务
                await self._reclaim_node_tasks(node_id)
                self._reclaimed_nodes.add(node_id)

    async def _reclaim_node_tasks(self, node_id: str) -> None:
        """回收节点任务

        将指定节点正在执行的任务重新放回队列
        """
        try:
            # 获取该节点正在执行的任务
            # 方案1：通过状态索引扫描
            running_task_ids = await self._storage.smembers(f"status:{TaskStatus.RUNNING.value}")

            reclaimed_count = 0

            for task_id in running_task_ids:
                if reclaimed_count >= self._config.max_reclaim_per_cycle:
                    break

                # 获取任务详情
                key = f"task:{task_id}"
                data = await self._storage.hgetall(key)

                if not data:
                    continue

                # 检查任务是否属于该节点
                task_node_id = data.get("node_id", "")
                if task_node_id != node_id:
                    continue

                # 回收任务
                success = await self._reclaim_single_task(task_id, data)
                if success:
                    reclaimed_count += 1

            self._total_reclaimed_tasks += reclaimed_count

            # 标记节点为已回收
            key = f"node:{node_id}"
            await self._storage.hset(key, "status", "reclaimed")

        except Exception as e:
            pass

    async def _reclaim_single_task(self, task_id: str, task_data: Dict) -> bool:
        """回收单个任务

        Args:
            task_id: 任务ID
            task_data: 任务数据

        Returns:
            是否回收成功
        """
        try:
            # 更新任务状态为 PENDING
            key = f"task:{task_id}"
            await self._storage.hset(key, "status", TaskStatus.PENDING.value)
            await self._storage.hset(key, "node_id", "")
            await self._storage.hset(key, "error", f"Reclaimed from dead node")

            # 获取优先级
            priority = int(task_data.get("priority", 2))

            # 重新入队
            await self._storage.zadd("queue:priority", {task_id: priority})

            # 更新状态索引
            await self._storage.srem(f"status:{TaskStatus.RUNNING.value}", task_id)
            await self._storage.sadd(f"status:{TaskStatus.PENDING.value}", task_id)

            return True

        except Exception as e:
            return False

    async def get_active_nodes(self) -> List[str]:
        """获取活跃节点列表"""
        node_ids = await self._storage.smembers("neotask:active_nodes")
        active_nodes = []

        for node_id in node_ids:
            if await self.is_node_alive(node_id):
                active_nodes.append(node_id)

        return active_nodes

    async def is_node_alive(self, node_id: str) -> bool:
        """检查节点是否存活"""
        key = f"node:{node_id}"
        data = await self._storage.hgetall(key)

        if not data:
            return False

        last_heartbeat = float(data.get("last_heartbeat", 0))
        status = data.get("status", "")

        is_alive = (
                status == "active" and
                (time.time() - last_heartbeat) < self._config.timeout
        )

        return is_alive

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "node_id": self._node_id,
            "is_running": self._running,
            "reclaimed_nodes": list(self._reclaimed_nodes),
            "total_reclaimed_tasks": self._total_reclaimed_tasks,
            "config": {
                "interval": self._config.interval,
                "timeout": self._config.timeout,
                "cleanup_interval": self._config.cleanup_interval
            }
        }
