"""
@FileName: coordinator.py
@Description: 协调器 - 多节点任务分发和协调
@Author: HiPeng
@Time: 2026/4/28
"""

import random
from dataclasses import dataclass
from typing import Optional

from neotask.distributed.node import NodeManager, NodeInfo
from neotask.queue.queue_scheduler import QueueScheduler


@dataclass
class CoordinatorConfig:
    """协调器配置"""
    # 负载均衡策略
    load_balance_strategy: str = "round_robin"  # round_robin, random, least_loaded
    # 任务分发模式
    distribution_mode: str = "shared"  # shared, partitioned
    # 是否启用节点亲和性
    enable_affinity: bool = False
    # 是否在任务中记录目标节点
    record_target_node: bool = True


class Coordinator:
    """协调器

    负责：
    - 多节点任务分发
    - 负载均衡
    - 节点故障转移

    设计模式：Strategy Pattern - 可插拔的负载均衡策略

    注意：由于队列是共享的（所有节点共享同一个Redis队列），
    不需要指定目标节点。每个节点从共享队列中消费任务。
    """

    def __init__(
            self,
            node_manager: NodeManager,
            queue_scheduler: QueueScheduler,
            config: Optional[CoordinatorConfig] = None
    ):
        self._node_manager = node_manager
        self._queue = queue_scheduler
        self._config = config or CoordinatorConfig()
        self._round_robin_index = 0

    async def distribute_task(
            self,
            task_id: str,
            priority: int,
            affinity_key: Optional[str] = None,
            delay: float = 0
    ) -> Optional[str]:
        """分发任务到共享队列

        在共享队列模式下，所有节点从同一个队列消费任务，
        因此不需要指定目标节点。负载均衡由消费者自然实现。

        Args:
            task_id: 任务ID
            priority: 优先级
            affinity_key: 亲和性键（用于分区模式，预留）
            delay: 延迟执行时间

        Returns:
            目标节点ID（共享模式下返回 None，因为由消费者决定）
        """
        # 将任务放入共享队列
        await self._queue.push(task_id, priority, delay)

        # 如果需要记录亲和性，可以存储到Redis
        if affinity_key and self._config.enable_affinity:
            # 存储任务亲和性信息，用于后续的路由决策
            await self._store_affinity(task_id, affinity_key)

        return None  # 共享模式不指定目标节点

    async def distribute_task_to_node(
            self,
            task_id: str,
            priority: int,
            target_node_id: str,
            delay: float = 0
    ) -> bool:
        """将任务分发到指定节点（预留用于分区模式）

        注意：当前实现使用共享队列，此方法为后续节点专属队列预留。

        Args:
            task_id: 任务ID
            priority: 优先级
            target_node_id: 目标节点ID
            delay: 延迟执行时间

        Returns:
            是否成功
        """
        # 存储节点关联信息
        key = f"neotask:node_task:{task_id}"
        client = await self._node_manager._get_client()
        await client.setex(key, 3600, target_node_id)

        # 仍然放入共享队列
        await self._queue.push(task_id, priority, delay)

        return True

    async def get_task_node(self, task_id: str) -> Optional[str]:
        """获取任务关联的节点"""
        client = await self._node_manager._get_client()
        key = f"neotask:node_task:{task_id}"
        return await client.get(key)

    async def _store_affinity(self, task_id: str, affinity_key: str) -> None:
        """存储任务亲和性信息"""
        client = await self._node_manager._get_client()
        key = f"neotask:affinity:{task_id}"
        await client.setex(key, 3600, affinity_key)

    async def _select_node(self) -> Optional[NodeInfo]:
        """选择目标节点（用于分区模式，当前预留）"""
        active_nodes = await self._node_manager.get_active_nodes()

        if not active_nodes:
            return None

        # 过滤掉本节点（可选）
        nodes = [n for n in active_nodes if n.node_id != self._node_manager.node_id]

        if not nodes:
            nodes = active_nodes

        if self._config.load_balance_strategy == "round_robin":
            self._round_robin_index = (self._round_robin_index + 1) % len(nodes)
            return nodes[self._round_robin_index]

        elif self._config.load_balance_strategy == "random":
            return random.choice(nodes)

        else:
            return nodes[0]

    async def _get_node_by_affinity(self, affinity_key: str) -> Optional[NodeInfo]:
        """根据亲和性键获取节点（分区模式）"""
        active_nodes = await self._node_manager.get_active_nodes()

        if not active_nodes:
            return None

        # 一致性哈希选择节点
        hash_value = hash(affinity_key)
        idx = hash_value % len(active_nodes)
        return active_nodes[idx]

    async def rebalance(self) -> None:
        """重新平衡任务（预留）"""
        # TODO: 实现任务重新平衡
        pass

    async def handle_node_failure(self, failed_node_id: str) -> None:
        """处理节点故障

        回收失败节点正在处理的任务
        """
        # TODO: 实现任务回收
        pass
