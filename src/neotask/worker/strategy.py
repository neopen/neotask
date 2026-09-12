"""
@FileName: strategy.py
@Description: 负载均衡策略 - 任务分发策略
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/8 00:00
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
import random


class LoadBalanceStrategy(ABC):
    """负载均衡策略基类"""

    @abstractmethod
    def select_worker(self, workers: List[int], stats: Dict[int, Any]) -> int:
        """选择worker"""
        pass


class RoundRobinStrategy(LoadBalanceStrategy):
    """轮询策略"""

    def __init__(self):
        self._counter = 0

    def select_worker(self, workers: List[int], stats: Dict[int, Any]) -> int:
        if not workers:
            raise ValueError("No workers available")

        worker_id = workers[self._counter % len(workers)]
        self._counter += 1
        return worker_id


class LeastLoadStrategy(LoadBalanceStrategy):
    """最少负载策略"""

    def select_worker(self, workers: List[int], stats: Dict[int, Any]) -> int:
        if not workers:
            raise ValueError("No workers available")

        # 选择活跃任务最少的worker
        return min(
            workers,
            key=lambda w: stats.get(w, {}).get("active_tasks", 0)
        )


class RandomStrategy(LoadBalanceStrategy):
    """随机策略"""

    def select_worker(self, workers: List[int], stats: Dict[int, Any]) -> int:
        if not workers:
            raise ValueError("No workers available")

        return random.choice(workers)


class WeightedStrategy(LoadBalanceStrategy):
    """加权策略"""

    def __init__(self, weights: Dict[int, float] = None):
        self._weights = weights or {}

    def select_worker(self, workers: List[int], stats: Dict[int, Any]) -> int:
        if not workers:
            raise ValueError("No workers available")

        # 计算每个worker的权重
        available_weights = []
        for w in workers:
            weight = self._weights.get(w, 1.0)
            # 根据负载调整权重
            active_tasks = stats.get(w, {}).get("active_tasks", 0)
            if active_tasks > 0:
                weight /= active_tasks
            available_weights.append(weight)

        # 加权随机选择
        total = sum(available_weights)
        r = random.uniform(0, total)
        cumulative = 0
        for w, weight in zip(workers, available_weights):
            cumulative += weight
            if r <= cumulative:
                return w

        return workers[0]


class ConsistentHashStrategy(LoadBalanceStrategy):
    """一致性哈希策略"""

    def __init__(self, virtual_nodes: int = 150):
        self._virtual_nodes = virtual_nodes
        self._ring: Dict[int, int] = {}

    def select_worker(self, workers: List[int], stats: Dict[int, Any]) -> int:
        if not workers:
            raise ValueError("No workers available")

        # 重建哈希环
        self._build_ring(workers)

        # 使用任务ID哈希选择worker
        # 这里简化处理，实际应该使用任务ID
        hash_value = random.randint(0, 2**32)

        # 找到哈希环上最近的节点
        for key in sorted(self._ring.keys()):
            if hash_value <= key:
                return self._ring[key]

        return self._ring[min(self._ring.keys())]

    def _build_ring(self, workers: List[int]) -> None:
        """构建哈希环"""
        self._ring.clear()

        for worker in workers:
            for i in range(self._virtual_nodes):
                hash_key = hash(f"{worker}:{i}") & 0xFFFFFFFF
                self._ring[hash_key] = worker