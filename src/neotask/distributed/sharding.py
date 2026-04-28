"""
@FileName: sharding.py
@Description: 任务分片 - 一致性哈希分片
@Author: HiPeng
@Time: 2026/4/28
"""

import hashlib
from abc import ABC, abstractmethod
from typing import List, Dict


class Sharder(ABC):
    """分片器抽象基类"""

    @abstractmethod
    def get_shard(self, key: str) -> int:
        """获取键对应的分片索引"""
        pass

    @abstractmethod
    def get_shard_count(self) -> int:
        """获取分片数量"""
        pass


class ConsistentHashSharder(Sharder):
    """一致性哈希分片器

    适用于分布式环境，节点增删时影响最小。

    使用示例：
        >>> sharder = ConsistentHashSharder(nodes=["node1", "node2", "node3"])
        >>> shard = sharder.get_shard("task-12345")
    """

    def __init__(
            self,
            nodes: List[str],
            virtual_nodes: int = 150,
            hash_fn: str = "md5"
    ):
        """初始化一致性哈希分片器

        Args:
            nodes: 节点列表
            virtual_nodes: 每个物理节点的虚拟节点数
            hash_fn: 哈希函数（md5, sha1, sha256）
        """
        self._nodes = nodes
        self._virtual_nodes = virtual_nodes
        self._hash_fn = hash_fn
        self._ring: Dict[int, str] = {}
        self._build_ring()

    def _build_ring(self) -> None:
        """构建哈希环"""
        self._ring.clear()

        for node in self._nodes:
            for i in range(self._virtual_nodes):
                key = f"{node}:{i}"
                hash_value = self._hash(key)
                self._ring[hash_value] = node

    def _hash(self, key: str) -> int:
        """计算哈希值"""
        if self._hash_fn == "md5":
            return int(hashlib.md5(key.encode()).hexdigest()[:8], 16)
        elif self._hash_fn == "sha1":
            return int(hashlib.sha1(key.encode()).hexdigest()[:8], 16)
        else:
            return int(hashlib.sha256(key.encode()).hexdigest()[:8], 16)

    def get_shard(self, key: str) -> str:
        """获取键对应的节点"""
        if not self._ring:
            return ""

        hash_value = self._hash(key)

        # 找到第一个大于等于 hash_value 的节点
        for ring_hash in sorted(self._ring.keys()):
            if ring_hash >= hash_value:
                return self._ring[ring_hash]

        # 如果没找到，返回第一个节点
        return self._ring[min(self._ring.keys())]

    def get_shard_count(self) -> int:
        return len(self._nodes)

    def add_node(self, node: str) -> None:
        """添加节点"""
        if node not in self._nodes:
            self._nodes.append(node)
            self._build_ring()

    def remove_node(self, node: str) -> None:
        """移除节点"""
        if node in self._nodes:
            self._nodes.remove(node)
            self._build_ring()


class ModuloSharder(Sharder):
    """取模分片器

    简单但节点变化时影响大。
    """

    def __init__(self, shard_count: int):
        self._shard_count = shard_count

    def get_shard(self, key: str) -> int:
        return hash(key) % self._shard_count

    def get_shard_count(self) -> int:
        return self._shard_count


class RangeSharder(Sharder):
    """范围分片器

    适用于有序键的范围分布。
    """

    def __init__(self, ranges: List[tuple]):
        """初始化范围分片器

        Args:
            ranges: 范围列表，如 [("a", "m"), ("n", "z")]
        """
        self._ranges = ranges

    def get_shard(self, key: str) -> int:
        for i, (start, end) in enumerate(self._ranges):
            if start <= key <= end:
                return i
        return -1

    def get_shard_count(self) -> int:
        return len(self._ranges)
