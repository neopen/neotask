"""
@FileName: test_sharding.py
@Description: 分片器测试
@Author: HiPeng
@Time: 2026/4/28
"""

from neotask.distributed.sharding import (
    ConsistentHashSharder, ModuloSharder, RangeSharder
)


class TestConsistentHashSharder:
    """一致性哈希分片器测试"""

    def test_get_shard_consistent(self):
        """测试同一键总是返回同一节点"""
        nodes = ["node1", "node2", "node3"]
        sharder = ConsistentHashSharder(nodes, virtual_nodes=100)

        key = "task_12345"
        result1 = sharder.get_shard(key)
        result2 = sharder.get_shard(key)

        assert result1 == result2

    def test_add_node_minimal_remapping(self):
        """测试添加节点时重新映射最小化"""
        nodes1 = ["node1", "node2", "node3"]
        nodes2 = ["node1", "node2", "node3", "node4"]

        sharder1 = ConsistentHashSharder(nodes1, virtual_nodes=100)
        sharder2 = ConsistentHashSharder(nodes2, virtual_nodes=100)

        # 测试多个键的重新映射比例
        test_keys = [f"key_{i}" for i in range(100)]
        remapped = 0

        for key in test_keys:
            shard1 = sharder1.get_shard(key)
            shard2 = sharder2.get_shard(key)
            if shard1 != shard2:
                remapped += 1

        # 重新映射比例应该小于 1/(n+1) * 100%
        # 4个节点时，比例应小于25%
        assert remapped < 30

    def test_remove_node_remapping(self):
        """测试移除节点时重新映射"""
        nodes1 = ["node1", "node2", "node3"]
        nodes2 = ["node1", "node2"]

        sharder1 = ConsistentHashSharder(nodes1, virtual_nodes=100)
        sharder2 = ConsistentHashSharder(nodes2, virtual_nodes=100)

        test_keys = [f"key_{i}" for i in range(100)]
        remapped = 0

        for key in test_keys:
            shard1 = sharder1.get_shard(key)
            shard2 = sharder2.get_shard(key)
            if shard1 != shard2:
                remapped += 1

        # 重新映射比例应该较低
        assert remapped < 50

    def test_virtual_nodes_balance(self):
        """测试虚拟节点对负载均衡的影响"""
        nodes = ["node1", "node2", "node3"]

        # 低虚拟节点数
        sharder_low = ConsistentHashSharder(nodes, virtual_nodes=10)
        # 高虚拟节点数
        sharder_high = ConsistentHashSharder(nodes, virtual_nodes=1000)

        test_keys = [f"key_{i}" for i in range(1000)]

        def get_distribution(sharder):
            dist = {"node1": 0, "node2": 0, "node3": 0}
            for key in test_keys:
                node = sharder.get_shard(key)
                dist[node] += 1
            return dist

        dist_low = get_distribution(sharder_low)
        dist_high = get_distribution(sharder_high)

        # 计算标准差（值越小越均衡）
        def stddev(dist):
            values = list(dist.values())
            mean = sum(values) / len(values)
            return (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5

        # 高虚拟节点数应该更均衡
        assert stddev(dist_high) <= stddev(dist_low)

    def test_empty_nodes(self):
        """测试空节点列表"""
        sharder = ConsistentHashSharder([])
        result = sharder.get_shard("test_key")
        assert result == ""

    def test_single_node(self):
        """测试单节点"""
        sharder = ConsistentHashSharder(["node1"])

        result1 = sharder.get_shard("key1")
        result2 = sharder.get_shard("key2")

        assert result1 == "node1"
        assert result2 == "node1"

    def test_hash_functions(self):
        """测试不同哈希函数"""
        nodes = ["node1", "node2"]

        # MD5
        sharder_md5 = ConsistentHashSharder(nodes, hash_fn="md5")
        # SHA1
        sharder_sha1 = ConsistentHashSharder(nodes, hash_fn="sha1")
        # SHA256
        sharder_sha256 = ConsistentHashSharder(nodes, hash_fn="sha256")

        key = "test_key"

        # 不同哈希函数可能产生不同结果
        result_md5 = sharder_md5.get_shard(key)
        result_sha1 = sharder_sha1.get_shard(key)

        # 每个函数都应该返回有效节点
        assert result_md5 in nodes
        assert result_sha1 in nodes

        # 但结果可能相同也可能不同，不做断言


class TestModuloSharder:
    """取模分片器测试"""

    def test_get_shard(self):
        """测试取模分片"""
        sharder = ModuloSharder(shard_count=5)

        # 同一键应返回相同结果
        key = "test_key"
        result1 = sharder.get_shard(key)
        result2 = sharder.get_shard(key)

        assert result1 == result2
        assert 0 <= result1 < 5

    def test_range(self):
        """测试结果范围"""
        sharder = ModuloSharder(shard_count=10)

        for i in range(100):
            key = f"key_{i}"
            shard = sharder.get_shard(key)
            assert 0 <= shard < 10

    def test_distribution(self):
        """测试分布均匀性"""
        sharder = ModuloSharder(shard_count=10)

        dist = [0] * 10
        for i in range(1000):
            key = f"key_{i}"
            shard = sharder.get_shard(key)
            dist[shard] += 1

        # 每个分片应该有接近 100 个任务
        for count in dist:
            assert 80 < count < 120

    def test_shard_count(self):
        """测试分片数量获取"""
        sharder = ModuloSharder(shard_count=7)
        assert sharder.get_shard_count() == 7


class TestRangeSharder:
    """范围分片器测试"""

    def test_get_shard(self):
        """测试范围分片"""
        ranges = [("a", "m"), ("n", "z")]
        sharder = RangeSharder(ranges)

        assert sharder.get_shard("apple") == 0
        assert sharder.get_shard("banana") == 0
        assert sharder.get_shard("orange") == 1
        assert sharder.get_shard("zebra") == 1

    def test_out_of_range(self):
        """测试超出范围"""
        ranges = [("a", "m"), ("n", "z")]
        sharder = RangeSharder(ranges)

        # 不在任何范围内的键
        assert sharder.get_shard("1") == -1

    def test_empty_ranges(self):
        """测试空范围列表"""
        sharder = RangeSharder([])

        assert sharder.get_shard("test") == -1
        assert sharder.get_shard_count() == 0

    def test_shard_count(self):
        """测试分片数量"""
        ranges = [("a", "b"), ("c", "d"), ("e", "f")]
        sharder = RangeSharder(ranges)
        assert sharder.get_shard_count() == 3
