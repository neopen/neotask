"""
@FileName: dead_letter.py
@Description: 死信队列 - 处理最终失败的任务
@Author: HiPeng
@Time: 2026/5/11
"""

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any, Callable

from neotask.models.task import Task


class DeadLetterReason(Enum):
    """死信原因"""
    MAX_RETRIES = "max_retries"  # 超过最大重试次数
    NODE_CRASH = "node_crash"  # 节点崩溃
    TIMEOUT = "timeout"  # 执行超时
    CANCELLED = "cancelled"  # 用户取消
    ORPHANED = "orphaned"  # 孤儿任务无法恢复
    BUSINESS_ERROR = "business_error"  # 业务逻辑错误


@dataclass
class DeadLetterEntry:
    """死信条目"""
    task_id: str
    original_task: Dict[str, Any]
    reason: DeadLetterReason
    failure_count: int
    last_error: str
    created_at: float = field(default_factory=time.time)
    retry_history: List[Dict] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class DeadLetterQueue:
    """死信队列

    负责处理最终失败的任务：
    - 存储失败任务详情
    - 支持查询和重放
    - 支持自动清理

    使用示例：
        >>> dlq = DeadLetterQueue(redis_url)
        >>> await dlq.send(task, DeadLetterReason.MAX_RETRIES, "Task failed after 3 retries")
        >>>
        >>> # 查询死信
        >>> entries = await dlq.list(limit=10)
        >>>
        >>> # 重放死信
        >>> await dlq.replay(entry.task_id)
    """

    # Redis 键名
    DLQ_KEY = "neotask:dead_letter"  # 死信列表 (List)
    DLQ_INDEX_KEY = "neotask:dead_letter:index"  # 死信索引 (Hash)
    DLQ_CONFIG_KEY = "neotask:dead_letter:config"  # 配置

    def __init__(
            self,
            redis_url: str,
            max_size: int = 10000,
            ttl: int = 604800  # 7天
    ):
        self._redis_url = redis_url
        self._max_size = max_size
        self._ttl = ttl
        self._client = None
        self._alert_callback: Optional[Callable] = None

    async def _get_client(self):
        """获取 Redis 客户端"""
        if self._client is None:
            import redis.asyncio as redis
            from redis.asyncio import ConnectionPool
            pool = ConnectionPool.from_url(self._redis_url, decode_responses=True)
            self._client = redis.Redis(connection_pool=pool)
        return self._client

    async def send(
            self,
            task: Task,
            reason: DeadLetterReason,
            error: str,
            metadata: Optional[Dict] = None
    ) -> bool:
        """发送任务到死信队列

        Args:
            task: 任务对象
            reason: 死信原因
            error: 错误信息
            metadata: 附加元数据

        Returns:
            是否成功
        """
        client = await self._get_client()

        entry = DeadLetterEntry(
            task_id=task.task_id,
            original_task=task.to_dict(),
            reason=reason,
            failure_count=task.retry_count,
            last_error=error,
            metadata=metadata or {}
        )

        # 存储死信
        data = json.dumps({
            "task_id": entry.task_id,
            "original_task": entry.original_task,
            "reason": reason.value,
            "failure_count": entry.failure_count,
            "last_error": entry.last_error,
            "created_at": entry.created_at,
            "metadata": entry.metadata
        }, default=str)

        # 使用 LPUSH 保持最新在头部
        await client.lpush(self.DLQ_KEY, data)
        await client.ltrim(self.DLQ_KEY, 0, self._max_size - 1)

        # 建立索引
        await client.hset(self.DLQ_INDEX_KEY, task.task_id, entry.created_at)

        # 设置过期时间
        await client.expire(self.DLQ_KEY, self._ttl)

        # 触发告警回调
        if self._alert_callback:
            await self._alert_callback(entry)

        return True

    async def get(self, task_id: str) -> Optional[DeadLetterEntry]:
        """获取死信条目

        Args:
            task_id: 任务ID

        Returns:
            死信条目，不存在则返回 None
        """
        client = await self._get_client()

        # 检查是否存在
        exists = await client.hexists(self.DLQ_INDEX_KEY, task_id)
        if not exists:
            return None

        # 遍历查找（简化实现，生产环境需要优化）
        entries = await self.list(limit=1000)
        for entry in entries:
            if entry.task_id == task_id:
                return entry
        return None

    async def list(
            self,
            limit: int = 100,
            offset: int = 0,
            reason: Optional[DeadLetterReason] = None
    ) -> List[DeadLetterEntry]:
        """列出死信

        Args:
            limit: 返回数量
            offset: 偏移量
            reason: 按原因过滤

        Returns:
            死信列表
        """
        client = await self._get_client()

        # 获取所有死信
        data_list = await client.lrange(self.DLQ_KEY, offset, offset + limit - 1)

        entries = []
        for data in data_list:
            try:
                item = json.loads(data)
                entry = DeadLetterEntry(
                    task_id=item["task_id"],
                    original_task=item["original_task"],
                    reason=DeadLetterReason(item["reason"]),
                    failure_count=item["failure_count"],
                    last_error=item["last_error"],
                    created_at=item["created_at"],
                    metadata=item.get("metadata", {})
                )
                if reason is None or entry.reason == reason:
                    entries.append(entry)
            except Exception:
                continue

        return entries

    async def replay(self, task_id: str) -> bool:
        """重放死信任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功重放
        """
        client = await self._get_client()

        # 获取死信
        entry = await self.get(task_id)
        if not entry:
            return False

        # 从死信队列移除
        await self.remove(task_id)

        # 重新创建任务（需要外部调度器）
        # 返回 task_id 供外部重新提交
        return True

    async def remove(self, task_id: str) -> bool:
        """从死信队列移除

        Args:
            task_id: 任务ID

        Returns:
            是否成功移除
        """
        client = await self._get_client()

        # 删除索引
        await client.hdel(self.DLQ_INDEX_KEY, task_id)

        # 从列表中删除（需要遍历，简化实现）
        data_list = await client.lrange(self.DLQ_KEY, 0, -1)
        new_list = []
        removed = False

        for data in data_list:
            try:
                item = json.loads(data)
                if item["task_id"] != task_id:
                    new_list.append(data)
                else:
                    removed = True
            except Exception:
                new_list.append(data)

        if removed:
            # 重建列表
            await client.delete(self.DLQ_KEY)
            if new_list:
                await client.rpush(self.DLQ_KEY, *new_list)

        return removed

    async def count(self) -> int:
        """获取死信数量"""
        client = await self._get_client()
        return await client.llen(self.DLQ_KEY)

    async def clear(self) -> int:
        """清空死信队列"""
        client = await self._get_client()
        count = await self.count()
        await client.delete(self.DLQ_KEY)
        await client.delete(self.DLQ_INDEX_KEY)
        return count

    def set_alert_callback(self, callback: Callable) -> None:
        """设置告警回调

        Args:
            callback: 告警回调函数，签名为 async def callback(entry: DeadLetterEntry)
        """
        self._alert_callback = callback

    async def close(self) -> None:
        """关闭连接"""
        if self._client:
            await self._client.close()
            self._client = None

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "total_count": 0,  # 需要异步获取
            "max_size": self._max_size,
            "ttl_seconds": self._ttl,
            "ttl_days": self._ttl / 86400
        }
