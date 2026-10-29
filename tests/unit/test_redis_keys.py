"""
@FileName: test_redis_keys.py
@Description: Redis 键统一根前缀（neotask:）测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

from neotask.common.constants import REDIS_KEY_PREFIX, redis_key
from neotask.lock.redis import DEFAULT_KEY_PREFIX, RedisLock
from neotask.queue.dead_letter import DeadLetterQueue
from neotask.storage.periodic import RedisPeriodicStore
from neotask.storage.redis import RedisQueueRepository, RedisTaskRepository

REDIS_URL = "redis://127.0.0.1:6379/9"


def test_redis_key_adds_root_prefix():
    """普通键名会补上根前缀"""
    assert redis_key("task:TSK001") == f"{REDIS_KEY_PREFIX}task:TSK001"
    assert redis_key("queue:priority") == f"{REDIS_KEY_PREFIX}queue:priority"


def test_redis_key_keeps_existing_root_prefix():
    """已带根前缀的键名不会重复叠加"""
    assert redis_key("neotask:task:TSK001") == "neotask:task:TSK001"


def test_task_repository_keys_use_root_prefix():
    """任务数据键与状态索引键都以 neotask: 开头"""
    repo = RedisTaskRepository(REDIS_URL)
    assert repo._task_key("TSK001") == "neotask:task:TSK001"
    assert repo._status_key("pending") == "neotask:status:pending"


def test_queue_repository_keys_use_root_prefix():
    """优先级队列与延迟队列键都以 neotask: 开头"""
    repo = RedisQueueRepository(REDIS_URL)
    assert repo._queue_key == "neotask:queue:priority"
    assert repo._delayed_key == "neotask:queue:delayed"


def test_lock_default_prefix_uses_root_prefix():
    """锁的默认前缀为 neotask:lock:，且 None 会被替换为默认值"""
    lock = RedisLock(REDIS_URL)
    assert lock._key_prefix == DEFAULT_KEY_PREFIX == "neotask:lock:"
    assert lock._get_key("task:TSK001") == "neotask:lock:task:TSK001"


def test_periodic_store_keys_use_root_prefix():
    """周期任务存储的键以根前缀开头"""
    store = RedisPeriodicStore(REDIS_URL)
    assert store._task_key("PRD_1") == "neotask:periodic_task:PRD_1"
    assert store._exec_key("PRD_1") == "neotask:periodic_exec:PRD_1"
    assert store._index_key == "neotask:periodic_tasks"


def test_dead_letter_keys_use_root_prefix():
    """死信队列的列表、索引、配置键都以根前缀开头"""
    assert DeadLetterQueue.DLQ_KEY == "neotask:dead_letter"
    assert DeadLetterQueue.DLQ_INDEX_KEY == "neotask:dead_letter:index"
    assert DeadLetterQueue.DLQ_CONFIG_KEY == "neotask:dead_letter:config"
