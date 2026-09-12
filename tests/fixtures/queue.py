"""
@FileName: queue.py
@Description: 队列相关 fixtures
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/29
"""

import pytest

from neotask.queue.delayed_queue import DelayedQueue
from neotask.queue.priority_queue import PriorityQueue
from neotask.queue.queue_scheduler import QueueScheduler


@pytest.fixture
def priority_queue():
    """优先级队列"""
    return PriorityQueue()


@pytest.fixture
def priority_queue_with_repo(memory_queue_repo):
    """带存储的优先级队列"""
    return PriorityQueue(memory_queue_repo)


@pytest.fixture
def delayed_queue():
    """延迟队列"""
    return DelayedQueue()


@pytest.fixture
def queue_scheduler(memory_queue_repo):
    """队列调度器"""
    return QueueScheduler(memory_queue_repo, max_size=1000)