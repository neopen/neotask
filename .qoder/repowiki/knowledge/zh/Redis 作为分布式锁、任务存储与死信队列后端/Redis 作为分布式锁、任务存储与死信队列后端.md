---
kind: external_dependency
name: Redis 作为分布式锁、任务存储与死信队列后端
slug: redis
category: external_dependency
category_hints:
    - vendor_identity
    - client_constraint
scope:
    - '**'
source_files:
    - src/neotask/storage/redis.py
    - src/neotask/lock/redis.py
    - src/neotask/queue/dead_letter.py
---

### Redis
- 角色：本项目唯一的外部依赖，同时承担三块职责——持久化任务与状态索引（`RedisTaskRepository`）、优先级/延迟队列（`RedisQueueRepository`）、分布式锁（`RedisLock`）以及死信队列（`DeadLetterQueue`）。
- 集成点：
  - `neotask.storage.redis.RedisTaskRepository`：以 `task:{id}` 存 JSON，用 `status:{state}` set 做状态索引；批量更新走 pipeline。
  - `neotask.storage.redis.RedisQueueRepository`：优先级队列用 ZSET `queue:priority`，延迟队列用 ZSET `queue:delayed`；出队通过 Lua 脚本原子弹出，避免竞争。
  - `neotask.lock.redis.RedisLock`：基于 `SET NX EX` + owner 校验的 Lua 脚本实现互斥，支持 `extend` 续期与 `scan_locks` 扫描清理。
  - `neotask.queue.dead_letter.DeadLetterQueue`：使用 List `neotask:dead_letter` + Hash 索引 `neotask:dead_letter:index`，默认 TTL 7 天。
- 注意：`RedisLock.extend` 当前仅比对本地 `_owner`，在多实例并发持有同一 key 的场景下存在竞态风险，需确认官方文档中 `GET+EXPIRE` 是否应改为单条 Lua。