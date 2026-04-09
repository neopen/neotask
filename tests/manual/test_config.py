from neotask.api.task_scheduler import SchedulerConfig

# 测试 SchedulerConfig 类
print("Testing SchedulerConfig...")

# 测试默认构造函数
config1 = SchedulerConfig()
print(f"Default config: {config1}")
print(f"storage_type: {config1.storage_type}")
print(f"sqlite_path: {config1.sqlite_path}")
print(f"redis_url: {config1.redis_url}")
print(f"worker_concurrency: {config1.worker_concurrency}")
print(f"max_retries: {config1.max_retries}")
print(f"retry_delay: {config1.retry_delay}")
print(f"enable_persistence: {config1.enable_persistence}")

# 测试 memory() 类方法
config2 = SchedulerConfig.memory()
print(f"\nMemory config: {config2}")
print(f"storage_type: {config2.storage_type}")

# 测试 sqlite() 类方法
config3 = SchedulerConfig.sqlite("test.db")
print(f"\nSQLite config: {config3}")
print(f"storage_type: {config3.storage_type}")
print(f"sqlite_path: {config3.sqlite_path}")

# 测试 redis() 类方法
config4 = SchedulerConfig.redis("redis://localhost:6379")
print(f"\nRedis config: {config4}")
print(f"storage_type: {config4.storage_type}")
print(f"redis_url: {config4.redis_url}")

print("\nAll tests passed!")