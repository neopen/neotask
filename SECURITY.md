# 安全策略

## 目录

- [支持版本](#支持版本)
- [报告漏洞](#报告漏洞)
- [安全响应流程](#安全响应流程)
- [已知安全问题](#已知安全问题)
- [安全最佳实践](#安全最佳实践)
- [安全更新政策](#安全更新政策)

---

## 支持版本

我们仅为最新版本提供安全更新：

| 版本   | 支持状态     | 安全更新截止    |
| ------ | ------------ | --------------- |
| 0.3.x  | 积极维护  | 持续更新        |
| 0.2.x  | 有限支持  | 下一个次要版本  |
| 0.1.x  | 停止支持  | 2026-04-15      |

**建议**: 始终使用最新稳定版本以获得最新的安全修复。

```bash
# 检查当前版本
pip show neotask

# 升级到最新版本
pip install --upgrade neotask
```

---

## 报告漏洞

### 负责任披露

如果你发现安全漏洞，请**不要**通过公开 Issue 报告。我们重视负责任的披露，以保护用户免受潜在攻击。

### 报告方式

请通过以下方式私下报告：

1. **电子邮件**（首选）: helpengx@gmail.com
2. **GitHub Security Advisory**: https://github.com/neopen/neotask/security/advisories/new

### 报告内容

请尽可能提供详细信息：

- **漏洞类型**: （如：注入、XSS、权限绕过等）
- **受影响版本**: 具体的版本号
- **复现步骤**: 详细的操作步骤
- **潜在影响**: 漏洞可能造成的危害
- **概念验证**: 如果可能，提供 PoC 代码
- **修复建议**: 如果你有修复思路，欢迎分享

### 响应时间承诺

- **初步响应**: 48 小时内确认收到
- **评估完成**: 7 天内完成漏洞评估
- **修复发布**: 根据严重程度，14-30 天内发布修复

### 漏洞严重程度分级

| 等级 | 描述                               | 响应时间 |
| :--- | :--------------------------------- | :------- |
| 严重 | 远程代码执行、未授权访问、数据泄露 | 7 天内   |
| 高   | 权限提升、敏感信息泄露、拒绝服务   | 14 天内  |
| 中   | 信息泄露、安全功能绕过             | 21 天内  |
| 低   | 需要特定条件才能利用的漏洞         | 30 天内  |



---

## 安全响应流程

### 1. 接收报告

- 确认收到漏洞报告
- 分配内部跟踪编号
- 指定安全团队成员负责

### 2. 评估

- 验证漏洞的真实性
- 评估影响范围和严重程度
- 确定受影响的版本和用户

### 3. 修复开发

- 开发安全补丁
- 内部测试和代码审查
- 准备更新说明（暂不公开细节）

### 4. 发布

- 发布新版本包含安全修复
- 更新安全公告
- 通知已知用户（如适用）

### 5. 披露

- 公开发布安全公告
- 更新 CHANGELOG.md
- 给予报告者致谢（经同意）

---

## 已知安全问题

### 当前无公开的安全漏洞

如有发现，将在此处列出：

| CVE 编号 | 严重程度 | 描述 | 受影响版本 | 修复版本 | 状态 |
| -------- | -------- | ---- | ---------- | -------- | ---- |
| 暂无     | -        | -    | -          | -        | -    |

---

## 安全最佳实践

### 部署建议

#### 1. 使用最新稳定版本
```bash
# 始终安装最新版本
pip install --upgrade neotask

# 或指定版本范围
pip install "neotask>=0.3.0,<0.4.0"
```

#### 2. 生产环境配置
```python 
from neotask import TaskPool, TaskPoolConfig

# 使用持久化存储而非内存存储
config = TaskPoolConfig(
    storage_type="sqlite",      # 或 "redis"
    sqlite_path="/var/lib/neotask/tasks.db",
    max_retries=3,
    worker_concurrency=10,
    task_timeout=300,           # 5分钟超时
    queue_max_size=10000        # 限制队列大小
)

pool = TaskPool(executor=my_executor, config=config)
```
#### 2. 生产环境配置
```python 
from neotask import TaskPool, TaskPoolConfig
使用持久化存储而非内存存储
config = TaskPoolConfig( storage_type="sqlite", # 或 "redis" max_retries=3, worker_concurrency=10, )
pool = TaskPool(executor=my_executor, config=config)
```

#### 3. 输入验证
```python
from neotask import TaskPool, TaskPoolConfig

# 使用持久化存储而非内存存储
config = TaskPoolConfig(
    storage_type="sqlite",      # 或 "redis"
    sqlite_path="/var/lib/neotask/tasks.db",
    max_retries=3,
    worker_concurrency=10,
    task_timeout=300,           # 5分钟超时
    queue_max_size=10000        # 限制队列大小
)

pool = TaskPool(executor=my_executor, config=config)
```

#### 4. 资源限制
```python
config = TaskPoolConfig(
    task_timeout=300,           # 5分钟超时
    queue_max_size=10000,       # 限制队列大小
    worker_concurrency=10,      # 限制并发数
    max_retries=3,              # 限制重试次数
)
```
#### 5. 错误处理
```python
async def robust_executor(data):
    try:
        result = perform_sensitive_operation(data)
        return sanitize_output(result)  # 清理输出
    except Exception as e:
        # 记录详细日志（内部）
        logger.error(f"Task failed: {e}", exc_info=True)
        # 返回通用错误消息（外部）
        raise TaskError("Task execution failed") from e
```
#### 6. Cron 表达式安全

```python
from neotask import TaskScheduler

# 限制 Cron 任务频率，避免过于频繁执行
config = SchedulerConfig(
    scan_interval=1.0,
    default_max_runs=1000       # 限制最大执行次数
)

scheduler = TaskScheduler(executor=executor, config=config)

# 避免过于频繁的 Cron 表达式
# 错误：每秒执行
scheduler.submit_cron(data, "* * * * * *")  # 6字段无效

# 正确：每分钟执行
scheduler.submit_cron(data, "* * * * *")
```



### Redis 安全配置

如果使用 Redis 存储，请注意以下安全配置：

#### 1. 使用密码认证

```python
# 配置文件中设置密码
# redis.conf: requirepass your_strong_password

config = TaskPoolConfig(
    storage_type="redis",
    redis_url="redis://:your_strong_password@localhost:6379/0"
)
```



#### 2. 使用 TLS/SSL 加密

```python
config = TaskPoolConfig(
    storage_type="redis",
    redis_url="rediss://:password@localhost:6379/0?ssl_cert_reqs=required"
)
```



#### 3. 网络隔离

```bash
# 绑定到本地或内网地址
# redis.conf: bind 127.0.0.1 192.168.1.100

# 使用防火墙限制访问
iptables -A INPUT -p tcp --dport 6379 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 6379 -j DROP
```



#### 4. 连接池限制

```python
config = TaskPoolConfig(
    storage_type="redis",
    redis_url="redis://localhost:6379",
    # 限制连接池大小
    # 在 RedisTaskRepository 中配置 max_connections
)
```



### SQLite 安全配置

```python
# 使用文件权限保护数据库文件
import os

db_path = "/var/lib/neotask/tasks.db"
os.chmod(db_path, 0o600)  # 仅所有者可读写

config = TaskPoolConfig(
    storage_type="sqlite",
    sqlite_path=db_path
)
```



### 日志安全

```python
import logging

# 避免记录敏感信息
class SensitiveDataFilter(logging.Filter):
    def filter(self, record):
        # 过滤密码、令牌等敏感信息
        sensitive_patterns = ['password', 'token', 'secret', 'key']
        for pattern in sensitive_patterns:
            if pattern in record.getMessage().lower():
                return False
        return True

# 配置日志过滤器
logger = logging.getLogger("neotask")
logger.addFilter(SensitiveDataFilter())
```



------

## 安全更新政策

### 更新频率

- **严重漏洞**: 7 天内发布修复
- **高危漏洞**: 14 天内发布修复
- **中低危漏洞**: 随下一个版本发布

### 安全公告

安全公告将通过以下渠道发布：

1. **GitHub Security Advisories**
2. **CHANGELOG.md** 中的安全相关条目
3. **项目邮件列表**

### 订阅安全通知

- **Watch GitHub 仓库**: 点击仓库右上角的 "Watch" → "Custom" → "Security alerts"
- **关注 Releases**: 获取新版本通知

------

## 联系方式

- **安全报告**: helpengx@gmail.com
- **PGP 密钥**: 可联系获取
- **GitHub**: https://github.com/neopen/neotask/security

------

## 致谢

感谢以下安全研究人员和白帽子的贡献：

- 待补充

------

**最后更新**: 2026-04-22

