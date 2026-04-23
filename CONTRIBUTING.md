# 贡献指南

感谢你考虑为 NeoTask 做出贡献！本指南将帮助你了解如何参与项目开发。

## 目录

- [行为准则](#行为准则)
- [如何贡献](#如何贡献)
- [开发环境设置](#开发环境设置)
- [代码规范](#代码规范)
- [提交流程](#提交流程)
- [测试要求](#测试要求)
- [文档编写](#文档编写)
- [Issue 提交](#issue-提交)
- [Pull Request 流程](#pull-request-流程)
- [发布流程](#发布流程)

---

## 行为准则

本项目采用 [Contributor Covenant](https://www.contributor-covenant.org/) 行为准则。我们期望所有参与者尊重他人、保持友善、专注于技术讨论。

---

## 如何贡献

你可以通过以下方式贡献：

1. **报告 Bug** - 提交详细的 Issue
2. **功能建议** - 提出新功能想法
3. **代码贡献** - 修复 Bug 或实现新功能
4. **文档改进** - 完善文档和示例
5. **代码审查** - 审查他人的 Pull Request
6. **社区帮助** - 回答其他用户的问题

---

## 开发环境设置

### 前置要求

- Python 3.9+
- Git
- pip 或 poetry

### 快速开始

```bash
# 1. Fork 并克隆仓库
git clone https://github.com/neopen/neotask.git
cd neotask

# 2. 添加上游远程
git remote add upstream https://github.com/neopen/neotask.git

# 3. 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate     # Windows

# 4. 安装开发依赖
pip install -e ".[dev]"

# 5. 验证安装
pytest tests/ -v
```



### 项目结构

```text
neotask/
├── __init__.py              # 包入口，导出公共 API
├── api/                     # 用户 API 层
│   ├── task_pool.py         # TaskPool - 即时任务入口
│   └── task_scheduler.py    # TaskScheduler - 定时任务入口
├── core/                    # 核心组件
│   ├── lifecycle.py         # 任务生命周期管理
│   ├── dispatcher.py        # 任务分发器
│   ├── future.py            # Future 异步等待
│   └── context.py           # 任务上下文
├── queue/                   # 队列模块
│   ├── base.py              # 队列抽象基类
│   ├── priority_queue.py    # 优先级队列
│   ├── delayed_queue.py     # 延迟队列
│   └── scheduler.py         # 队列调度器
├── storage/                 # 存储模块
│   ├── base.py              # 存储抽象基类
│   ├── memory.py            # 内存存储
│   ├── sqlite.py            # SQLite 存储
│   ├── redis.py             # Redis 存储
│   └── factory.py           # 存储工厂
├── executor/                # 执行器模块
│   ├── base.py              # 执行器基类
│   ├── async_executor.py    # 异步执行器
│   ├── thread_executor.py   # 线程执行器
│   ├── process_executor.py  # 进程执行器
│   └── factory.py           # 执行器工厂
├── lock/                    # 分布式锁模块
│   ├── base.py              # 锁基类
│   ├── memory.py            # 内存锁
│   ├── redis.py             # Redis 锁
│   ├── watchdog.py          # 看门狗续期
│   └── factory.py           # 锁工厂
├── worker/                  # Worker 模块
│   ├── pool.py              # Worker 池
│   ├── supervisor.py        # Worker 监督者
│   ├── prefetcher.py        # 任务预取器
│   └── reclaimer.py         # 任务回收器
├── scheduler/               # 调度器模块 (v0.3)
│   ├── cron_parser.py       # Cron 表达式解析器
│   ├── time_wheel.py        # 时间轮
│   └── periodic.py          # 周期任务管理器
├── event/                   # 事件模块
│   └── bus.py               # 事件总线
├── monitor/                 # 监控模块
│   ├── metrics.py           # 指标收集器
│   ├── health.py            # 健康检查
│   └── reporter.py          # 指标上报器
├── models/                  # 数据模型
│   ├── task.py              # 任务模型
│   ├── config.py            # 配置模型
│   └── schedule.py          # 调度模型 (v0.3)
├── common/                  # 公共模块
│   └── exceptions.py        # 异常定义
└── web/                     # Web UI (计划中)
    └── ...
```



------

## 代码规范

### Python 风格指南

我们遵循 [PEP 8](https://peps.python.org/pep-0008/) 和以下约定：

```python
# 推荐
def process_task(task_id: str, data: dict) -> dict:
    """处理任务的简短描述。
    
    详细描述（如需要）。
    
    Args:
        task_id: 任务ID
        data: 任务数据
        
    Returns:
        处理结果字典
        
    Raises:
        TaskNotFoundError: 任务不存在时
    """
    result = {"status": "ok"}
    return result

# 避免
def ProcessTask(taskId,data):
    result={"status":"ok"}
    return result
```



### 代码格式化工具

项目使用以下工具保持一致的代码风格：

- **Black** - 代码格式化
- **isort** - 导入排序
- **Ruff** - 快速 linting
- **mypy** - 类型检查

```bash
# 格式化代码
black src/ tests/
isort src/ tests/

# 运行 linting
ruff check src/ tests/

# 类型检查
mypy src/neotask/
```



### 命名规范

- **模块名**: 小写 + 下划线（`task_pool.py`）
- **类名**: PascalCase（`TaskPool`, `WorkerPool`）
- **函数/变量**: snake_case（`submit_task`, `worker_count`）
- **常量**: 大写 + 下划线（`MAX_RETRIES`, `DEFAULT_TIMEOUT`）
- **私有成员**: 前导下划线（`_internal_method`）

### 类型注解

所有公共 API 必须包含类型注解：

```python
from typing import Optional, Dict, Any

class TaskPool:
    def submit(
        self,
        data: Dict[str, Any],
        priority: int = 2,
        delay: float = 0.0
    ) -> str:
        """提交任务到队列。
        
        Args:
            data: 任务数据
            priority: 优先级 (0=CRITICAL, 1=HIGH, 2=NORMAL, 3=LOW)
            delay: 延迟执行时间（秒）
            
        Returns:
            任务ID
        """
        ...
```



------

## 提交流程

### Git 工作流

我们使用 [GitHub Flow](https://guides.github.com/introduction/flow/)：

```bash
# 1. 从上游同步最新代码
git checkout main
git pull upstream main

# 2. 创建特性分支
git checkout -b feature/test
# 或
git checkout -b fix/bug-description
# 或
git checkout -b docs/update-readme

# 3. 进行开发并提交
git add .
git commit -m "feat: add new feature"

# 4. 推送到你的 fork
git push origin feature/test

# 5. 在 GitHub 上创建 Pull Request
```



### Commit 消息规范

遵循 [Conventional Commits](https://www.conventionalcommits.org/)：

**Type 类型**:

- `feat`: 新功能
- `fix`: Bug 修复
- `docs`: 文档变更
- `style`: 代码格式（不影响功能）
- `refactor`: 重构
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建过程或辅助工具变动

**示例**:

```bash
# 好的 commit 消息
git commit -m "feat(scheduler): add cron expression support"
git commit -m "fix(queue): resolve race condition in priority queue"
git commit -m "docs(readme): update installation instructions"

# 避免
git commit -m "update code"
git commit -m "fix stuff"
```



**Scope 范围**（可选）:

- `scheduler` - 定时调度模块
- `queue` - 队列模块
- `storage` - 存储模块
- `executor` - 执行器模块
- `lock` - 锁模块
- `worker` - Worker 模块
- `monitor` - 监控模块
- `api` - 公共 API
- `docs` - 文档

------

## 测试要求

### 测试覆盖率

- **新功能**: 必须包含单元测试，覆盖率 ≥ 80%
- **Bug 修复**: 必须包含回归测试
- **核心模块**: 总体覆盖率保持在 85% 以上

### 运行测试

```bash
# 运行所有测试
pytest tests/ -v

# 运行特定模块测试
pytest tests/unit/test_task_pool.py -v
pytest tests/integration/test_e2e.py -v

# 查看覆盖率
pytest --cov=src/neotask tests/

# 生成 HTML 覆盖率报告
pytest --cov=src/neotask --cov-report=html tests/
# 打开 htmlcov/index.html 查看
```



### 编写测试

```python
import pytest
from neotask import TaskPool

@pytest.mark.asyncio
async def test_submit_task():
    """测试任务提交功能。"""
    async def dummy_executor(data):
        return {"result": data}
    
    pool = TaskPool(executor=dummy_executor)
    await pool.start()
    
    try:
        task_id = pool.submit({"test": "data"})
        assert task_id is not None
        
        result = await pool.wait_for_result(task_id, timeout=5.0)
        assert result == {"result": {"test": "data"}}
    finally:
        await pool.shutdown()
```



### 测试分类

使用 pytest markers 标记测试类型：

```python
@pytest.mark.unit
def test_simple_function():
    """单元测试"""
    
@pytest.mark.integration
@pytest.mark.slow
async def test_full_workflow():
    """集成测试（慢）"""
```



### 测试目录结构

```text
tests/
├── __init__.py
├── conftest.py                 # 共享 fixtures
├── unit/                       # 单元测试
│   ├── test_cron_parser.py
│   ├── test_delayed_queue.py
│   ├── test_time_wheel.py
│   ├── test_periodic_manager.py
│   ├── test_task_pool.py
│   └── test_storage.py
├── integration/                # 集成测试
│   ├── test_task_scheduler.py
│   ├── test_workflow.py
│   └── test_distributed.py
└── examples/                   # 示例测试
    └── test_examples.py
```



------

## 文档编写

### 文档位置

- **README.md**: 项目概述和快速开始
- **CHANGELOG.md**: 版本变更记录
- **CONTRIBUTING.md**: 贡献指南
- **SECURITY.md**: 安全策略
- **docs/**: 详细文档（API 参考、配置说明等）
- **examples/**: 可运行的示例代码
- **docstrings**: 代码内文档

### 文档规范

- 使用清晰的中文或英文
- 包含代码示例
- 提供实际应用场景
- 保持更新

### 示例代码要求

每个示例应该：

1. 可以独立运行
2. 包含注释说明
3. 展示特定功能
4. 遵循最佳实践

```python
"""功能演示：XXX

这个示例展示了如何使用 XXX 功能。
"""

from neotask import TaskPool

async def main():
    # 1. 创建任务池
    pool = TaskPool(executor=my_executor)
    
    # 2. 提交任务
    task_id = pool.submit({"key": "value"})
    
    # 3. 获取结果
    result = pool.wait_for_result(task_id)
    print(f"Result: {result}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```



------

## Issue 提交

### Bug 报告

提交 Bug 时请包含：

1. **标题**: 简洁描述问题
2. **环境信息**:
   - Python 版本
   - 操作系统
   - NeoTask 版本
3. **复现步骤**: 详细的操作步骤
4. **预期行为**: 应该发生什么
5. **实际行为**: 实际发生了什么
6. **错误日志**: 完整的 traceback
7. **最小复现代码**: 如果可以，提供能复现问题的代码

**模板**:

~~~markdown
## Bug 描述
简要描述问题

## 环境
- Python: 3.11
- OS: Ubuntu 22.04
- NeoTask: 0.3.0

## 复现步骤
1. 
2. 
3. 

## 预期行为
...

## 实际行为
...

## 错误日志
traceback here

## 复现代码
# minimal code to reproduce

### 功能请求

1. **标题**: 清晰描述功能
2. **背景**: 为什么需要这个功能
3. **提案**: 你希望如何实现
4. **替代方案**: 是否考虑过其他方案
5. **附加信息**: 相关链接、参考资料

---

## Pull Request 流程

### 提交前检查清单

- [ ] 代码遵循项目规范
- [ ] 添加了必要的测试
- [ ] 所有测试通过 (`pytest tests/`)
- [ ] 代码格式化完成 (`black`, `isort`, `ruff`)
- [ ] 类型检查通过 (`mypy`)
- [ ] 更新了相关文档
- [ ] Commit 消息符合规范
- [ ] 分支是最新的（已 rebase 或 merge main）

### PR 描述模板

```markdown
## 描述
简要描述这个 PR 做了什么

## 相关 Issue
Fixes #123

## 变更类型
[ ] Bug 修复
[ ] 新功能
[ ] 文档更新
[ ] 重构
[ ] 性能优化

## 测试
[ ] 添加了单元测试
[ ] 所有测试通过
[ ] 手动测试完成

## 截图（如适用）
添加 UI 变更的截图

## 检查清单
[ ] 代码遵循项目规范
[ ] 已自我审查代码
[ ] 已添加注释
[ ] 文档已更新
~~~



### 审查流程

1. 至少需要 1 个维护者批准
2. CI 测试必须全部通过
3. 解决所有评论和建议
4. 保持 PR 聚焦单一功能

------

## 发布流程

### 版本管理

我们使用 [语义化版本](https://semver.org/)：

- **MAJOR**: 不兼容的 API 变更
- **MINOR**: 向下兼容的新功能
- **PATCH**: 向下兼容的 Bug 修复

### 发布步骤（仅维护者）

```bash
# 1. 更新版本号
# 在 pyproject.toml 和 src/neotask/__init__.py 中

# 2. 更新 CHANGELOG.md
# 记录所有变更

# 3. 提交变更
git commit -am "chore: bump version to 0.4.0"

# 4. 创建标签
git tag -a v0.4.0 -m "Release v0.4.0"

# 5. 推送
git push origin main
git push origin v0.4.0

# 6. 构建分发包
python -m build

# 7. 发布到 PyPI
twine upload dist/*
```



------

## 常见问题

### Q: 我的 PR 多久会被审查？

A: 通常在 2-3 个工作日内，复杂的可能需要更长时间。

### Q: 我可以同时提交多个功能的 PR 吗？

A: 不建议。每个 PR 应该聚焦单一功能，便于审查和测试。

### Q: 如何申请成为维护者？

A: 持续贡献高质量的代码和审查，活跃参与社区讨论。

### Q: 代码审查主要关注什么？

A: 代码质量、测试覆盖、性能影响、安全性、可维护性。

### Q: 如何为新功能编写测试？

A: 单元测试放在 `tests/unit/`，集成测试放在 `tests/integration/`，使用 pytest markers 标记测试类型。

### Q: 文档需要同时提供中英文吗？

A: 优先使用中文，英文版本后续逐步完善。

------

## 联系方式

- **GitHub Issues**: https://github.com/neopen/neotask/issues
- **Email**: helpengx@gmail.com
- **项目主页**: https://github.com/neopen/neotask

------

## 致谢

感谢所有为 NeoTask 做出贡献的开发者！你的每一份贡献都让这个项目变得更好。

特别感谢：

- 所有提交代码的贡献者
- 报告 Bug 的用户
- 提出改进建议的社区成员
- 编写和完善文档的志愿者

------

**再次感谢你的贡献！** 🎉
