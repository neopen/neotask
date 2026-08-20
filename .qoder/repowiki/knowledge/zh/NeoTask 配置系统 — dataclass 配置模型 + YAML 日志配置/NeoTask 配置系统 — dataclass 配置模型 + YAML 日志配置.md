---
kind: configuration_system
name: NeoTask 配置系统 — dataclass 配置模型 + YAML 日志配置
category: configuration_system
scope:
    - '**'
source_files:
    - src/neotask/models/config.py
    - src/neotask/config/logging.yaml
    - src/neotask/common/logger.py
    - pyproject.toml
---

## 1. 系统概览

NeoTask 没有引入外部配置框架（如 pydantic-settings、dynaconf、python-decouple），而是采用**纯 Python dataclass 配置模型 + YAML 日志配置文件**的轻量组合方案：

- **运行时配置**：通过 `src/neotask/models/config.py` 中一组强类型的 `@dataclass` 配置对象，由调用方在代码中直接构造并传入各子系统。
- **日志配置**：通过 `src/neotask/config/logging.yaml` 声明式定义，由 `common/logger.py` 中的 `LoggingConfigManager` 加载并转换为标准 `logging.dictConfig` 格式。
- **构建/工具链配置**：集中在根级 `pyproject.toml`（PEP 621），涵盖依赖、可选功能集、pytest、ruff、mypy、bumpversion、twine 等。
- **环境变量**：仅测试与集成场景使用 `os.environ.get(...)` 注入 Redis URL 等，应用核心不读取 `.env`。

## 2. 关键文件与包

| 路径 | 作用 |
|---|---|
| `src/neotask/models/config.py` | 所有运行时配置 dataclass（Storage/Lock/Worker/Queue/WebUI/Executor/Distributed/TaskPool/Scheduler/Task） |
| `src/neotask/config/settings.py` | 占位文件，当前为空，未承载全局设置入口 |
| `src/neotask/config/logging.yaml` | 日志级别、formatter、handlers、logger 映射 |
| `src/neotask/common/logger.py` | `LoggingConfigManager` + `Logger`，从 YAML 加载并初始化 logging |
| `pyproject.toml` | 项目元数据、依赖、可选 extras、工具链全部配置 |
| `tests/unit/test_redis_storage.py` 等 | 通过 `os.environ.get("REDIS_URL")` 注入测试环境 |

## 3. 架构与设计约定

### 3.1 运行时配置模型（dataclass）

- 每个子系统一个独立 dataclass，字段即配置项，提供默认值保证“零配置可用”。
- 大量使用 `@classmethod` 工厂方法表达常见组合：
  - `StorageConfig.memory()/redis()/sqlite()`
  - `LockConfig.redis(url, timeout)`
  - `WebUIConfig.enable(port, auto_open)`
  - `SchedulerConfig.high_performance()/lightweight()`
  - `TaskPoolConfig.redis(url, node_id, enable_prefetch, prefetch_size)`
- `TaskConfig` 作为统一入口，聚合 storage/lock/worker/queue/executor/webui 子配置，并提供 `memory()/redis()/sqlite()/with_webui()` 快捷构造。
- `__post_init__` 用于生成 `node_id`（基于 `socket.gethostname()`）。
- 模块顶部暴露 `DEFAULT_TASK_POOL_CONFIG` / `DEFAULT_SCHEDULER_CONFIG` / `DEFAULT_TASK_CONFIG` 常量供快速启动。

### 3.2 日志配置（YAML → dictConfig）

- `LoggingConfigManager` 以包内 `config/logging.yaml` 为唯一来源，支持缓存与键路径访问（`get("levels","app")`）。
- `to_logging_dict()` 将扁平 YAML 结构转换为标准 `logging.config.dictConfig` 字典，再被 `Logger` 实例化时应用。
- 内置 `DailyRotatingFileHandler` 与彩色控制台输出，Windows 下自动降级。
- 默认禁用 urllib3/requests/httpx 等第三方库噪音日志。

### 3.3 构建与工具链（pyproject.toml）

- PEP 621 元数据 + setuptools 后端；可选 extras：`ui`、`redis`、`sqlite`、`monitor`、`full`、`dev`、`docs`。
- CLI 入口 `neotask = "neotask.cli:main"`（CLI main 目前为空壳）。
- 代码质量：black + isort + ruff + mypy，pre-commit hooks 在提交前运行。
- 版本管理：bumpversion 同步更新 `src/neotask/__init__.py` 与 `pyproject.toml`。
- 发布：twine 指向 PyPI legacy 端点。

### 3.4 环境变量使用现状

- 应用核心**不**读取 `.env` 或 `os.environ`；所有运行时参数通过 dataclass 对象显式传递。
- 测试层广泛使用 `os.environ.get("REDIS_URL"|"REDIS_TEST_URL"|"TEST_REDIS_URL")` 注入 Redis 地址，属于测试隔离策略。

## 4. 开发者应遵循的规则

1. **新增配置项**：在 `models/config.py` 对应 dataclass 中添加字段，给出合理默认值，必要时补充 `@classmethod` 工厂方法。
2. **不要引入外部配置框架**：保持 dataclass 纯 Python 风格，避免增加运行时依赖。
3. **日志配置修改**：只编辑 `config/logging.yaml`，通过 `LoggingConfigManager` 生效；不要在代码里硬编码 formatter/handler。
4. **环境变量仅限测试**：生产/示例代码不应直接读 `os.environ`；如需可覆盖行为，优先通过 dataclass 字段传入。
5. **CLI 扩展**：若未来需要命令行参数驱动配置，应在 `cli/main.py` 解析后组装 dataclass 对象，而非直接读写文件。
6. **构建/工具链变更**：统一在 `pyproject.toml` 中维护，勿散落至 `setup.cfg` 或单独脚本。
