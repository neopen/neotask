---
kind: logging_system
name: 日志系统 — 基于 logging + YAML 配置的多输出彩色日志框架
category: logging_system
scope:
    - '**'
source_files:
    - src/neotask/common/logger.py
    - src/neotask/config/logging.yaml
    - src/neotask/utils/console_colors.py
    - src/neotask/utils/log_utils.py
---

## 1. 系统与架构概览
NeoTask 的日志子系统围绕 Python 标准库 `logging` 构建，通过自研的 `LoggingConfigManager` 加载 `src/neotask/config/logging.yaml` 配置文件，动态生成 `dictConfig` 字典并注册处理器（Console / RotatingFile），同时提供 `Logger` 包装类与模块级便捷函数（`debug/info/warning/error/critical/log_with_context/log_performance`）供业务代码使用。控制台输出在 TTY 环境下自动启用 ANSI 颜色（Windows 依赖 `colorama`），文件输出采用按天命名 + 大小轮转策略。

## 2. 核心组件与关键文件
- `src/neotask/common/logger.py`：日志配置管理器、自定义 `DailyRotatingFileHandler`、全局 `Logger` 包装类及便捷函数入口；导出模块级 `logger` 实例与 `debug/info/...` 快捷方法。
- `src/neotask/config/logging.yaml`：集中式日志配置，定义统一 formatter、console/file handler、各 logger 级别（root/uvicorn/app/llm）及 handler 分配。
- `src/neotask/utils/console_colors.py`：跨平台颜色支持（`colorama` 优先，否则回退 ANSI），提供 `LevelOnlyColoredFormatter` 仅对级别着色。
- `src/neotask/utils/log_utils.py`：异常堆栈打印工具 `_generate_dated_filename` 等辅助函数。
- 调用方示例：`api/task_pool.py`、`core/engine.py`、`worker/pool.py`、`queue/priority_queue.py` 等均通过 `from neotask.common.logger import debug/info/...` 使用统一日志接口。

## 3. 设计约定与行为规则
- **配置驱动**：所有 handler/formatter/level 均从 `logging.yaml` 读取，新增 logger 或 sink 只需修改该文件，无需改代码。
- **双输出默认**：根 logger 同时绑定 console 与 file 两个 handler；第三方库（urllib3/requests/httpx/asyncio 等）默认降级到 WARNING 以减少噪音。
- **文件轮转策略**：文件名形如 `logs/neotask_YYYY-MM-DD.log`，单文件最大 10MB，保留最近 5 份备份；`DailyRotatingFileHandler` 封装了日期切换逻辑。
- **彩色输出**：仅在 stdout 为 TTY 且满足平台条件时启用；Windows 未安装 `colorama` 会给出警告提示。
- **结构化上下文**：通过 `log_with_context(level, message, context)` 将键值对以 `key=value` 形式拼接到消息尾部；`log_function_call` / `log_performance` 提供常用场景封装。
- **禁用现有 logger**：初始化时 `disable_existing_loggers=False`，避免覆盖外部已配置的 logger。

## 4. 开发者应遵循的规则
1. 统一通过 `from neotask.common.logger import info/debug/warning/error/critical` 记录日志，不要直接 `import logging` 创建新 logger。
2. 需要带上下文的日志时使用 `log_with_context`，性能相关日志使用 `log_performance`。
3. 如需新增独立 logger 或调整级别，编辑 `src/neotask/config/logging.yaml` 中对应 `levels` 与 `handlers` 条目。
4. 不要在业务代码里硬编码日志格式或路径，全部由 YAML 配置管理。
5. 异常堆栈优先使用 `logger.exception(msg)` 而非自行拼接 traceback。