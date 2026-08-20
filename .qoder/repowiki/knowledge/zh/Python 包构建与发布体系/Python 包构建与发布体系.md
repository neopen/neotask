---
kind: build_system
name: Python 包构建与发布体系
category: build_system
scope:
    - '**'
source_files:
    - pyproject.toml
    - .github/workflows/publish-pypi.yml
    - .pre-commit-config.yaml
    - MANIFEST.in
    - src/neotask/__init__.py
---

## 构建系统概览

NeoTask 采用现代 Python 项目标准，基于 pyproject.toml + setuptools 的 PEP 517/518 构建后端，配合 GitHub Actions 实现从源码到 PyPI 的自动化发布。

### 核心构建工具链
- 构建后端：setuptools.build_meta（要求 setuptools>=61.0）
- 打包工具：python -m build（PEP 517 兼容）
- 分发格式：wheel + sdist（由 MANIFEST.in 控制源分发包内容）
- 依赖管理：pyproject.toml 中 [project] 声明主依赖，[project.optional-dependencies] 定义可选功能集（ui、redis、sqlite、monitor、full、dev、docs）

### 版本管理与发布流程
- 版本号来源：双处同步维护
  - src/neotask/__init__.py 中的 __version__ = "1.0.0"
  - pyproject.toml 中的 version = "1.0.0"
- 版本升级：通过 bumpversion 工具（配置在 [tool.bumpversion]），自动更新上述两处并生成 git tag（格式 v{new_version}）
- PyPI 发布：GitHub Actions 触发条件为 release published，使用 pypa/gh-action-pypi-publish 进行可信发布（trusted publishing，需要 id-token: write 权限）

### 代码质量与预提交钩子
.pre-commit-config.yaml 定义了完整的本地开发质量门禁：
- 基础检查：trailing-whitespace、end-of-file-fixer、check-yaml/toml/json、detect-private-key
- 代码规范：ruff（lint + format，替代 black/isort）、mypy 类型检查
- 文档格式：mdformat
- 手动阶段：包含一个标记为 manual 的 pytest 钩子用于兼容性测试

### 测试与覆盖率
- 框架：pytest + pytest-asyncio（auto 模式）
- 覆盖率：pytest-cov，输出 term-missing、html、xml 三种报告
- 测试分类：通过 markers 区分 unit / integration / slow / asyncio
- 测试路径：tests/ 下按功能域组织（unit、integration、distributed、benchmark、chaos、v.x 等）

### 包数据与入口点
- 包发现：setuptools.packages.find(where=["src"])，仅包含 neotask* 包
- 静态资源：web/static/*、web/templates/*、py.typed 通过 package-data 声明
- CLI 入口：neotask = "neotask.cli:main" 注册可执行命令

### 关键约束与约定
- Python 最低版本：3.9（同时支持 3.9–3.13）
- 行长度限制：100 字符（black/ruff 统一）
- 构建产物目录：dist/、build/、*.egg-info 被 MANIFEST.in 全局排除
- 开发环境隔离：.venv/、venv/、env/ 均被忽略和排除