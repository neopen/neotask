---
kind: dependency_management
name: Python 依赖管理 — pyproject.toml + setuptools 构建体系
category: dependency_management
scope:
    - '**'
source_files:
    - pyproject.toml
    - .pre-commit-config.yaml
    - .github/workflows/publish-pypi.yml
    - src/neotask/__init__.py
    - MANIFEST.in
---

## 1. 使用的系统与工具链
- **包元数据与依赖声明**：`pyproject.toml`（PEP 621），由 `setuptools.build_meta` 作为后端构建。
- **可选依赖分组**：通过 `[project.optional-dependencies]` 将 UI、Redis、SQLite、监控、开发、文档等能力拆分为可组合的 extras，如 `neotask[ui,redis,sqlite,monitor]`。
- **版本管理与发布**：使用 `bumpversion` 同步更新 `src/neotask/__init__.py` 与 `pyproject.toml` 中的版本号；通过 `twine` 上传至 PyPI（仓库地址已配置为 `https://upload.pypi.org/legacy/`）。
- **预提交钩子**：`.pre-commit-config.yaml` 固定了 ruff、mypy、black、mdformat 等工具的精确版本，保证本地与 CI 行为一致。
- **CI 发布**：`.github/workflows/publish-pypi.yml` 触发 PyPI 发布流程。
- **无 lockfile / vendor 策略**：仓库未包含 `requirements.txt`、`poetry.lock`、`Pipfile.lock` 或 vendored 第三方代码，依赖解析完全交由 pip/setuptools 在运行时完成。

## 2. 关键文件与位置
- `pyproject.toml`：项目元数据、核心依赖、可选依赖、构建/测试/格式化/发布全部配置集中于此。
- `src/neotask/__init__.py`：暴露 `__version__ = "1.0.0"`，与 bumpversion 目标保持一致。
- `.pre-commit-config.yaml`：锁定 pre-commit hooks 版本，避免“在我机器上正常”问题。
- `.github/workflows/publish-pypi.yml`：PyPI 发布流水线入口。
- `MANIFEST.in`：配合 setuptools 打包时控制非 Python 资源文件的 inclusion。

## 3. 架构与约定
- **最小核心依赖**：仅保留 `aiosqlite`、`redis`、`psutil`、`croniter` 四个运行时依赖，其余功能通过 optional-dependencies 按需引入，降低默认安装体积。
- **版本约束风格**：统一采用 `>=X.Y.Z` 宽松下限约束，不锁上限，便于生态演进；但 pre-commit hooks 使用精确版本以保障可重复性。
- **构建后端单一化**：始终使用 `setuptools.build_meta`，不引入 Poetry/PDM 等多工具并存，简化贡献者上手成本。
- **双入口脚本**：通过 `[project.scripts]` 注册 CLI 命令 `neotask = "neotask.cli:main"`，供用户直接调用。
- **类型与检查分离**：mypy/ruff/black/isort 的配置集中在 `pyproject.toml` 对应 section，与依赖声明同仓维护，避免散落配置。

## 4. 开发者应遵循的规则
1. **新增依赖必须写入 `pyproject.toml`**：核心依赖放入 `dependencies`，可选能力放入对应的 `[project.optional-dependencies]` 组，禁止在源码中硬编码 import 未声明的第三方包。
2. **保持版本下限合理**：使用 `>=` 指定最低兼容版本，避免随意升级导致破坏性变更；如需严格锁定，应在 CI 环境单独生成 lockfile 而非提交到仓库。
3. **可选依赖互斥/组合清晰**：例如 `full = ["neotask[ui,redis,sqlite,monitor]"]` 提供一键全量安装，新功能应以 extra 形式渐进式暴露。
4. **预提交钩子版本固定**：修改 `.pre-commit-config.yaml` 时务必同时运行 `pre-commit autoupdate` 并记录变更，确保团队与 CI 使用相同工具版本。
5. **发布前执行 bumpversion**：通过 `bumpversion` 统一提升主/次/补丁号，自动 commit 并打 tag，再走 GitHub Actions 发布到 PyPI。
6. **不要提交 lockfile/vendor**：本项目明确不纳入 lockfile 或 vendored 第三方代码，依赖解析交给 pip；若需离线部署，请在 CI 侧生成 wheel 并分发。