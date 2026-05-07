---
name: example-creator
description: NeoTask 示例文件创建指南 — 遵循项目编号规范和代码风格创建新的 usage example
---

# NeoTask 示例创建技能

## 概述

本技能指导在 `examples/` 目录中创建新的 NeoTask 用法示例。示例文件遵循统一的编号、结构和风格规范。

## 编号规范

```
examples/
├── 00_quick_start.py     # 最快上手（5 行代码）
├── 01_simple.py          # 基础用法
├── 02_context_manager.py # 上下文管理器
├── 03_priority.py        # 优先级队列
├── 04_events.py          # 事件回调
├── 05_cron_tasks.py      # Cron 定时任务
├── 06_delayed_tasks.py   # 延时任务
├── 07_batch.py           # 批量任务
├── 08_periodic.py        # 周期管理
├── 09_retry_and_cancel.py # 重试与取消
├── 10_task_query.py      # 任务查询
├── ...
├── 99_all_features.py    # 完整功能演示
└── readme.txt            # 索引文件
```

编号规则：
- `00` — 快速入门（最简代码）
- `01-19` — 按主题递增，每个示例聚焦一个特性
- `20-98` — 高级/组合用法
- `99` — 全功能演示（保留）

## 文件模板

```python
"""
@FileName: NN_descriptive_name.py
@Description: 简短中文描述 — 这个示例演示什么
@Author: HiPeng
@Time: YYYY/M/D
"""

import asyncio
from neotask import TaskPool, TaskPoolConfig


# 定义任务处理函数
async def process_data(data: dict) -> dict:
    """简短描述处理逻辑"""
    await asyncio.sleep(0.1)
    return {"result": "success", "data": data}


async def main():
    # 1. 配置
    # 2. 执行
    # 3. 输出结果
    pass


if __name__ == "__main__":
    asyncio.run(main())
```

## 风格要求

- **执行器函数**：命名清晰（`process_data`, `send_email`, `generate_report`），含类型注解
- **main() 结构**：配置 → 提交 → 等待 → 输出
- **打印输出**：中文描述，带缩进层次，让运行结果易读
- **executor 耗时**：用 `asyncio.sleep(0.1)` 模拟，不要太长
- **清理**：使用 `with` 语句或 `finally: pool.shutdown()`
- **导入**：只导入实际使用的类
- **类型注解**：函数签名加类型注解

## 创建新示例步骤

### 1. 确定编号

查看 `examples/readme.txt` 找到下一个可用编号。不要覆盖已有文件。

### 2. 创建 Python 文件

按上述模板创建 `examples/NN_feature_name.py`。

### 3. 更新索引

在 `examples/readme.txt` 中添加一行：

```
NN_feature_name.py   主题    简短说明
```

格式对齐：文件名左对齐，Tab 分隔主题和说明。

### 4. 验证运行

```bash
python examples/NN_feature_name.py
```

确保输出清晰、无报错。

## 示例：创建一个新示例

用户说："创建一个展示任务超时处理的示例"

```python
"""
@FileName: 11_timeout.py
@Description: 任务超时示例 — 设置超时时间和超时处理
@Author: HiPeng
@Time: 2026/5/7
"""

import asyncio
from neotask import TaskPool, TaskPoolConfig


async def slow_task(data: dict) -> dict:
    """模拟耗时任务"""
    await asyncio.sleep(data.get("duration", 5))
    return {"status": "done"}


async def fast_task(data: dict) -> dict:
    """快速任务"""
    await asyncio.sleep(0.1)
    return {"status": "done"}


async def main():
    config = TaskPoolConfig(
        worker_concurrency=2,
        task_timeout=2,  # 2 秒超时
        max_retries=0,
    )

    with TaskPool(executor=slow_task, config=config) as pool:
        # 快速任务 — 正常完成
        fast_id = pool.submit({"duration": 0.1})
        print(f"快速任务: {fast_id}")

        # 慢速任务 — 将超时
        slow_id = pool.submit({"duration": 5})
        print(f"慢速任务: {slow_id}")

        # 等待结果
        fast_result = pool.wait_for_result(fast_id, timeout=10)
        print(f"快速任务结果: {fast_result}")

        slow_result = pool.wait_for_result(slow_id, timeout=10)
        print(f"慢速任务结果: {slow_result}")

        # 查看统计
        stats = pool.get_stats()
        print(f"\n统计: 总数={stats['total']}, 成功={stats['completed']}, "
              f"失败={stats['failed']}")


if __name__ == "__main__":
    asyncio.run(main())
```

## 已有示例速查

| 编号 | 文件 | 主题 |
|---|---|---|
| 00 | `00_quick_start.py` | 5 行最简示例 |
| 01 | `01_simple.py` | 基础提交和等待 |
| 02 | `02_context_manager.py` | with 语句自动管理 |
| 03 | `03_priority.py` | 优先级调度 |
| 04 | `04_events.py` | 生命周期事件回调 |
| 05 | `05_cron_tasks.py` | Cron 表达式定时任务 |
| 06 | `06_delayed_tasks.py` | 延时和定时执行 |
| 07 | `07_batch.py` | 批量提交和并发等待 |
| 08 | `08_periodic.py` | 周期任务管理 |
| 09 | `09_retry_and_cancel.py` | 重试策略和取消 |
| 10 | `10_task_query.py` | 状态查询和统计 |
| 99 | `99_all_features.py` | 全功能演示 |

## 向用户询问

创建新示例前确认：

1. **主题**：演示什么功能？
2. **复杂度**：简单（1 个特性）/ 中等（组合 2-3 个）/ 完整工作流？
3. **存储后端**：memory（默认）/ sqlite / redis？
4. **是否更新 readme.txt**：通常需要。
