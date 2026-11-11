"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: main.py
@Description: CLI 入口 - 提供 --benchmark / --version / --help
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/10
"""

import sys
from typing import Optional, Sequence

_HELP = """NeoTask CLI

用法：
  neotask --benchmark [参数...]   运行轻量性能自测（用自己的 executor 验证吞吐/延迟）
  neotask benchmark [参数...]     同上（子命令写法）
  neotask --version               显示版本号
  neotask --help                  显示本帮助

基准参数（与 python -m neotask.benchmark 完全一致）：
  --executor MODULE:FUNC  执行器，如 my_tasks:handle 或 ./bench.py:my_task
  --tasks N               测量的任务数（默认 200）
  --concurrency N         Worker 并发数（默认 10）
  --warmup N              预热任务数（默认 0）
  --payload JSON          任务数据模板（默认 {"index": i}）
  --timeout S             等待完成的超时时间（秒，默认 120）
  --no-prefetch           关闭 Worker 预取
  --json                  输出 JSON，便于 CI/脚本消费
"""


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI 分发入口，返回进程退出码。"""
    args = list(sys.argv[1:] if argv is None else argv)

    if not args or args[0] in ("-h", "--help", "help"):
        print(_HELP)
        return 0

    if args[0] in ("-v", "--version", "version"):
        from neotask import __version__

        print(f"neotask {__version__}")
        return 0

    if args[0] in ("--benchmark", "benchmark"):
        # 延迟导入：只有真正跑基准时才加载任务池依赖
        from neotask.benchmark import main as benchmark_main

        return benchmark_main(args[1:], prog="neotask --benchmark")

    print(f"未知命令: {args[0]}", file=sys.stderr)
    print("使用 neotask --help 查看可用命令。", file=sys.stderr)
    return 2
