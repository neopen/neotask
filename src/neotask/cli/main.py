"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: main.py
@Description: CLI 入口 - 提供 --help / --version
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/10
"""

import sys
from typing import Optional, Sequence

_HELP = """NeoTask CLI

用法：
  neotask --help       显示本帮助
  neotask --version    显示版本号

NeoTask 是纯 Python 异步任务队列，提供实时、延时、周期与 Cron 任务调度。
在代码中通过 TaskPool / TaskScheduler 使用；详见项目 README 与文档。
"""


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI 入口，返回进程退出码。"""
    args = list(sys.argv[1:] if argv is None else argv)

    if not args or args[0] in ("-h", "--help", "help"):
        print(_HELP)
        return 0

    if args[0] in ("-v", "--version", "version"):
        from neotask import __version__

        print(f"neotask {__version__}")
        return 0

    print(f"未知命令: {args[0]}", file=sys.stderr)
    print("使用 neotask --help 查看可用命令。", file=sys.stderr)
    return 2
