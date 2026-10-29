"""
@FileName: __main__.py
@Description: 支持 python -m neotask（等价于 neotask CLI）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

from neotask.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
