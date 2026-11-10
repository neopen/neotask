"""
@FileName: test_cli.py
@Description: CLI 入口测试 - 覆盖 --help / --version / 未知命令与 python -m neotask
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/10
"""

import subprocess
import sys

import pytest

from neotask import __version__
from neotask.cli import main


@pytest.mark.parametrize("argv", [[], ["-h"], ["--help"], ["help"]])
def test_help_prints_usage(capsys, argv):
    assert main(argv) == 0

    out = capsys.readouterr().out
    assert "NeoTask CLI" in out
    assert "--version" in out


@pytest.mark.parametrize("argv", [["-v"], ["--version"], ["version"]])
def test_version_prints_package_version(capsys, argv):
    assert main(argv) == 0

    assert capsys.readouterr().out.strip() == f"neotask {__version__}"


def test_unknown_command_returns_usage_error(capsys):
    assert main(["run"]) == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "未知命令: run" in captured.err
    assert "neotask --help" in captured.err


def test_python_dash_m_entry_point():
    result = subprocess.run(
        [sys.executable, "-m", "neotask", "--version"],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == f"neotask {__version__}"
