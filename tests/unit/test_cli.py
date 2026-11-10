"""
@FileName: test_cli.py
@Description: CLI 入口测试 - 覆盖 --help / --version / --benchmark 与 python -m neotask
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/10
"""

import json
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
    assert "--benchmark" in out


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


class TestCliBenchmark:
    """验证 --benchmark 参数分发与帮助信息"""

    def test_help_lists_benchmark(self, capsys):
        exit_code = main([])
        assert exit_code == 0
        output = capsys.readouterr().out
        assert "--benchmark" in output
        assert "--executor" in output

    def test_benchmark_flag(self, capsys):
        exit_code = main(["--benchmark", "--tasks", "6", "--concurrency", "2", "--json"])
        assert exit_code == 0

        data = json.loads(capsys.readouterr().out)
        assert data["tasks"] == 6
        assert data["succeeded"] == 6
        assert data["warmup"] == 0
        assert data["payload_template"] is None

    def test_benchmark_subcommand_alias(self, capsys):
        exit_code = main(["benchmark", "--tasks", "4", "--concurrency", "2", "--json"])
        assert exit_code == 0
        assert json.loads(capsys.readouterr().out)["tasks"] == 4

    def test_benchmark_passes_custom_parameters(self, capsys):
        exit_code = main(
            [
                "--benchmark",
                "--tasks",
                "5",
                "--concurrency",
                "2",
                "--warmup",
                "2",
                "--payload",
                '{"size": 3}',
                "--json",
            ]
        )
        assert exit_code == 0

        data = json.loads(capsys.readouterr().out)
        assert data["warmup"] == 2
        assert data["payload_template"] == {"size": 3}
