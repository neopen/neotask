"""
@FileName: test_cli.py
@Description: CLI 入口（neotask --benchmark）单元测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

import json

from neotask.cli import main


class TestCliMain:
    """验证 --benchmark 参数分发与帮助信息"""

    def test_help_lists_benchmark(self, capsys):
        exit_code = main([])
        assert exit_code == 0
        output = capsys.readouterr().out
        assert "--benchmark" in output
        assert "--executor" in output

    def test_benchmark_flag(self, capsys):
        exit_code = main(
            ["--benchmark", "--tasks", "6", "--concurrency", "2", "--json"]
        )
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

    def test_unknown_command(self, capsys):
        exit_code = main(["nope"])
        assert exit_code == 2
        assert "未知命令" in capsys.readouterr().err
