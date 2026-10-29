"""
@FileName: test_benchmark.py
@Description: 轻量基准测试入口（neotask.benchmark）单元测试
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

import asyncio
import json
import time

import pytest

from neotask.benchmark import (
    BenchmarkResult,
    demo_executor,
    load_executor,
    main,
    run_benchmark,
    run_benchmark_async,
)


class TestRunBenchmark:
    """验证指标汇总与执行器数据传递"""

    @pytest.mark.asyncio
    async def test_basic_metrics(self):
        """正常任务应产出完整吞吐/延迟指标"""

        async def executor(data):
            await asyncio.sleep(0.002)
            return {"index": data["index"]}

        result = await run_benchmark_async(executor, tasks=20, concurrency=4)

        assert isinstance(result, BenchmarkResult)
        assert result.succeeded == 20
        assert result.failed == 0
        assert result.unfinished == 0
        assert result.executor_mode == "async"
        assert result.duration_s > 0
        assert result.throughput > 0
        assert result.success_rate == 1.0
        assert result.latency_p50_ms > 0
        assert result.latency_p99_ms >= result.latency_p50_ms
        assert result.latency_max_ms >= result.latency_min_ms

        payload = result.to_dict()
        assert payload["tasks"] == 20
        assert payload["storage"] == "memory"
        assert payload["executor_mode"] == "async"
        assert payload["warmup"] == 0
        assert payload["timeout_s"] == 120.0
        assert payload["payload_template"] is None
        assert payload["latency_p99_ms"] >= payload["latency_p50_ms"]

    @pytest.mark.asyncio
    async def test_failures_are_counted_without_retry(self):
        """失败任务单独计数，且不因重试放大数量"""

        async def failing(_data):
            raise RuntimeError("boom")

        result = await run_benchmark_async(failing, tasks=10, concurrency=2)

        assert result.succeeded == 0
        assert result.failed == 10
        assert result.success_rate == 0.0
        assert result.latency_p50_ms == 0.0
        assert result.error_samples
        assert "boom" in result.error_samples[0]

    @pytest.mark.asyncio
    async def test_payload_passthrough(self):
        """payload 模板应逐条传递并自动补充 index"""
        seen = []

        async def executor(data):
            seen.append(data)
            return {"ok": True}

        result = await run_benchmark_async(
            executor,
            tasks=6,
            concurrency=2,
            payload={"size": 7},
        )

        assert result.succeeded == 6
        assert result.payload_template == {"size": 7}
        assert all(item["size"] == 7 for item in seen)
        assert sorted(item["index"] for item in seen) == [0, 1, 2, 3, 4, 5]

    def test_sync_wrapper(self):
        """同步入口应能在普通脚本中直接调用"""

        async def executor(_data):
            return {"ok": True}

        result = run_benchmark(executor, tasks=10, concurrency=2)
        assert result.succeeded == 10

    @pytest.mark.asyncio
    async def test_sync_executor_runs_in_thread(self):
        """同步执行器应自动走线程池，避免阻塞事件循环"""

        def sync_executor(data):
            time.sleep(0.002)
            return {"index": data["index"]}

        result = await run_benchmark_async(sync_executor, tasks=10, concurrency=4)
        assert result.succeeded == 10
        assert result.executor_mode == "thread"

    def test_invalid_arguments(self):
        """非法参数应尽早失败"""

        async def executor(_data):
            return {}

        with pytest.raises(ValueError):
            run_benchmark(executor, tasks=0)
        with pytest.raises(ValueError):
            run_benchmark(executor, tasks=1, concurrency=0)

    @pytest.mark.asyncio
    async def test_sync_wrapper_rejects_running_loop(self):
        """事件循环内应提示改用 run_benchmark_async"""

        async def executor(_data):
            return {}

        with pytest.raises(RuntimeError, match="run_benchmark_async"):
            run_benchmark(executor, tasks=1)


class TestLoadExecutor:
    """命令行执行器加载"""

    def test_load_from_module(self):
        assert load_executor("neotask.benchmark:demo_executor") is demo_executor

    @pytest.mark.asyncio
    async def test_load_from_file(self, tmp_path):
        module_file = tmp_path / "my_tasks.py"
        module_file.write_text(
            "async def handle(data):\n"
            "    return {'index': data['index']}\n",
            encoding="utf-8",
        )

        # Windows 绝对路径带盘符冒号，加载逻辑应只按最后一个冒号切分
        func = load_executor(f"{module_file}:handle")
        assert callable(func)
        assert await func({"index": 1}) == {"index": 1}

    def test_invalid_spec(self):
        with pytest.raises(ValueError):
            load_executor("no_colon")
        with pytest.raises(ValueError):
            load_executor("neotask.benchmark:missing")


class TestCli:
    """命令行入口"""

    def test_json_output(self, capsys):
        exit_code = main(["--tasks", "8", "--concurrency", "2", "--json"])
        assert exit_code == 0

        output = capsys.readouterr().out
        data = json.loads(output)
        assert data["tasks"] == 8
        assert data["succeeded"] == 8
        assert data["success_rate"] == 1.0
        assert data["warmup"] == 0
        assert data["payload_template"] is None

    def test_invalid_executor_returns_error(self, capsys):
        exit_code = main(["--executor", "missing_module:fn", "--tasks", "1"])
        assert exit_code == 1
        assert "基准测试失败" in capsys.readouterr().err
