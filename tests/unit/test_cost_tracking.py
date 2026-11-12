"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_cost_tracking.py
@Description: 执行器侧成本上报上下文测试（CostRecorder + contextvar）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import pytest

from neotask.core.cost import (
    CostRecorder,
    get_cost_recorder,
    report_cost,
    reset_cost_recorder,
    set_cost_recorder,
)


class TestCostRecorder:
    """累加器本身的语义"""

    def test_empty_before_report(self):
        recorder = CostRecorder()

        assert recorder.reported is False
        assert recorder.payload() == {}

    def test_accumulates_across_reports(self):
        recorder = CostRecorder()
        recorder.add(tokens_in=100, tokens_out=20, api_calls=1,
                     estimated_cost=0.01, currency="USD")
        recorder.add(tokens_in=50, tokens_out=10, api_calls=2,
                     estimated_cost=0.02, currency="USD")

        assert recorder.reported is True
        assert recorder.payload() == {
            "tokens_in": 150,
            "tokens_out": 30,
            "api_calls": 3,
            "estimated_cost": pytest.approx(0.03),
            "currency": "USD",
        }

    def test_currency_conflict_marks_mixed(self):
        recorder = CostRecorder()
        recorder.add(estimated_cost=1.0, currency="USD")
        recorder.add(estimated_cost=1.0, currency="EUR")

        assert recorder.payload()["currency"] == "MIXED"

    def test_report_without_currency(self):
        """只上报用量不带币种时 currency 为空串"""
        recorder = CostRecorder()
        recorder.add(tokens_in=5)

        payload = recorder.payload()
        assert payload["tokens_in"] == 5
        assert payload["currency"] == ""


class TestReportCostContext:
    """contextvar 绑定与 report_cost 返回值"""

    def test_report_without_context_returns_false(self):
        assert get_cost_recorder() is None
        assert report_cost(tokens_in=1) is False

    def test_report_with_context(self):
        recorder = CostRecorder()
        token = set_cost_recorder(recorder)
        try:
            assert get_cost_recorder() is recorder
            assert report_cost(tokens_in=10, tokens_out=5, api_calls=1,
                               estimated_cost=0.5, currency="USD") is True
        finally:
            reset_cost_recorder(token)

        assert recorder.payload()["tokens_in"] == 10
        assert recorder.payload()["api_calls"] == 1
        assert get_cost_recorder() is None
