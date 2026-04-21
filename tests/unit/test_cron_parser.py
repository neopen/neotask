"""
@FileName: test_cron_parser.py
@Description: Cron表达式解析器单元测试
@Author: HiPeng
@Time: 2026/4/21
"""

from datetime import datetime

import pytest

from neotask.scheduler.cron_parser import CronParser, CronExpression, parse_predefined


class TestCronParser:
    """Cron解析器测试"""

    # ========== 基础解析测试 ==========

    def test_parse_valid_expression(self):
        """测试解析有效表达式"""
        cron = CronParser.parse("0 9 * * *")
        assert cron is not None
        assert cron.expression == "0 9 * * *"

    def test_parse_invalid_expression(self):
        """测试解析无效表达式"""
        with pytest.raises(ValueError, match="Expected 5 fields"):
            CronParser.parse("0 9 * *")

        with pytest.raises(ValueError, match="Expected 5 fields"):
            CronParser.parse("0 9 * * * *")

    def test_is_valid(self):
        """测试验证方法"""
        assert CronParser.is_valid("0 9 * * *") is True
        assert CronParser.is_valid("*/5 * * * *") is True
        assert CronParser.is_valid("0 9 * *") is False
        assert CronParser.is_valid("invalid") is False

    # ========== 分钟字段测试 ==========

    def test_minute_star(self):
        """测试每分钟"""
        cron = CronParser.parse("* * * * *")
        now = datetime(2024, 1, 1, 10, 0, 0)
        next_run = cron.next(after=now)
        # 应该立即执行（当前分钟）
        assert next_run.minute == 0 or next_run.minute == now.minute

    def test_minute_specific(self):
        """测试指定分钟"""
        cron = CronParser.parse("30 * * * *")
        now = datetime(2024, 1, 1, 10, 15, 0)
        next_run = cron.next(after=now)
        assert next_run.minute == 30
        assert next_run.hour == 10

    def test_minute_step(self):
        """测试步长分钟"""
        cron = CronParser.parse("*/15 * * * *")
        now = datetime(2024, 1, 1, 10, 10, 0)
        next_run = cron.next(after=now)
        assert next_run.minute in [15, 30, 45, 0]

    def test_minute_list(self):
        """测试分钟列表"""
        cron = CronParser.parse("5,10,15 * * * *")
        now = datetime(2024, 1, 1, 10, 8, 0)
        next_run = cron.next(after=now)
        assert next_run.minute == 10

    def test_minute_range(self):
        """测试分钟范围"""
        cron = CronParser.parse("10-20 * * * *")
        now = datetime(2024, 1, 1, 10, 5, 0)
        next_run = cron.next(after=now)
        assert 10 <= next_run.minute <= 20

    # ========== 小时字段测试 ==========

    def test_hour_specific(self):
        """测试指定小时"""
        cron = CronParser.parse("0 9 * * *")
        now = datetime(2024, 1, 1, 8, 0, 0)
        next_run = cron.next(after=now)
        assert next_run.hour == 9
        assert next_run.minute == 0

    def test_hour_range(self):
        """测试小时范围"""
        cron = CronParser.parse("0 9-17 * * *")
        now = datetime(2024, 1, 1, 8, 0, 0)
        next_run = cron.next(after=now)
        assert 9 <= next_run.hour <= 17

    def test_hour_step(self):
        """测试步长小时"""
        cron = CronParser.parse("0 */2 * * *")
        now = datetime(2024, 1, 1, 9, 0, 0)
        next_run = cron.next(after=now)
        assert next_run.hour % 2 == 0

    # ========== 日期字段测试 ==========

    def test_day_specific(self):
        """测试指定日期"""
        cron = CronParser.parse("0 0 15 * *")
        now = datetime(2024, 1, 10, 0, 0, 0)
        next_run = cron.next(after=now)
        assert next_run.day == 15

    def test_day_last(self):
        """测试月末最后一天"""
        cron = CronParser.parse("0 0 L * *")
        now = datetime(2024, 1, 20, 0, 0, 0)
        next_run = cron.next(after=now)
        # 1月最后一天是31日
        assert next_run.day == 31

    # ========== 星期字段测试 ==========

    def test_weekday_specific(self):
        """测试指定星期"""
        # 0=周日, 1=周一, ..., 6=周六
        cron = CronParser.parse("0 9 * * 1")  # 周一
        now = datetime(2024, 1, 1, 8, 0, 0)  # 2024-01-01 是周一
        next_run = cron.next(after=now)
        # 应该是当天或下周一
        assert next_run.weekday() == 0  # Python weekday: 0=周一

    def test_weekday_range(self):
        """测试星期范围"""
        cron = CronParser.parse("0 9 * * 1-5")  # 周一到周五
        now = datetime(2024, 1, 6, 8, 0, 0)  # 周六
        next_run = cron.next(after=now)
        assert next_run.weekday() <= 4  # 周一到周五

    # ========== 组合字段测试 ==========

    def test_combined_cron(self):
        """测试组合表达式"""
        # 工作日9:30
        cron = CronParser.parse("30 9 * * 1-5")
        now = datetime(2024, 1, 1, 9, 0, 0)
        next_run = cron.next(after=now)
        assert next_run.hour == 9
        assert next_run.minute == 30
        assert next_run.weekday() <= 4

    def test_complex_cron(self):
        """测试复杂表达式"""
        # 每月1号和15号，以及每周一的9:00
        cron = CronParser.parse("0 9 1,15 * 1")
        now = datetime(2024, 1, 10, 9, 0, 0)
        next_run = cron.next(after=now)
        # 应该是下一个周一或15号
        assert next_run.hour == 9
        assert next_run.minute == 0

    # ========== 预定义表达式测试 ==========

    def test_predefined_yearly(self):
        """测试每年"""
        cron = parse_predefined("@yearly")
        assert cron is not None
        now = datetime(2024, 1, 2, 0, 0, 0)
        next_run = cron.next(after=now)
        assert next_run.month == 1
        assert next_run.day == 1

    def test_predefined_monthly(self):
        """测试每月"""
        cron = parse_predefined("@monthly")
        now = datetime(2024, 1, 2, 0, 0, 0)
        next_run = cron.next(after=now)
        assert next_run.day == 1

    def test_predefined_weekly(self):
        """测试每周"""
        cron = parse_predefined("@weekly")
        now = datetime(2024, 1, 1, 0, 0, 0)  # 周一
        next_run = cron.next(after=now)
        assert next_run.weekday() == 6  # 周日

    def test_predefined_daily(self):
        """测试每天"""
        cron = parse_predefined("@daily")
        now = datetime(2024, 1, 1, 10, 0, 0)
        next_run = cron.next(after=now)
        assert next_run.hour == 0
        assert next_run.minute == 0
        assert next_run.day == 2  # 第二天

    def test_predefined_hourly(self):
        """测试每小时"""
        cron = parse_predefined("@hourly")
        now = datetime(2024, 1, 1, 10, 30, 0)
        next_run = cron.next(after=now)
        assert next_run.minute == 0
        assert next_run.hour == 11

    # ========== 获取多次执行时间测试 ==========

    def test_get_next_n(self):
        """测试获取接下来N次执行时间"""
        cron = CronParser.parse("0 9 * * *")
        next_runs = cron.get_next_n(5)
        assert len(next_runs) == 5

        # 检查是否都是9点
        for run in next_runs:
            assert run.hour == 9
            assert run.minute == 0

        # 检查是否递增
        for i in range(4):
            assert next_runs[i] < next_runs[i + 1]

    # ========== 描述功能测试 ==========

    def test_describe_daily(self):
        """测试描述 - 每天"""
        desc = CronParser.describe("0 9 * * *")
        assert "9点" in desc or "9" in desc

    def test_describe_minute(self):
        """测试描述 - 分钟"""
        desc = CronParser.describe("*/5 * * * *")
        assert "5分钟" in desc

    def test_describe_weekday(self):
        """测试描述 - 工作日"""
        desc = CronParser.describe("0 9 * * 1-5")
        assert "周一" in desc or "周五" in desc


class TestCronExpression:
    """CronExpression 类测试"""

    def test_next_method(self):
        """测试 next 方法"""
        cron = CronExpression("0 9 * * *")
        next_run = cron.next()
        assert next_run.hour == 9
        assert next_run.minute == 0

    def test_next_with_after(self):
        """测试带起始时间的 next 方法"""
        cron = CronExpression("0 9 * * *")
        after = datetime(2024, 1, 1, 9, 30, 0)
        next_run = cron.next(after=after)
        # 应该是第二天9点
        assert next_run.day == 2
        assert next_run.hour == 9

    def test_previous_method(self):
        """测试 previous 方法"""
        cron = CronExpression("0 9 * * *")
        before = datetime(2024, 1, 1, 10, 0, 0)
        prev_run = cron.previous(before=before)
        assert prev_run.day == 1
        assert prev_run.hour == 9

    def test_str_repr(self):
        """测试字符串表示"""
        cron = CronExpression("0 9 * * *")
        assert str(cron) == "0 9 * * *"
        assert repr(cron) == "CronExpression('0 9 * * *')"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
