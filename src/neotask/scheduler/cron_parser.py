"""
@FileName: cron_parser.py
@Description: Cron表达式解析器 - 基于 croniter 实现
@Author: HiPeng
@Time: 2026/4/21
"""

from datetime import datetime, timedelta
from typing import List, Optional

try:
    from croniter import croniter
    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False
    import warnings
    warnings.warn("croniter not installed, using fallback parser. Install with: pip install croniter")


class CronExpression:
    """Cron表达式

    基于 croniter 库实现，支持标准 Cron 表达式。
    """

    def __init__(self, expression: str):
        self.expression = expression.strip()
        self._validate()
        if HAS_CRONITER:
            self._croniter = None
        else:
            self._fallback_parser = FallbackCronExpression(expression)

    def _validate(self) -> None:
        """验证表达式格式"""
        parts = self.expression.split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression: {self.expression}. "
                "Expected 5 fields: minute hour day month weekday"
            )

    def _get_croniter(self, start_time: datetime):
        """获取 croniter 实例"""
        if HAS_CRONITER:
            # 每次创建新实例，避免状态问题
            return croniter(self.expression, start_time)
        else:
            raise RuntimeError("croniter not installed. Run: pip install croniter")

    def next(self, after: Optional[datetime] = None) -> datetime:
        """获取下一次执行时间

        Args:
            after: 起始时间，默认为当前时间

        Returns:
            下一次执行时间
        """
        if not HAS_CRONITER:
            return self._fallback_parser.next(after)

        start = after or datetime.now()
        cron = self._get_croniter(start)
        return cron.get_next(datetime)

    def previous(self, before: Optional[datetime] = None) -> datetime:
        """获取上一次执行时间

        Args:
            before: 截止时间，默认为当前时间

        Returns:
            上一次执行时间
        """
        if not HAS_CRONITER:
            return self._fallback_parser.previous(before)

        start = before or datetime.now()
        cron = self._get_croniter(start)
        return cron.get_prev(datetime)

    def get_next_n(self, n: int, after: Optional[datetime] = None) -> List[datetime]:
        """获取接下来n次执行时间

        Args:
            n: 获取次数
            after: 起始时间

        Returns:
            执行时间列表
        """
        results = []
        current = after or datetime.now()

        for _ in range(n):
            next_time = self.next(after=current)
            results.append(next_time)
            # 加一秒确保下次查询不会返回相同时间
            current = next_time + timedelta(seconds=1)

        return results

    def __str__(self) -> str:
        return self.expression

    def __repr__(self) -> str:
        return f"CronExpression('{self.expression}')"


class CronParser:
    """Cron表达式解析器"""

    @staticmethod
    def parse(expression: str) -> CronExpression:
        """解析Cron表达式"""
        return CronExpression(expression)

    @staticmethod
    def is_valid(expression: str) -> bool:
        """验证Cron表达式是否有效"""
        try:
            if HAS_CRONITER:
                croniter(expression, datetime.now())
            else:
                CronExpression(expression)
            return True
        except Exception:
            return False

    @staticmethod
    def describe(expression: str) -> str:
        """生成Cron表达式的中文描述"""
        parts = expression.split()
        if len(parts) != 5:
            return "无效的Cron表达式"

        minute, hour, day, month, weekday = parts
        descriptions = []

        # 分钟
        if minute == "*":
            descriptions.append("每分钟")
        elif minute.startswith("*/"):
            descriptions.append(f"每{minute[2:]}分钟")
        elif minute == "0":
            pass
        else:
            descriptions.append(f"第{minute}分钟")

        # 小时
        if hour == "*":
            if descriptions and descriptions[-1] == "每分钟":
                descriptions[-1] = "每小时"
            else:
                descriptions.append("每小时")
        elif hour.startswith("*/"):
            descriptions.append(f"每{hour[2:]}小时")
        elif hour == "0":
            descriptions.append("午夜")
        else:
            descriptions.append(f"{hour}点")

        # 日期
        if day == "*" or day == "?":
            pass
        elif day == "L":
            descriptions.append("每月最后一天")
        else:
            descriptions.append(f"每月{day}号")

        # 月份
        if month != "*":
            month_names = ["1月", "2月", "3月", "4月", "5月", "6月",
                          "7月", "8月", "9月", "10月", "11月", "12月"]
            if "-" in month:
                start, end = month.split("-")
                descriptions.append(f"{month_names[int(start)-1]}至{month_names[int(end)-1]}")
            else:
                descriptions.append(month_names[int(month)-1])

        # 星期
        if weekday != "*" and weekday != "?":
            weekday_names = ["周日", "周一", "周二", "周三", "周四", "周五", "周六"]
            if "-" in weekday:
                start, end = weekday.split("-")
                descriptions.append(f"{weekday_names[int(start)]}至{weekday_names[int(end)]}")
            elif weekday == "L":
                descriptions.append("最后一个工作日")
            else:
                try:
                    descriptions.append(weekday_names[int(weekday)])
                except (ValueError, IndexError):
                    pass

        if not descriptions:
            return "立即执行"

        return " ".join(descriptions)


class FallbackCronExpression:
    """备用Cron表达式解析器（当croniter未安装时使用）"""

    def __init__(self, expression: str):
        self.expression = expression
        self._fields = self._parse_simple(expression)

    def _parse_simple(self, expression: str) -> List[set]:
        parts = expression.split()
        fields = []
        fields.append(self._parse_simple_field(parts[0], 0, 59))
        fields.append(self._parse_simple_field(parts[1], 0, 23))
        fields.append(self._parse_simple_field(parts[2], 1, 31))
        fields.append(self._parse_simple_field(parts[3], 1, 12))
        fields.append(self._parse_simple_field(parts[4], 0, 6))
        return fields

    def _parse_simple_field(self, field: str, min_val: int, max_val: int) -> set:
        if field == "*" or field == "?":
            return set(range(min_val, max_val + 1))
        if field.startswith("*/"):
            step = int(field[2:])
            return set(range(min_val, max_val + 1, step))
        if "," in field:
            result = set()
            for part in field.split(","):
                if "-" in part:
                    start, end = part.split("-")
                    result.update(range(int(start), int(end) + 1))
                else:
                    result.add(int(part))
            return result
        if "-" in field:
            start, end = field.split("-")
            return set(range(int(start), int(end) + 1))
        return {int(field)}

    def next(self, after: Optional[datetime] = None) -> datetime:
        now = after or datetime.now()
        return now + timedelta(minutes=1)

    def previous(self, before: Optional[datetime] = None) -> datetime:
        now = before or datetime.now()
        return now - timedelta(minutes=1)

    def get_next_n(self, n: int, after: Optional[datetime] = None) -> List[datetime]:
        results = []
        current = after or datetime.now()
        for _ in range(n):
            current = self.next(after=current)
            results.append(current)
            current = current + timedelta(seconds=1)
        return results


class FallbackCronParser:
    @staticmethod
    def describe(expression: str) -> str:
        parts = expression.split()
        if len(parts) != 5:
            return "无效的Cron表达式"
        return f"Cron表达式: {expression}"


# 预定义常用Cron表达式
PREDEFINED_CRONS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


def parse_predefined(name: str) -> Optional[CronExpression]:
    """解析预定义的Cron表达式"""
    if name in PREDEFINED_CRONS:
        return CronParser.parse(PREDEFINED_CRONS[name])
    return None


def parse_cron(expression: str) -> CronExpression:
    """解析Cron表达式的便捷函数"""
    return CronParser.parse(expression)