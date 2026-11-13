"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: cost.py
@Description: 任务成本上报上下文 - 让 executor 中的业务代码把 token/API 成本
             回传给 Worker。Task cost reporting context so user task code can
             attach cost metadata to the task being executed.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import contextvars
from typing import Any, Dict, Optional

# 当前任务绑定的成本累加器；由 WorkerPool 在执行任务前设置。
_current_cost_recorder: "contextvars.ContextVar[Optional[CostRecorder]]" = (
    contextvars.ContextVar("neotask_cost_recorder", default=None)
)


class CostRecorder:
    """绑定到单次执行的累加器

    业务代码通常不需要直接创建它，只需在 executor 中调用
    :func:`report_cost`；显式形态见 ``pool.report_cost(task_id, ...)``。
    多次上报按字段累加，币种以首个上报为准，出现冲突币种时标记为 ``MIXED``。
    """

    def __init__(self) -> None:
        self._totals: Dict[str, Any] = {
            "tokens_in": 0,
            "tokens_out": 0,
            "api_calls": 0,
            "estimated_cost": 0.0,
            "currency": "",
        }
        self._reported = False

    def add(
            self,
            tokens_in: int = 0,
            tokens_out: int = 0,
            api_calls: int = 0,
            estimated_cost: Optional[float] = None,
            currency: Optional[str] = None,
    ) -> None:
        """累加一次上报"""
        self._reported = True
        self._totals["tokens_in"] += int(tokens_in or 0)
        self._totals["tokens_out"] += int(tokens_out or 0)
        self._totals["api_calls"] += int(api_calls or 0)
        if estimated_cost is not None:
            self._totals["estimated_cost"] += float(estimated_cost)

        if currency:
            current = self._totals["currency"]
            if not current:
                self._totals["currency"] = currency
            elif current != currency:
                self._totals["currency"] = "MIXED"

    @property
    def reported(self) -> bool:
        """本次执行是否至少上报过一次成本"""
        return self._reported

    def payload(self) -> Dict[str, Any]:
        """成本明细快照；未上报时返回空字典"""
        if not self._reported:
            return {}
        return dict(self._totals)


def set_cost_recorder(recorder: CostRecorder) -> contextvars.Token:
    """绑定当前任务的成本累加器（内部 API，由 WorkerPool 调用）"""
    return _current_cost_recorder.set(recorder)


def reset_cost_recorder(token: contextvars.Token) -> None:
    """恢复绑定前的成本累加器（内部 API，由 WorkerPool 调用）"""
    _current_cost_recorder.reset(token)


def get_cost_recorder() -> Optional[CostRecorder]:
    """获取当前任务绑定的成本累加器，没有则返回 None"""
    return _current_cost_recorder.get()


def report_cost(
        tokens_in: int = 0,
        tokens_out: int = 0,
        api_calls: int = 0,
        estimated_cost: Optional[float] = None,
        currency: Optional[str] = None,
) -> bool:
    """在异步/线程执行器中上报任务成本

    Args:
        tokens_in: 本次消耗的输入 token 数
        tokens_out: 本次消耗的输出 token 数
        api_calls: 本次调用的外部 API 次数
        estimated_cost: 本次产生的费用金额；为 None 时只累计其它字段
        currency: 币种标识，如 ``USD``；同一任务内出现不同币种会标记为 ``MIXED``

    Returns:
        是否成功登记。任务不在 Worker 执行上下文中时返回 ``False``。

    Note:
        进程执行器（``ProcessExecutor``）运行在独立进程中，拿不到该上下文，
        请在父进程使用 ``pool.report_cost(task_id, ...)``。
    """
    recorder = _current_cost_recorder.get()
    if recorder is None:
        return False
    recorder.add(
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        api_calls=api_calls,
        estimated_cost=estimated_cost,
        currency=currency,
    )
    return True
