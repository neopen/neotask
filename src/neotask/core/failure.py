"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: failure.py
@Description: 业务级失败登记上下文 - 让 executor 中的业务代码将"业务判定不通过"
             的原因回传给 Worker。Business failure recording context so user task
             code can report a business-level failure reason to the worker.
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026-11-12
"""

import contextvars
from typing import Optional

#: 当前执行绑定的失败登记器；由 WorkerPool 在执行任务前设置。
_current_failure_recorder: "contextvars.ContextVar[Optional[BusinessFailure]]" = (
    contextvars.ContextVar("neotask_failure_recorder", default=None)
)


class BusinessFailure:
    """一次任务执行的失败登记簿

    只做登记，不立刻终结任务：业务函数返回后由 Worker 按登记的原因收尾。
    这样 async / thread 执行器都能工作，无需依赖中断执行的能力；进程执行器
    在独立进程中运行，拿不到本上下文，只能使用 ``pool.report_failure(task_id, ...)``
    显式形态。

    首次登记生效，后续调用不覆盖原因，避免任务尾部的重复上报改写首条判定。
    """

    def __init__(self) -> None:
        self._reason: Optional[str] = None
        self._detail: str = ""

    def record(self, reason: str, detail: str = "") -> None:
        """登记失败原因

        Args:
            reason: 业务级原因标识，如 ``qa_failed``
            detail: 可选的补充说明
        """
        if self._reason is not None:
            return
        self._reason = reason or "business_failure"
        self._detail = detail or ""

    @property
    def reason(self) -> Optional[str]:
        """已登记的业务原因；未登记时为 ``None``"""
        return self._reason

    @property
    def triggered(self) -> bool:
        """本次执行是否登记过业务失败"""
        return self._reason is not None

    @property
    def message(self) -> str:
        """写入 ``error`` 字段的文本：有补充说明时拼在原因后"""
        if self._reason is None:
            return ""
        return f"{self._reason}: {self._detail}" if self._detail else self._reason


def set_failure_recorder(recorder: BusinessFailure) -> contextvars.Token:
    """绑定当前任务的失败登记器（内部 API，由 WorkerPool 调用）"""
    return _current_failure_recorder.set(recorder)


def reset_failure_recorder(token: contextvars.Token) -> None:
    """恢复绑定前的失败登记器（内部 API，由 WorkerPool 调用）"""
    _current_failure_recorder.reset(token)


def get_failure_recorder() -> Optional[BusinessFailure]:
    """获取当前任务绑定的失败登记器，没有则返回 None"""
    return _current_failure_recorder.get()


def report_failure(reason: str, detail: str = "") -> bool:
    """在任务执行体内登记业务级失败

    调用后任务函数继续执行到自然返回，Worker 收尾时按登记的原因把任务判为
    ``FAILED``，并**跳过自动重试**：业务判定失败重试通常没有意义，只会重复消耗
    同样的输入与成本。系统异常导致的失败仍走原有重试链路。

    同步函数，无需 ``await``；在异步任务与线程执行器中的同步任务里都可用
    （``ThreadExecutor`` 会显式复制 contextvars）。进程执行器拿不到该上下文。

    Args:
        reason: 业务失败原因标识，如 ``qa_failed``
        detail: 可选补充说明，会拼进任务的 ``error`` 字段

    Returns:
        是否成功登记。不在 Worker 执行上下文中时返回 False。

    Example:
        >>> from neotask import report_failure
        >>> async def review(data):
        ...     if data["score"] < 60:
        ...         report_failure("qa_failed", f"score={data['score']}")
        ...         return {"score": data["score"]}
    """
    recorder = _current_failure_recorder.get()
    if recorder is None:
        return False
    recorder.record(reason, detail)
    return True
