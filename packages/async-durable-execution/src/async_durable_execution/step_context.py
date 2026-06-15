from __future__ import annotations

from contextvars import ContextVar, Token

from async_durable_execution.types import StepContext


_current_step_context: ContextVar[StepContext | None] = ContextVar(
    "async_durable_execution.current_step_context",
    default=None,
)


def get_step_context() -> StepContext:
    """Return the StepContext for the currently executing step."""
    step_context = _current_step_context.get()
    if step_context is None:
        msg = "get_step_context() can only be used while a step function is executing."
        raise RuntimeError(msg)
    return step_context


def _set_step_context(step_context: StepContext) -> Token[StepContext | None]:
    return _current_step_context.set(step_context)


def _reset_step_context(token: Token[StepContext | None]) -> None:
    _current_step_context.reset(token)
