from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from .primitive.base import OperationContext

T = TypeVar("T")


_current_context: ContextVar = ContextVar(
    "async_durable_execution.current_context",
    default=None,
)


def set_current_context(context) -> Token:
    """Bind the active durable context for the current async task."""
    return _current_context.set(context)


def reset_current_context(token: Token) -> None:
    """Restore the previous durable context after a temporary override."""
    _current_context.reset(token)


def get_current_context():
    """Return the currently active durable execution context.

    Raises:
        RuntimeError: If called outside a durable handler, step, callback submitter,
            or wait-for-condition checker.
    """
    current_context = _current_context.get()
    if current_context is None:
        msg = (
            "get_current_context() can only be used while a durable function, "
            "step function, wait_for_callback submitter, or "
            "wait_for_condition check, or SerDes operation is executing."
        )
        raise RuntimeError(msg)
    return current_context


async def invoke_user_callable(
    context: OperationContext,
    func: Callable[..., Awaitable[T]],
    *args,
    **kwargs,
) -> T:
    """Invoke user code while temporarily binding the supplied durable context."""
    token = set_current_context(context)
    try:
        return await func(*args, **kwargs)
    finally:
        reset_current_context(token)
