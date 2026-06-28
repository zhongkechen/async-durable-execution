from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .primitive.base import OperationContext
    from .serdes import SerDesContext


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


@contextmanager
def bind_current_context(context: OperationContext | SerDesContext):
    """Temporarily bind the supplied durable context while invoking user code."""
    token = set_current_context(context)
    try:
        yield
    finally:
        reset_current_context(token)
