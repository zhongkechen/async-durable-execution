from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

from .exceptions import InvalidStateError

if TYPE_CHECKING:
    from .primitive.base import OperationContext
    from .serdes import SerDesContext


_current_context: ContextVar = ContextVar(
    "async_durable_execution.current_context",
    default=None,
)
_durable_definition_operation: ContextVar[str | None] = ContextVar(
    "async_durable_execution.durable_definition_operation",
    default=None,
)


def ensure_durable_operations_allowed(operation_name: str) -> None:
    """Reject durable operation creation during a synchronous definition phase."""
    definition_operation = _durable_definition_operation.get()
    if definition_operation is None:
        return

    msg = (
        f"{operation_name} cannot be used while defining a "
        f"{definition_operation}. Durable operations may only run after the "
        "definition has been validated."
    )
    raise InvalidStateError(msg)


@contextmanager
def bind_durable_definition(operation_name: str):
    """Mark a synchronous definition phase in the current context."""
    token = _durable_definition_operation.set(operation_name)
    try:
        yield
    finally:
        _durable_definition_operation.reset(token)


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
    ensure_durable_operations_allowed("Durable operations")
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
