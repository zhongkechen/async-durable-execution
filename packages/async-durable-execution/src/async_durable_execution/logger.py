"""Logging helpers for durable execution contexts."""

from __future__ import annotations

import logging
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import TYPE_CHECKING

from async_durable_execution.types import (
    Context,
    LoggerInterface,
    StepContext,
    WaitForCallbackContext,
    WaitForConditionCheckContext,
)


if TYPE_CHECKING:
    from async_durable_execution.context import ExecutionState
    from async_durable_execution.models import OperationIdentifier


_current_context: ContextVar[Context | None] = ContextVar(
    "async_durable_execution.current_context",
    default=None,
)
_configured_logger_ids: set[int] = set()
_configured_handler_ids: set[int] = set()


@dataclass(frozen=True)
class LogInfo:
    execution_state: ExecutionState
    parent_id: str | None = None
    operation_id: str | None = None
    name: str | None = None
    attempt: int | None = None

    @classmethod
    def from_operation_identifier(
        cls,
        execution_state: ExecutionState,
        op_id: OperationIdentifier,
        attempt: int | None = None,
    ) -> LogInfo:
        return cls(
            execution_state=execution_state,
            parent_id=op_id.parent_id,
            operation_id=op_id.operation_id,
            name=op_id.name,
            attempt=attempt,
        )

    def with_parent_id(self, parent_id: str) -> LogInfo:
        return LogInfo(
            execution_state=self.execution_state,
            parent_id=parent_id,
            operation_id=self.operation_id,
            name=self.name,
            attempt=self.attempt,
        )


class DurableContextFilter(logging.Filter):
    """Add durable execution metadata from the active contextvar to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        context = _current_context.get()
        if context is None:
            return True

        if _is_replaying(context):
            return False

        for key, value in build_context_log_extra(context).items():
            if value is not None and not hasattr(record, key):
                setattr(record, key, value)
        return True


def build_context_log_extra(context: Context) -> dict[str, object]:
    """Build structured log fields from the active execution context."""
    extra: dict[str, object] = {}
    execution_arn = getattr(context, "execution_arn", None)
    if execution_arn:
        extra["executionArn"] = context.execution_arn
    parent_id = getattr(context, "parent_id", None)
    if parent_id:
        extra["parentId"] = context.parent_id
    operation_id = getattr(context, "operation_id", None)
    if operation_id:
        extra["operationId"] = context.operation_id
    operation_name = getattr(context, "operation_name", None)
    if operation_name:
        extra["operationName"] = context.operation_name
    if isinstance(context, WaitForCallbackContext):
        extra["callbackId"] = context.callback_id
    attempt = getattr(context, "attempt", None)
    if attempt is not None:
        extra["attempt"] = attempt
    return extra


def configure_durable_logger(logger: LoggerInterface) -> LoggerInterface:
    """Attach DurableContextFilter to a stdlib-compatible logger and handlers."""
    add_filter = getattr(logger, "addFilter", None)
    filters = getattr(logger, "filters", ())
    if not callable(add_filter):
        return logger

    logger_id = id(logger)
    if logger_id not in _configured_logger_ids and not any(
        isinstance(item, DurableContextFilter) for item in filters
    ):
        add_filter(DurableContextFilter())
        _configured_logger_ids.add(logger_id)

    for handler in getattr(logger, "handlers", ()):
        handler_id = id(handler)
        if handler_id in _configured_handler_ids:
            continue
        if not any(isinstance(item, DurableContextFilter) for item in handler.filters):
            handler.addFilter(DurableContextFilter())
        _configured_handler_ids.add(handler_id)
    return logger


def set_current_context(context: Context) -> Token[Context | None]:
    return _current_context.set(context)


def reset_current_context(token: Token[Context | None]) -> None:
    _current_context.reset(token)


def get_current_context() -> Context | None:
    return _current_context.get()


def _is_replaying(context: Context) -> bool:
    state = getattr(context, "state", None)
    if state is None and isinstance(
        context,
        (StepContext, WaitForCallbackContext, WaitForConditionCheckContext),
    ):
        state = context.execution_state
    if state is None:
        return False
    return bool(state.is_replaying())


__all__ = [
    "DurableContextFilter",
    "LoggerInterface",
    "LogInfo",
    "build_context_log_extra",
    "configure_durable_logger",
]
