"""Logging helpers for durable execution contexts."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from async_durable_execution import ValidationError
from .operation.base import OperationContext
from .context import _current_context
from .types import (
    LoggerInterface,
)

if TYPE_CHECKING:
    from .state import ExecutionState
    from .models import OperationIdentifier

_configured_logger_ids: set[int] = set()
_configured_handler_ids: set[int] = set()


@dataclass(frozen=True)
class LogInfo:
    """Structured durable-execution metadata that can be attached to logs."""

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


def build_context_log_extra(context: OperationContext) -> dict[str, object]:
    """Build structured log fields from the active execution context."""
    extra: dict[str, object] = {}
    execution_arn = context.durable_execution_arn
    if execution_arn:
        extra["executionArn"] = execution_arn
    parent_id = context.parent_id
    if parent_id:
        extra["parentId"] = context.parent_id
    operation_id = context.operation_id
    if operation_id:
        extra["operationId"] = context.operation_id
    operation_name = context.operation_name
    if operation_name:
        extra["operationName"] = context.operation_name

    callback_id = getattr(context, "callback_id", None)
    if callback_id:
        extra["callbackId"] = callback_id
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


def _is_replaying(context: OperationContext) -> bool:
    state = context.execution_state
    if state is None:
        raise ValidationError(
            "The execution state is None",
        )
    return bool(state.is_replaying())


__all__ = [
    "DurableContextFilter",
    "LoggerInterface",
    "LogInfo",
    "build_context_log_extra",
    "configure_durable_logger",
]
