"""Core logging helpers for durable execution contexts."""

from __future__ import annotations

import logging

from .context import OperationContext, _current_context
from .exceptions import ValidationError


class DurableContextFilter(logging.Filter):
    """Add durable execution metadata from the active contextvar to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        context = _current_context.get()
        if context is None:
            return True
        if not hasattr(context, "execution_state"):
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
        # `executionArn` is used here while `durableExecutionArn` is used everywhere else because
        # that's what the Lambda Console expects in log records.
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


def configure_durable_logger(logger):
    """Attach DurableContextFilter to a stdlib-compatible logger and handlers."""
    add_filter = getattr(logger, "addFilter", None)
    filters = getattr(logger, "filters", ())
    if not callable(add_filter):
        return logger

    if not any(isinstance(item, DurableContextFilter) for item in filters):
        add_filter(DurableContextFilter())

    for handler in getattr(logger, "handlers", ()):
        if not any(isinstance(item, DurableContextFilter) for item in handler.filters):
            handler.addFilter(DurableContextFilter())
    return logger


def _is_replaying(context: OperationContext) -> bool:
    if context.execution_state is None:
        raise ValidationError(
            "The execution state is None",
        )
    return bool(context.is_replaying())


__all__ = [
    "DurableContextFilter",
    "build_context_log_extra",
    "configure_durable_logger",
]
