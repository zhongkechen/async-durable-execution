"""Task-local public views over journal-owned namespaces."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Generic

from ._types import InvalidStateError, OperationSubType, T

active: ContextVar[Any] = ContextVar("ade.active", default=None)
defining: ContextVar[bool] = ContextVar("ade.defining", default=False)


@dataclass(frozen=True)
class Identity:
    operation_id: str | None
    sub_type: Any = OperationSubType.EXECUTION
    parent_id: str | None = None
    name: str | None = None


@dataclass(frozen=True)
class OperationContext:
    """Metadata shared by handler, operation, and composition context views.

    execution_state and operation_identifier are SDK-managed opaque references.
    Use the public properties to inspect execution and operation identity.
    """

    execution_state: Any
    operation_identifier: Any

    @property
    def operation_id(self):
        """Return the active operation identifier, or None for a scope without one."""
        return self.operation_identifier.operation_id

    @property
    def parent_id(self):
        """Return the parent operation identifier, or None at the execution root."""
        return self.operation_identifier.parent_id

    @property
    def operation_name(self):
        """Return the configured operation name, if any."""
        return self.operation_identifier.name

    @property
    def durable_execution_arn(self):
        """Return the durable execution ARN associated with this context."""
        return self.execution_state.durable_execution_arn

    @property
    def lambda_context(self):
        """Return the underlying Lambda invocation context, when supplied."""
        return self.execution_state.lambda_context

    @property
    def recursive_level(self):
        """Return the recursion depth propagated by recurse(with_recursive_level=True)."""
        return self.execution_state.recursive_level

    def is_replaying(self):
        """Return False for an operation body that is actively executing user code."""
        return False


@dataclass(frozen=True)
class DurableContext(OperationContext):
    """Context in which durable operations may be reserved and composed.

    Obtain this view with get_durable_context() inside a handler or child scope.
    Step and serialization contexts cannot create nested durable operations.

    Attributes:
        execution_state (Any): Opaque SDK-managed execution state.
        operation_identifier (Any): Opaque identity backing the public metadata
            properties.
        step_id_prefix (str | None): Namespace for this scope's operation identities.
        replaying (bool): Initial replay-state snapshot; use is_replaying() for current
            state.
    """

    step_id_prefix: str | None = None
    replaying: bool = False

    @property
    def operation_id_generator_prefix(self):
        """Return this scope's explicit identity namespace, falling back to its parent ID."""
        return (
            self.step_id_prefix if self.step_id_prefix is not None else self.parent_id
        )

    @property
    def is_virtual(self):
        """Return whether the identity namespace differs from the persisted parent scope."""
        return self.operation_id_generator_prefix != self.parent_id

    def is_replaying(self):
        """Return whether this scope is currently consuming previously recorded operations."""
        cursor = self.__dict__.get("_cursor")
        return self.replaying if cursor is None else cursor.replaying

    def create_child_context(self, operation_id, *, is_virtual=False, replaying=None):
        """Create a child context view without scheduling a durable operation.

        Use run_in_child_context() to execute a durable child body. This method only
        constructs its contextual metadata.

        Args:
            operation_id (str): Namespace to assign to the child.
            is_virtual (bool): Keep the enclosing persisted parent when True.
            replaying (bool | None): Initial replay state; None inherits the current
                state.

        Returns:
            (DurableContext): A child view associated with the same durable execution.
        """
        context = DurableContext(
            self.execution_state,
            Identity(None, parent_id=self.parent_id if is_virtual else operation_id),
            operation_id,
            self.is_replaying() if replaying is None else replaying,
        )
        return context


@dataclass(frozen=True)
class StepContext(OperationContext):
    """Metadata visible inside the callable executed by a durable step.

    Read attempt with get_step_context(); nested durable operations are not
    allowed in this scope.

    Attributes:
        attempt (int | None): One-based attempt number, or None if unavailable.
    """

    attempt: int | None = None


@dataclass(frozen=True)
class WaitForConditionCheckContext(StepContext):
    """Step metadata supplied to a wait_for_condition check.

    Use get_wait_for_condition_check_context() to read the current one-based
    attempt and the operation's execution metadata.
    """

    pass


@dataclass(frozen=True)
class WaitForCallbackContext(OperationContext):
    """Metadata provided while the wait_for_callback submitter executes.

    Attributes:
        callback_id (str): Callback identifier to send to the external worker.
    """

    callback_id: str = ""


@dataclass(frozen=True)
class WithRetryContext(DurableContext):
    """Durable context for the current with_retry body attempt.

    The body may compose durable operations, and each retry exposes its attempt
    through get_with_retry_context().

    Attributes:
        attempt (int): One-based attempt number for the retried body.
    """

    attempt: int = 1


@dataclass(frozen=True)
class MapItemContext(DurableContext, Generic[T]):
    """Durable child context describing the current map item.

    Attributes:
        index (int): Zero-based position of this item in the materialized input.
        items (Sequence): Complete map input sequence; treat it as read-only.
    """

    index: int = 0
    items: Any = field(default_factory=tuple)


@dataclass(frozen=True)
class SerDesContext:
    """Identity and operation metadata supplied to codecs and pipeline stages.

    Use get_serdes_context() in a value codec, or the explicit context argument
    in a stage. Runtime serializers populate these fields; standalone pipeline
    calls may receive an otherwise empty context.

    Attributes:
        operation_id (str): Operation whose value is being converted.
        durable_execution_arn (str): Execution that owns the value.
        recursive_level (int): Propagated recursion depth.
        entity_id (str): Durable owner identity, normally operation/{operation_id}.
        operation_name (str | None): Configured operation name.
        parent_id (str | None): Parent operation identifier.
        operation_type (OperationType | None): Backend operation category.
        operation_sub_type (OperationSubType | str | None): SDK or extension operation
            label.
        attempt (int | None): Attempt associated with this value, when available.
        original_value (Any): Original Python value during serialization; None during
            reads.
    """

    operation_id: str = ""
    durable_execution_arn: str = ""
    recursive_level: int = 0
    entity_id: str = ""
    operation_name: str | None = None
    parent_id: str | None = None
    operation_type: Any = None
    operation_sub_type: Any = None
    attempt: int | None = None
    original_value: Any = None


@contextmanager
def binding(context):
    token = active.set(context)
    try:
        yield context
    finally:
        active.reset(token)


def get_current_context():
    """Return the active handler, operation, or serialization context.

    Prefer a specific getter when the expected scope is known.

    Raises:
        RuntimeError: No durable context is active.
        InvalidStateError: Called while a DAG definition is being evaluated.
    """
    if defining.get():
        raise InvalidStateError("Durable operations cannot run while defining a DAG")
    value = active.get()
    if value is None:
        raise RuntimeError("No durable execution context is active")
    return value


def require(kind, function):
    context = get_current_context()
    if not isinstance(context, kind):
        raise RuntimeError(f"{function} cannot be used in this execution context")
    return context


def get_durable_context():
    """Return the active context that can compose durable operations.

    Raises:
        RuntimeError: Called outside a handler or durable child scope, including inside
            a step.
        InvalidStateError: Called while defining a DAG.
    """
    return require(DurableContext, "get_durable_context()")


def get_step_context():
    """Return metadata for the executing step or condition-check attempt.

    Raises:
        RuntimeError: No step-compatible context is active.
    """
    return require(StepContext, "get_step_context()")


def get_serdes_context():
    """Return identity metadata for the active serializer or deserializer.

    Raises:
        RuntimeError: No serialization context is active.
    """
    return require(SerDesContext, "get_serdes_context()")


def get_map_item_context():
    """Return the current map item's index, input sequence, and durable context.

    Raises:
        RuntimeError: Called outside a map item body.
    """
    return require(MapItemContext, "get_map_item_context()")


def get_with_retry_context():
    """Return the current with_retry attempt and durable scope metadata.

    Raises:
        RuntimeError: Called outside a with_retry body.
    """
    return require(WithRetryContext, "get_with_retry_context()")


def get_wait_for_callback_context():
    """Return the callback identifier and metadata for the active submitter.

    Raises:
        RuntimeError: Called outside a wait_for_callback submitter.
    """
    return require(WaitForCallbackContext, "get_wait_for_callback_context()")


def get_wait_for_condition_check_context():
    """Return the attempt and metadata for the active condition check.

    Raises:
        RuntimeError: Called outside a wait_for_condition check.
    """
    return require(
        WaitForConditionCheckContext, "get_wait_for_condition_check_context()"
    )


class ReplayFilter(logging.Filter):
    def filter(self, record):
        context = active.get()
        if not isinstance(context, OperationContext):
            return True
        if context.is_replaying():
            return False
        for name, value in {
            "executionArn": context.durable_execution_arn,
            "operationId": context.operation_id,
            "parentId": context.parent_id,
            "operationName": context.operation_name,
            "attempt": getattr(context, "attempt", None),
            "callbackId": getattr(context, "callback_id", None),
        }.items():
            if value is not None and not hasattr(record, name):
                setattr(record, name, value)
        return True


def install_logging():
    root = logging.getLogger()
    for logger in (root, *root.handlers):
        if not any(isinstance(f, ReplayFilter) for f in logger.filters):
            logger.addFilter(ReplayFilter())
