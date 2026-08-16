from __future__ import annotations

import functools
import hashlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from .exceptions import InvalidStateError
from .models import (
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationSubType,
)

if TYPE_CHECKING:
    from .models import LambdaContext
    from .state import ExecutionState


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SerDesContext:
    """Context for serialization operations."""

    operation_id: str = ""
    durable_execution_arn: str = ""
    recursive_level: int = 0


class OperationIdGenerator:
    """Generate deterministic operation ids within a durable execution scope."""

    def __init__(self, prefix: str | None) -> None:
        self._prefix = prefix
        self._counter = 0
        self._claimed_local_ids: set[str] = set()
        self._unconsumed_reservations: dict[str, bool] = {}
        self._unconsumed_checkpoint_count = 0
        self._reservation_selection_started = False

    def increment(self) -> int:
        self._counter += 1
        return self._counter

    def get_current(self) -> int:
        return self._counter

    def _create_id(self, value: str) -> str:
        """Hash one context-local identity value."""
        prefix = self._prefix
        step_id = f"{prefix}-{value}" if prefix else value
        return hashlib.blake2b(step_id.encode()).hexdigest()[:64]

    def _create_id_for_local_id(self, local_id: str) -> str:
        """Generate an id in the caller-provided local-id namespace."""
        return self._create_id(f"local:{local_id}")

    def _create_step_id_for_logical_step(self, step: int) -> str:
        """Generate the stable operation id for a logical step."""
        return self._create_id(str(step))

    def create_step_id(self) -> str:
        """Generate an operation id and advance the logical step counter."""
        return self._create_step_id_for_logical_step(self.increment())

    def create_step_id_for_local_id(self, local_id: str) -> str:
        """Generate an operation id from a stable caller-provided local id."""
        if not isinstance(local_id, str):
            msg = "local_operation_id must be a string"
            raise TypeError(msg)
        if not local_id.strip():
            msg = "local_operation_id must not be blank"
            raise ValueError(msg)
        if self._reservation_selection_started:
            msg = (
                "local_operation_id reservations must be created before any "
                "reserved operation is selected"
            )
            raise RuntimeError(msg)
        if local_id in self._claimed_local_ids:
            msg = f"local_operation_id is already reserved: {local_id}"
            raise ValueError(msg)

        self._claimed_local_ids.add(local_id)
        return self._create_id_for_local_id(local_id)

    def _register_reservation(
        self,
        operation_id: str,
        *,
        has_checkpoint: bool,
    ) -> None:
        """Track an allocated reservation until workflow code selects it."""
        previous = self._unconsumed_reservations.get(operation_id)
        if operation_id in self._unconsumed_reservations:
            if previous == has_checkpoint:
                return
            if previous:
                self._unconsumed_checkpoint_count -= 1
        self._unconsumed_reservations[operation_id] = has_checkpoint
        if has_checkpoint:
            self._unconsumed_checkpoint_count += 1

    def _consume_reservation(self, operation_id: str) -> None:
        """Discard a reservation after workflow code selects it."""
        self._mark_reservation_selected()
        has_checkpoint = self._unconsumed_reservations.pop(operation_id, False)
        if has_checkpoint:
            self._unconsumed_checkpoint_count -= 1

    def _mark_reservation_selected(self) -> None:
        """Prevent explicit local ids from being registered after selection."""
        self._reservation_selection_started = True

    def _has_unconsumed_checkpoint(self) -> bool:
        """Return whether any allocated reservation still has replay history."""
        return self._unconsumed_checkpoint_count > 0


@dataclass(frozen=True)
class OperationContext:
    """Base context shared by durable execution scopes."""

    execution_state: ExecutionState
    operation_identifier: OperationIdentifier

    @property
    def lambda_context(self) -> LambdaContext | None:
        """Return the Lambda context for the active invocation."""
        return self.execution_state.lambda_context

    @property
    def durable_execution_arn(self) -> str:
        """Return the ARN of the durable execution."""
        return self.execution_state.durable_execution_arn

    @property
    def parent_id(self) -> str | None:
        return self.operation_identifier.parent_id

    @property
    def operation_id(self) -> str | None:
        return self.operation_identifier.operation_id

    @property
    def operation_name(self) -> str | None:
        return self.operation_identifier.name

    @property
    def recursive_level(self) -> int:
        """Return the recursion depth recorded on the execution input."""
        return self.execution_state.recursive_level

    def is_replaying(self) -> bool:
        """Return whether the active context is replaying prior user code."""
        return False


@dataclass(frozen=True)
class DurableContext(OperationContext):
    """Runtime context available to a durable handler or child context."""

    step_id_prefix: str | None = None
    replaying: bool = False

    @functools.cached_property
    def step_counter(self) -> OperationIdGenerator:
        return OperationIdGenerator(self.operation_id_generator_prefix)

    @property
    def is_virtual(self) -> bool:
        return self.operation_identifier.parent_id != self.operation_id_generator_prefix

    @property
    def operation_id_generator_prefix(self) -> str | None:
        return (
            self.step_id_prefix
            if self.step_id_prefix is not None
            else self.operation_identifier.parent_id
        )

    def create_child_context(
        self,
        operation_id: str,
        *,
        is_virtual: bool = False,
        replaying: bool | None = None,
    ) -> DurableContext:
        """Create a child context for the given operation."""
        child_parent_id = self.parent_id if is_virtual else operation_id
        logger.debug(
            "Creating child context for operation %s (is_virtual=%s)",
            operation_id,
            is_virtual,
        )
        return DurableContext(
            execution_state=self.execution_state,
            operation_identifier=OperationIdentifier(
                operation_id=None,
                sub_type=OperationSubType.EXECUTION,
                parent_id=child_parent_id,
            ),
            step_id_prefix=operation_id,
            replaying=self.is_replaying() if replaying is None else replaying,
        )

    def is_replaying(self) -> bool:
        """Return True while this context is replaying prior operations."""
        return self.replaying

    def _set_replay_status_new(self) -> None:
        object.__setattr__(self, "replaying", False)

    def _peek_next_operation_id(self) -> str:
        return self.step_counter._create_step_id_for_logical_step(  # noqa: SLF001
            self.step_counter.get_current() + 1
        )

    def _next_operation_result(self) -> Operation | None:
        return self._operation_result(self._peek_next_operation_id())

    def _operation_result(self, operation_id: str) -> Operation | None:
        return self.execution_state.operations.get(operation_id)

    def _next_operation_exists(self) -> bool:
        return self._next_operation_result() is not None

    def _next_reserved_or_sequential_operation_exists(self) -> bool:
        if self.step_counter._has_unconsumed_checkpoint():  # noqa: SLF001
            return True
        return self._next_operation_exists()

    def _next_operation_is_terminal_checkpoint(self) -> bool:
        return self._operation_is_terminal_checkpoint(self._peek_next_operation_id())

    def _operation_is_terminal_checkpoint(self, operation_id: str) -> bool:
        operation = self._operation_result(operation_id)
        if operation is None:
            return False
        return operation.status in {
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.CANCELLED,
            OperationStatus.STOPPED,
            OperationStatus.TIMED_OUT,
        }

    @contextmanager
    def _replay_aware(
        self,
        *,
        operation_id: str | None = None,
        executes_user_code: bool = False,
        consume_reservation: bool = True,
    ) -> Iterator[None]:
        """Update replay status around one durable operation.

        `operation_id` identifies an operation that was allocated before entering
        this scope. Pre-allocated reservations are tracked independently from the
        sequential counter so launch order does not end replay prematurely.
        """
        was_replaying = self.is_replaying()
        current_operation_id = operation_id or self._peek_next_operation_id()
        if operation_id is not None and consume_reservation:
            self.step_counter._consume_reservation(operation_id)  # noqa: SLF001
        current_exists = was_replaying and (
            self._operation_result(current_operation_id) is not None
        )
        current_terminal = was_replaying and self._operation_is_terminal_checkpoint(
            current_operation_id
        )
        flip_after = (
            was_replaying
            and not executes_user_code
            and current_exists
            and not current_terminal
        )

        if was_replaying and (
            not current_exists or (executes_user_code and not current_terminal)
        ):
            self._set_replay_status_new()

        try:
            yield
        finally:
            if flip_after:
                self._set_replay_status_new()
            elif self.is_replaying():
                next_operation_exists = (
                    self._next_reserved_or_sequential_operation_exists()
                    if operation_id is not None
                    else self._next_operation_exists()
                )
                if not next_operation_exists:
                    self._set_replay_status_new()


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
def bind_durable_definition(operation_name: str) -> Iterator[None]:
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


def get_current_context() -> OperationContext | SerDesContext:
    """Return the currently active durable execution context.

    Raises:
        RuntimeError: If called outside supported durable user code.
    """
    ensure_durable_operations_allowed("Durable operations")
    current_context = _current_context.get()
    if current_context is None:
        msg = (
            "get_current_context() can only be used while a durable function, "
            "step function, flow node, wait_for_callback submitter, or "
            "wait_for_condition check, or SerDes operation is executing."
        )
        raise RuntimeError(msg)
    return current_context


def get_durable_context() -> DurableContext:
    """Return the current context after validating durable operations are allowed."""
    current_context = get_current_context()
    if (
        not hasattr(current_context, "execution_state")
        or not hasattr(current_context, "operation_identifier")
        or not hasattr(current_context, "step_counter")
        or not hasattr(current_context, "create_child_context")
    ):
        operation_name = getattr(current_context, "operation_name", None)
        msg = (
            f"{operation_name or 'Durable operations'} can only be used while a "
            "durable function or child context is executing."
        )
        raise RuntimeError(msg)
    return cast(DurableContext, current_context)


@contextmanager
def bind_current_context(
    context: OperationContext | SerDesContext,
) -> Iterator[None]:
    """Temporarily bind the supplied durable context while invoking user code."""
    token = set_current_context(context)
    try:
        yield
    finally:
        reset_current_context(token)
