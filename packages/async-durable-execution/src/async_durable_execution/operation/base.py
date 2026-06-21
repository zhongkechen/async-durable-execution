"""Base classes and shared helpers for operation executors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from ..exceptions import CallableRuntimeError
from ..models import (
    ErrorObject,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationType,
)
from ..serdes import SerDes, deserialize, serialize


if TYPE_CHECKING:
    import datetime
    from ..models import OperationUpdate
    from ..state import ExecutionState

T = TypeVar("T")
S = TypeVar("S")


@dataclass(frozen=True)
class OperationContext:
    """Protocol defining the interface for durable execution contexts."""

    execution_state: ExecutionState
    operation_identifier: OperationIdentifier

    @property
    def durable_execution_arn(self) -> str:
        """Get the ARN of the Durable Execution."""
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


@dataclass(frozen=True)
class CheckpointedResult:
    """Result of a checkpointed operation."""

    operation: Operation | None = None
    status: OperationStatus | None = None
    result: str | None = None
    error: ErrorObject | None = None

    @classmethod
    def create_from_operation(cls, operation: Operation) -> CheckpointedResult:
        """Create a result from an operation."""
        result: str | None = None
        error: ErrorObject | None = None
        match operation.operation_type:
            case OperationType.STEP:
                step_details = operation.step_details
                result = step_details.result if step_details else None
                error = step_details.error if step_details else None
            case OperationType.CALLBACK:
                callback_details = operation.callback_details
                result = callback_details.result if callback_details else None
                error = callback_details.error if callback_details else None
            case OperationType.CHAINED_INVOKE:
                invoke_details = operation.chained_invoke_details
                result = invoke_details.result if invoke_details else None
                error = invoke_details.error if invoke_details else None
            case OperationType.CONTEXT:
                context_details = operation.context_details
                result = context_details.result if context_details else None
                error = context_details.error if context_details else None

        return cls(
            operation=operation,
            status=operation.status,
            result=result,
            error=error,
        )

    @classmethod
    def create_not_found(cls) -> CheckpointedResult:
        """Create a result when the checkpoint was not found."""
        return cls(operation=None)

    def is_existent(self) -> bool:
        """Return true if a checkpoint of any type exists."""
        return self.operation is not None

    def is_succeeded(self) -> bool:
        """Return True if the checkpointed operation is SUCCEEDED."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.SUCCEEDED

    def is_cancelled(self) -> bool:
        if op := self.operation:
            return op.status is OperationStatus.CANCELLED
        return False

    def is_failed(self) -> bool:
        """Return True if the checkpointed operation is FAILED."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.FAILED

    def is_stopped(self) -> bool:
        """Return True if the checkpointed operation is STOPPED."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.STOPPED

    def is_started(self) -> bool:
        """Return True if the checkpointed operation is STARTED."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.STARTED

    def is_started_or_ready(self) -> bool:
        """Return True if the checkpointed operation is STARTED or READY."""
        op = self.operation
        if not op:
            return False
        return op.status in {OperationStatus.STARTED, OperationStatus.READY}

    def is_pending(self) -> bool:
        """Return True if the checkpointed operation is PENDING."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.PENDING

    def is_ready(self) -> bool:
        """Return True if the checkpointed operation is READY."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.READY

    def is_timed_out(self) -> bool:
        """Return True if the checkpointed operation is TIMED_OUT."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.TIMED_OUT

    def is_replay_children(self) -> bool:
        op = self.operation
        if not op:
            return False
        return op.context_details.replay_children if op.context_details else False

    def raise_callable_error(self, msg: str | None = None) -> None:
        if self.error is None:
            err_msg = (
                msg
                or "Unknown error. No ErrorObject exists on the Checkpoint Operation."
            )
            raise CallableRuntimeError(
                message=err_msg,
                error_type=None,
                data=None,
                stack_trace=None,
            )

        raise self.error.to_callable_runtime_error()

    def get_next_attempt_timestamp(self) -> datetime.datetime | None:
        if self.operation and self.operation.step_details:
            return self.operation.step_details.next_attempt_timestamp
        return None


CHECKPOINT_NOT_FOUND = CheckpointedResult.create_not_found()


def get_checkpoint_result(
    state: ExecutionState,
    checkpoint_id: str,
) -> CheckpointedResult:
    """Get a checkpoint result from execution state."""
    checkpoint = state.operations.get(checkpoint_id)
    if isinstance(checkpoint, Operation):
        return CheckpointedResult.create_from_operation(checkpoint)

    if checkpoint is None:
        return CHECKPOINT_NOT_FOUND

    return checkpoint


class OperationExecutor(ABC, Generic[T]):
    """Base class for durable operations with shared state and serdes helpers."""

    def __init__(
        self,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
    ) -> None:
        self.state = state
        self.operation_identifier = operation_identifier

    @property
    def operation_id(self) -> str:
        """Return the required operation id for this executor."""
        return self.operation_identifier.require_operation_id()

    @property
    def operation_name(self) -> str | None:
        """Return the human-readable operation name, if provided."""
        return self.operation_identifier.name

    @property
    def durable_execution_arn(self) -> str:
        """Return the durable execution ARN for serialization helpers."""
        return self.state.durable_execution_arn

    def get_checkpointed_result(self) -> CheckpointedResult:
        """Load the current checkpoint state for this operation."""
        return get_checkpoint_result(self.state, self.operation_id)

    async def create_checkpoint(
        self,
        operation_update: OperationUpdate,
        *,
        is_sync: bool | None = None,
    ) -> None:
        """Persist a checkpoint update for this operation."""
        if is_sync is None:
            await self.state.create_checkpoint(
                operation_update=operation_update,
            )
            return

        await self.state.create_checkpoint(
            operation_update=operation_update,
            is_sync=is_sync,
        )

    def serialize_value(self, value: S, serdes: SerDes[S] | None) -> str:
        """Serialize a value using operation-scoped metadata."""
        return serialize(
            serdes=serdes,
            value=value,
            operation_id=self.operation_id,
            durable_execution_arn=self.durable_execution_arn,
        )

    def deserialize_value(self, data: str, serdes: SerDes[S] | None) -> S:
        """Deserialize a value using operation-scoped metadata."""
        return deserialize(
            serdes=serdes,
            data=data,
            operation_id=self.operation_id,
            durable_execution_arn=self.durable_execution_arn,
        )

    @abstractmethod
    async def execute(self, checkpointed_result: CheckpointedResult) -> T:
        """Execute operation logic with checkpoint data."""
        ...  # pragma: no cover

    @abstractmethod
    async def start(self) -> T:
        """Start a new operation with no existing checkpoint."""
        ...  # pragma: no cover

    @abstractmethod
    async def replay(self, operation: Operation) -> T:
        """Replay an operation from an existing checkpoint."""
        ...  # pragma: no cover

    async def process(self) -> T:
        """Process the operation, including replay and checkpoint handling."""
        checkpointed_result = self.get_checkpointed_result()
        if not checkpointed_result.is_existent():
            return await self.start()
        if checkpointed_result.operation is None:
            msg = f"Missing checkpoint operation for replay: {self.operation_id}"
            raise CallableRuntimeError(
                message=msg,
                error_type=None,
                data=None,
                stack_trace=None,
            )
        return await self.replay(checkpointed_result.operation)
