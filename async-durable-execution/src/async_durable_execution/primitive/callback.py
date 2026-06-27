"""Implementation for the backend-supported create_callback operation."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from .child import get_durable_context
from ..config import duration_to_seconds
from ..exceptions import ExecutionError, SuspendExecution, TerminationReason
from ..models import (
    CallbackOptions,
    CallbackTimeoutType,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationUpdate,
    OperationSubType,
)
from .base import (
    CheckpointedResult,
    OperationExecutor,
    get_checkpoint_result,
)
from ..serdes import deserialize, SerDes, PassThroughSerDes

if TYPE_CHECKING:
    from ..serdes import SerDes
    from ..state import ExecutionState

T = TypeVar("T")  # Result type

logger = logging.getLogger(__name__)

PASS_THROUGH_SERDES: SerDes[Any] = PassThroughSerDes()


class CallbackError(ExecutionError):
    """Error in callback handling."""

    def __init__(self, message: str, callback_id: str | None = None):
        super().__init__(message, TerminationReason.CALLBACK_ERROR)
        self.callback_id = callback_id


class CallbackOperationExecutor(OperationExecutor[str]):
    """Executor for callback operations."""

    def __init__(
        self,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
    ):
        """Initialize the callback operation executor.

        Args:
            state: The execution state
            operation_identifier: The operation identifier
            timeout: Optional maximum time to wait for callback completion.
            heartbeat_timeout: Optional maximum time to wait between callback heartbeats.
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.timeout = timeout
        self.heartbeat_timeout = heartbeat_timeout

        if timeout is not None:
            duration_to_seconds(timeout, "timeout")
        if heartbeat_timeout is not None:
            duration_to_seconds(heartbeat_timeout, "heartbeat_timeout")

    async def start(self) -> str:
        """Start a new callback operation."""
        callback_options = CallbackOptions(
            timeout_seconds=duration_to_seconds(self.timeout, "timeout")
            if self.timeout is not None
            else 0,
            heartbeat_timeout_seconds=duration_to_seconds(
                self.heartbeat_timeout, "heartbeat_timeout"
            )
            if self.heartbeat_timeout is not None
            else 0,
        )

        create_callback_operation: OperationUpdate = OperationUpdate.create_callback(
            identifier=self.operation_identifier,
            callback_options=callback_options,
        )

        operation = await self.create_checkpoint(create_callback_operation)

        if not operation:
            msg = f"Missing callback details for operation: {self.operation_identifier.operation_id}"
            raise CallbackError(msg)
        return await self.replay(operation)

    async def replay(self, operation: Operation) -> str:
        """Replay an existing callback operation from its checkpoint."""
        if not operation.callback_details:
            msg = (
                f"Missing callback details for operation: "
                f"{self.operation_identifier.operation_id}"
            )
            raise CallbackError(msg)

        return await self.execute(operation)

    async def execute(self, operation: Operation) -> str:  # type: ignore[override]
        """Execute callback operation by extracting the callback_id.

        Callbacks don't execute logic - they just extract and return the callback_id
        from the operation data.

        Args:
            operation: The callback operation containing callback_details

        Returns:
            The callback_id from the checkpoint

        Raises:
            CallbackError: If callback_details are missing (should never happen)
        """
        if not operation.callback_details:
            msg = f"Missing callback details for operation: {self.operation_identifier.operation_id}"
            raise CallbackError(msg)

        return operation.callback_details.callback_id


async def create_callback(
    *,
    name: str | None = None,
    timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    serdes: SerDes | None = None,
) -> Callback:
    """Create a durable callback handle that external systems can complete later.

    Args:
        name: Optional durable operation name.
        timeout: Optional maximum time to wait for callback completion.
        heartbeat_timeout: Optional maximum time to wait between callback heartbeats.
        serdes: Optional serializer for callback results.
    """
    context = get_durable_context("create_callback")
    with context._replay_aware():
        operation_id: str = context.step_counter.create_step_id()

        executor: CallbackOperationExecutor = CallbackOperationExecutor(
            state=context.execution_state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                sub_type=OperationSubType.CALLBACK,
                parent_id=context.parent_id,
                name=name,
            ),
            timeout=timeout,
            heartbeat_timeout=heartbeat_timeout,
        )
        callback_id: str = await executor.process()
        return Callback(
            callback_id=callback_id,
            operation_id=operation_id,
            state=context.execution_state,
            serdes=serdes,
        )


class Callback(Generic[T]):  # noqa: PYI059
    """A future that will block on result() until callback_id returns."""

    def __init__(
        self,
        callback_id: str,
        operation_id: str,
        state: ExecutionState,
        serdes: SerDes[T] | None = None,
    ):
        self.callback_id: str = callback_id
        self.operation_id: str = operation_id
        self.state: ExecutionState = state
        self.serdes: SerDes[T] | None = serdes

    async def result(self) -> T | None:
        """Return the result of the future. Will block until result is available.

        This will suspend the current execution while waiting for the result to
        become available. Durable Functions will replay the execution once the
        result is ready, and proceed when it reaches the .result() call.

        Use the callback id with the following APIs to send back the result, error or
        heartbeats: SendDurableExecutionCallbackSuccess, SendDurableExecutionCallbackFailure
        and SendDurableExecutionCallbackHeartbeat.
        """
        operation = self.state.operations.get(self.operation_id)

        if not isinstance(operation, Operation):
            msg = "Callback operation must exist"
            raise CallbackError(message=msg, callback_id=self.callback_id)

        if operation.status in {
            OperationStatus.FAILED,
            OperationStatus.CANCELLED,
            OperationStatus.TIMED_OUT,
            OperationStatus.STOPPED,
        }:
            msg = _format_callback_error_message(operation)
            raise CallbackError(message=msg, callback_id=self.callback_id)

        if operation.status is OperationStatus.SUCCEEDED:
            if (
                not operation.callback_details
                or operation.callback_details.result is None
            ):
                return None  # type: ignore

            return await deserialize(
                serdes=self.serdes if self.serdes is not None else PASS_THROUGH_SERDES,
                data=operation.callback_details.result,
                operation_id=self.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )

        # operation exists; it has not terminated (successfully or otherwise)
        # therefore we should wait
        msg = "Callback result not received yet. Suspending execution while waiting for result."
        raise SuspendExecution(msg)


def _format_callback_error_message(operation: Operation) -> str:
    """Build a stable callback error message from checkpoint state."""
    error = operation.callback_details.error if operation.callback_details else None
    if not error or not error.message:
        return "Callback failed"

    message = error.message
    if (
        operation.status is OperationStatus.TIMED_OUT
        and error.type in {timeout.value for timeout in CallbackTimeoutType}
        and error.type not in message
    ):
        return f"{message}: {error.type}"

    return message
