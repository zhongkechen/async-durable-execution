"""Implementation for the backend-supported create_callback operation."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Generic, TypeVar

from .base import OperationExecutor
from .._core import (
    CallbackOptions,
    CallbackTimeoutType,
    Duration,
    DurableContext,
    ExecutionError,
    ExecutionState,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationSubType,
    OperationUpdate,
    PassThroughSerDes,
    SerDes,
    SuspendExecution,
    TerminationReason,
    _register_sdk_control_error_type,
    create_eager_task,
    deserialize,
    duration_to_seconds,
    ensure_durable_operations_allowed,
    get_durable_context,
)

T = TypeVar("T")  # Result type

logger = logging.getLogger(__name__)

PASS_THROUGH_SERDES: SerDes[Any] = PassThroughSerDes()
_LEGACY_CALLBACK_ERROR_TYPE_NAMES = (
    "async_durable_execution.exceptions.CallbackError",
    "async_durable_execution.primitive.callback.CallbackError",
)


class CallbackError(ExecutionError):
    """Error in callback handling."""

    def __init__(self, message: str, callback_id: str | None = None) -> None:
        super().__init__(message, TerminationReason.CALLBACK_ERROR)
        self.callback_id = callback_id


def _encode_callback_error_payload(error: ExecutionError) -> str | None:
    if not isinstance(error, CallbackError):
        msg = "CallbackError codec received an incompatible exception."
        raise TypeError(msg)
    return error.callback_id


def _restore_callback_error(
    message: str,
    payload: str | None,
) -> CallbackError:
    return CallbackError(message=message, callback_id=payload)


_register_sdk_control_error_type(
    CallbackError,
    encode_payload=_encode_callback_error_payload,
    restore=_restore_callback_error,
    legacy_exception_type_names=_LEGACY_CALLBACK_ERROR_TYPE_NAMES,
)


class CallbackOperationExecutor(OperationExecutor[str]):
    """Executor for callback operations."""

    def __init__(
        self,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        timeout: Duration | None = None,
        heartbeat_timeout: Duration | None = None,
    ) -> None:
        """Initialize the callback operation executor.

        Args:
            state: The execution state
            operation_identifier: The operation identifier
            timeout: Optional maximum time to wait for callback completion.
            heartbeat_timeout: Optional maximum time to wait between callback heartbeats.
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.timeout_seconds = (
            duration_to_seconds(timeout, "timeout") if timeout is not None else 0
        )
        self.heartbeat_timeout_seconds = (
            duration_to_seconds(heartbeat_timeout, "heartbeat_timeout")
            if heartbeat_timeout is not None
            else 0
        )

    async def start(self) -> str:
        """Start a new callback operation."""
        callback_options = CallbackOptions(
            timeout_seconds=self.timeout_seconds,
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
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

    async def execute(self, operation: Operation | None) -> str:
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
        if operation is None or not operation.callback_details:
            msg = f"Missing callback details for operation: {self.operation_identifier.operation_id}"
            raise CallbackError(msg)

        return operation.callback_details.callback_id


def create_callback(
    *,
    name: str | None = None,
    timeout: Duration | None = None,
    heartbeat_timeout: Duration | None = None,
    serdes: SerDes | None = None,
) -> asyncio.Task[Callback]:
    """Create a durable callback handle that external systems can complete later.

    Args:
        name: Optional durable operation name.
        timeout: Optional maximum time to wait for callback completion.
        heartbeat_timeout: Optional maximum time to wait between callback heartbeats.
        serdes: Optional serializer for callback results.
    """
    ensure_durable_operations_allowed("create_callback()")
    context = get_durable_context()

    with context._replay_aware():
        operation_id: str = context.step_counter.create_step_id()
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.CALLBACK,
            parent_id=context.parent_id,
            name=name,
        )

        return create_eager_task(
            lambda: _create_callback(
                context=context,
                operation_identifier=operation_identifier,
                operation_id=operation_id,
                timeout=timeout,
                heartbeat_timeout=heartbeat_timeout,
                serdes=serdes,
            ),
        )


async def _create_callback(
    *,
    context: DurableContext,
    operation_identifier: OperationIdentifier,
    operation_id: str,
    timeout: Duration | None = None,
    heartbeat_timeout: Duration | None = None,
    serdes: SerDes | None = None,
) -> Callback:
    executor: CallbackOperationExecutor = CallbackOperationExecutor(
        state=context.execution_state,
        operation_identifier=operation_identifier,
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


class Callback(Generic[T]):
    """A future that will block on result() until callback_id returns."""

    def __init__(
        self,
        callback_id: str,
        operation_id: str,
        state: ExecutionState,
        serdes: SerDes[T] | None = None,
    ) -> None:
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
                return None

            return await deserialize(
                serdes=self.serdes if self.serdes is not None else PASS_THROUGH_SERDES,
                data=operation.callback_details.result,
                operation_id=self.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
                recursive_level=self.state.recursive_level,
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
