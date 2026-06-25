"""Implementation for the backend-supported create_callback operation."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ..models import CallbackTimeoutType

from .child import _get_durable_context
from ..config import duration_to_seconds
from ..exceptions import ExecutionError, SuspendExecution, TerminationReason
from ..models import (
    CallbackOptions,
    Operation,
    OperationIdentifier,
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


@dataclass(frozen=True)
class CallbackConfig:
    """Configuration for callbacks."""

    timeout: timedelta = field(default_factory=timedelta)
    heartbeat_timeout: timedelta = field(default_factory=timedelta)
    serdes: SerDes | None = None

    def __post_init__(self):
        duration_to_seconds(self.timeout, "timeout")
        duration_to_seconds(self.heartbeat_timeout, "heartbeat_timeout")

    @property
    def timeout_seconds(self) -> int:
        """Get timeout in seconds."""
        return duration_to_seconds(self.timeout, "timeout")

    @property
    def heartbeat_timeout_seconds(self) -> int:
        """Get heartbeat timeout in seconds."""
        return duration_to_seconds(self.heartbeat_timeout, "heartbeat_timeout")


class CallbackOperationExecutor(OperationExecutor[str]):
    """Executor for callback operations."""

    def __init__(
        self,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        config: CallbackConfig | None,
    ):
        """Initialize the callback operation executor.

        Args:
            state: The execution state
            operation_identifier: The operation identifier
            config: The callback configuration (optional)
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.config = config

    async def start(self) -> str:
        """Start a new callback operation."""
        callback_options: CallbackOptions = (
            CallbackOptions(
                timeout_seconds=self.config.timeout_seconds,
                heartbeat_timeout_seconds=self.config.heartbeat_timeout_seconds,
            )
            if self.config
            else CallbackOptions()
        )

        create_callback_operation: OperationUpdate = OperationUpdate.create_callback(
            identifier=self.operation_identifier,
            callback_options=callback_options,
        )

        await self.create_checkpoint(create_callback_operation)

        checkpointed_result = self.get_checkpointed_result()
        if not checkpointed_result.operation:
            msg = f"Missing callback details for operation: {self.operation_identifier.operation_id}"
            raise CallbackError(msg)
        return await self.replay(checkpointed_result.operation)

    async def replay(self, operation: Operation) -> str:
        """Replay an existing callback operation from its checkpoint."""
        if not operation.callback_details:
            msg = (
                f"Missing callback details for operation: "
                f"{self.operation_identifier.operation_id}"
            )
            raise CallbackError(msg)

        return await self.execute(CheckpointedResult.create_from_operation(operation))

    async def execute(self, checkpointed_result: CheckpointedResult) -> str:
        """Execute callback operation by extracting the callback_id.

        Callbacks don't execute logic - they just extract and return the callback_id
        from the checkpoint data.

        Args:
            checkpointed_result: The checkpoint data containing callback_details

        Returns:
            The callback_id from the checkpoint

        Raises:
            CallbackError: If callback_details are missing (should never happen)
        """
        if (
            not checkpointed_result.operation
            or not checkpointed_result.operation.callback_details
        ):
            msg = f"Missing callback details for operation: {self.operation_identifier.operation_id}"
            raise CallbackError(msg)

        return checkpointed_result.operation.callback_details.callback_id


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
    context = _get_durable_context("create_callback")
    config = CallbackConfig(
        timeout=timeout if timeout is not None else timedelta(),
        heartbeat_timeout=heartbeat_timeout
        if heartbeat_timeout is not None
        else timedelta(),
        serdes=serdes,
    )
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
            config=config,
        )
        callback_id: str = await executor.process()
        return Callback(
            callback_id=callback_id,
            operation_id=operation_id,
            state=context.execution_state,
            serdes=config.serdes,
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
        checkpointed_result: CheckpointedResult = get_checkpoint_result(
            self.state,
            self.operation_id,
        )

        if not checkpointed_result.is_existent():
            msg = "Callback operation must exist"
            raise CallbackError(message=msg, callback_id=self.callback_id)

        if (
            checkpointed_result.is_failed()
            or checkpointed_result.is_cancelled()
            or checkpointed_result.is_timed_out()
            or checkpointed_result.is_stopped()
        ):
            msg = _format_callback_error_message(checkpointed_result)
            raise CallbackError(message=msg, callback_id=self.callback_id)

        if checkpointed_result.is_succeeded():
            if checkpointed_result.result is None:
                return None  # type: ignore

            return await deserialize(
                serdes=self.serdes if self.serdes is not None else PASS_THROUGH_SERDES,
                data=checkpointed_result.result,
                operation_id=self.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )

        # operation exists; it has not terminated (successfully or otherwise)
        # therefore we should wait
        msg = "Callback result not received yet. Suspending execution while waiting for result."
        raise SuspendExecution(msg)


def _format_callback_error_message(checkpointed_result: CheckpointedResult) -> str:
    """Build a stable callback error message from checkpoint state."""
    error = checkpointed_result.error
    if not error or not error.message:
        return "Callback failed"

    message = error.message
    if (
        checkpointed_result.is_timed_out()
        and error.type in {timeout.value for timeout in CallbackTimeoutType}
        and error.type not in message
    ):
        return f"{message}: {error.type}"

    return message
