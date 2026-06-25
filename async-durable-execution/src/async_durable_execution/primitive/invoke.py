"""Implement the Durable invoke operation."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import TYPE_CHECKING, Generic, TypeVar

from .child import _get_durable_context

from ..exceptions import ExecutionError, suspend_with_optional_resume_delay
from ..models import (
    ChainedInvokeOptions,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationUpdate,
    OperationSubType,
)

# Import base classes for operation executor pattern
from .base import CheckpointedResult, OperationExecutor
from ..serdes import (
    DEFAULT_JSON_SERDES,
)

if TYPE_CHECKING:
    from ..serdes import SerDes
    from ..state import ExecutionState

P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InvokeConfig(Generic[P, R]):
    """Configuration for invoke operations."""

    serdes_payload: SerDes[P] | None = None
    serdes_result: SerDes[R] | None = None
    tenant_id: str | None = None


class InvokeOperationExecutor(OperationExecutor[R]):
    """Executor for invoke operations."""

    def __init__(
        self,
        function_name: str,
        payload: P,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        config: InvokeConfig[P, R],
    ):
        """Initialize the invoke operation executor.

        Args:
            function_name: Name of the function to invoke
            payload: The payload to pass to the invoked function
            state: The execution state
            operation_identifier: The operation identifier
            config: Configuration for the invoke operation
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.function_name = function_name
        self.payload = payload
        self.config = config

    async def start(self) -> R:
        """Start a new invoke operation."""
        serialized_payload: str = await self.serialize_value(
            value=self.payload,
            serdes=self.config.serdes_payload or DEFAULT_JSON_SERDES,
        )
        start_operation: OperationUpdate = OperationUpdate.create_invoke_start(
            identifier=self.operation_identifier,
            payload=serialized_payload,
            chained_invoke_options=ChainedInvokeOptions(
                function_name=self.function_name,
                tenant_id=self.config.tenant_id,
            ),
        )
        await self.create_checkpoint(start_operation, is_sync=True)

        logger.debug(
            "🚀 Invoke %s started, will check for immediate response",
            self.operation_name or self.function_name,
        )

        checkpointed_result = self.get_checkpointed_result()
        if not checkpointed_result.operation:
            error_msg = "Missing invoke operation after START checkpoint."
            raise ExecutionError(error_msg)
        return await self.replay(checkpointed_result.operation)

    async def replay(self, operation: Operation) -> R:
        """Replay an existing invoke operation from its checkpoint."""
        if operation.status is OperationStatus.SUCCEEDED:
            checkpointed_result = CheckpointedResult.create_from_operation(operation)
            if checkpointed_result.result is None:
                return None  # type: ignore[return-value]

            result: R = await self.deserialize_value(
                data=checkpointed_result.result,
                serdes=self.config.serdes_result or DEFAULT_JSON_SERDES,
            )
            return result

        # Terminal failures
        if (
            operation.status is OperationStatus.FAILED
            or operation.status is OperationStatus.TIMED_OUT
            or operation.status is OperationStatus.STOPPED
        ):
            CheckpointedResult.create_from_operation(operation).raise_callable_error()

        checkpointed_result = CheckpointedResult.create_from_operation(operation)
        if operation.status is OperationStatus.STARTED:
            logger.debug(
                "⏳ Invoke %s still in progress, will suspend",
                self.operation_name or self.function_name,
            )
            return await self.execute(checkpointed_result)

        return await self.execute(checkpointed_result)

    async def execute(self, _checkpointed_result: CheckpointedResult) -> R:
        """Execute invoke operation by suspending to wait for async completion.

        The invoke operation doesn't execute synchronously - it suspends and
        the backend executes the invoked function asynchronously.

        Args:
            checkpointed_result: The checkpoint data (unused, but required by interface)

        Returns:
            Never returns - always suspends

        Raises:
            Always suspends via suspend_with_optional_resume_delay
            ExecutionError: If suspend doesn't raise (should never happen)
        """
        msg: str = f"Invoke {self.operation_identifier.operation_id} started, suspending for completion"
        suspend_with_optional_resume_delay(msg)
        # This line should never be reached since suspend_with_optional_resume_delay always raises
        error_msg: str = "suspend_with_optional_resume_delay should have raised an exception, but did not."
        raise ExecutionError(error_msg) from None


async def invoke(
    function_name: str,
    payload: P,
    *,
    name: str | None = None,
    serdes_payload: SerDes[P] | None = None,
    serdes_result: SerDes[R] | None = None,
    tenant_id: str | None = None,
) -> R:
    """Invoke another durable Lambda function and wait for its durable result.

    Args:
        function_name: Qualified Lambda function name or ARN to invoke.
        payload: Payload to send to the invoked function.
        name: Optional durable operation name.
        serdes_payload: Optional serializer for the invocation payload.
        serdes_result: Optional deserializer for the invocation result.
        tenant_id: Optional tenant identifier for the chained invocation.
    """
    context = _get_durable_context("invoke")
    config = InvokeConfig[P, R](
        serdes_payload=serdes_payload,
        serdes_result=serdes_result,
        tenant_id=tenant_id,
    )
    with context._replay_aware():
        operation_id = context.step_counter.create_step_id()

        executor: InvokeOperationExecutor[R] = InvokeOperationExecutor(
            function_name=function_name,
            payload=payload,
            state=context.execution_state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                sub_type=OperationSubType.CHAINED_INVOKE,
                parent_id=context.parent_id,
                name=name,
            ),
            config=config,
        )
        return await executor.process()
