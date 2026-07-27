"""Implement the Durable invoke operation."""

from __future__ import annotations

import asyncio
import logging
from typing import TypeVar, cast

# Import base classes for operation executor pattern
from .base import OperationExecutor
from ..core import (
    DEFAULT_JSON_SERDES,
    CallableRuntimeError,
    ChainedInvokeOptions,
    DurableContext,
    ExecutionError,
    ExecutionState,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationSubType,
    OperationUpdate,
    SerDes,
    create_eager_task,
    get_durable_context,
    suspend_with_optional_resume_delay,
)

P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type

logger = logging.getLogger(__name__)


class InvokeOperationExecutor(OperationExecutor[R]):
    """Executor for invoke operations."""

    def __init__(
        self,
        function_name: str,
        payload: P,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        serdes_payload: SerDes[P] | None = None,
        serdes_result: SerDes[R] | None = None,
        tenant_id: str | None = None,
    ):
        """Initialize the invoke operation executor.

        Args:
            function_name: Name of the function to invoke
            payload: The payload to pass to the invoked function
            state: The execution state
            operation_identifier: The operation identifier
            serdes_payload: Optional serializer for the invocation payload
            serdes_result: Optional deserializer for the invocation result
            tenant_id: Optional tenant identifier for the chained invocation
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.function_name = function_name
        self.payload = payload
        self.serdes_payload = serdes_payload
        self.serdes_result = serdes_result
        self.tenant_id = tenant_id

    async def start(self) -> R:
        """Start a new invoke operation."""
        serialized_payload: str = await self.serialize_value(
            value=self.payload,
            serdes=self.serdes_payload or DEFAULT_JSON_SERDES,
        )
        start_operation: OperationUpdate = OperationUpdate.create_invoke_start(
            identifier=self.operation_identifier,
            payload=serialized_payload,
            chained_invoke_options=ChainedInvokeOptions(
                function_name=self.function_name,
                tenant_id=self.tenant_id,
            ),
        )
        await self.create_checkpoint(start_operation, is_sync=True)

        logger.debug(
            "🚀 Invoke %s started, will suspend for completion",
            self.operation_name or self.function_name,
        )

        return await self.execute()

    async def replay(self, operation: Operation) -> R:
        """Replay an existing invoke operation from its checkpoint."""
        invoke_details = operation.chained_invoke_details
        if operation.status is OperationStatus.SUCCEEDED:
            result_data = invoke_details.result if invoke_details else None
            if result_data is None:
                return cast("R", None)

            result: R = await self.deserialize_value(
                data=result_data,
                serdes=self.serdes_result or DEFAULT_JSON_SERDES,
            )
            return result

        # Terminal failures
        if (
            operation.status is OperationStatus.FAILED
            or operation.status is OperationStatus.TIMED_OUT
            or operation.status is OperationStatus.STOPPED
        ):
            error = invoke_details.error if invoke_details else None
            if error is None:
                raise CallableRuntimeError(
                    message="Unknown error. No ErrorObject exists on the Checkpoint Operation.",
                    error_type=None,
                    data=None,
                    stack_trace=None,
                )

            raise CallableRuntimeError.from_error_object(error)

        if operation.status is OperationStatus.STARTED:
            logger.debug(
                "⏳ Invoke %s still in progress, will suspend",
                self.operation_name or self.function_name,
            )
            return await self.execute()

        return await self.execute()

    async def execute(self, operation: Operation | None = None) -> R:
        """Execute invoke operation by suspending to wait for async completion.

        The invoke operation doesn't execute synchronously - it suspends and
        the backend executes the invoked function asynchronously.

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


def invoke(
    function_name: str,
    payload: P,
    *,
    name: str | None = None,
    serdes_payload: SerDes[P] | None = None,
    serdes_result: SerDes[R] | None = None,
    tenant_id: str | None = None,
) -> asyncio.Task[R]:
    """Invoke another durable Lambda function and wait for its durable result.

    Args:
        function_name: Qualified Lambda function name or ARN to invoke.
        payload: Payload to send to the invoked function.
        name: Optional durable operation name.
        serdes_payload: Optional serializer for the invocation payload.
        serdes_result: Optional deserializer for the invocation result.
        tenant_id: Optional tenant identifier for the chained invocation.
    """
    context = get_durable_context()

    with context._replay_aware():
        operation_id = context.step_counter.create_step_id()
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.CHAINED_INVOKE,
            parent_id=context.parent_id,
            name=name,
        )

        return create_eager_task(
            lambda: _invoke(
                function_name=function_name,
                payload=payload,
                context=context,
                operation_identifier=operation_identifier,
                serdes_payload=serdes_payload,
                serdes_result=serdes_result,
                tenant_id=tenant_id,
            ),
        )


async def _invoke(
    function_name: str,
    payload: P,
    *,
    context: DurableContext,
    operation_identifier: OperationIdentifier,
    serdes_payload: SerDes[P] | None = None,
    serdes_result: SerDes[R] | None = None,
    tenant_id: str | None = None,
) -> R:
    executor: InvokeOperationExecutor[R] = InvokeOperationExecutor(
        function_name=function_name,
        payload=payload,
        state=context.execution_state,
        operation_identifier=operation_identifier,
        serdes_payload=serdes_payload,
        serdes_result=serdes_result,
        tenant_id=tenant_id,
    )
    return await executor.process()
