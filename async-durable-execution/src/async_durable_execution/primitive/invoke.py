"""Implement the Durable invoke operation."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, TypeVar, cast

# Import base classes for operation executor pattern
from .base import OperationExecutor
from .child import get_durable_context
from ..exceptions import (
    CallableRuntimeError,
    ExecutionError,
    ValidationError,
    suspend_with_optional_resume_delay,
)
from ..models import (
    ChainedInvokeOptions,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationUpdate,
    OperationSubType,
)
from ..serdes import (
    DEFAULT_JSON_SERDES,
)
from ..state import RECURSIVE_LEVEL_INPUT_FIELD
from ..task import create_eager_task

if TYPE_CHECKING:
    from .child import DurableContext
    from ..serdes import SerDes
    from ..state import ExecutionState

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


def _is_qualified_function_arn(function_name: str) -> bool:
    """Return whether a Lambda function ARN includes a qualifier."""
    parts = function_name.split(":")
    return len(parts) >= 8 and parts[0] == "arn" and parts[5] == "function"


def _is_qualified_function_name(function_name: str) -> bool:
    """Return whether a short Lambda function name includes a qualifier."""
    return ":" in function_name and not function_name.startswith("arn:")


def _append_qualifier(function_name: str, qualifier: str | None) -> str:
    """Append a Lambda qualifier when one is available and needed."""
    if not qualifier:
        return function_name
    if _is_qualified_function_arn(function_name) or _is_qualified_function_name(
        function_name
    ):
        return function_name
    return f"{function_name}:{qualifier}"


def _resolve_recursive_function_name(
    context: DurableContext,
    explicit_function_name: str | None,
) -> str:
    if explicit_function_name:
        return explicit_function_name

    lambda_context = context.lambda_context
    if lambda_context is None:
        msg = "recurse requires a Lambda context or explicit function_name."
        raise RuntimeError(msg)

    function_version = getattr(lambda_context, "function_version", None)
    invoked_function_arn = getattr(lambda_context, "invoked_function_arn", None)
    if invoked_function_arn:
        return _append_qualifier(invoked_function_arn, function_version)

    context_function_name = getattr(lambda_context, "function_name", None)
    if context_function_name:
        return _append_qualifier(context_function_name, function_version)

    msg = "recurse could not determine the current Lambda function name."
    raise RuntimeError(msg)


def _validate_recursive_payload(context: DurableContext, payload: P) -> None:
    current_input = context.execution_state.get_input_event()
    if payload == current_input:
        msg = "recurse payload must differ from the current execution input."
        raise ValidationError(msg)


def _add_recursive_level(context: DurableContext, payload: P) -> P:
    if not isinstance(payload, dict):
        msg = "recurse payload must be a dict when with_recursive_level is enabled."
        raise ValidationError(msg)

    payload_with_level = payload.copy()
    payload_with_level[RECURSIVE_LEVEL_INPUT_FIELD] = context.recursive_level + 1
    return cast("P", payload_with_level)


def recurse(
    payload: P,
    *,
    name: str | None = None,
    function_name: str | None = None,
    with_recursive_level: bool = False,
    serdes_payload: SerDes[P] | None = None,
    serdes_result: SerDes[R] | None = None,
    tenant_id: str | None = None,
) -> asyncio.Task[R]:
    """Invoke the current durable Lambda function and wait for its result.

    This is a convenience wrapper around :func:`invoke` for recursive workflows
    such as divide-and-conquer algorithms. Each recursive call is a separate
    durable execution, so the current execution records a chained invoke instead
    of growing a Python call stack.

    Args:
        payload: Payload to send to the recursive invocation.
        name: Optional durable operation name.
        function_name: Optional qualified Lambda function name or ARN. When omitted,
            the current Lambda context is used.
        with_recursive_level: When true, copy the payload and set
            ``__recursive_level`` to the current context level plus one.
        serdes_payload: Optional serializer for the invocation payload.
        serdes_result: Optional deserializer for the invocation result.
        tenant_id: Optional tenant identifier. Defaults to the current Lambda context
            tenant id when present.
    """
    context = get_durable_context()
    recursive_payload = (
        _add_recursive_level(context, payload) if with_recursive_level else payload
    )
    _validate_recursive_payload(context, recursive_payload)
    target_function_name = _resolve_recursive_function_name(context, function_name)
    recursive_tenant_id = tenant_id
    if recursive_tenant_id is None and context.lambda_context is not None:
        recursive_tenant_id = getattr(context.lambda_context, "tenant_id", None)

    return invoke(
        function_name=target_function_name,
        payload=recursive_payload,
        name=name,
        serdes_payload=serdes_payload,
        serdes_result=serdes_result,
        tenant_id=recursive_tenant_id,
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
