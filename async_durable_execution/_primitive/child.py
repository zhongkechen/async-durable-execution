"""Implementation for run_in_child_context."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeAlias, TypeVar, cast

from .base import OperationExecutor
from .._core import (
    CallableRuntimeError,
    ContextOptions,
    DurableContext,
    ErrorObject,
    ExecutionError,
    ExecutionState,
    InvocationError,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationSubTypeValue,
    OperationType,
    OperationUpdate,
    SerDes,
    _encode_sdk_control_error_data,
    _restore_sdk_control_error,
    bind_current_context,
    create_eager_task,
    deserialize,
    get_durable_context,
    serialize,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")
C_contra = TypeVar("C_contra", contravariant=True)

SummaryGenerator: TypeAlias = Callable[[C_contra], str]
"""Create a compact JSON summary for an oversized child context result."""

# Checkpoint size limit in bytes (256KB)
CHECKPOINT_SIZE_LIMIT = 256 * 1024


class ChildOperationExecutor(OperationExecutor[T]):
    """Executor for child context operations."""

    SERDES_OPERATION_TYPE = OperationType.CONTEXT

    def __init__(
        self,
        func: Callable[[], Awaitable[T]],
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        *,
        serdes: SerDes | None = None,
        summary_generator: SummaryGenerator | None = None,
        is_virtual: bool = False,
        before_result_checkpoint: Callable[[T], Awaitable[None]] | None = None,
        on_result_preparation_error: (
            Callable[[BaseException], Awaitable[None]] | None
        ) = None,
        deserialize_result_before_checkpoint: bool = False,
    ) -> None:
        """Initialize the child operation executor.

        Args:
            func: The child context function to execute
            state: The execution state
            operation_identifier: The operation identifier
            serdes: Optional serializer for the child context result.
            summary_generator: Optional summary generator for large child results.
            is_virtual: Whether this child context should skip lifecycle checkpoints.
            before_result_checkpoint: Optional hook after result serialization and
                summary preparation but before the success checkpoint.
            on_result_preparation_error: Optional hook for serialization,
                deserialization, summary failures, or cancellation before the child
                context records failure.
            deserialize_result_before_checkpoint: Whether deserialization belongs
                to result preparation before the success checkpoint.
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.func = func
        self.serdes = serdes
        self.summary_generator = summary_generator
        self.is_virtual = is_virtual
        self.before_result_checkpoint = before_result_checkpoint
        self.on_result_preparation_error = on_result_preparation_error
        self.deserialize_result_before_checkpoint = deserialize_result_before_checkpoint

    async def start(self) -> T:
        """Start a new child context operation."""
        if not self.is_virtual:
            start_operation: OperationUpdate = OperationUpdate.create_context_start(
                identifier=self.operation_identifier,
                sub_type=self.operation_identifier.sub_type,
            )
            await self.create_checkpoint(start_operation, is_sync=False)

        return await self.execute(None)

    async def replay(self, operation: Operation) -> T:
        """Replay an existing child context operation from its checkpoint."""
        if (
            operation.status is OperationStatus.SUCCEEDED
            and not self._is_replay_children(operation)
        ):
            logger.debug(
                "Child context already completed, skipping execution for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_name,
            )
            result_payload = self._get_result(operation)
            if result_payload is None:
                return cast("T", None)

            result: T = await deserialize(
                serdes=self.serdes,
                data=result_payload,
                operation_id=self.operation_id,
                durable_execution_arn=self.durable_execution_arn,
                recursive_level=self.state.recursive_level,
                operation_name=operation.name,
                parent_id=operation.parent_id,
                operation_type=operation.operation_type,
                operation_sub_type=operation.sub_type,
            )
            return result

        if operation.status is OperationStatus.SUCCEEDED and self._is_replay_children(
            operation
        ):
            return await self.execute(operation)

        if operation.status is OperationStatus.FAILED:
            self._raise_callable_error(operation)

        return await self.execute(operation)

    async def execute(self, operation: Operation | None) -> T:
        """Execute child context function with error handling and large payload support.

        Args:
            operation: The checkpointed operation state, if any

        Returns:
            The result of executing the child context function

        Raises:
            SuspendExecution: Re-raised without checkpointing
            InvocationError: Re-raised without checkpointing when retryable
            ExecutionError: Re-raised after checkpointing FAIL
            CallableRuntimeError: Raised for other exceptions after checkpointing FAIL
        """
        logger.debug(
            "▶️ Executing child context for id: %s, name: %s",
            self.operation_identifier.operation_id,
            self.operation_identifier.name,
        )
        try:
            replaying_children = self._is_replay_children(operation)
            raw_result: T = await self.func()

            if self.is_virtual:
                logger.debug(
                    "Virtual context: Exiting child context without creating another checkpoint. id: %s, name: %s",
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                )
                return raw_result

            # If in replay_children mode, return without checkpointing
            if replaying_children:
                logger.debug(
                    "ReplayChildren mode: Executed child context again on replay due to large payload. Exiting child context without creating another checkpoint. id: %s, name: %s",
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                )
                return raw_result

            try:
                # Serialize result
                serialized_result: str = await serialize(
                    serdes=self.serdes,
                    value=raw_result,
                    operation_id=self.operation_id,
                    durable_execution_arn=self.durable_execution_arn,
                    recursive_level=self.state.recursive_level,
                    operation_name=self.operation_identifier.name,
                    parent_id=self.operation_identifier.parent_id,
                    operation_type=self.SERDES_OPERATION_TYPE,
                    operation_sub_type=self.operation_identifier.sub_type,
                )

                # Check payload size and use ReplayChildren mode if needed
                # Summary Generator Logic:
                # When the serialized result exceeds 256KB, we use ReplayChildren mode to avoid
                # checkpointing large payloads. Instead, we checkpoint a compact summary and mark
                # the operation for replay. This matches the TypeScript implementation behavior.
                #
                # See TypeScript reference:
                # - aws-durable-execution-sdk-js/src/handlers/run-in-child-context-handler/run-in-child-context-handler.ts (lines ~200-220)
                #
                # The summary generator creates a JSON summary with metadata (type, counts, status)
                # instead of the full BatchResult. During replay, the child context is re-executed
                # to reconstruct the full result rather than deserializing from the checkpoint.
                replay_children: bool = False
                if len(serialized_result) > CHECKPOINT_SIZE_LIMIT:
                    logger.debug(
                        "Large payload detected, using ReplayChildren mode: id: %s, name: %s, payload_size: %d, limit: %d",
                        self.operation_identifier.operation_id,
                        self.operation_identifier.name,
                        len(serialized_result),
                        CHECKPOINT_SIZE_LIMIT,
                    )
                    replay_children = True
                    # Use summary generator if provided, otherwise use empty string (matches TypeScript)
                    serialized_result = (
                        self.summary_generator(raw_result)
                        if self.summary_generator
                        else ""
                    )

                prepared_result: T = raw_result
                if self.deserialize_result_before_checkpoint and not replay_children:
                    prepared_result = await deserialize(
                        serdes=self.serdes,
                        data=serialized_result,
                        operation_id=self.operation_id,
                        durable_execution_arn=self.durable_execution_arn,
                        recursive_level=self.state.recursive_level,
                        operation_name=self.operation_identifier.name,
                        parent_id=self.operation_identifier.parent_id,
                        operation_type=self.SERDES_OPERATION_TYPE,
                        operation_sub_type=self.operation_identifier.sub_type,
                    )
            except asyncio.CancelledError as result_cancellation:
                if self.on_result_preparation_error is not None:
                    await self.on_result_preparation_error(result_cancellation)
                raise
            except Exception as result_error:
                if self.on_result_preparation_error is not None:
                    await self.on_result_preparation_error(result_error)
                raise

            if self.before_result_checkpoint is not None:
                await self.before_result_checkpoint(prepared_result)

            # Checkpoint SUCCEED
            success_operation: OperationUpdate = OperationUpdate.create_context_succeed(
                identifier=self.operation_identifier,
                payload=serialized_result,
                sub_type=self.operation_identifier.sub_type,
                context_options=ContextOptions(replay_children=replay_children),
            )
            # Checkpoint child context SUCCEED with blocking (is_sync=True, default).
            # Must ensure the child context result is persisted before returning to the parent.
            # This guarantees the result is durable and child operations won't be re-executed on replay
            # (unless replay_children=True for large payloads).
            await self.create_checkpoint(success_operation)

            logger.debug(
                "✅ Successfully completed child context for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            if self.deserialize_result_before_checkpoint:
                return prepared_result
        except Exception as e:
            if isinstance(e, InvocationError) and e.is_retryable():
                raise

            error_object = ErrorObject.from_exception(e)
            sdk_error_data = _encode_sdk_control_error_data(e)
            if sdk_error_data is not None:
                error_object = ErrorObject(
                    message=error_object.message,
                    type=error_object.type,
                    data=sdk_error_data,
                    stack_trace=error_object.stack_trace,
                )

            # Virtual deliberately does not write checkpoints, but exception still propagates below
            if not self.is_virtual:
                fail_operation: OperationUpdate = OperationUpdate.create_context_fail(
                    identifier=self.operation_identifier,
                    error=error_object,
                    sub_type=self.operation_identifier.sub_type,
                )
                # Checkpoint child context FAIL with blocking (is_sync=True, default).
                # Must ensure the failure state is persisted before raising the exception.
                # This guarantees the error is durable and child operations won't be re-executed on replay.
                await self.create_checkpoint(fail_operation)

            # Preserve SDK control errors for the top-level execution handler
            # after checkpointing their failure.
            if isinstance(e, InvocationError | ExecutionError):
                raise
            if sdk_error_data is not None:
                control_error = _restore_sdk_control_error(
                    error_object.message or str(e),
                    error_object.type,
                    sdk_error_data,
                )
                if control_error is not None:
                    raise control_error from e
            raise CallableRuntimeError.from_error_object(error_object) from e

        if replay_children:
            return raw_result

        # Default child contexts preserve the established behavior of making
        # SUCCEED durable before deserializing the returned value.
        return await deserialize(
            serdes=self.serdes,
            data=serialized_result,
            operation_id=self.operation_id,
            durable_execution_arn=self.durable_execution_arn,
            recursive_level=self.state.recursive_level,
            operation_name=self.operation_identifier.name,
            parent_id=self.operation_identifier.parent_id,
            operation_type=self.SERDES_OPERATION_TYPE,
            operation_sub_type=self.operation_identifier.sub_type,
        )

    @staticmethod
    def _is_replay_children(operation: Operation | None) -> bool:
        if operation is None or operation.context_details is None:
            return False
        return operation.context_details.replay_children

    @staticmethod
    def _get_result(operation: Operation) -> str | None:
        if operation.context_details is None:
            return None
        return operation.context_details.result

    @staticmethod
    def _raise_callable_error(operation: Operation) -> None:
        error = operation.context_details.error if operation.context_details else None
        if error is None:
            msg = "Unknown error. No ErrorObject exists on the Checkpoint Operation."
            raise CallableRuntimeError(
                message=msg,
                error_type=None,
                data=None,
                stack_trace=None,
            )

        control_error = _restore_sdk_control_error(
            error.message or "Child context failed",
            error.type,
            error.data,
        )
        if control_error is not None:
            raise control_error

        raise CallableRuntimeError.from_error_object(error)


def run_in_child_context(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
) -> asyncio.Task[T]:
    """Execute a durable sub-workflow inside its own child context.

    Args:
        func: The child context function to execute.
        name: Optional durable operation name.
        serdes: Optional serializer for the child context result.
        summary_generator: Optional summary generator for large child results.
        is_virtual: Whether the child context should skip lifecycle checkpoints.
    """

    from .._operation.child import run_in_child_context as operation_child_context

    return operation_child_context(
        func,
        name=name,
        serdes=serdes,
        summary_generator=summary_generator,
        is_virtual=is_virtual,
    )


def _create_child_context_task(
    func: Callable[[], Awaitable[T]],
    *,
    sub_type: OperationSubTypeValue,
    name: str | None = None,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
) -> asyncio.Task[T]:
    context = get_durable_context()

    operation_id = context.step_counter.create_step_id()
    child_context = context.create_child_context(
        operation_id=operation_id,
        is_virtual=is_virtual,
    )
    operation_identifier = OperationIdentifier(
        operation_id=operation_id,
        sub_type=sub_type,
        parent_id=context.parent_id,
        name=name,
    )

    async def run_child_context() -> T:
        with context._replay_aware():
            return await _run_child_context(
                func,
                context=context,
                child_context=child_context,
                operation_identifier=operation_identifier,
                serdes=serdes,
                summary_generator=summary_generator,
                is_virtual=is_virtual,
            )

    return create_eager_task(run_child_context)


async def _run_in_child_context(
    func: Callable[[], Awaitable[T]],
    *,
    sub_type: OperationSubTypeValue,
    name: str | None = None,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
) -> T:
    """Execute a durable sub-workflow with an explicit operation subtype."""
    context = get_durable_context()
    with context._replay_aware():
        operation_id = context.step_counter.create_step_id()

        child_context = context.create_child_context(
            operation_id=operation_id,
            is_virtual=is_virtual,
        )
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=sub_type,
            parent_id=context.parent_id,
            name=name,
        )

        return await _run_child_context(
            func,
            context=context,
            child_context=child_context,
            operation_identifier=operation_identifier,
            serdes=serdes,
            summary_generator=summary_generator,
            is_virtual=is_virtual,
        )


async def _run_child_context(
    func: Callable[[], Awaitable[T]],
    *,
    context: DurableContext,
    child_context: DurableContext,
    operation_identifier: OperationIdentifier,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
    before_result_checkpoint: Callable[[T], Awaitable[None]] | None = None,
    on_result_preparation_error: (
        Callable[[BaseException], Awaitable[None]] | None
    ) = None,
    deserialize_result_before_checkpoint: bool = False,
) -> T:
    async def callable_with_child_context() -> T:
        with bind_current_context(child_context):
            return await func()

    async def before_result_checkpoint_with_child_context(result: T) -> None:
        if before_result_checkpoint is None:
            return
        with bind_current_context(child_context):
            await before_result_checkpoint(result)

    async def result_preparation_error_with_child_context(
        error: BaseException,
    ) -> None:
        if on_result_preparation_error is None:
            return
        with bind_current_context(child_context):
            await on_result_preparation_error(error)

    executor: ChildOperationExecutor[T] = ChildOperationExecutor(
        callable_with_child_context,
        context.execution_state,
        operation_identifier,
        serdes=serdes,
        summary_generator=summary_generator,
        is_virtual=is_virtual,
        before_result_checkpoint=(
            before_result_checkpoint_with_child_context
            if before_result_checkpoint is not None
            else None
        ),
        on_result_preparation_error=(
            result_preparation_error_with_child_context
            if on_result_preparation_error is not None
            else None
        ),
        deserialize_result_before_checkpoint=deserialize_result_before_checkpoint,
    )
    return await executor.process()
