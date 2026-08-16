"""Implement the Durable step operation."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, TypeVar, cast

from .base import OperationExecutor
from .._core import (
    CallableRuntimeError,
    Duration,
    DurableContext,
    ErrorObject,
    ExecutionError,
    ExecutionState,
    InvocationError,
    Operation,
    OperationContext,
    OperationIdentifier,
    OperationStatus,
    OperationUpdate,
    RetryStrategy,
    SerDes,
    TerminationReason,
    _encode_sdk_control_error_data,
    _restore_sdk_control_error,
    bind_current_context,
    duration_to_seconds,
    get_current_context,
    suspend_with_optional_resume_delay,
    suspend_with_optional_resume_timestamp,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ..extension import (
        ExtensionStepFunction,
        ExtensionStepResult,
        ExtensionStepRetryStrategy,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _error_object_from_exception(
    error: Exception,
    *,
    invocation_retryable: bool | None = None,
) -> ErrorObject:
    error_object = ErrorObject.from_exception(error)
    sdk_error_data = _encode_sdk_control_error_data(
        error,
        invocation_retryable=invocation_retryable,
    )
    if sdk_error_data is None:
        return error_object
    return ErrorObject(
        message=error_object.message,
        type=error_object.type,
        data=sdk_error_data,
        stack_trace=error_object.stack_trace,
    )


class StepInterruptedError(InvocationError):
    """Raised when a step is interrupted before it checkpointed at the end."""

    def __init__(self, message: str, step_id: str | None = None) -> None:
        super().__init__(message, TerminationReason.STEP_INTERRUPTED)
        self.step_id = step_id


class StepSemantics(Enum):
    """Checkpoint timing guarantees for a durable step attempt."""

    AT_MOST_ONCE_PER_RETRY = "AT_MOST_ONCE_PER_RETRY"
    AT_LEAST_ONCE_PER_RETRY = "AT_LEAST_ONCE_PER_RETRY"


class StepOperationExecutor(OperationExecutor[T]):
    """Executor for step operations."""

    def __init__(
        self,
        func: Callable[[], Awaitable[T]],
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
        step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes: SerDes | None = None,
    ) -> None:
        """Initialize the step operation executor.

        Args:
            func: The step function to execute
            state: The execution state
            operation_identifier: The operation identifier
            retry_strategy: Optional retry strategy for step failures
            step_semantics: Checkpoint timing guarantee for the step attempt
            serdes: Optional serializer/deserializer for the step result
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.func = func
        self.retry_strategy = retry_strategy
        self.step_semantics = step_semantics
        self.serdes = serdes

    async def start(self) -> T:
        """Start a new step operation."""
        start_operation: OperationUpdate = OperationUpdate.create_step_start(
            identifier=self.operation_identifier,
        )
        is_sync: bool = self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
        await self.create_checkpoint(start_operation, is_sync=is_sync)

        return await self.execute(None)

    async def replay(self, operation: Operation) -> T:
        """Replay an existing step operation from its checkpoint."""
        if operation.status is OperationStatus.SUCCEEDED:
            logger.debug(
                "Step already completed, skipping execution for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_name,
            )
            result_payload = (
                operation.step_details.result if operation.step_details else None
            )
            if result_payload is None:
                return cast("T", None)

            result: T = await self.deserialize_value(
                data=result_payload,
                serdes=self.serdes,
            )
            return result

        if operation.status is OperationStatus.FAILED:
            self._raise_callable_error(operation)

        if operation.status is OperationStatus.PENDING:
            scheduled_timestamp = (
                operation.step_details.next_attempt_timestamp
                if operation.step_details
                else None
            )
            suspend_with_optional_resume_timestamp(
                msg=f"Retry scheduled for {self.operation_name or self.operation_identifier.operation_id} will retry at timestamp {scheduled_timestamp}",
                datetime_timestamp=scheduled_timestamp,
            )

        if (
            operation.status is OperationStatus.STARTED
            and self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
        ):
            msg: str = f"Step operation_id={self.operation_identifier.operation_id} name={self.operation_identifier.name} was previously interrupted"
            await self.retry_handler(StepInterruptedError(msg), operation)
            self._raise_callable_error(operation)

        if (
            operation.status is OperationStatus.STARTED
            and self.step_semantics is StepSemantics.AT_LEAST_ONCE_PER_RETRY
        ):
            return await self.execute(operation)

        if operation.status is OperationStatus.READY:
            start_operation: OperationUpdate = OperationUpdate.create_step_start(
                identifier=self.operation_identifier,
            )
            is_sync: bool = self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
            await self.create_checkpoint(start_operation, is_sync=is_sync)

            return await self.execute(operation)

        return await self.execute(operation)

    async def execute(self, operation: Operation | None) -> T:
        """Execute step function with error handling and retry logic.

        Args:
            operation: The checkpointed operation state, if any

        Returns:
            The result of executing the step function

        Raises:
            ExecutionError: For fatal errors that should not be retried
            May raise other exceptions that will be handled by retry_handler
        """
        # Get current attempt - checkpointed attempts + 1
        attempt: int = 1
        if operation and operation.step_details:
            attempt = operation.step_details.attempt + 1

        step_context: StepContext = StepContext(
            attempt=attempt,
            execution_state=self.state,
            operation_identifier=self.operation_identifier,
        )

        try:
            # This is the actual code provided by the caller to execute durably inside the step
            with bind_current_context(step_context):
                raw_result = await self.func()

            serialized_result: str = await self.serialize_value(
                value=raw_result,
                serdes=self.serdes,
            )

            success_operation: OperationUpdate = OperationUpdate.create_step_succeed(
                identifier=self.operation_identifier,
                payload=serialized_result,
            )

            # Checkpoint SUCCEED operation with blocking (is_sync=True, default).
            # Must ensure the success state is persisted before returning the result to the caller.
            # This guarantees the step result is durable and won't be lost if Lambda terminates.
            await self.create_checkpoint(success_operation)

            logger.debug(
                "✅ Successfully completed step for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            return await self.deserialize_value(
                data=serialized_result,
                serdes=self.serdes,
            )
        except Exception as e:
            if isinstance(e, ExecutionError):
                # No retry on fatal - e.g checkpoint exception
                logger.debug(
                    "💥 Fatal error for id: %s, name: %s",
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                )
                # This bubbles up to execution.durable_execution, where it will exit with FAILED
                raise

            logger.exception(
                "❌ failed step for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )

            await self.retry_handler(e, operation)
            # If we've failed to raise an exception from the retry_handler, then we are in a
            # weird state, and should crash terminate the execution
            msg = "retry handler should have raised an exception, but did not."
            raise ExecutionError(msg) from None

    async def retry_handler(
        self,
        error: Exception,
        operation: Operation | None,
    ) -> None:
        """Checkpoint and suspend for replay if retry required, otherwise raise error.

        Args:
            error: The exception that occurred during step execution
            operation: The checkpointed operation state, if any

        Raises:
            SuspendExecution: If retry is scheduled
            StepInterruptedError: If the error is a StepInterruptedError
            CallableRuntimeError: If retry is exhausted or error is not retryable
        """
        error_object = _error_object_from_exception(error)

        retry_strategy = self.retry_strategy or RetryStrategy.default()

        retry_attempt: int = (
            operation.step_details.attempt
            if operation and operation.step_details
            else 0
        )
        delay_seconds: int | None = None
        try:
            retry_delay = retry_strategy(error, retry_attempt + 1)
            if retry_delay is not None:
                delay_seconds = duration_to_seconds(retry_delay, "retry delay")
        except Exception as retry_error:
            fail_error_object = _error_object_from_exception(
                retry_error,
                invocation_retryable=False,
            )
            fail_operation: OperationUpdate = OperationUpdate.create_step_fail(
                identifier=self.operation_identifier, error=fail_error_object
            )

            # Checkpoint FAIL operation with blocking (is_sync=True, default).
            # Must ensure the failure state is persisted before raising the exception.
            # This guarantees the error is durable and the step won't be retried on replay.
            await self.create_checkpoint(fail_operation)

            if isinstance(retry_error, StepInterruptedError):
                raise retry_error

            raise CallableRuntimeError.from_error_object(fail_error_object)

        if retry_delay is None:
            fail_error_object = _error_object_from_exception(
                error,
                invocation_retryable=False,
            )
            fail_operation = OperationUpdate.create_step_fail(
                identifier=self.operation_identifier, error=fail_error_object
            )
            await self.create_checkpoint(fail_operation)

            if isinstance(error, StepInterruptedError):
                raise error

            raise CallableRuntimeError.from_error_object(fail_error_object)

        assert delay_seconds is not None

        logger.debug(
            "Retrying step for id: %s, name: %s, attempt: %s",
            self.operation_identifier.operation_id,
            self.operation_identifier.name,
            retry_attempt + 1,
        )

        # Because we are issuing a retry and create an OperationUpdate, enforce a
        # minimum delay of one second here to match model behavior.
        if delay_seconds < 1:
            logger.warning(
                (
                    "Retry delay_seconds step for id: %s, name: %s,"
                    "attempt: %s is %d < 1. Setting to minimum of 1 seconds."
                ),
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
                retry_attempt + 1,
                delay_seconds,
            )
            delay_seconds = 1

        retry_operation: OperationUpdate = OperationUpdate.create_step_retry(
            identifier=self.operation_identifier,
            error=error_object,
            next_attempt_delay_seconds=delay_seconds,
        )

        # Checkpoint RETRY operation with blocking (is_sync=True, default).
        # Must ensure retry state is persisted before suspending execution.
        # This guarantees the retry attempt count and next attempt timestamp are durable.
        await self.create_checkpoint(retry_operation)

        suspend_with_optional_resume_delay(
            msg=(
                f"Retry scheduled for {self.operation_identifier.operation_id} "
                f"in {delay_seconds} seconds"
            ),
            delay_seconds=delay_seconds,
        )

    @staticmethod
    def _raise_callable_error(operation: Operation) -> None:
        error = operation.step_details.error if operation.step_details else None
        if error is None:
            msg = "Unknown error. No ErrorObject exists on the Checkpoint Operation."
            raise CallableRuntimeError(
                message=msg,
                error_type=None,
                data=None,
                stack_trace=None,
            )

        raise CallableRuntimeError.from_error_object(error)


class StatefulStepOperationExecutor(OperationExecutor[T]):
    """Executor for stateful extension-authored step operations."""

    def __init__(
        self,
        func: ExtensionStepFunction[T],
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        *,
        initial_state: T | None,
        retry_strategy: ExtensionStepRetryStrategy[T] | None,
        step_semantics: StepSemantics,
        serdes: SerDes[T] | None,
        raise_original_error: bool = False,
    ) -> None:
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.func = func
        self.initial_state = initial_state
        self.retry_strategy = retry_strategy
        self.step_semantics = step_semantics
        self.serdes = serdes
        self.raise_original_error = raise_original_error

    async def start(self) -> T:
        start = OperationUpdate.create_step_start(self.operation_identifier)
        await self.create_checkpoint(
            start,
            is_sync=self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY,
        )
        return await self._execute(None)

    async def replay(self, operation: Operation) -> T:
        if operation.status is OperationStatus.SUCCEEDED:
            payload = operation.step_details.result if operation.step_details else None
            if payload is None:
                return cast("T", None)
            return await self.deserialize_value(payload, self.serdes)

        if operation.status is OperationStatus.FAILED:
            self._raise_failed_operation(operation)

        if operation.status is OperationStatus.PENDING:
            resume_at = (
                operation.step_details.next_attempt_timestamp
                if operation.step_details
                else None
            )
            suspend_with_optional_resume_timestamp(
                msg=f"Extension step {self.operation_name or self.operation_id} is pending",
                datetime_timestamp=resume_at,
            )

        if (
            operation.status is OperationStatus.STARTED
            and self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
        ):
            msg = (
                f"Extension step operation_id={self.operation_id} "
                f"name={self.operation_name} was previously interrupted"
            )
            state = await self._load_state(operation)
            attempt = self._attempt(operation)
            return await self._handle_failure(
                StepInterruptedError(msg, self.operation_id),
                state,
                attempt,
            )

        if operation.status is OperationStatus.READY:
            start = OperationUpdate.create_step_start(self.operation_identifier)
            await self.create_checkpoint(
                start,
                is_sync=self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY,
            )

        return await self._execute(operation)

    async def _execute(self, operation: Operation | None) -> T:
        from ..extension import ExtensionStepResult

        state = await self._load_state(operation)
        attempt = self._attempt(operation)
        step_context = StepContext(
            attempt=attempt,
            execution_state=self.state,
            operation_identifier=self.operation_identifier,
        )
        try:
            with bind_current_context(step_context):
                outcome = await self.func(state)

            if not isinstance(outcome, ExtensionStepResult):
                msg = (
                    "Extension step functions must return "
                    "ExtensionStepResult.succeed(...) or ExtensionStepResult.retry(...)"
                )
                raise TypeError(msg)

            if outcome.is_retry:
                delay_seconds, payload = await self._prepare_retry(outcome)
            else:
                payload = await self.serialize_value(outcome.value, self.serdes)
        except InvocationError as error:
            if error.is_retryable():
                raise
            return await self._handle_failure(error, state, attempt)
        except Exception as error:
            return await self._handle_failure(error, state, attempt)

        if outcome.is_retry:
            return await self._schedule_retry(
                delay_seconds=delay_seconds,
                payload=payload,
            )

        await self.create_checkpoint(
            OperationUpdate.create_step_succeed(
                self.operation_identifier,
                payload,
            )
        )
        return await self.deserialize_value(payload, self.serdes)

    async def _load_state(self, operation: Operation | None) -> T | None:
        if (
            operation is not None
            and operation.step_details is not None
            and operation.step_details.result is not None
        ):
            return await self.deserialize_value(
                operation.step_details.result,
                self.serdes,
            )
        return self.initial_state

    @staticmethod
    def _attempt(operation: Operation | None) -> int:
        if operation is None or operation.step_details is None:
            return 1
        return operation.step_details.attempt + 1

    async def _handle_failure(
        self,
        error: Exception,
        state: T | None,
        attempt: int,
    ) -> T:
        from ..extension import ExtensionStepResult

        if self.retry_strategy is None:
            return await self._fail(error)

        try:
            decision = self.retry_strategy(error, state, attempt)
        except Exception as retry_error:
            return await self._fail(retry_error)

        if decision is None:
            return await self._fail(error)
        if not isinstance(decision, ExtensionStepResult) or not decision.is_retry:
            msg = (
                "Extension step retry_strategy must return "
                "ExtensionStepResult.retry(...) or None"
            )
            return await self._fail(TypeError(msg))

        try:
            delay_seconds, payload = await self._prepare_retry(decision)
        except Exception as retry_error:
            return await self._fail(retry_error)

        return await self._schedule_retry(
            delay_seconds=delay_seconds,
            payload=payload,
        )

    async def _prepare_retry(
        self,
        outcome: ExtensionStepResult[T],
    ) -> tuple[int, str]:
        assert outcome.retry_delay is not None
        delay_seconds = max(
            1,
            duration_to_seconds(outcome.retry_delay, "retry delay"),
        )
        payload = await self.serialize_value(outcome.value, self.serdes)
        return delay_seconds, payload

    async def _schedule_retry(
        self,
        *,
        delay_seconds: int,
        payload: str,
    ) -> T:
        await self.create_checkpoint(
            OperationUpdate.create_step_retry(
                self.operation_identifier,
                next_attempt_delay_seconds=delay_seconds,
                payload=payload,
                error=None,
            )
        )
        suspend_with_optional_resume_delay(
            msg=f"Extension step {self.operation_id} will retry",
            delay_seconds=delay_seconds,
        )
        raise AssertionError("suspend_with_optional_resume_delay must raise")

    async def _fail(self, error: Exception) -> T:
        error_object = _error_object_from_exception(
            error,
            invocation_retryable=False,
        )
        await self.create_checkpoint(
            OperationUpdate.create_step_fail(
                self.operation_identifier,
                error_object,
            )
        )
        if self.raise_original_error:
            raise error
        control_error = _restore_sdk_control_error(
            error_object.message or str(error),
            error_object.type,
            error_object.data,
        )
        if control_error is not None:
            raise control_error
        raise CallableRuntimeError.from_error_object(error_object)

    @staticmethod
    def _raise_failed_operation(operation: Operation) -> None:
        error = operation.step_details.error if operation.step_details else None
        if error is None:
            error = ErrorObject.from_message(
                "Unknown error. No ErrorObject exists on the checkpoint operation."
            )
        control_error = _restore_sdk_control_error(
            error.message or "Extension step failed",
            error.type,
            error.data,
        )
        if control_error is not None:
            raise control_error
        raise CallableRuntimeError.from_error_object(error)


def step(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
    step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    serdes: SerDes | None = None,
) -> asyncio.Task[T]:
    """Compatibility import for the canonical operation-layer helper."""
    from .._operation.step import step as operation_step

    return operation_step(
        func,
        name=name,
        retry_strategy=retry_strategy,
        step_semantics=step_semantics,
        serdes=serdes,
    )


async def _step(
    func: Callable[[], Awaitable[T]],
    *,
    context: DurableContext,
    operation_identifier: OperationIdentifier,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
    step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    serdes: SerDes | None = None,
) -> T:
    executor: StepOperationExecutor[T] = StepOperationExecutor(
        func=func,
        state=context.execution_state,
        operation_identifier=operation_identifier,
        retry_strategy=retry_strategy,
        step_semantics=step_semantics,
        serdes=serdes,
    )
    return await executor.process()


async def _stateful_step(
    func: ExtensionStepFunction[T],
    *,
    context: DurableContext,
    operation_identifier: OperationIdentifier,
    initial_state: T | None,
    retry_strategy: ExtensionStepRetryStrategy[T] | None,
    step_semantics: StepSemantics,
    serdes: SerDes[T] | None,
    raise_original_error: bool = False,
) -> T:
    executor: StatefulStepOperationExecutor[T] = StatefulStepOperationExecutor(
        func=func,
        state=context.execution_state,
        operation_identifier=operation_identifier,
        initial_state=initial_state,
        retry_strategy=retry_strategy,
        step_semantics=step_semantics,
        serdes=serdes,
        raise_original_error=raise_original_error,
    )
    return await executor.process()


@dataclass(frozen=True)
class StepContext(OperationContext):
    """Context exposed while a step function is executing."""

    attempt: int | None = None


def get_step_context() -> StepContext:
    """Return the active `StepContext`."""
    current_context = get_current_context()
    if not isinstance(current_context, StepContext):
        msg = "get_step_context() can only be used while a step function is executing."
        raise RuntimeError(msg)
    return current_context
