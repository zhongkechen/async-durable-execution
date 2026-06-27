"""Implement the Durable step operation."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, TypeVar

from ..context import get_current_context
from ..async_tools import assert_async_callable, invoke_user_callable
from ..config import RetryPresets
from ..exceptions import (
    ExecutionError,
    InvalidStateError,
    InvocationError,
    TerminationReason,
    suspend_with_optional_resume_delay,
    suspend_with_optional_resume_timestamp,
)
from ..models import (
    ErrorObject,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationUpdate,
    RetryDecision,
    OperationSubType,
)
from .child import _get_durable_context
from .base import (
    CHECKPOINT_NOT_FOUND,
    CheckpointedResult,
    OperationExecutor,
    OperationContext,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ..serdes import SerDes
    from ..state import ExecutionState
    from ..types import LambdaContext

logger = logging.getLogger(__name__)

T = TypeVar("T")


class StepInterruptedError(InvocationError):
    """Raised when a step is interrupted before it checkpointed at the end."""

    def __init__(self, message: str, step_id: str | None = None):
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
        retry_strategy: Callable[[Exception, int], RetryDecision] | None = None,
        step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes: SerDes | None = None,
        lambda_context: LambdaContext | None = None,
    ):
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
        self.lambda_context = lambda_context

    async def start(self) -> T:
        """Start a new step operation."""
        start_operation: OperationUpdate = OperationUpdate.create_step_start(
            identifier=self.operation_identifier,
        )
        is_sync: bool = self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
        await self.create_checkpoint(start_operation, is_sync=is_sync)

        checkpointed_result = CHECKPOINT_NOT_FOUND
        if is_sync:
            refreshed_result: CheckpointedResult = self.get_checkpointed_result()
            if not refreshed_result.is_started():
                error_msg: str = f"Unexpected status after START checkpoint: {refreshed_result.status}"
                raise InvalidStateError(error_msg)
            checkpointed_result = refreshed_result

        return await self.execute(checkpointed_result)

    async def replay(self, operation: Operation) -> T:
        """Replay an existing step operation from its checkpoint."""
        if operation.status is OperationStatus.SUCCEEDED:
            checkpointed_result = CheckpointedResult.create_from_operation(operation)
            logger.debug(
                "Step already completed, skipping execution for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_name,
            )
            if checkpointed_result.result is None:
                return None  # type: ignore[return-value]

            result: T = await self.deserialize_value(
                data=checkpointed_result.result,
                serdes=self.serdes,
            )
            return result

        if operation.status is OperationStatus.FAILED:
            CheckpointedResult.create_from_operation(operation).raise_callable_error()

        if operation.status is OperationStatus.PENDING:
            checkpointed_result = CheckpointedResult.create_from_operation(operation)
            scheduled_timestamp = checkpointed_result.get_next_attempt_timestamp()
            suspend_with_optional_resume_timestamp(
                msg=f"Retry scheduled for {self.operation_name or self.operation_identifier.operation_id} will retry at timestamp {scheduled_timestamp}",
                datetime_timestamp=scheduled_timestamp,
            )

        if (
            operation.status is OperationStatus.STARTED
            and self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
        ):
            checkpointed_result = CheckpointedResult.create_from_operation(operation)
            msg: str = f"Step operation_id={self.operation_identifier.operation_id} name={self.operation_identifier.name} was previously interrupted"
            await self.retry_handler(StepInterruptedError(msg), checkpointed_result)
            checkpointed_result.raise_callable_error()

        if (
            operation.status is OperationStatus.STARTED
            and self.step_semantics is StepSemantics.AT_LEAST_ONCE_PER_RETRY
        ):
            return await self.execute(
                CheckpointedResult.create_from_operation(operation)
            )

        if operation.status is OperationStatus.READY:
            start_operation: OperationUpdate = OperationUpdate.create_step_start(
                identifier=self.operation_identifier,
            )
            is_sync: bool = self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
            await self.create_checkpoint(start_operation, is_sync=is_sync)

            checkpointed_result = CheckpointedResult.create_from_operation(operation)
            if is_sync:
                refreshed_result: CheckpointedResult = self.get_checkpointed_result()
                if not refreshed_result.is_started():
                    error_msg: str = f"Unexpected status after START checkpoint: {refreshed_result.status}"
                    raise InvalidStateError(error_msg)
                checkpointed_result = refreshed_result

            return await self.execute(checkpointed_result)

        return await self.execute(CheckpointedResult.create_from_operation(operation))

    async def execute(self, checkpointed_result: CheckpointedResult) -> T:
        """Execute step function with error handling and retry logic.

        Args:
            checkpointed_result: The checkpoint data containing operation state

        Returns:
            The result of executing the step function

        Raises:
            ExecutionError: For fatal errors that should not be retried
            May raise other exceptions that will be handled by retry_handler
        """
        # Get current attempt - checkpointed attempts + 1
        attempt: int = 1
        if checkpointed_result.operation and checkpointed_result.operation.step_details:
            attempt = checkpointed_result.operation.step_details.attempt + 1

        step_context: StepContext = StepContext(
            attempt=attempt,
            execution_state=self.state,
            operation_identifier=self.operation_identifier,
            lambda_context=self.lambda_context,
        )

        try:
            # This is the actual code provided by the caller to execute durably inside the step
            wrapped_user_func = self.state.wrap_user_function(
                self.func,
                self.operation_identifier,
                False,
                attempt,
            )
            raw_result = await invoke_user_callable(step_context, wrapped_user_func)

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
            return raw_result  # noqa: TRY300
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

            await self.retry_handler(e, checkpointed_result)
            # If we've failed to raise an exception from the retry_handler, then we are in a
            # weird state, and should crash terminate the execution
            msg = "retry handler should have raised an exception, but did not."
            raise ExecutionError(msg) from None

    async def retry_handler(
        self,
        error: Exception,
        checkpointed_result: CheckpointedResult,
    ):
        """Checkpoint and suspend for replay if retry required, otherwise raise error.

        Args:
            error: The exception that occurred during step execution
            checkpointed_result: The checkpoint data containing operation state

        Raises:
            SuspendExecution: If retry is scheduled
            StepInterruptedError: If the error is a StepInterruptedError
            CallableRuntimeError: If retry is exhausted or error is not retryable
        """
        error_object = ErrorObject.from_exception(error)

        retry_strategy = self.retry_strategy or RetryPresets.default()

        retry_attempt: int = (
            checkpointed_result.operation.step_details.attempt
            if (
                checkpointed_result.operation
                and checkpointed_result.operation.step_details
            )
            else 0
        )
        retry_decision: RetryDecision = retry_strategy(error, retry_attempt + 1)

        if retry_decision.should_retry:
            logger.debug(
                "Retrying step for id: %s, name: %s, attempt: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
                retry_attempt + 1,
            )

            # because we are issuing a retry and create an OperationUpdate
            # we enforce a minimum delay second of 1, to match model behaviour.
            # we localize enforcement and keep it outside suspension methods as:
            # a) those are used throughout the codebase, e.g. in wait(..) <- enforcement is done in context
            # b) they shouldn't know model specific details <- enforcement is done above
            # and c) this "issue" arises from retry-decision and we shouldn't push it down
            delay_seconds = retry_decision.delay_seconds
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
                    f"Retry scheduled for {self.operation_identifier.operation_id}"
                    f"in {retry_decision.delay_seconds} seconds"
                ),
                delay_seconds=delay_seconds,
            )

        # no retry
        fail_operation: OperationUpdate = OperationUpdate.create_step_fail(
            identifier=self.operation_identifier, error=error_object
        )

        # Checkpoint FAIL operation with blocking (is_sync=True, default).
        # Must ensure the failure state is persisted before raising the exception.
        # This guarantees the error is durable and the step won't be retried on replay.
        await self.create_checkpoint(fail_operation)

        if isinstance(error, StepInterruptedError):
            raise error

        raise error_object.to_callable_runtime_error()


async def step(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None,
    step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    serdes: SerDes | None = None,
) -> T:
    """Run user code as a checkpointed durable step.

    Durable steps are the main way to isolate non-deterministic work such as API
    calls, clock reads, UUID generation, and database access from replayed code.
    """
    context = _get_durable_context()
    assert_async_callable(func)
    step_name = name if name is not None else getattr(func, "__name__", None)
    logger.debug("Step name: %s", step_name)
    with context._replay_aware(executes_user_code=True):
        operation_id = context.step_counter.create_step_id()

        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.STEP,
            parent_id=context.parent_id,
            name=step_name,
        )
        if context.lambda_context is None:
            executor: StepOperationExecutor[T] = StepOperationExecutor(
                func=func,
                state=context.execution_state,
                operation_identifier=operation_identifier,
                retry_strategy=retry_strategy,
                step_semantics=step_semantics,
                serdes=serdes,
            )
        else:
            executor = StepOperationExecutor(
                func=func,
                state=context.execution_state,
                operation_identifier=operation_identifier,
                retry_strategy=retry_strategy,
                step_semantics=step_semantics,
                serdes=serdes,
                lambda_context=context.lambda_context,
            )
        return await executor.process()


@dataclass(frozen=True)
class StepContext(OperationContext):
    """Context exposed while a step function is executing."""

    attempt: int | None = None


def get_attempt() -> int | None:
    """Return the current step attempt number inside a step/check callback."""
    current_context = get_step_context(
        "get_attempt() can only be used while a step function is executing.",
    )
    return current_context.attempt


def get_step_context(
    message: str,
):
    """Return the active `StepContext` or raise a caller-provided error message."""
    current_context = get_current_context()
    if current_context is None or not isinstance(current_context, StepContext):
        raise RuntimeError(message)
    return current_context
