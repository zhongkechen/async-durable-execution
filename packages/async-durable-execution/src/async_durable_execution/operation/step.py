"""Implement the Durable step operation."""

from __future__ import annotations

import functools
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar, ParamSpec

from .. import get_current_context
from ..async_tools import assert_async_callable, get_callable_name
from ..config import (
    RetryPresets,
    StepConfig,
    StepSemantics,
)
from ..exceptions import (
    ExecutionError,
    InvalidStateError,
    StepInterruptedError,
)
from ..models import (
    ErrorObject,
    OperationIdentifier,
    OperationUpdate,
    RetryDecision,
    OperationSubType,
)
from ..context import reset_current_context, set_current_context
from .child import _get_durable_context
from .base import (
    OperationExecutor,
    OperationContext,
)
from ..suspend import (
    suspend_with_optional_resume_delay,
    suspend_with_optional_resume_timestamp,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ..state import (
        CheckpointedResult,
        ExecutionState,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T")
Params = ParamSpec("Params")


class StepOperationExecutor(OperationExecutor[T]):
    """Executor for step operations."""

    def __init__(
        self,
        func: Callable[[], Awaitable[T]],
        config: StepConfig,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
    ):
        """Initialize the step operation executor.

        Args:
            func: The step function to execute
            config: The step configuration
            state: The execution state
            operation_identifier: The operation identifier
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.func = func
        self.config = config

    async def process(self) -> T:
        """Process step checkpoint state and execute when appropriate."""
        checkpointed_result: CheckpointedResult = self.get_checkpointed_result()

        # Terminal success - deserialize and return
        if checkpointed_result.is_succeeded():
            logger.debug(
                "Step already completed, skipping execution for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_name,
            )
            if checkpointed_result.result is None:
                return None  # type: ignore[return-value]

            result: T = self.deserialize_value(
                data=checkpointed_result.result,
                serdes=self.config.serdes,
            )
            return result

        # Terminal failure
        if checkpointed_result.is_failed():
            # Have to throw the exact same error on replay as the checkpointed failure
            checkpointed_result.raise_callable_error()

        # Pending retry
        if checkpointed_result.is_pending():
            scheduled_timestamp = checkpointed_result.get_next_attempt_timestamp()
            # Normally, we'd ensure that a suspension here would be for > 0 seconds;
            # however, this is coming from a checkpoint, and we can trust that it is a correct target timestamp.
            suspend_with_optional_resume_timestamp(
                msg=f"Retry scheduled for {self.operation_name or self.operation_identifier.operation_id} will retry at timestamp {scheduled_timestamp}",
                datetime_timestamp=scheduled_timestamp,
            )

        # Handle interrupted AT_MOST_ONCE (replay scenario only)
        # This check only applies on REPLAY when a new Lambda invocation starts after interruption.
        # A STARTED checkpoint with AT_MOST_ONCE on entry means the previous invocation
        # was interrupted and it should NOT re-execute.
        #
        # This check is skipped on fresh executions because:
        #   - First call (fresh): checkpoint doesn't exist → is_started() returns False → skip this check
        #   - After creating sync checkpoint and refreshing: if status is STARTED, we return
        #     directly into execute() without re-running process() from the top
        if (
            checkpointed_result.is_started()
            and self.config.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
        ):
            # Step was previously interrupted in a prior invocation - handle retry
            msg: str = f"Step operation_id={self.operation_identifier.operation_id} name={self.operation_identifier.name} was previously interrupted"
            await self.retry_handler(StepInterruptedError(msg), checkpointed_result)
            checkpointed_result.raise_callable_error()

        # Ready to execute if STARTED + AT_LEAST_ONCE
        if (
            checkpointed_result.is_started()
            and self.config.step_semantics is StepSemantics.AT_LEAST_ONCE_PER_RETRY
        ):
            return await self.execute(checkpointed_result)

        # Create START checkpoint if nonexistent or READY
        if not checkpointed_result.is_existent() or checkpointed_result.is_ready():
            start_operation: OperationUpdate = OperationUpdate.create_step_start(
                identifier=self.operation_identifier,
            )
            # Checkpoint START operation with appropriate synchronization:
            # - AtMostOncePerRetry: Use blocking checkpoint (is_sync=True) to prevent duplicate execution.
            #   The step must not execute until the START checkpoint is persisted, ensuring exactly-once semantics.
            # - AtLeastOncePerRetry: Use non-blocking checkpoint (is_sync=False) for performance optimization.
            #   The step can execute immediately without waiting for checkpoint persistence, allowing at-least-once semantics.
            is_sync: bool = (
                self.config.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
            )
            await self.create_checkpoint(start_operation, is_sync=is_sync)

            # After creating sync checkpoint, check the status
            if is_sync:
                # Refresh checkpoint result to check for immediate response
                refreshed_result: CheckpointedResult = self.get_checkpointed_result()

                # START checkpoint only returns STARTED status
                # Any errors would be thrown as runtime exceptions during checkpoint creation
                if not refreshed_result.is_started():
                    # This should never happen - defensive check
                    error_msg: str = f"Unexpected status after START checkpoint: {refreshed_result.status}"
                    raise InvalidStateError(error_msg)

                # If we reach here, status must be STARTED - ready to execute
                checkpointed_result = refreshed_result

        return await self.execute(checkpointed_result)

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
        )

        try:
            # This is the actual code provided by the caller to execute durably inside the step
            wrapped_user_func = self.state.wrap_user_function(
                self.func,
                self.operation_identifier,
                False,
                attempt,
            )
            token = set_current_context(step_context)
            try:
                raw_result = await wrapped_user_func()
            finally:
                reset_current_context(token)

            serialized_result: str = self.serialize_value(
                value=raw_result,
                serdes=self.config.serdes,
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

        retry_strategy = self.config.retry_strategy or RetryPresets.default()

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
    name: str | None = None,
    config: StepConfig | None = None,
) -> T:
    context = _get_durable_context()
    assert_async_callable(func)
    step_name = name or get_callable_name(func, include_original_name=False)
    logger.debug("Step name: %s", step_name)
    if not config:
        config = StepConfig()
    operation_id = context.step_counter.create_step_id()

    executor: StepOperationExecutor[T] = StepOperationExecutor(
        func=func,
        config=config,
        state=context.execution_state,
        operation_identifier=OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.STEP,
            parent_id=context.parent_id,
            name=step_name,
        ),
    )
    result: T = await executor.process()
    context.execution_state.track_replay(operation_id=operation_id)
    return result


def durable_step(
    func: Callable[Params, Awaitable[T]],
) -> Callable[Params, Callable[[], Awaitable[T]]]:
    """Wrap an async function so calling it returns a zero-argument step callable.

    The returned callable is suitable for passing to `step()`,
    which keeps durable step creation explicit while avoiding manual `partial(...)`
    wrapping at the callsite.
    """
    assert_async_callable(func)

    @functools.wraps(func)
    def wrapper(
        *args: Params.args, **kwargs: Params.kwargs
    ) -> Callable[[], Awaitable[T]]:
        return functools.partial(func, *args, **kwargs)

    return wrapper


@dataclass(frozen=True)
class StepContext(OperationContext):
    attempt: int | None = None


def get_attempt() -> int | None:
    current_context = get_step_context(
        "get_attempt() can only be used while a step function is executing.",
    )
    return current_context.attempt


def get_step_context(
    message: str,
):
    current_context = get_current_context()
    if current_context is None or not isinstance(current_context, StepContext):
        raise RuntimeError(message)
    return current_context
