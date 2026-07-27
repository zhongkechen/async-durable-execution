"""Implement the durable wait_for_condition operation."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from .._core import (
    CallableRuntimeError,
    Duration,
    DurableContext,
    ErrorObject,
    ExecutionError,
    ExecutionState,
    InvocationError,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationSubType,
    OperationUpdate,
    SerDes,
    ValidationError,
    _DelayStrategy,
    _encode_sdk_control_error_data,
    _register_sdk_control_error_type,
    _restore_sdk_control_error,
    bind_current_context,
    create_eager_task,
    duration_to_seconds,
    get_current_context,
    get_durable_context,
    suspend_with_optional_resume_delay,
    suspend_with_optional_resume_timestamp,
)
from .._primitive.base import OperationExecutor
from .._primitive.step import StepContext

if TYPE_CHECKING:
    from collections.abc import Awaitable


T = TypeVar("T")

logger = logging.getLogger(__name__)


PollingStrategyFunction = Callable[[T, int], Duration | None]
_LEGACY_WAIT_FOR_CONDITION_ERROR_TYPE_NAMES = (
    "async_durable_execution.exceptions.WaitForConditionError",
    "async_durable_execution.extension.wait_for_condition.WaitForConditionError",
)


class WaitForConditionError(ExecutionError):
    """Raised when a wait_for_condition operation exhausts its attempts."""


def _restore_wait_for_condition_error(
    message: str,
    _payload: str | None,
) -> WaitForConditionError:
    return WaitForConditionError(message)


_register_sdk_control_error_type(
    WaitForConditionError,
    restore=_restore_wait_for_condition_error,
    legacy_exception_type_names=_LEGACY_WAIT_FOR_CONDITION_ERROR_TYPE_NAMES,
)


@dataclass
class PollingStrategy(_DelayStrategy, Generic[T]):
    """Polling strategy for `wait_for_condition()`."""

    def __call__(self, result: T, attempts_made: int) -> int | None:
        """Return the next polling delay, or None to stop polling."""
        if result:
            return None

        if attempts_made >= self.max_attempts:
            msg = (
                f"wait_for_condition exhausted {self.max_attempts} attempts "
                "before the condition was met"
            )
            raise WaitForConditionError(msg)

        return self.calculate_delay(attempts_made)


class WaitForConditionOperationExecutor(OperationExecutor[T]):
    """Executor for wait_for_condition operations."""

    def __init__(
        self,
        check: Callable[[T | None], Awaitable[T]],
        initial_state: T | None,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        polling_strategy: PollingStrategyFunction[T] | None = None,
        serdes: SerDes | None = None,
    ):
        """Initialize the wait_for_condition executor.

        Args:
            check: The check function to evaluate the condition
            initial_state: The state to pass to the first condition evaluation
            state: The execution state
            operation_identifier: The operation identifier
            polling_strategy: Optional strategy for deciding whether and when to poll
            serdes: Optional serializer/deserializer for state payloads
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.check = check
        self.initial_state = initial_state
        self.polling_strategy = polling_strategy
        self.serdes = serdes
        self.default_polling_strategy = PollingStrategy[T]()

    async def start(self) -> T:
        """Start a new wait_for_condition operation."""
        start_operation = OperationUpdate.create_wait_for_condition_start(
            identifier=self.operation_identifier,
        )
        await self.create_checkpoint(start_operation, is_sync=False)
        return await self.execute(None)

    async def replay(self, operation: Operation) -> T:
        """Replay an existing wait_for_condition operation from its checkpoint."""
        if operation.status is OperationStatus.SUCCEEDED:
            logger.debug(
                "wait_for_condition already completed for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_name,
            )
            result = (
                operation.step_details.result
                if operation.step_details is not None
                else None
            )
            if result is None:
                return cast("T", None)
            return await self.deserialize_value(
                data=result,
                serdes=self.serdes,
            )

        if operation.status is OperationStatus.FAILED:
            error = (
                operation.step_details.error
                if operation.step_details is not None
                else None
            )
            if error is None:
                msg = (
                    "Unknown error. No ErrorObject exists on the Checkpoint Operation."
                )
                raise CallableRuntimeError(
                    message=msg,
                    error_type=None,
                    data=None,
                    stack_trace=None,
                )
            control_error = _restore_sdk_control_error(
                error.message or "wait_for_condition failed",
                error.type,
                error.data,
            )
            if control_error is not None:
                raise control_error

            raise CallableRuntimeError.from_error_object(error)

        if operation.status is OperationStatus.PENDING:
            scheduled_timestamp = (
                operation.step_details.next_attempt_timestamp
                if operation.step_details is not None
                else None
            )
            suspend_with_optional_resume_timestamp(
                msg=f"wait_for_condition {self.operation_name or self.operation_identifier.operation_id} will retry at timestamp {scheduled_timestamp}",
                datetime_timestamp=scheduled_timestamp,
            )

        if operation.status is not OperationStatus.STARTED:
            start_operation = OperationUpdate.create_wait_for_condition_start(
                identifier=self.operation_identifier,
            )
            await self.create_checkpoint(start_operation, is_sync=False)

        return await self.execute(operation)

    async def execute(self, operation: Operation | None) -> T:
        """Execute check function and handle decision.

        Args:
            operation: The checkpoint operation, if one exists.

        Returns:
            The final state when condition is met

        Raises:
            Suspends if condition not met
            Raises error if check function fails
        """
        operation_details = operation.step_details if operation is not None else None

        try:
            # Determine current state from checkpoint
            if (
                operation is not None
                and operation.status in {OperationStatus.STARTED, OperationStatus.READY}
                and operation_details is not None
                and operation_details.result is not None
            ):
                current_state = await self.deserialize_value(
                    data=operation_details.result,
                    serdes=self.serdes,
                )
            else:
                current_state = self.initial_state

            # The checkpoint stores completed attempts, so the current attempt is one more.
            attempt: int = 1
            if operation_details is not None:
                attempt = operation_details.attempt + 1

            check_context = WaitForConditionCheckContext(
                attempt=attempt,
                execution_state=self.state,
                operation_identifier=self.operation_identifier,
            )
            with bind_current_context(check_context):
                new_state = await self.check(current_state)

            serialized_state = await self.serialize_value(
                value=new_state,
                serdes=self.serdes,
            )

            logger.debug(
                "wait_for_condition check completed: %s, name: %s, attempt: %s",
                self.operation_identifier.operation_id,
                self.operation_name,
                attempt,
            )

            suspend_delay_seconds = self._resolve_delay_seconds(new_state, attempt)
            if suspend_delay_seconds is None:
                success_operation = OperationUpdate.create_wait_for_condition_succeed(
                    identifier=self.operation_identifier,
                    payload=serialized_state,
                )
                await self.create_checkpoint(success_operation)

                logger.debug(
                    "✅ wait_for_condition stopped polling for id: %s, name: %s",
                    self.operation_identifier.operation_id,
                    self.operation_name,
                )
                return await self.deserialize_value(
                    data=serialized_state,
                    serdes=self.serdes,
                )

            delay_seconds = suspend_delay_seconds

            # We enforce a minimum delay second of 1, to match model behaviour.
            if delay_seconds < 1:
                logger.warning(
                    (
                        "wait_for_condition delay_seconds step for id: %s, name: %s,"
                        "is %d < 1. Setting to minimum of 1 seconds."
                    ),
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                    delay_seconds,
                )
                delay_seconds = 1

            retry_operation = OperationUpdate.create_wait_for_condition_retry(
                identifier=self.operation_identifier,
                payload=serialized_state,
                next_attempt_delay_seconds=delay_seconds,
            )

            # Checkpoint RETRY operation with blocking (is_sync=True, default).
            # Must ensure the current state and next attempt timestamp are persisted before suspending.
            # This guarantees the polling state is durable and will resume correctly on the next invocation.
            await self.create_checkpoint(retry_operation)

            suspend_with_optional_resume_delay(
                msg=f"wait_for_condition {self.operation_identifier.name or self.operation_identifier.operation_id} will retry in {suspend_delay_seconds} seconds",
                delay_seconds=suspend_delay_seconds,
            )

        except Exception as e:
            if isinstance(e, InvocationError) and e.is_retryable():
                raise

            # Mark as failed - waitForCondition doesn't have its own retry logic for errors
            # If the check function throws, it's considered a failure
            logger.exception(
                "❌ wait_for_condition failed for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )

            error = ErrorObject.from_exception(e)
            sdk_error_data = _encode_sdk_control_error_data(e)
            if sdk_error_data is not None:
                error = ErrorObject(
                    message=error.message,
                    type=error.type,
                    data=sdk_error_data,
                    stack_trace=error.stack_trace,
                )

            fail_operation = OperationUpdate.create_wait_for_condition_fail(
                identifier=self.operation_identifier,
                error=error,
            )
            # Checkpoint FAIL operation with blocking (is_sync=True, default).
            # Must ensure the failure state is persisted before raising the exception.
            # This guarantees the error is durable and the condition won't be re-evaluated on replay.
            await self.create_checkpoint(fail_operation)
            raise

        msg: str = "wait_for_condition should never reach this point"
        raise ExecutionError(msg)

    def _resolve_delay_seconds(self, new_state: T, attempt: int) -> int | None:
        polling_strategy = self.polling_strategy or self.default_polling_strategy
        wait_delay = polling_strategy(new_state, attempt)

        if wait_delay is None:
            return None

        if isinstance(wait_delay, int | timedelta):
            return duration_to_seconds(wait_delay, "polling_strategy delay")

        msg = "wait_for_condition polling_strategy must return int seconds, timedelta, or None"
        raise ValidationError(msg)


def wait_for_condition(
    check: Callable[[T | None], Awaitable[T]],
    *,
    initial_state: T | None = None,
    name: str | None = None,
    polling_strategy: PollingStrategyFunction[T] | None = None,
    serdes: SerDes | None = None,
) -> asyncio.Task[T]:
    """Poll durable state until the configured strategy decides to stop waiting.

    The check receives the current state, beginning with `initial_state`,
    and returns the next state. The polling strategy receives that result and
    returns the next polling delay, or None to stop polling and complete with
    the latest result.
    """
    context = get_durable_context()

    with context._replay_aware(executes_user_code=True):
        operation_id = context.step_counter.create_step_id()
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            parent_id=context.parent_id,
            name=name,
        )

        return create_eager_task(
            lambda: _wait_for_condition(
                check=check,
                context=context,
                operation_identifier=operation_identifier,
                initial_state=initial_state,
                polling_strategy=polling_strategy,
                serdes=serdes,
            ),
        )


async def _wait_for_condition(
    check: Callable[[T | None], Awaitable[T]],
    *,
    context: DurableContext,
    operation_identifier: OperationIdentifier,
    initial_state: T | None = None,
    polling_strategy: PollingStrategyFunction[T] | None = None,
    serdes: SerDes | None = None,
) -> T:
    executor: WaitForConditionOperationExecutor[T] = WaitForConditionOperationExecutor(
        check=check,
        initial_state=initial_state,
        state=context.execution_state,
        operation_identifier=operation_identifier,
        polling_strategy=polling_strategy,
        serdes=serdes,
    )
    return await executor.process()


@dataclass(frozen=True)
class WaitForConditionCheckContext(StepContext):
    """Context available during wait_for_condition checker execution."""

    pass


def get_wait_for_condition_check_context() -> WaitForConditionCheckContext:
    """Return the active `WaitForConditionCheckContext`."""
    current_context = get_current_context()
    if not isinstance(current_context, WaitForConditionCheckContext):
        msg = (
            "get_wait_for_condition_check_context() can only be used while a "
            "wait_for_condition check is executing."
        )
        raise RuntimeError(msg)
    return current_context
