"""Implement the durable wait_for_condition operation."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

from .step import StepContext
from ..async_tools import assert_async_callable

from ..config import WaitForConditionConfig
from ..context import (
    reset_current_context,
    set_current_context,
)
from .child import _get_durable_context
from ..exceptions import (
    ExecutionError,
    ValidationError,
)
from ..models import (
    ErrorObject,
    OperationIdentifier,
    OperationUpdate,
    OperationSubType,
)
from .base import OperationExecutor
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
    from ..models import WaitForConditionDecision


T = TypeVar("T")

logger = logging.getLogger(__name__)


class WaitForConditionOperationExecutor(OperationExecutor[T]):
    """Executor for wait_for_condition operations."""

    def __init__(
        self,
        check: Callable[[T], Awaitable[T]],
        config: WaitForConditionConfig[T],
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
    ):
        """Initialize the wait_for_condition executor.

        Args:
            check: The check function to evaluate the condition
            config: Configuration for the wait_for_condition operation
            state: The execution state
            operation_identifier: The operation identifier
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.check = check
        self.config = config

    async def process(self) -> T:
        """Process wait_for_condition checkpoint state and execute the checker."""
        checkpointed_result = self.get_checkpointed_result()

        # Check if already completed
        if checkpointed_result.is_succeeded():
            logger.debug(
                "wait_for_condition already completed for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_name,
            )
            if checkpointed_result.result is None:
                return None  # type: ignore[return-value]
            result = self.deserialize_value(
                data=checkpointed_result.result,
                serdes=self.config.serdes,
            )
            return result

        # Terminal failure
        if checkpointed_result.is_failed():
            checkpointed_result.raise_callable_error()

        # Pending retry
        if checkpointed_result.is_pending():
            scheduled_timestamp = checkpointed_result.get_next_attempt_timestamp()
            suspend_with_optional_resume_timestamp(
                msg=f"wait_for_condition {self.operation_name or self.operation_identifier.operation_id} will retry at timestamp {scheduled_timestamp}",
                datetime_timestamp=scheduled_timestamp,
            )

        # Create START checkpoint if not started
        if not checkpointed_result.is_started():
            start_operation = OperationUpdate.create_wait_for_condition_start(
                identifier=self.operation_identifier,
            )
            # Checkpoint wait_for_condition START with non-blocking (is_sync=False).
            # This is purely for observability - we don't need to wait for persistence before
            # executing the check function. The START checkpoint just records that polling began.
            await self.create_checkpoint(start_operation, is_sync=False)
            # For async checkpoint, no immediate response possible
            # Proceed directly to execute with current checkpoint data

        return await self.execute(checkpointed_result)

    async def execute(self, checkpointed_result: CheckpointedResult) -> T:
        """Execute check function and handle decision.

        Args:
            checkpointed_result: The checkpoint data

        Returns:
            The final state when condition is met

        Raises:
            Suspends if condition not met
            Raises error if check function fails
        """
        # Determine current state from checkpoint
        if checkpointed_result.is_started_or_ready() and checkpointed_result.result:
            try:
                current_state = self.deserialize_value(
                    data=checkpointed_result.result,
                    serdes=self.config.serdes,
                )
            except Exception:
                # Default to initial state if there's an error getting checkpointed state
                logger.exception(
                    "⚠️ wait_for_condition failed to deserialize state for id: %s, name: %s. Using initial state.",
                    self.operation_identifier.operation_id,
                    self.operation_name,
                )
                current_state = self.config.initial_state
        else:
            current_state = self.config.initial_state

        # Get attempt number - current attempt is checkpointed attempts + 1
        # The checkpoint stores completed attempts, so the current attempt being executed is one more
        attempt: int = 1
        if checkpointed_result.operation and checkpointed_result.operation.step_details:
            attempt = checkpointed_result.operation.step_details.attempt + 1

        try:
            step_context = StepContext(
                attempt=attempt,
                execution_state=self.state,
                operation_identifier=self.operation_identifier,
            )
            wrapped_user_func = self.state.wrap_user_function(
                self.check,
                self.operation_identifier,
                False,
                attempt,
            )
            token = set_current_context(
                WaitForConditionCheckContext(
                    attempt=attempt,
                    execution_state=step_context.execution_state,
                    operation_identifier=self.operation_identifier,
                )
            )
            try:
                new_state = await wrapped_user_func(current_state)
            finally:
                reset_current_context(token)

            # Check if condition is met with the wait strategy
            decision: WaitForConditionDecision = self.config.wait_strategy(
                new_state, attempt
            )

            serialized_state = self.serialize_value(
                value=new_state,
                serdes=self.config.serdes,
            )

            logger.debug(
                "wait_for_condition check completed: %s, name: %s, attempt: %s",
                self.operation_identifier.operation_id,
                self.operation_name,
                attempt,
            )

            if not decision.should_continue:
                # Condition is met - complete successfully
                success_operation = OperationUpdate.create_wait_for_condition_succeed(
                    identifier=self.operation_identifier,
                    payload=serialized_state,
                )
                # Checkpoint SUCCEED operation with blocking (is_sync=True, default).
                # Must ensure the final state is persisted before returning to the caller.
                # This guarantees the condition result is durable and won't be re-evaluated on replay.
                await self.create_checkpoint(success_operation)

                logger.debug(
                    "✅ wait_for_condition completed for id: %s, name: %s",
                    self.operation_identifier.operation_id,
                    self.operation_name,
                )
                return new_state

            # Condition not met - schedule retry
            # We enforce a minimum delay second of 1, to match model behaviour.
            delay_seconds = decision.delay_seconds
            if delay_seconds is not None and delay_seconds < 1:
                logger.warning(
                    (
                        "WaitDecision delay_seconds step for id: %s, name: %s,"
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
                msg=f"wait_for_condition {self.operation_identifier.name or self.operation_identifier.operation_id} will retry in {decision.delay_seconds} seconds",
                delay_seconds=decision.delay_seconds,
            )

        except Exception as e:
            # Mark as failed - waitForCondition doesn't have its own retry logic for errors
            # If the check function throws, it's considered a failure
            logger.exception(
                "❌ wait_for_condition failed for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )

            fail_operation = OperationUpdate.create_wait_for_condition_fail(
                identifier=self.operation_identifier,
                error=ErrorObject.from_exception(e),
            )
            # Checkpoint FAIL operation with blocking (is_sync=True, default).
            # Must ensure the failure state is persisted before raising the exception.
            # This guarantees the error is durable and the condition won't be re-evaluated on replay.
            await self.create_checkpoint(fail_operation)
            raise

        msg: str = (
            "wait_for_condition should never reach this point"  # pragma: no cover
        )
        raise ExecutionError(msg)  # pragma: no cover


async def wait_for_condition(
    check: Callable[[T], Awaitable[T]],
    config: WaitForConditionConfig[T],
    name: str | None = None,
) -> T:
    context = _get_durable_context("wait_for_condition")
    if check is None:
        msg = "`check` is required for wait_for_condition"
        raise ValidationError(msg)
    if not config:
        msg = "`config` is required for wait_for_condition"
        raise ValidationError(msg)
    assert_async_callable(check, label="check")

    operation_id = context.step_counter.create_step_id()
    executor: WaitForConditionOperationExecutor[T] = WaitForConditionOperationExecutor(
        check=check,
        config=config,
        state=context.execution_state,
        operation_identifier=OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            parent_id=context.parent_id,
            name=name,
        ),
    )
    result: T = await executor.process()
    context.execution_state.track_replay(operation_id=operation_id)
    return result


@dataclass(frozen=True)
class WaitForConditionCheckContext(StepContext):
    """Context available during wait_for_condition checker execution."""

    pass
