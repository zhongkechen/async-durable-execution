"""Implement the durable wait_for_condition operation."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from ..config import Duration, JitterStrategy, duration_to_seconds
from ..context import (
    bind_current_context,
)
from ..exceptions import (
    CallableRuntimeError,
    ExecutionError,
    ValidationError,
    suspend_with_optional_resume_delay,
    suspend_with_optional_resume_timestamp,
)
from ..models import (
    ErrorObject,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationUpdate,
    OperationSubType,
)
from ..primitive.base import OperationExecutor
from ..primitive.child import get_durable_context
from ..primitive.step import StepContext
from ..task import create_eager_task

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from ..primitive.child import DurableContext
    from ..serdes import SerDes
    from ..state import ExecutionState


T = TypeVar("T")

logger = logging.getLogger(__name__)


WaitDelayStrategyFunction = Callable[[T, int], Duration | None]


@dataclass
class WaitDelayStrategy(Generic[T]):
    """Polling delay strategy for `wait_for_condition()`."""

    max_attempts: int = 60
    initial_delay: Duration = 5
    max_delay: Duration = 300
    backoff_rate: int | float = 1.5
    jitter_strategy: JitterStrategy = field(default=JitterStrategy.FULL)
    timeout: Duration | None = None

    def __post_init__(self):
        self.initial_delay = duration_to_seconds(self.initial_delay, "initial_delay")
        self.max_delay = duration_to_seconds(self.max_delay, "max_delay")
        if self.timeout is not None:
            self.timeout = duration_to_seconds(self.timeout, "timeout")

    @property
    def initial_delay_seconds(self) -> int:
        """Get initial delay in seconds."""
        return duration_to_seconds(self.initial_delay, "initial_delay")

    @property
    def max_delay_seconds(self) -> int:
        """Get max delay in seconds."""
        return duration_to_seconds(self.max_delay, "max_delay")

    @property
    def timeout_seconds(self) -> int | None:
        """Get timeout in seconds."""
        if self.timeout is None:
            return None
        return duration_to_seconds(self.timeout, "timeout")

    def __call__(self, result: T, attempts_made: int) -> int | None:
        """Return the next polling delay, or None to stop polling."""
        if attempts_made >= self.max_attempts:
            return None

        base_delay: float = min(
            self.initial_delay_seconds * (self.backoff_rate ** (attempts_made - 1)),
            self.max_delay_seconds,
        )
        delay_with_jitter: float = self.jitter_strategy.apply_jitter(base_delay)
        final_delay: int = max(1, math.ceil(delay_with_jitter))

        return final_delay


class WaitForConditionOperationExecutor(OperationExecutor[T]):
    """Executor for wait_for_condition operations."""

    def __init__(
        self,
        check: Callable[[T | None], Awaitable[T]],
        initial_state: T | None,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        wait_strategy: WaitDelayStrategyFunction[T] | None = None,
        serdes: SerDes | None = None,
    ):
        """Initialize the wait_for_condition executor.

        Args:
            check: The check function to evaluate the condition
            initial_state: The state to pass to the first condition evaluation
            state: The execution state
            operation_identifier: The operation identifier
            wait_strategy: Optional strategy for deciding retry delays
            serdes: Optional serializer/deserializer for state payloads
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.check = check
        self.initial_state = initial_state
        self.wait_strategy = wait_strategy
        self.serdes = serdes
        self.default_wait_strategy = WaitDelayStrategy[T]()

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
        # Determine current state from checkpoint
        operation_details = operation.step_details if operation is not None else None
        if (
            operation is not None
            and operation.status in {OperationStatus.STARTED, OperationStatus.READY}
            and operation_details is not None
            and operation_details.result
        ):
            try:
                current_state = await self.deserialize_value(
                    data=operation_details.result,
                    serdes=self.serdes,
                )
            except Exception:
                # Default to initial state if there's an error getting checkpointed state
                logger.exception(
                    "⚠️ wait_for_condition failed to deserialize state for id: %s, name: %s. Using initial state.",
                    self.operation_identifier.operation_id,
                    self.operation_name,
                )
                current_state = self.initial_state
        else:
            current_state = self.initial_state

        # Get attempt number - current attempt is checkpointed attempts + 1
        # The checkpoint stores completed attempts, so the current attempt being executed is one more
        attempt: int = 1
        if operation_details is not None:
            attempt = operation_details.attempt + 1

        try:
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

            if new_state:
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
                return await self.deserialize_value(  # noqa: TRY300
                    data=serialized_state,
                    serdes=self.serdes,
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
                return await self.deserialize_value(  # noqa: TRY300
                    data=serialized_state,
                    serdes=self.serdes,
                )

            # Condition not met - schedule retry.
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

    def _resolve_delay_seconds(self, new_state: T, attempt: int) -> int | None:
        wait_strategy = self.wait_strategy or self.default_wait_strategy
        wait_delay = wait_strategy(new_state, attempt)

        if wait_delay is None:
            return None

        if isinstance(wait_delay, int | timedelta):
            return duration_to_seconds(wait_delay, "wait_strategy delay")

        msg = "wait_for_condition wait_strategy must return int seconds, timedelta, or None"
        raise ValidationError(msg)


def wait_for_condition(
    check: Callable[[T | None], Awaitable[T]],
    *,
    initial_state: T | None = None,
    name: str | None = None,
    wait_strategy: WaitDelayStrategyFunction[T] | None = None,
    serdes: SerDes | None = None,
) -> asyncio.Task[T]:
    """Poll durable state until the configured strategy decides to stop waiting.

    The check receives the current state, beginning with `initial_state`,
    and returns the next state. Truthy check results complete the operation.
    Falsey results continue polling with the wait strategy's delay. The wait
    strategy can return None to stop polling.
    """
    context = get_durable_context("wait_for_condition")

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
                wait_strategy=wait_strategy,
                serdes=serdes,
            ),
        )


async def _wait_for_condition(
    check: Callable[[T | None], Awaitable[T]],
    *,
    context: DurableContext,
    operation_identifier: OperationIdentifier,
    initial_state: T | None = None,
    wait_strategy: WaitDelayStrategyFunction[T] | None = None,
    serdes: SerDes | None = None,
) -> T:
    executor: WaitForConditionOperationExecutor[T] = WaitForConditionOperationExecutor(
        check=check,
        initial_state=initial_state,
        state=context.execution_state,
        operation_identifier=operation_identifier,
        wait_strategy=wait_strategy,
        serdes=serdes,
    )
    return await executor.process()


@dataclass(frozen=True)
class WaitForConditionCheckContext(StepContext):
    """Context available during wait_for_condition checker execution."""

    pass
