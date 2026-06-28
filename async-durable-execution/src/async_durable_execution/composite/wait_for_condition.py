"""Implement the durable wait_for_condition operation."""

from __future__ import annotations

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

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from ..serdes import SerDes
    from ..state import ExecutionState


T = TypeVar("T")

logger = logging.getLogger(__name__)


def _decision_delay_to_seconds(delay: Duration) -> int:
    try:
        return duration_to_seconds(delay, "delay")
    except ValidationError as error:
        raise ValueError(str(error)) from error


@dataclass(frozen=True)
class WaitForConditionDecision:
    """Decision about whether to continue waiting."""

    should_continue: bool
    delay: Duration

    def __post_init__(self):
        object.__setattr__(self, "delay", _decision_delay_to_seconds(self.delay))

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return _decision_delay_to_seconds(self.delay)

    @classmethod
    def continue_waiting(
        cls,
        delay: Duration = 0,
    ) -> WaitForConditionDecision:
        """Create a decision to continue waiting."""
        return cls(should_continue=True, delay=delay)

    @classmethod
    def stop_polling(cls) -> WaitForConditionDecision:
        """Create a decision to stop polling."""
        return cls(should_continue=False, delay=0)


ConditionResult = tuple[T, WaitForConditionDecision]
WaitDelayStrategy = Callable[[T, int], Duration]


@dataclass
class WaitStrategyBuilder(Generic[T]):
    """Build polling strategies for `wait_for_condition()`."""

    should_continue_polling: Callable[[T], bool] | None = None
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

    def build(self) -> Callable[[T, int], int]:
        """Build a wait strategy callable from this builder."""

        def wait_strategy(result: T, attempts_made: int) -> int:
            if (
                self.should_continue_polling is not None
                and not self.should_continue_polling(result)
            ):
                return 0

            if (
                self.should_continue_polling is not None
                and attempts_made >= self.max_attempts
            ):
                return 0

            base_delay: float = min(
                self.initial_delay_seconds * (self.backoff_rate ** (attempts_made - 1)),
                self.max_delay_seconds,
            )
            delay_with_jitter: float = self.jitter_strategy.apply_jitter(base_delay)
            final_delay: int = max(1, math.ceil(delay_with_jitter))

            return final_delay

        return wait_strategy


class WaitForConditionOperationExecutor(OperationExecutor[T]):
    """Executor for wait_for_condition operations."""

    def __init__(
        self,
        check: Callable[[T | None], Awaitable[ConditionResult[T]]],
        initial_state: T | None,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        wait_strategy: WaitDelayStrategy[T] | None = None,
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
        self.default_wait_strategy = WaitStrategyBuilder[T]().build()

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
                condition_result = await self.check(current_state)

            new_state, decision = self._resolve_condition_result(condition_result)

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

            # Condition not met - schedule retry. The check decides whether
            # to keep polling; the wait strategy only supplies the retry delay.
            suspend_delay_seconds = self._resolve_delay_seconds(new_state, attempt)
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

    def _resolve_condition_result(
        self,
        condition_result: object,
    ) -> tuple[T, WaitForConditionDecision]:
        if (
            isinstance(condition_result, tuple)
            and len(condition_result) == 2
            and isinstance(condition_result[1], WaitForConditionDecision)
        ):
            return cast(T, condition_result[0]), condition_result[1]

        msg = "wait_for_condition check must return (state, WaitForConditionDecision)"
        raise ValidationError(msg)

    def _resolve_delay_seconds(self, new_state: T, attempt: int) -> int:
        wait_strategy = self.wait_strategy or self.default_wait_strategy
        wait_delay = wait_strategy(new_state, attempt)

        if isinstance(wait_delay, int | timedelta):
            return duration_to_seconds(wait_delay, "wait_strategy delay")

        msg = "wait_for_condition wait_strategy must return int seconds or timedelta"
        raise ValidationError(msg)


async def wait_for_condition(
    check: Callable[[T | None], Awaitable[ConditionResult[T]]] | None = None,
    *,
    initial_state: T | None = None,
    name: str | None = None,
    wait_strategy: WaitDelayStrategy[T] | None = None,
    serdes: SerDes | None = None,
) -> T:
    """Poll durable state until the configured strategy decides to stop waiting.

    The check receives the current state, beginning with `initial_state`,
    and returns the next state plus a decision to continue or stop. The optional
    wait strategy only decides how long to wait before the next poll.
    """
    context = get_durable_context("wait_for_condition")
    if check is None:
        msg = "`check` is required for wait_for_condition"
        raise ValidationError(msg)

    with context._replay_aware(executes_user_code=True):
        operation_id = context.step_counter.create_step_id()
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            parent_id=context.parent_id,
            name=name,
        )
        executor: WaitForConditionOperationExecutor[T] = (
            WaitForConditionOperationExecutor(
                check=check,
                initial_state=initial_state,
                state=context.execution_state,
                operation_identifier=operation_identifier,
                wait_strategy=wait_strategy,
                serdes=serdes,
            )
        )
        return await executor.process()


@dataclass(frozen=True)
class WaitForConditionCheckContext(StepContext):
    """Context available during wait_for_condition checker execution."""

    pass
