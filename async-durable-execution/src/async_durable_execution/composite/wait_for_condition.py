"""Implement the durable wait_for_condition operation."""

from __future__ import annotations

import math
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from ..primitive.step import StepContext
from ..async_tools import assert_async_callable
from ..config import JitterStrategy, duration_to_seconds
from ..context import (
    reset_current_context,
    set_current_context,
)
from ..primitive.child import _get_durable_context
from ..exceptions import (
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
from ..primitive.base import (
    CHECKPOINT_NOT_FOUND,
    CheckpointedResult,
    OperationExecutor,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from ..serdes import SerDes
    from ..state import ExecutionState
    from ..types import LambdaContext


T = TypeVar("T")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WaitDecision:
    """Decision about whether to wait and with what delay."""

    should_wait: bool
    delay: timedelta

    def __post_init__(self):
        if self.delay.total_seconds() < 0:
            msg = "delay must be non-negative"
            raise ValueError(msg)

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return int(self.delay.total_seconds())

    @classmethod
    def wait(cls, delay: timedelta) -> WaitDecision:
        """Create a wait decision."""
        return cls(should_wait=True, delay=delay)

    @classmethod
    def no_wait(cls) -> WaitDecision:
        """Create a no-wait decision."""
        return cls(should_wait=False, delay=timedelta())


@dataclass(frozen=True)
class WaitForConditionDecision:
    """Decision about whether to continue waiting."""

    should_continue: bool
    delay: timedelta

    def __post_init__(self):
        if self.delay.total_seconds() < 0:
            msg = "delay must be non-negative"
            raise ValueError(msg)

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return int(self.delay.total_seconds())

    @classmethod
    def continue_waiting(
        cls,
        delay: timedelta = timedelta(),
    ) -> WaitForConditionDecision:
        """Create a decision to continue waiting."""
        return cls(should_continue=True, delay=delay)

    @classmethod
    def stop_polling(cls) -> WaitForConditionDecision:
        """Create a decision to stop polling."""
        return cls(should_continue=False, delay=timedelta())


ConditionResult = tuple[T, WaitForConditionDecision]
WaitDelayStrategy = Callable[[T, int], WaitDecision | timedelta]


@dataclass(frozen=True)
class WaitForConditionConfig(Generic[T]):
    """Configuration for wait_for_condition."""

    wait_strategy: WaitDelayStrategy[T] | None = None
    serdes: SerDes | None = None


@dataclass
class WaitStrategyBuilder(Generic[T]):
    """Build polling strategies for `wait_for_condition()`."""

    should_continue_polling: Callable[[T], bool] | None = None
    max_attempts: int = 60
    initial_delay: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    max_delay: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    backoff_rate: int | float = 1.5
    jitter_strategy: JitterStrategy = field(default=JitterStrategy.FULL)
    timeout: timedelta | None = None

    def __post_init__(self):
        duration_to_seconds(self.initial_delay, "initial_delay")
        duration_to_seconds(self.max_delay, "max_delay")
        if self.timeout is not None:
            duration_to_seconds(self.timeout, "timeout")

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

    def build(self) -> Callable[[T, int], WaitDecision]:
        """Build a wait strategy callable from this builder."""

        def wait_strategy(result: T, attempts_made: int) -> WaitDecision:
            if (
                self.should_continue_polling is not None
                and not self.should_continue_polling(result)
            ):
                return WaitDecision.no_wait()

            if (
                self.should_continue_polling is not None
                and attempts_made >= self.max_attempts
            ):
                return WaitDecision.no_wait()

            base_delay: float = min(
                self.initial_delay_seconds * (self.backoff_rate ** (attempts_made - 1)),
                self.max_delay_seconds,
            )
            delay_with_jitter: float = self.jitter_strategy.apply_jitter(base_delay)
            final_delay: int = max(1, math.ceil(delay_with_jitter))

            return WaitDecision.wait(timedelta(seconds=final_delay))

        return wait_strategy


class WaitForConditionOperationExecutor(OperationExecutor[T]):
    """Executor for wait_for_condition operations."""

    def __init__(
        self,
        check: Callable[[T | None], Awaitable[ConditionResult[T]]],
        config: WaitForConditionConfig[T],
        initial_state: T | None,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        lambda_context: LambdaContext | None = None,
    ):
        """Initialize the wait_for_condition executor.

        Args:
            check: The check function to evaluate the condition
            config: Configuration for the wait_for_condition operation
            initial_state: The state to pass to the first condition evaluation
            state: The execution state
            operation_identifier: The operation identifier
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.check = check
        self.config = config
        self.initial_state = initial_state
        self.lambda_context = lambda_context
        self.default_wait_strategy = WaitStrategyBuilder[T]().build()

    async def start(self) -> T:
        """Start a new wait_for_condition operation."""
        start_operation = OperationUpdate.create_wait_for_condition_start(
            identifier=self.operation_identifier,
        )
        await self.create_checkpoint(start_operation, is_sync=False)
        return await self.execute(CHECKPOINT_NOT_FOUND)

    async def replay(self, operation: Operation) -> T:
        """Replay an existing wait_for_condition operation from its checkpoint."""
        if operation.status is OperationStatus.SUCCEEDED:
            checkpointed_result = CheckpointedResult.create_from_operation(operation)
            logger.debug(
                "wait_for_condition already completed for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_name,
            )
            if checkpointed_result.result is None:
                return None  # type: ignore[return-value]
            result = await self.deserialize_value(
                data=checkpointed_result.result,
                serdes=self.config.serdes,
            )
            return result

        if operation.status is OperationStatus.FAILED:
            CheckpointedResult.create_from_operation(operation).raise_callable_error()

        if operation.status is OperationStatus.PENDING:
            checkpointed_result = CheckpointedResult.create_from_operation(operation)
            scheduled_timestamp = checkpointed_result.get_next_attempt_timestamp()
            suspend_with_optional_resume_timestamp(
                msg=f"wait_for_condition {self.operation_name or self.operation_identifier.operation_id} will retry at timestamp {scheduled_timestamp}",
                datetime_timestamp=scheduled_timestamp,
            )

        checkpointed_result = CheckpointedResult.create_from_operation(operation)
        if operation.status is not OperationStatus.STARTED:
            start_operation = OperationUpdate.create_wait_for_condition_start(
                identifier=self.operation_identifier,
            )
            await self.create_checkpoint(start_operation, is_sync=False)

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
                current_state = await self.deserialize_value(
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
                current_state = self.initial_state
        else:
            current_state = self.initial_state

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
                lambda_context=self.lambda_context,
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
                    lambda_context=step_context.lambda_context,
                )
            )
            try:
                condition_result = await wrapped_user_func(current_state)
            finally:
                reset_current_context(token)

            new_state, decision = self._resolve_condition_result(condition_result)

            serialized_state = await self.serialize_value(
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

            # Condition not met - schedule retry. The check decides whether
            # to keep polling; the wait strategy only supplies the retry delay.
            suspend_delay_seconds = self._resolve_delay_seconds(new_state, attempt)
            delay_seconds = suspend_delay_seconds

            # We enforce a minimum delay second of 1, to match model behaviour.
            if delay_seconds < 1:
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
        wait_strategy = self.config.wait_strategy or self.default_wait_strategy
        wait_decision = wait_strategy(new_state, attempt)

        return self._wait_decision_to_seconds(wait_decision)

    def _wait_decision_to_seconds(
        self,
        wait_decision: WaitDecision | timedelta,
    ) -> int:
        if isinstance(wait_decision, WaitDecision):
            return wait_decision.delay_seconds

        if isinstance(wait_decision, timedelta):
            return int(wait_decision.total_seconds())

        msg = "wait_for_condition wait_strategy must return timedelta or WaitDecision"
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
    context = _get_durable_context("wait_for_condition")
    if check is None:
        msg = "`check` is required for wait_for_condition"
        raise ValidationError(msg)
    config = WaitForConditionConfig(
        wait_strategy=wait_strategy,
        serdes=serdes,
    )
    assert_async_callable(check, label="check")

    operation_id = context.step_counter.create_step_id()
    operation_identifier = OperationIdentifier(
        operation_id=operation_id,
        sub_type=OperationSubType.WAIT_FOR_CONDITION,
        parent_id=context.parent_id,
        name=name,
    )
    if context.lambda_context is None:
        executor: WaitForConditionOperationExecutor[T] = (
            WaitForConditionOperationExecutor(
                check=check,
                config=config,
                initial_state=initial_state,
                state=context.execution_state,
                operation_identifier=operation_identifier,
            )
        )
    else:
        executor = WaitForConditionOperationExecutor(
            check=check,
            config=config,
            initial_state=initial_state,
            state=context.execution_state,
            operation_identifier=operation_identifier,
            lambda_context=context.lambda_context,
        )
    result: T = await executor.process()
    context.execution_state.track_replay(operation_id=operation_id)
    return result


@dataclass(frozen=True)
class WaitForConditionCheckContext(StepContext):
    """Context available during wait_for_condition checker execution."""

    pass
