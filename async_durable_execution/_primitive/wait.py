"""Implement the durable wait operation."""

from __future__ import annotations

import asyncio
import logging

from .base import OperationExecutor
from .._core import (
    Duration,
    DurableContext,
    ExecutionState,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationSubType,
    OperationUpdate,
    ValidationError,
    WaitOptions,
    create_eager_task,
    duration_to_seconds,
    get_durable_context,
    suspend_with_optional_resume_delay,
)

logger = logging.getLogger(__name__)


class WaitOperationExecutor(OperationExecutor[None]):
    """Executor for wait operations."""

    def __init__(
        self,
        seconds: int,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
    ) -> None:
        """Initialize the wait operation executor.

        Args:
            seconds: Number of seconds to wait
            state: The execution state
            operation_identifier: The operation identifier
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.seconds = seconds

    async def start(self) -> None:
        """Start a new wait operation."""
        operation: OperationUpdate = OperationUpdate.create_wait_start(
            identifier=self.operation_identifier,
            wait_options=WaitOptions(wait_seconds=self.seconds),
        )
        await self.create_checkpoint(operation, is_sync=True)

        logger.debug(
            "Wait checkpoint created for id: %s, name: %s, will suspend",
            self.operation_identifier.operation_id,
            self.operation_identifier.name,
        )

        return await self.execute()

    async def replay(self, operation: Operation) -> None:
        """Replay an existing wait operation from its checkpoint."""
        if operation.status is OperationStatus.SUCCEEDED:
            logger.debug(
                "Wait already completed, skipping wait for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            return None

        await self.execute()

    async def execute(self, operation: Operation | None = None) -> None:
        """Execute wait by suspending.

        Wait operations 'execute' by suspending execution until the timer completes.
        This method never returns normally - it always suspends.

        Raises:
            SuspendExecution: Always suspends to wait for timer completion
        """
        msg: str = f"Wait for {self.seconds} seconds"
        suspend_with_optional_resume_delay(msg, self.seconds)  # throws suspend


def wait(duration: Duration, *, name: str | None = None) -> asyncio.Task[None]:
    """Suspend the durable execution for at least the given duration.

    Args:
        duration: Seconds or timedelta to pause. Must be at least one second.
        name: Optional operation name shown in execution history.
    """
    context = get_durable_context()
    seconds = duration_to_seconds(duration)
    if seconds < 1:
        msg = "duration must be at least 1 second"
        raise ValidationError(msg)

    with context._replay_aware():
        operation_id = context.step_counter.create_step_id()
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.WAIT,
            parent_id=context.parent_id,
            name=name,
        )

        return create_eager_task(
            lambda: _wait(
                seconds=seconds,
                context=context,
                operation_identifier=operation_identifier,
            ),
        )


async def _wait(
    *,
    seconds: int,
    context: DurableContext,
    operation_identifier: OperationIdentifier,
) -> None:
    executor: WaitOperationExecutor = WaitOperationExecutor(
        seconds=seconds,
        state=context.execution_state,
        operation_identifier=operation_identifier,
    )
    await executor.process()
