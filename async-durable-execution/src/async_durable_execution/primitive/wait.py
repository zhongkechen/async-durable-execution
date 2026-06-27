"""Implement the durable wait operation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..exceptions import ValidationError, suspend_with_optional_resume_delay
from ..config import Duration, duration_to_seconds
from .child import get_durable_context
from ..models import (
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationSubType,
    OperationUpdate,
    WaitOptions,
)
from .base import OperationExecutor

if TYPE_CHECKING:
    from ..state import ExecutionState

logger = logging.getLogger(__name__)


class WaitOperationExecutor(OperationExecutor[None]):
    """Executor for wait operations."""

    def __init__(
        self,
        seconds: int,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
    ):
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

    async def execute(self) -> None:  # type: ignore[override]
        """Execute wait by suspending.

        Wait operations 'execute' by suspending execution until the timer completes.
        This method never returns normally - it always suspends.

        Raises:
            SuspendExecution: Always suspends to wait for timer completion
        """
        msg: str = f"Wait for {self.seconds} seconds"
        suspend_with_optional_resume_delay(msg, self.seconds)  # throws suspend


async def wait(duration: Duration, *, name: str | None = None) -> None:
    """Suspend the durable execution for at least the given duration.

    Args:
        duration: Seconds or timedelta to pause. Must be at least one second.
        name: Optional operation name shown in execution history.
    """
    context = get_durable_context("wait")
    seconds = duration_to_seconds(duration)
    if seconds < 1:
        msg = "duration must be at least 1 second"
        raise ValidationError(msg)
    with context._replay_aware():
        operation_id = context.step_counter.create_step_id()

        executor: WaitOperationExecutor = WaitOperationExecutor(
            seconds=seconds,
            state=context.execution_state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                sub_type=OperationSubType.WAIT,
                parent_id=context.parent_id,
                name=name,
            ),
        )
        await executor.process()
