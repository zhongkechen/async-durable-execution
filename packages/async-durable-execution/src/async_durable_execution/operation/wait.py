"""Implement the durable wait operation."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from ..exceptions import ValidationError
from ..config import duration_to_seconds
from .child import _get_durable_context, DurableContext
from ..models import OperationIdentifier, OperationUpdate, WaitOptions, OperationSubType
from .base import OperationExecutor
from ..suspend import suspend_with_optional_resume_delay


if TYPE_CHECKING:
    from ..state import (
        CheckpointedResult,
        ExecutionState,
    )

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

    async def process(self) -> None:
        """Process wait checkpoint state and suspend until completion."""
        checkpointed_result: CheckpointedResult = self.get_checkpointed_result()

        # Terminal success - wait completed
        if checkpointed_result.is_succeeded():
            logger.debug(
                "Wait already completed, skipping wait for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            return None

        # Create START checkpoint if not exists
        if not checkpointed_result.is_existent():
            operation: OperationUpdate = OperationUpdate.create_wait_start(
                identifier=self.operation_identifier,
                wait_options=WaitOptions(wait_seconds=self.seconds),
            )
            # Checkpoint wait START with blocking (is_sync=True, default).
            # Must ensure the wait operation and scheduled timestamp are persisted before suspending.
            # This guarantees the wait will resume at the correct time on the next invocation.
            await self.create_checkpoint(operation, is_sync=True)

            logger.debug(
                "Wait checkpoint created for id: %s, name: %s, will check for immediate response",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )

            checkpointed_result = self.get_checkpointed_result()
            if checkpointed_result.is_succeeded():
                return None

        await self.execute(checkpointed_result)
        return None

    async def execute(self, _checkpointed_result: CheckpointedResult) -> None:
        """Execute wait by suspending.

        Wait operations 'execute' by suspending execution until the timer completes.
        This method never returns normally - it always suspends.

        Args:
            _checkpointed_result: The checkpoint data (unused for wait)

        Raises:
            SuspendExecution: Always suspends to wait for timer completion
        """
        msg: str = f"Wait for {self.seconds} seconds"
        suspend_with_optional_resume_delay(msg, self.seconds)  # throws suspend


async def _wait_in_context(
    context: DurableContext,
    duration: timedelta,
    name: str | None = None,
) -> None:
    seconds = duration_to_seconds(duration)
    if seconds < 1:
        msg = "duration must be at least 1 second"
        raise ValidationError(msg)
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
    context.execution_state.track_replay(operation_id=operation_id)


async def wait(duration: timedelta, name: str | None = None) -> None:
    context = _get_durable_context("wait")
    await _wait_in_context(context, duration=duration, name=name)
