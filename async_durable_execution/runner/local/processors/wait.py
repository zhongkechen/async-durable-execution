"""Wait operation processor for handling WAIT operation updates."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ....core.models import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
    WaitDetails,
)
from .base import (
    OperationProcessor,
)
from ...exceptions import (
    InvalidParameterValueException,
)
from ..time_scale import scale_delay

VALID_ACTIONS_FOR_WAIT = frozenset(
    [
        OperationAction.START,
        OperationAction.CANCEL,
    ]
)


class WaitProcessor(OperationProcessor):
    """Processes WAIT operation updates with timer scheduling."""

    valid_actions = VALID_ACTIONS_FOR_WAIT
    _ALLOWED_STATUS_TO_CANCEL = frozenset(
        [
            OperationStatus.STARTED,
        ]
    )

    @classmethod
    def validate(cls, current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate WAIT operation update."""
        super().validate(current_state, update)

        match update.action:
            case OperationAction.START:
                if current_state is not None:
                    msg_wait_exists: str = "Cannot start a WAIT that already exist."
                    raise InvalidParameterValueException(msg_wait_exists)
            case OperationAction.CANCEL:
                if (
                    current_state is None
                    or current_state.status not in cls._ALLOWED_STATUS_TO_CANCEL
                ):
                    msg_wait_cancel: str = "Cannot cancel a WAIT that does not exist or has already completed."
                    raise InvalidParameterValueException(msg_wait_cancel)

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: Any,
        execution_arn: str,
    ) -> Operation:
        """Process WAIT operation update with scheduler integration for timers."""
        match update.action:
            case OperationAction.START:
                wait_seconds = (
                    update.wait_options.wait_seconds if update.wait_options else 0
                )
                scaled_wait_seconds = scale_delay(wait_seconds)

                scheduled_end_timestamp = datetime.now(timezone.utc) + timedelta(
                    seconds=scaled_wait_seconds
                )

                # Create WaitDetails with scheduled timestamp
                wait_details = WaitDetails(
                    scheduled_end_timestamp=scheduled_end_timestamp
                )

                # Create new operation with wait details
                wait_operation = Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                    name=update.name,
                    start_timestamp=datetime.now(timezone.utc),
                    end_timestamp=None,
                    sub_type=update.sub_type,
                    execution_details=None,
                    context_details=None,
                    step_details=None,
                    wait_details=wait_details,
                    callback_details=None,
                    chained_invoke_details=None,
                )

                # Schedule wait timer to complete after delay
                notifier.schedule_wait_timer(
                    execution_arn, update.operation_id, scaled_wait_seconds
                )
                return wait_operation
            case OperationAction.CANCEL:
                # TODO: need to cancel the WAIT in the executor
                # TODO: increase sequence id
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.CANCELLED,
                )
            case _:
                msg: str = "Invalid action for WAIT operation."

                raise InvalidParameterValueException(msg)
