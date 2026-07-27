"""Step operation processor for handling STEP operation updates."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ...._core import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
    StepDetails,
)
from .base import (
    OperationProcessor,
)
from ...exceptions import (
    InvalidParameterValueException,
)
from ..time_scale import scale_delay

VALID_ACTIONS_FOR_STEP = frozenset(
    [
        OperationAction.START,
        OperationAction.FAIL,
        OperationAction.RETRY,
        OperationAction.SUCCEED,
    ]
)


class StepProcessor(OperationProcessor):
    """Processes STEP operation updates with retry scheduling."""

    valid_actions = VALID_ACTIONS_FOR_STEP
    _ALLOWED_STATUS_TO_CLOSE = frozenset(
        [
            OperationStatus.STARTED,
            OperationStatus.READY,
        ]
    )
    _ALLOWED_STATUS_TO_START = frozenset(
        [
            OperationStatus.READY,
        ]
    )
    _ALLOWED_STATUS_TO_REATTEMPT = frozenset(
        [
            OperationStatus.STARTED,
            OperationStatus.READY,
        ]
    )

    @classmethod
    def validate(cls, current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate STEP operation update."""
        super().validate(current_state, update)
        if current_state is None:
            return

        match update.action:
            case OperationAction.START:
                if current_state.status not in cls._ALLOWED_STATUS_TO_START:
                    msg_step_start: str = "Invalid current STEP state to start."
                    raise InvalidParameterValueException(msg_step_start)
            case OperationAction.FAIL | OperationAction.SUCCEED:
                if current_state.status not in cls._ALLOWED_STATUS_TO_CLOSE:
                    msg_step_close: str = "Invalid current STEP state to close."
                    raise InvalidParameterValueException(msg_step_close)
                if update.action == OperationAction.FAIL and update.payload is not None:
                    msg_fail_payload: str = "Cannot provide a Payload for FAIL action."
                    raise InvalidParameterValueException(msg_fail_payload)
                if (
                    update.action == OperationAction.SUCCEED
                    and update.error is not None
                ):
                    msg_succeed_error: str = (
                        "Cannot provide an Error for SUCCEED action."
                    )
                    raise InvalidParameterValueException(msg_succeed_error)
            case OperationAction.RETRY:
                if current_state.status not in cls._ALLOWED_STATUS_TO_REATTEMPT:
                    msg_step_retry: str = "Invalid current STEP state to re-attempt."
                    raise InvalidParameterValueException(msg_step_retry)
                if update.step_options is None:
                    msg_step_options: str = "Invalid StepOptions for the given action."
                    raise InvalidParameterValueException(msg_step_options)
                if update.error is not None and update.payload is not None:
                    msg_retry_both: str = (
                        "Cannot provide both error and payload to RETRY a STEP."
                    )
                    raise InvalidParameterValueException(msg_retry_both)

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: Any,
        execution_arn: str,
    ) -> Operation:
        """Process STEP operation update with scheduler integration for retries."""
        match update.action:
            case OperationAction.START:
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.STARTED,
                )
            case OperationAction.RETRY:
                # set Status=PENDING, next attempt time, attempt count + 1
                delay = (
                    update.step_options.next_attempt_delay_seconds
                    if update.step_options
                    else 0
                )
                scaled_delay = scale_delay(delay)
                next_attempt_time = datetime.now(timezone.utc) + timedelta(
                    seconds=scaled_delay
                )

                # Build new step_details with incremented attempt
                current_attempt = (
                    current_op.step_details.attempt
                    if current_op and current_op.step_details
                    else 0
                )
                new_step_details = StepDetails(
                    attempt=current_attempt + 1,
                    next_attempt_timestamp=next_attempt_time,
                    result=(
                        current_op.step_details.result
                        if current_op and current_op.step_details
                        else None
                    ),
                    error=(
                        current_op.step_details.error
                        if current_op and current_op.step_details
                        else None
                    ),
                )

                # Create new operation with updated step_details
                retry_operation = Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.PENDING,
                    parent_id=update.parent_id,
                    name=update.name,
                    start_timestamp=(
                        current_op.start_timestamp
                        if current_op
                        else datetime.now(timezone.utc)
                    ),
                    end_timestamp=None,
                    sub_type=update.sub_type,
                    execution_details=current_op.execution_details
                    if current_op
                    else None,
                    context_details=current_op.context_details if current_op else None,
                    step_details=new_step_details,
                    wait_details=current_op.wait_details if current_op else None,
                    callback_details=current_op.callback_details
                    if current_op
                    else None,
                    chained_invoke_details=current_op.chained_invoke_details
                    if current_op
                    else None,
                )

                # Schedule step retry timer to fire after delay
                notifier.schedule_step_retry(
                    execution_arn, update.operation_id, scaled_delay
                )
                return retry_operation
            case OperationAction.SUCCEED:
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.SUCCEEDED,
                )
            case OperationAction.FAIL:
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.FAILED,
                )
            case _:
                msg: str = "Invalid action for STEP operation."

                raise InvalidParameterValueException(msg)
