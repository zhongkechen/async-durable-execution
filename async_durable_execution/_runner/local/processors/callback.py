"""Callback operation processor for handling CALLBACK operation updates."""

from __future__ import annotations

import datetime
from typing import Any

from ...._core import (
    CallbackDetails,
    CallbackOptions,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from .base import OperationProcessor
from ...exceptions import InvalidParameterValueException
from ..model import CallbackToken


VALID_ACTIONS_FOR_CALLBACK = frozenset(
    [
        OperationAction.START,
    ]
)


class CallbackProcessor(OperationProcessor):
    """Processes CALLBACK operation updates with activity scheduling."""

    valid_actions = VALID_ACTIONS_FOR_CALLBACK

    @classmethod
    def validate(cls, current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate CALLBACK operation update."""
        super().validate(current_state, update)
        if current_state is not None:
            msg_callback_exists: str = "Cannot start a CALLBACK that already exist."
            raise InvalidParameterValueException(msg_callback_exists)

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: Any,
        execution_arn: str,
    ) -> Operation:
        """Process CALLBACK operation update with scheduler integration for activities."""
        match update.action:
            case OperationAction.START:
                callback_token: CallbackToken = CallbackToken(
                    execution_arn=execution_arn,
                    operation_id=update.operation_id,
                )

                callback_id: str = callback_token.to_str()

                callback_details: CallbackDetails | None = (
                    CallbackDetails(
                        callback_id=callback_id,
                        result=update.payload,
                        error=update.error,
                    )
                    if update.operation_type == OperationType.CALLBACK
                    else None
                )

                status: OperationStatus = OperationStatus.STARTED

                start_time: datetime.datetime | None = self._get_start_time(current_op)

                end_time: datetime.datetime | None = self._get_end_time(
                    current_op, status
                )

                operation: Operation = Operation(
                    operation_id=update.operation_id,
                    parent_id=update.parent_id,
                    name=update.name,
                    start_timestamp=start_time,
                    end_timestamp=end_time,
                    operation_type=update.operation_type,
                    status=status,
                    sub_type=update.sub_type,
                    callback_details=callback_details,
                )
                callback_options: CallbackOptions | None = update.callback_options

                notifier.schedule_callback_timeouts(
                    execution_arn, callback_options, callback_id
                )
                return operation
            case _:
                msg: str = "Invalid action for CALLBACK operation."
                raise InvalidParameterValueException(msg)
