"""Execution operation processor for handling EXECUTION operation updates."""

from __future__ import annotations

from typing import Any

from ....core.models import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationUpdate,
)
from .base import (
    OperationProcessor,
)
from ...exceptions import (
    InvalidParameterValueException,
)


VALID_ACTIONS_FOR_EXECUTION = frozenset(
    [
        OperationAction.SUCCEED,
        OperationAction.FAIL,
    ]
)


class ExecutionProcessor(OperationProcessor):
    """Processes EXECUTION operation updates for workflow completion."""

    valid_actions = VALID_ACTIONS_FOR_EXECUTION

    @classmethod
    def validate(cls, current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate EXECUTION operation update."""
        super().validate(current_state, update)

        match update.action:
            case OperationAction.SUCCEED:
                if update.error is not None:
                    msg_exec_succeed_error: str = (
                        "Cannot provide an Error for SUCCEED action."
                    )
                    raise InvalidParameterValueException(msg_exec_succeed_error)
            case OperationAction.FAIL:
                if update.payload is not None:
                    msg_exec_fail_payload: str = (
                        "Cannot provide a Payload for FAIL action."
                    )
                    raise InvalidParameterValueException(msg_exec_fail_payload)

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,  # noqa: ARG002
        notifier: Any,
        execution_arn: str,
    ) -> Operation | None:
        """Process EXECUTION operation update for workflow completion/failure."""
        match update.action:
            case OperationAction.SUCCEED:
                notifier.complete_execution(execution_arn, update.payload)
            case _:
                # intentional. actual service will fail any EXECUTION update that is not SUCCEED.
                error = update.error or ErrorObject.from_message(
                    "There is no error details but EXECUTION checkpoint action is not SUCCEED."
                )
                # All EXECUTION failures go through normal fail path
                # Timeout/Stop status is set by executor based on the operation that caused it
                notifier.fail_execution(execution_arn, error)
        # TODO: Svc doesn't actually create checkpoint for EXECUTION. might have to for localrunner though.
        return None
