"""Context operation processor for handling CONTEXT operation updates."""

from __future__ import annotations

from typing import TYPE_CHECKING

from async_durable_execution.models import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
)
from .base import (
    OperationProcessor,
)
from ..exceptions import (
    InvalidParameterValueException,
)


if TYPE_CHECKING:
    from ..observer import ExecutionNotifier


VALID_ACTIONS_FOR_CONTEXT = frozenset(
    [
        OperationAction.START,
        OperationAction.FAIL,
        OperationAction.SUCCEED,
    ]
)


class ContextProcessor(OperationProcessor):
    """Processes CONTEXT operation updates for execution context management."""

    valid_actions = VALID_ACTIONS_FOR_CONTEXT
    _ALLOWED_STATUS_TO_CLOSE = frozenset(
        [
            OperationStatus.STARTED,
        ]
    )

    @classmethod
    def validate(cls, current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate CONTEXT operation update."""
        super().validate(current_state, update)

        match update.action:
            case OperationAction.START:
                if current_state is not None:
                    msg_context_exists: str = (
                        "Cannot start a CONTEXT that already exist."
                    )
                    raise InvalidParameterValueException(msg_context_exists)
            case OperationAction.FAIL | OperationAction.SUCCEED:
                if (
                    current_state is not None
                    and current_state.status not in cls._ALLOWED_STATUS_TO_CLOSE
                ):
                    msg_context_close: str = "Invalid current CONTEXT state to close."
                    raise InvalidParameterValueException(msg_context_close)
                if update.action == OperationAction.FAIL and update.payload is not None:
                    msg_context_fail_payload: str = (
                        "Cannot provide a Payload for FAIL action."
                    )
                    raise InvalidParameterValueException(msg_context_fail_payload)
                if (
                    update.action == OperationAction.SUCCEED
                    and update.error is not None
                ):
                    msg_context_succeed_error: str = (
                        "Cannot provide an Error for SUCCEED action."
                    )
                    raise InvalidParameterValueException(msg_context_succeed_error)

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,  # noqa: ARG002
        execution_arn: str,  # noqa: ARG002
    ) -> Operation:
        """Process CONTEXT operation update for context state transitions."""
        match update.action:
            case OperationAction.START:
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.STARTED,
                )
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
                msg: str = "Invalid action for CONTEXT operation."
                raise InvalidParameterValueException(msg)
