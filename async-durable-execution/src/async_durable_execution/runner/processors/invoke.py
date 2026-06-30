"""Chained invoke operation processor for local runner mocks."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from async_durable_execution.models import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
)
from .base import OperationProcessor
from ..exceptions import InvalidParameterValueException

if TYPE_CHECKING:
    from ..observer import ExecutionNotifier


VALID_ACTIONS_FOR_INVOKE = frozenset(
    [
        OperationAction.START,
        OperationAction.CANCEL,
    ]
)


class ChainedInvokeProcessor(OperationProcessor):
    """Processes CHAINED_INVOKE updates and applies configured local mock results."""

    valid_actions = VALID_ACTIONS_FOR_INVOKE
    _ALLOWED_STATUS_TO_CANCEL = frozenset(
        [
            OperationStatus.STARTED,
        ]
    )

    def __init__(self) -> None:
        self._mock_results: dict[str, str | None] = {}

    def mock_result(self, function_name: str, result: Any) -> None:
        """Register a local mock result for a chained invoke function."""
        self._mock_results[function_name] = json.dumps(result)

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,  # noqa: ARG002
        execution_arn: str,  # noqa: ARG002
    ) -> Operation:
        """Process CHAINED_INVOKE updates for local execution."""
        if update.action is OperationAction.CANCEL:
            return self._translate_update_to_operation(
                update=update,
                current_operation=current_op,
                status=OperationStatus.CANCELLED,
            )
        if update.action is not OperationAction.START:
            msg = "Invalid action for CHAINED_INVOKE operation."
            raise InvalidParameterValueException(msg)

        status = OperationStatus.STARTED
        result = None
        if update.chained_invoke_options:
            function_name = update.chained_invoke_options.function_name
            if function_name in self._mock_results:
                status = OperationStatus.SUCCEEDED
                result = self._mock_results[function_name]

        mocked_update = update
        if result is not None:
            mocked_update = OperationUpdate(
                operation_id=update.operation_id,
                parent_id=update.parent_id,
                operation_type=update.operation_type,
                sub_type=update.sub_type,
                action=update.action,
                name=update.name,
                payload=result,
                chained_invoke_options=update.chained_invoke_options,
            )

        return self._translate_update_to_operation(
            update=mocked_update,
            current_operation=current_op,
            status=status,
        )

    @classmethod
    def validate(cls, current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate CHAINED_INVOKE operation update."""
        super().validate(current_state, update)

        match update.action:
            case OperationAction.START:
                if current_state is not None:
                    msg_invoke_exists: str = (
                        "Cannot start an INVOKE that already exist."
                    )
                    raise InvalidParameterValueException(msg_invoke_exists)
            case OperationAction.CANCEL:
                if (
                    current_state is None
                    or current_state.status not in cls._ALLOWED_STATUS_TO_CANCEL
                ):
                    msg_invoke_cancel: str = "Cannot cancel an INVOKE that does not exist or has already completed."
                    raise InvalidParameterValueException(msg_invoke_cancel)
