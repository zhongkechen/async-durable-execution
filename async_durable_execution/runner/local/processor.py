"""Checkpoint validation and operation transformation helpers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

from ...core import (
    Operation,
    OperationAction,
    OperationType,
    OperationUpdate,
)
from ..exceptions import (
    InvalidParameterValueException,
)
from .processors import (
    create_default_processors,
)
from .processors.base import (
    OperationProcessor,
)
from .processors.invoke import ChainedInvokeProcessor

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from .execution import Execution

MAX_ERROR_PAYLOAD_SIZE_BYTES = 32768


class OperationTransformer:
    """Transforms OperationUpdates to Operations while maintaining order and triggering scheduler actions."""

    def __init__(
        self,
        processors: MutableMapping[OperationType, OperationProcessor] | None = None,
    ):
        self.processors = processors or create_default_processors()

    def mock_invoke_result(self, function_name: str, result: object) -> None:
        """Register a local mock result for a chained invoke function."""
        processor = self.processors.get(OperationType.CHAINED_INVOKE)
        if isinstance(processor, ChainedInvokeProcessor):
            processor.mock_result(function_name=function_name, result=result)

    def process_updates(
        self,
        updates: list[OperationUpdate],
        current_operations: list[Operation],
        runner,
        execution_arn: str,
    ) -> tuple[list[Operation], list[OperationUpdate]]:
        """Transform updates maintaining operation order and return (operations, updates)."""
        op_map = {op.operation_id: op for op in current_operations}

        # Start with copy of current operations list
        result_operations = current_operations.copy()

        for update in updates:
            processor = self.processors.get(update.operation_type)
            if processor:
                current_op = op_map.get(update.operation_id)
                updated_op = processor.process(
                    update=update,
                    current_op=current_op,
                    notifier=runner,
                    execution_arn=execution_arn,
                )

                if updated_op is not None:
                    if update.operation_id in op_map:
                        # Update existing operation in-place
                        for i, op in enumerate(result_operations):  # pragma: no branch
                            # no branch coverage because result_operation empty not reachable here
                            if op.operation_id == update.operation_id:
                                result_operations[i] = updated_op
                                break
                    else:
                        # Append new operation to end
                        result_operations.append(updated_op)

                    # Update map for future lookups
                    op_map[update.operation_id] = updated_op
            else:
                msg: str = (
                    f"Checkpoint for {update.operation_type} is not implemented yet."
                )
                raise InvalidParameterValueException(msg)

        return result_operations, updates


class CheckpointValidator:
    """Validates checkpoint input based on current state."""

    @staticmethod
    def validate_input(
        updates: list[OperationUpdate],
        execution: Execution,
        processors: Mapping[OperationType, OperationProcessor] | None = None,
    ) -> None:
        """Perform validation on the given input based on the current state."""
        if not updates:
            return

        CheckpointValidator._validate_conflicting_execution_update(updates)
        CheckpointValidator._validate_parent_id_and_duplicate_id(updates, execution)

        for update in updates:
            CheckpointValidator._validate_operation_update(
                update, execution, processors
            )

    @staticmethod
    def _validate_conflicting_execution_update(updates: list[OperationUpdate]) -> None:
        """Validate that there are no conflicting execution updates."""
        execution_updates = [
            update
            for update in updates
            if update.operation_type == OperationType.EXECUTION
        ]

        if len(execution_updates) > 1:
            msg_multiple_exec: str = "Cannot checkpoint multiple EXECUTION updates."

            raise InvalidParameterValueException(msg_multiple_exec)

        if execution_updates and updates[-1].operation_type != OperationType.EXECUTION:
            msg_exec_last: str = "EXECUTION checkpoint must be the last update."

            raise InvalidParameterValueException(msg_exec_last)

    @staticmethod
    def _validate_operation_update(
        update: OperationUpdate,
        execution: Execution,
        processors: Mapping[OperationType, OperationProcessor] | None,
    ) -> None:
        """Validate a single operation update."""
        CheckpointValidator._validate_inconsistent_operation_metadata(update, execution)
        CheckpointValidator._validate_payload_sizes(update)
        CheckpointValidator._validate_operation_status_transition(
            update, execution, processors
        )

    @staticmethod
    def _validate_payload_sizes(update: OperationUpdate) -> None:
        """Validate that operation payload sizes are not too large."""
        if update.error is not None:
            payload = json.dumps(update.error.to_dict())
            if len(payload) > MAX_ERROR_PAYLOAD_SIZE_BYTES:
                msg: str = f"Error object size must be less than {MAX_ERROR_PAYLOAD_SIZE_BYTES} bytes."
                raise InvalidParameterValueException(msg)

    @staticmethod
    def _validate_operation_status_transition(
        update: OperationUpdate,
        execution: Execution,
        processors: Mapping[OperationType, OperationProcessor] | None,
    ) -> None:
        """Validate that the operation status transition is valid."""
        current_state = None
        for operation in execution.operations:
            if operation.operation_id == update.operation_id:
                current_state = operation
                break

        processor = (processors or create_default_processors()).get(
            update.operation_type
        )
        if processor is None:
            msg: str = "Invalid operation type."
            raise InvalidParameterValueException(msg)

        processor.validate(current_state, update)

    @staticmethod
    def _validate_inconsistent_operation_metadata(
        update: OperationUpdate, execution: Execution
    ) -> None:
        """Validate that operation metadata is consistent with existing operation."""
        current_state = None
        for operation in execution.operations:
            if operation.operation_id == update.operation_id:
                current_state = operation
                break

        if current_state is not None:
            if (
                update.operation_type is not None
                and update.operation_type != current_state.operation_type
            ):
                msg: str = "Inconsistent operation type."
                raise InvalidParameterValueException(msg)

            if (
                update.sub_type is not None
                and update.sub_type != current_state.sub_type
            ):
                msg_subtype: str = "Inconsistent operation subtype."
                raise InvalidParameterValueException(msg_subtype)

            if update.name is not None and update.name != current_state.name:
                msg_name: str = "Inconsistent operation name."
                raise InvalidParameterValueException(msg_name)

            if (
                update.parent_id is not None
                and update.parent_id != current_state.parent_id
            ):
                msg_parent: str = "Inconsistent parent operation id."
                raise InvalidParameterValueException(msg_parent)

    @staticmethod
    def _validate_parent_id_and_duplicate_id(
        updates: list[OperationUpdate], execution: Execution
    ) -> None:
        """Validate parent IDs and check for duplicate operation IDs.

        Validate that any provided parentId is valid, and also validate no duplicate operation is being
        updated at the same time (unless it is a STEP/CONTEXT starting + performing one more non-START action).
        """
        operations_started: MutableMapping[str, OperationUpdate] = {}
        last_updates_seen: MutableMapping[str, OperationUpdate] = {}

        for update in updates:
            if CheckpointValidator._is_invalid_duplicate_update(
                update, last_updates_seen
            ):
                msg_duplicate: str = (
                    "Cannot checkpoint multiple operations with the same ID."
                )
                raise InvalidParameterValueException(msg_duplicate)

            if not CheckpointValidator._is_valid_parent_for_update(
                execution, update, operations_started
            ):
                msg_parent: str = "Invalid parent operation id."
                raise InvalidParameterValueException(msg_parent)

            if update.action == OperationAction.START:
                operations_started[update.operation_id] = update

            last_updates_seen[update.operation_id] = update

    @staticmethod
    def _is_invalid_duplicate_update(
        update: OperationUpdate, last_updates_seen: MutableMapping[str, OperationUpdate]
    ) -> bool:
        """Check if this is an invalid duplicate update."""
        last_update = last_updates_seen.get(update.operation_id)
        if last_update is None:
            return False

        if last_update.operation_type in (OperationType.STEP, OperationType.CONTEXT):
            # Allow duplicate for STEP/CONTEXT if last was START and current is not START
            allow_duplicate = (
                last_update.action == OperationAction.START
                and update.action != OperationAction.START
            )
            return not allow_duplicate

        return True

    @staticmethod
    def _is_valid_parent_for_update(
        execution: Execution,
        update: OperationUpdate,
        operations_started: MutableMapping[str, OperationUpdate],
    ) -> bool:
        """Check if the parent ID is valid for the update."""
        parent_id = update.parent_id

        if parent_id is None:
            return True

        # Check if parent is in operations started in this batch
        if parent_id in operations_started:
            parent_update = operations_started[parent_id]
            return parent_update.operation_type == OperationType.CONTEXT

        # Check if parent exists in current execution state
        for operation in execution.operations:
            if operation.operation_id == parent_id:
                return operation.operation_type == OperationType.CONTEXT

        return False
