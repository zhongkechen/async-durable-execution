"""Unit tests for invoke operation validator."""

import pytest

from async_durable_execution.models import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.runner.processors.invoke import (
    ChainedInvokeProcessor,
)
from async_durable_execution.runner.exceptions import (
    InvalidParameterValueException,
)


def test_validate_start_action_with_no_current_state():
    """Test START action with no current state."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
    )
    ChainedInvokeProcessor.validate(None, update)


def test_validate_start_action_with_existing_state():
    """Test START action with existing state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot start an INVOKE that already exist",
    ):
        ChainedInvokeProcessor.validate(current_state, update)


def test_validate_cancel_action_with_started_state():
    """Test CANCEL action with STARTED state."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.CANCEL,
    )
    ChainedInvokeProcessor.validate(current_state, update)


def test_validate_cancel_action_with_no_current_state():
    """Test CANCEL action with no current state raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.CANCEL,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot cancel an INVOKE that does not exist or has already completed",
    ):
        ChainedInvokeProcessor.validate(None, update)


def test_validate_cancel_action_with_completed_state():
    """Test CANCEL action with completed state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.CANCEL,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot cancel an INVOKE that does not exist or has already completed",
    ):
        ChainedInvokeProcessor.validate(current_state, update)


def test_validate_invalid_action():
    """Test invalid action raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.SUCCEED,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        ChainedInvokeProcessor.validate(None, update)
