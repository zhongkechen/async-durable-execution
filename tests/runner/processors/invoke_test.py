"""Unit tests for invoke operation validator."""

import pytest

from async_durable_execution.core.models import (
    ChainedInvokeOptions,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.runner.local.processors.invoke import (
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


def test_process_start_without_mock_result_starts_invoke():
    """Test START action creates a started invoke when no mock result is registered."""
    processor = ChainedInvokeProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
        name="test-invoke",
        chained_invoke_options=ChainedInvokeOptions(function_name="unmocked-function"),
    )

    result = processor.process(update, None, notifier=None, execution_arn="test-arn")

    assert result.operation_id == "test-id"
    assert result.operation_type == OperationType.CHAINED_INVOKE
    assert result.status == OperationStatus.STARTED
    assert result.name == "test-invoke"
    assert result.chained_invoke_details.result is None


def test_process_start_with_mock_result_succeeds_invoke():
    """Test START action returns the registered local mock result."""
    processor = ChainedInvokeProcessor()
    processor.mock_result("mocked-function", {"ok": True})
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
        chained_invoke_options=ChainedInvokeOptions(function_name="mocked-function"),
    )

    result = processor.process(update, None, notifier=None, execution_arn="test-arn")

    assert result.status == OperationStatus.SUCCEEDED
    assert result.chained_invoke_details.result == '{"ok": true}'


def test_process_cancel_translates_to_cancelled_invoke():
    """Test CANCEL action marks a started invoke as cancelled."""
    processor = ChainedInvokeProcessor()
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.CANCEL,
        chained_invoke_options=ChainedInvokeOptions(function_name="test-function"),
    )

    result = processor.process(
        update, current_state, notifier=None, execution_arn="test-arn"
    )

    assert result.status == OperationStatus.CANCELLED
    assert result.start_timestamp == current_state.start_timestamp
    assert result.end_timestamp is not None


def test_process_invalid_action_raises():
    """Test process rejects non-START/CANCEL invoke updates."""
    processor = ChainedInvokeProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.SUCCEED,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for CHAINED_INVOKE operation",
    ):
        processor.process(update, None, notifier=None, execution_arn="test-arn")
