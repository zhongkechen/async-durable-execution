"""Tests for context operation validator."""

import pytest

from async_durable_execution.models import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.runner.processors.context import (
    VALID_ACTIONS_FOR_CONTEXT,
    ContextProcessor,
)
from async_durable_execution.runner.exceptions import (
    InvalidParameterValueException,
)


def test_valid_actions_for_context():
    """Test that VALID_ACTIONS_FOR_CONTEXT contains expected actions."""
    expected_actions = {
        OperationAction.START,
        OperationAction.FAIL,
        OperationAction.SUCCEED,
    }
    assert expected_actions == VALID_ACTIONS_FOR_CONTEXT


def test_validate_start_action_with_no_current_state():
    """Test START action validation when no current state exists."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )

    # Should not raise exception
    ContextProcessor.validate(None, update)


def test_validate_start_action_with_existing_state():
    """Test START action validation when current state already exists."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot start a CONTEXT that already exist.",
    ):
        ContextProcessor.validate(current_state, update)


def test_validate_succeed_action_with_started_state():
    """Test SUCCEED action validation with STARTED state."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        payload="success_payload",
    )

    # Should not raise exception
    ContextProcessor.validate(current_state, update)


def test_validate_fail_action_with_started_state():
    """Test FAIL action validation with STARTED state."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    error = ErrorObject(
        message="test error", type="TestError", data=None, stack_trace=None
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        error=error,
    )

    # Should not raise exception
    ContextProcessor.validate(current_state, update)


def test_validate_succeed_action_with_invalid_status():
    """Test SUCCEED action validation with invalid status."""
    invalid_statuses = [
        OperationStatus.PENDING,
        OperationStatus.READY,
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
        OperationStatus.TIMED_OUT,
        OperationStatus.STOPPED,
    ]

    for status in invalid_statuses:
        current_state = Operation(
            operation_id="test-id",
            operation_type=OperationType.CONTEXT,
            status=status,
        )
        update = OperationUpdate(
            operation_id="test-id",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.SUCCEED,
            payload="success_payload",
        )

        with pytest.raises(
            InvalidParameterValueException,
            match="Invalid current CONTEXT state to close.",
        ):
            ContextProcessor.validate(current_state, update)


def test_validate_fail_action_with_invalid_status():
    """Test FAIL action validation with invalid status."""
    invalid_statuses = [
        OperationStatus.PENDING,
        OperationStatus.READY,
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
        OperationStatus.TIMED_OUT,
        OperationStatus.STOPPED,
    ]

    error = ErrorObject(
        message="test error", type="TestError", data=None, stack_trace=None
    )

    for status in invalid_statuses:
        current_state = Operation(
            operation_id="test-id",
            operation_type=OperationType.CONTEXT,
            status=status,
        )
        update = OperationUpdate(
            operation_id="test-id",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.FAIL,
            error=error,
        )

        with pytest.raises(
            InvalidParameterValueException,
            match="Invalid current CONTEXT state to close.",
        ):
            ContextProcessor.validate(current_state, update)


def test_validate_fail_action_with_payload():
    """Test FAIL action validation when payload is provided."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        payload="invalid_payload",
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot provide a Payload for FAIL action.",
    ):
        ContextProcessor.validate(current_state, update)


def test_validate_succeed_action_with_error():
    """Test SUCCEED action validation when error is provided."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    error = ErrorObject(
        message="test error", type="TestError", data=None, stack_trace=None
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        error=error,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot provide an Error for SUCCEED action.",
    ):
        ContextProcessor.validate(current_state, update)


def test_validate_close_actions_with_no_current_state():
    """Test SUCCEED and FAIL actions validation when no current state exists."""
    # SUCCEED with no current state should pass
    succeed_update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        payload="success_payload",
    )
    ContextProcessor.validate(None, succeed_update)

    # FAIL with no current state should pass
    error = ErrorObject(
        message="test error", type="TestError", data=None, stack_trace=None
    )
    fail_update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        error=error,
    )
    ContextProcessor.validate(None, fail_update)


def test_validate_invalid_action():
    """Test validation with invalid action."""
    invalid_actions = [
        OperationAction.RETRY,
        OperationAction.CANCEL,
    ]

    for action in invalid_actions:
        update = OperationUpdate(
            operation_id="test-id",
            operation_type=OperationType.CONTEXT,
            action=action,
        )

        with pytest.raises(
            InvalidParameterValueException,
            match="Invalid action for the given operation type",
        ):
            ContextProcessor.validate(None, update)
