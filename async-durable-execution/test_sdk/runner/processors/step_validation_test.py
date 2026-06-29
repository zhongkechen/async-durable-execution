"""Unit tests for step operation validator."""

import pytest

from async_durable_execution.models import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StepOptions,
)
from async_durable_execution.runner.processors.step import (
    StepProcessor,
)
from async_durable_execution.runner.exceptions import (
    InvalidParameterValueException,
)


def test_validate_with_no_current_state():
    """Test validation with no current state."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    StepProcessor.validate(None, update)


def test_validate_start_action_with_ready_state():
    """Test START action with READY state."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.READY,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    StepProcessor.validate(current_state, update)


def test_validate_start_action_with_invalid_state():
    """Test START action with invalid state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid current STEP state to start"
    ):
        StepProcessor.validate(current_state, update)


def test_validate_succeed_action_with_started_state():
    """Test SUCCEED action with STARTED state."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        payload={"result": "success"},
    )
    StepProcessor.validate(current_state, update)


def test_validate_fail_action_with_ready_state():
    """Test FAIL action with READY state."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.READY,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.FAIL,
        error=ErrorObject(
            message="Test error", type="TestError", data=None, stack_trace=None
        ),
    )
    StepProcessor.validate(current_state, update)


def test_validate_fail_action_with_invalid_state():
    """Test FAIL action with invalid state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.FAIL,
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid current STEP state to close"
    ):
        StepProcessor.validate(current_state, update)


def test_validate_fail_action_with_payload():
    """Test FAIL action with payload raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.FAIL,
        payload={"invalid": "payload"},
    )

    with pytest.raises(
        InvalidParameterValueException, match="Cannot provide a Payload for FAIL action"
    ):
        StepProcessor.validate(current_state, update)


def test_validate_succeed_action_with_error():
    """Test SUCCEED action with error raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        error=ErrorObject(
            message="Test error", type="TestError", data=None, stack_trace=None
        ),
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot provide an Error for SUCCEED action",
    ):
        StepProcessor.validate(current_state, update)


def test_validate_retry_action_with_started_state():
    """Test RETRY action with STARTED state."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        step_options=StepOptions(next_attempt_delay_seconds=3),
    )
    StepProcessor.validate(current_state, update)


def test_validate_retry_action_with_ready_state():
    """Test RETRY action with READY state."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.READY,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        step_options=StepOptions(next_attempt_delay_seconds=3),
    )
    StepProcessor.validate(current_state, update)


def test_validate_retry_action_with_invalid_state():
    """Test RETRY action with invalid state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        step_options=StepOptions(next_attempt_delay_seconds=3),
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid current STEP state to re-attempt"
    ):
        StepProcessor.validate(current_state, update)


def test_validate_retry_action_without_step_options():
    """Test RETRY action without step options raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid StepOptions for the given action"
    ):
        StepProcessor.validate(current_state, update)


def test_validate_retry_action_with_both_error_and_payload():
    """Test RETRY action with both error and payload raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        step_options=StepOptions(next_attempt_delay_seconds=3),
        error=ErrorObject(
            message="Test error", type="TestError", data=None, stack_trace=None
        ),
        payload={"result": "success"},
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot provide both error and payload to RETRY a STEP",
    ):
        StepProcessor.validate(current_state, update)


def test_validate_invalid_action():
    """Test invalid action raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.CANCEL,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        StepProcessor.validate(current_state, update)
