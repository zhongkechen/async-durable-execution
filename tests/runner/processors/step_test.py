"""Tests for step operation processor."""

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from async_durable_execution.core.models import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StepDetails,
    StepOptions,
)
from async_durable_execution.runner.local.processors.step import (
    StepProcessor,
)
from async_durable_execution.runner.exceptions import (
    InvalidParameterValueException,
)


class MockNotifier:
    """Mock notifier for testing."""

    def __init__(self):
        self.completed_calls = []
        self.failed_calls = []
        self.wait_timer_calls = []
        self.step_retry_calls = []

    def complete_execution(self, execution_arn, result=None):
        self.completed_calls.append((execution_arn, result))

    def fail_execution(self, execution_arn, error):
        self.failed_calls.append((execution_arn, error))

    def schedule_wait_timer(self, execution_arn, operation_id, delay):
        self.wait_timer_calls.append((execution_arn, operation_id, delay))

    def schedule_step_retry(self, execution_arn, operation_id, delay):
        self.step_retry_calls.append((execution_arn, operation_id, delay))


def test_process_start_action():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="test-step",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "step-123"
    assert result.operation_type == OperationType.STEP
    assert result.status == OperationStatus.STARTED
    assert result.name == "test-step"
    assert result.step_details is not None


def test_process_start_action_with_current_operation():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="test-step",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp


def test_process_retry_action():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)
    current_op.step_details = StepDetails(attempt=1, result="previous-result")
    current_op.execution_details = None
    current_op.context_details = None
    current_op.wait_details = None
    current_op.callback_details = None
    current_op.chained_invoke_details = None

    step_options = StepOptions(next_attempt_delay_seconds=30)
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
        step_options=step_options,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "step-123"
    assert result.status == OperationStatus.PENDING
    assert result.step_details.attempt == 2
    assert result.step_details.result == "previous-result"
    assert result.step_details.next_attempt_timestamp is not None

    assert len(notifier.step_retry_calls) == 1
    assert notifier.step_retry_calls[0] == (execution_arn, "step-123", 30)


def test_process_retry_action_scales_delay(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.1")

    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)
    current_op.step_details = StepDetails(attempt=1)
    current_op.execution_details = None
    current_op.context_details = None
    current_op.wait_details = None
    current_op.callback_details = None
    current_op.chained_invoke_details = None

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
        step_options=StepOptions(next_attempt_delay_seconds=30),
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.step_details.next_attempt_timestamp is not None
    assert notifier.step_retry_calls[0] == (execution_arn, "step-123", 3.0)


def test_process_retry_action_without_step_options():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)
    current_op.step_details = StepDetails(attempt=0)
    current_op.execution_details = None
    current_op.context_details = None
    current_op.wait_details = None
    current_op.callback_details = None
    current_op.chained_invoke_details = None

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.step_details.attempt == 1
    assert len(notifier.step_retry_calls) == 1
    assert notifier.step_retry_calls[0] == (execution_arn, "step-123", 0)


def test_process_retry_action_without_current_operation():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    step_options = StepOptions(next_attempt_delay_seconds=15)
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
        step_options=step_options,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.step_details.attempt == 1
    assert result.step_details.result is None
    assert result.step_details.error is None


def test_process_retry_action_without_current_step_details():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)
    current_op.step_details = None
    current_op.execution_details = None
    current_op.context_details = None
    current_op.wait_details = None
    current_op.callback_details = None
    current_op.chained_invoke_details = None

    step_options = StepOptions(next_attempt_delay_seconds=45)
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
        step_options=step_options,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.step_details.attempt == 1


def test_process_succeed_action():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        name="test-step",
        payload="success-result",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "step-123"
    assert result.status == OperationStatus.SUCCEEDED
    assert result.step_details.result == "success-result"


def test_process_succeed_action_with_current_operation():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)
    current_op.step_details = StepDetails()

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        name="test-step",
        payload="success-result",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp
    assert result.status == OperationStatus.SUCCEEDED
    assert result.step_details.attempt == 1


def test_process_fail_action():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    error = ErrorObject.from_message("step failed")
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.FAIL,
        name="test-step",
        error=error,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "step-123"
    assert result.status == OperationStatus.FAILED
    assert result.step_details.error == error


def test_process_fail_action_with_current_operation():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)
    current_op.step_details = StepDetails()

    error = ErrorObject.from_message("step failed")
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.FAIL,
        name="test-step",
        error=error,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp
    assert result.status == OperationStatus.FAILED
    assert result.step_details.attempt == 1


def test_process_invalid_action():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.CANCEL,
        name="test-step",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for STEP operation"
    ):
        processor.process(update, None, notifier, execution_arn)


def test_process_with_parent_id():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="test-step",
        parent_id="parent-456",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.parent_id == "parent-456"


def test_process_with_sub_type():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="test-step",
        sub_type="lambda",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.sub_type == "lambda"


def test_retry_preserves_current_operation_details():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)
    current_op.step_details = StepDetails(
        attempt=2, result="old-result", error=ErrorObject.from_message("old-error")
    )
    current_op.execution_details = Mock()
    current_op.context_details = Mock()
    current_op.wait_details = Mock()
    current_op.callback_details = Mock()
    current_op.chained_invoke_details = Mock()

    step_options = StepOptions(next_attempt_delay_seconds=60)
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
        step_options=step_options,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.step_details.attempt == 3
    assert result.step_details.result == "old-result"
    assert result.step_details.error == current_op.step_details.error
    assert result.execution_details == current_op.execution_details
    assert result.context_details == current_op.context_details
    assert result.wait_details == current_op.wait_details
    assert result.callback_details == current_op.callback_details
    assert result.chained_invoke_details == current_op.chained_invoke_details


def test_no_completed_or_failed_calls_for_non_execution_actions():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="test-step",
    )

    processor.process(update, None, notifier, execution_arn)

    assert len(notifier.completed_calls) == 0
    assert len(notifier.failed_calls) == 0
    assert len(notifier.wait_timer_calls) == 0


def test_no_step_retry_calls_for_non_retry_actions():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        name="test-step",
    )

    processor.process(update, None, notifier, execution_arn)

    assert len(notifier.step_retry_calls) == 0


# Step validation tests

"""Unit tests for step operation validator."""

import pytest

from async_durable_execution.core.models import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StepOptions,
)
from async_durable_execution.runner.local.processors.step import (
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
