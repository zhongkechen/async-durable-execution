"""Tests for context operation processor."""

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from async_durable_execution.models import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.runner.local.processors.context import (
    ContextProcessor,
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
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "context-123"
    assert result.operation_type == OperationType.CONTEXT
    assert result.status == OperationStatus.STARTED
    assert result.name == "test-context"
    assert result.context_details is not None


def test_process_start_action_with_current_operation():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp
    assert result.status == OperationStatus.STARTED


def test_process_succeed_action():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        name="test-context",
        payload="success-result",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "context-123"
    assert result.status == OperationStatus.SUCCEEDED
    assert result.context_details.result == "success-result"
    assert result.context_details.error is None


def test_process_succeed_action_with_current_operation():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        name="test-context",
        payload="success-result",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp
    assert result.status == OperationStatus.SUCCEEDED


def test_process_fail_action():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    error = ErrorObject.from_message("context failed")
    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        name="test-context",
        error=error,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "context-123"
    assert result.status == OperationStatus.FAILED
    assert result.context_details.error == error
    assert result.context_details.result is None


def test_process_fail_action_with_current_operation():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(timezone.utc)

    error = ErrorObject.from_message("context failed")
    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        name="test-context",
        error=error,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp
    assert result.status == OperationStatus.FAILED


def test_process_fail_action_with_payload_and_error():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    error = ErrorObject.from_message("context failed")
    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        name="test-context",
        payload="partial-result",
        error=error,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.context_details.result == "partial-result"
    assert result.context_details.error == error


def test_process_invalid_action():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.RETRY,
        name="test-context",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CONTEXT operation"
    ):
        processor.process(update, None, notifier, execution_arn)


def test_process_cancel_action():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.CANCEL,
        name="test-context",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CONTEXT operation"
    ):
        processor.process(update, None, notifier, execution_arn)


def test_process_with_parent_id():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
        parent_id="parent-456",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.parent_id == "parent-456"


def test_process_with_sub_type():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
        sub_type="parallel",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.sub_type == "parallel"


def test_process_start_without_payload():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.context_details.result is None
    assert result.context_details.error is None


def test_process_succeed_without_payload():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.context_details.result is None
    assert result.context_details.error is None


def test_process_fail_without_error():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.context_details.result is None
    assert result.context_details.error is None


def test_no_notifier_calls():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
    )

    processor.process(update, None, notifier, execution_arn)

    assert len(notifier.completed_calls) == 0
    assert len(notifier.failed_calls) == 0
    assert len(notifier.wait_timer_calls) == 0
    assert len(notifier.step_retry_calls) == 0


def test_end_timestamp_set_for_terminal_states():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.end_timestamp is not None


def test_end_timestamp_not_set_for_non_terminal_states():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.end_timestamp is None


# Context validation tests

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
from async_durable_execution.runner.local.processors.context import (
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
