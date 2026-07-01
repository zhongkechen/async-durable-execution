"""Tests for callback operation processor."""

from unittest.mock import Mock

import pytest

from async_durable_execution.models import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.runner.local.processors.callback import (
    CallbackProcessor,
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
        self.callback_timeout_calls = []

    def complete_execution(self, execution_arn, result=None):
        self.completed_calls.append((execution_arn, result))

    def fail_execution(self, execution_arn, error):
        self.failed_calls.append((execution_arn, error))

    def schedule_wait_timer(self, execution_arn, operation_id, delay):
        self.wait_timer_calls.append((execution_arn, operation_id, delay))

    def schedule_step_retry(self, execution_arn, operation_id, delay):
        self.step_retry_calls.append((execution_arn, operation_id, delay))

    def schedule_callback_timeouts(self, execution_arn, callback_options, callback_id):
        self.callback_timeout_calls.append(
            (execution_arn, callback_options, callback_id)
        )


def test_process_start_action():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
    )

    result = processor.process(
        update, None, notifier, "arn:aws:states:us-east-1:123456789012:execution:test"
    )

    assert isinstance(result, Operation)
    assert result.operation_id == "callback-123"
    assert result.operation_type == OperationType.CALLBACK
    assert result.status == OperationStatus.STARTED
    assert result.name == "test-callback"
    assert result.callback_details is not None


def test_process_start_action_with_current_operation():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    current_op = Mock()
    current_op.start_timestamp = Mock()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
    )

    result = processor.process(
        update,
        current_op,
        notifier,
        "arn:aws:states:us-east-1:123456789012:execution:test",
    )

    assert isinstance(result, Operation)
    assert result.operation_id == "callback-123"
    assert result.status == OperationStatus.STARTED
    assert result.start_timestamp == current_op.start_timestamp


def test_process_invalid_action():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.SUCCEED,
        name="test-callback",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CALLBACK operation"
    ):
        processor.process(
            update,
            None,
            notifier,
            "arn:aws:states:us-east-1:123456789012:execution:test",
        )


def test_process_fail_action():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.FAIL,
        name="test-callback",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CALLBACK operation"
    ):
        processor.process(
            update,
            None,
            notifier,
            "arn:aws:states:us-east-1:123456789012:execution:test",
        )


def test_process_cancel_action():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.CANCEL,
        name="test-callback",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CALLBACK operation"
    ):
        processor.process(
            update,
            None,
            notifier,
            "arn:aws:states:us-east-1:123456789012:execution:test",
        )


def test_process_retry_action():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.RETRY,
        name="test-callback",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CALLBACK operation"
    ):
        processor.process(
            update,
            None,
            notifier,
            "arn:aws:states:us-east-1:123456789012:execution:test",
        )


def test_process_with_payload():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
        payload="test-payload",
    )

    result = processor.process(
        update, None, notifier, "arn:aws:states:us-east-1:123456789012:execution:test"
    )

    assert result.callback_details.result == "test-payload"


def test_process_with_parent_id():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
        parent_id="parent-456",
    )

    result = processor.process(
        update, None, notifier, "arn:aws:states:us-east-1:123456789012:execution:test"
    )

    assert result.parent_id == "parent-456"


def test_process_with_sub_type():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
        sub_type="activity",
    )

    result = processor.process(
        update, None, notifier, "arn:aws:states:us-east-1:123456789012:execution:test"
    )

    assert result.sub_type == "activity"


def test_notifier_not_called_for_start():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
    )

    processor.process(
        update, None, notifier, "arn:aws:states:us-east-1:123456789012:execution:test"
    )

    assert len(notifier.completed_calls) == 0
    assert len(notifier.failed_calls) == 0
    assert len(notifier.wait_timer_calls) == 0
    assert len(notifier.step_retry_calls) == 0


# Callback validation tests

"""Unit tests for callback operation validator."""

import pytest

from async_durable_execution.models import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.runner.local.processors.callback import (
    CallbackProcessor,
)
from async_durable_execution.runner.exceptions import (
    InvalidParameterValueException,
)


def test_validate_start_action_with_no_current_state():
    """Test START action with no current state."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
    )
    CallbackProcessor.validate(None, update)


def test_validate_start_action_with_existing_state():
    """Test START action with existing state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot start a CALLBACK that already exist",
    ):
        CallbackProcessor.validate(current_state, update)


def test_validate_cancel_action_with_no_current_state():
    """Test CANCEL action with no current state raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.CANCEL,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        CallbackProcessor.validate(None, update)


def test_validate_cancel_action_with_completed_state():
    """Test CANCEL action with completed state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.CANCEL,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        CallbackProcessor.validate(current_state, update)


def test_validate_invalid_action():
    """Test invalid action raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.SUCCEED,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        CallbackProcessor.validate(None, update)
