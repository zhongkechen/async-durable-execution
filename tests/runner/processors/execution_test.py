"""Tests for execution operation processor."""

from typing import Any, no_type_check

from unittest.mock import Mock

from async_durable_execution._core.models import (
    ErrorObject,
    OperationAction,
    OperationType,
    OperationUpdate,
)
from async_durable_execution._runner.local.processors.execution import (
    ExecutionProcessor,
)


class MockNotifier:
    """Mock notifier for testing."""

    def __init__(self) -> None:
        self.completed_calls: list[Any] = []
        self.failed_calls: list[Any] = []
        self.wait_timer_calls: list[Any] = []
        self.step_retry_calls: list[Any] = []

    def complete_execution(self, execution_arn, result=None) -> None:
        self.completed_calls.append((execution_arn, result))

    def fail_execution(self, execution_arn, error) -> None:
        self.failed_calls.append((execution_arn, error))

    def schedule_wait_timer(self, execution_arn, operation_id, delay) -> None:
        self.wait_timer_calls.append((execution_arn, operation_id, delay))

    def schedule_step_retry(self, execution_arn, operation_id, delay) -> None:
        self.step_retry_calls.append((execution_arn, operation_id, delay))


def test_process_succeed_action() -> None:
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
        payload="success-result",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.completed_calls) == 1
    assert notifier.completed_calls[0] == (execution_arn, "success-result")
    assert len(notifier.failed_calls) == 0


def test_process_succeed_action_with_current_operation() -> None:
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
        payload="success-result",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result is None
    assert len(notifier.completed_calls) == 1
    assert notifier.completed_calls[0] == (execution_arn, "success-result")


def test_process_succeed_action_without_payload() -> None:
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.completed_calls) == 1
    assert notifier.completed_calls[0] == (execution_arn, None)


def test_process_fail_action_with_error() -> None:
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    error = ErrorObject.from_message("execution failed")
    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.FAIL,
        error=error,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    assert notifier.failed_calls[0] == (execution_arn, error)
    assert len(notifier.completed_calls) == 0


def test_process_fail_action_without_error() -> None:
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.FAIL,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    execution_arn_arg, error_arg = notifier.failed_calls[0]
    assert execution_arn_arg == execution_arn
    assert isinstance(error_arg, ErrorObject)
    assert (
        "There is no error details but EXECUTION checkpoint action is not SUCCEED"
        in str(error_arg)
    )


def test_process_start_action() -> None:
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.START,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    execution_arn_arg, error_arg = notifier.failed_calls[0]
    assert execution_arn_arg == execution_arn
    assert isinstance(error_arg, ErrorObject)


def test_process_retry_action() -> None:
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.RETRY,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    execution_arn_arg, error_arg = notifier.failed_calls[0]
    assert execution_arn_arg == execution_arn
    assert isinstance(error_arg, ErrorObject)


def test_process_cancel_action() -> None:
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.CANCEL,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    execution_arn_arg, error_arg = notifier.failed_calls[0]
    assert execution_arn_arg == execution_arn
    assert isinstance(error_arg, ErrorObject)


def test_process_with_current_operation_and_error() -> None:
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    error = ErrorObject.from_message("custom error")

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.FAIL,
        error=error,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    assert notifier.failed_calls[0] == (execution_arn, error)


def test_no_wait_timer_or_step_retry_calls() -> None:
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
        payload="result",
    )

    processor.process(update, None, notifier, execution_arn)

    assert len(notifier.wait_timer_calls) == 0
    assert len(notifier.step_retry_calls) == 0


# Execution validation tests

"""Unit tests for execution operation validator."""

import pytest

from async_durable_execution._core.models import (
    ErrorObject,
    OperationAction,
    OperationType,
    OperationUpdate,
)
from async_durable_execution._runner.local.processors.execution import (
    ExecutionProcessor,
)
from async_durable_execution._runner.exceptions import (
    InvalidParameterValueException,
)


def test_validate_succeed_action() -> None:
    """Test SUCCEED action validation."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
        payload="success",
    )
    ExecutionProcessor.validate(None, update)


def test_validate_fail_action() -> None:
    """Test FAIL action validation."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.FAIL,
        error=ErrorObject(
            message="Test error", type="TestError", data=None, stack_trace=None
        ),
    )
    ExecutionProcessor.validate(None, update)


def test_validate_succeed_action_with_error() -> None:
    """Test SUCCEED action with error raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
        error=ErrorObject(
            message="Test error", type="TestError", data=None, stack_trace=None
        ),
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot provide an Error for SUCCEED action",
    ):
        ExecutionProcessor.validate(None, update)


def test_validate_fail_action_with_payload() -> None:
    """Test FAIL action with payload raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.FAIL,
        payload="invalid",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Cannot provide a Payload for FAIL action"
    ):
        ExecutionProcessor.validate(None, update)


def test_validate_invalid_action() -> None:
    """Test invalid action raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.START,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        ExecutionProcessor.validate(None, update)


def test_validate_fail_action_without_error() -> None:
    """Test FAIL action without error passes validation."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.FAIL,
    )
    ExecutionProcessor.validate(None, update)


def test_validate_succeed_action_without_payload() -> None:
    """Test SUCCEED action without payload passes validation."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
    )
    ExecutionProcessor.validate(None, update)
