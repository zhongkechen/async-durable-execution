"""Unit tests for execution operation validator."""

import pytest

from async_durable_execution.models import (
    ErrorObject,
    OperationAction,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.runner.processors.execution import (
    ExecutionProcessor,
)
from async_durable_execution.runner.exceptions import (
    InvalidParameterValueException,
)


def test_validate_succeed_action():
    """Test SUCCEED action validation."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
        payload="success",
    )
    ExecutionProcessor.validate(None, update)


def test_validate_fail_action():
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


def test_validate_succeed_action_with_error():
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


def test_validate_fail_action_with_payload():
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


def test_validate_invalid_action():
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


def test_validate_fail_action_without_error():
    """Test FAIL action without error passes validation."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.FAIL,
    )
    ExecutionProcessor.validate(None, update)


def test_validate_succeed_action_without_payload():
    """Test SUCCEED action without payload passes validation."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
    )
    ExecutionProcessor.validate(None, update)
