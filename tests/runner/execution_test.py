"""Unit tests for execution module."""

from typing import no_type_check

from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from async_durable_execution._core.execution import InvocationStatus
from async_durable_execution._core.models import (
    CallbackDetails,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
    StepDetails,
)
from async_durable_execution._runner.exceptions import (
    IllegalStateException,
    InvalidParameterValueException,
)
from async_durable_execution._runner.local.execution import Execution
from async_durable_execution._runner.local.model import StartDurableExecutionInput


@no_type_check
def test_execution_init() -> None:
    """Test Execution initialization."""
    arn = "test-arn"
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = []

    execution = Execution(arn, start_input, operations)

    assert execution.durable_execution_arn == arn
    assert execution.start_input == start_input
    assert execution.operations == operations
    assert execution.updates == []
    assert execution.used_tokens == set()
    assert execution.token_sequence == 0
    assert execution.is_complete is False
    assert execution.consecutive_failed_invocation_attempts == 0


@patch("async_durable_execution._runner.local.execution.uuid4")
def test_execution_new(mock_uuid4) -> None:
    """Test Execution.new static method."""
    mock_uuid = "test-uuid-123"
    mock_uuid4.return_value = mock_uuid

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id-1234",
    )

    execution = Execution.new(start_input)

    assert (
        execution.durable_execution_arn == str(mock_uuid) + "/test-invocation-id-1234"
    )
    assert execution.start_input == start_input
    assert execution.operations == []


@patch("async_durable_execution._runner.local.execution.datetime")
@no_type_check
def test_execution_start(mock_datetime) -> None:
    """Test Execution.start method."""
    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = mock_now

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
        input='{"key": "value"}',
    )
    execution = Execution("test-arn", start_input, [])

    execution.start()

    assert len(execution.operations) == 1
    operation = execution.operations[0]
    assert operation.operation_id == "test-invocation-id"
    assert operation.parent_id is None
    assert operation.name == "test-execution"
    assert operation.start_timestamp == mock_now
    assert operation.operation_type == OperationType.EXECUTION
    assert operation.status == OperationStatus.STARTED
    assert operation.execution_details.input_payload == '{"key": "value"}'


def test_get_operation_execution_started() -> None:
    """Test get_operation_execution_started method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [])
    execution.start()

    result = execution.get_operation_execution_started()

    assert result == execution.operations[0]
    assert result.operation_type == OperationType.EXECUTION


def test_get_operation_execution_started_not_started() -> None:
    """Test get_operation_execution_started raises error when not started."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [])

    with pytest.raises(IllegalStateException, match="execution not started"):
        execution.get_operation_execution_started()


def test_get_new_checkpoint_token() -> None:
    """Test get_new_checkpoint_token method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-id",
    )
    execution = Execution("test-arn", start_input, [])

    token1 = execution.get_new_checkpoint_token()
    token2 = execution.get_new_checkpoint_token()

    assert execution.token_sequence == 2
    assert token1 in execution.used_tokens
    assert token2 in execution.used_tokens
    assert token1 != token2


def test_get_navigable_operations() -> None:
    """Test get_navigable_operations method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.get_navigable_operations()

    assert result == operations


def test_get_assertable_operations() -> None:
    """Test get_assertable_operations method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution_op = Operation(
        operation_id="exec-op",
        parent_id=None,
        name="execution",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )
    step_op = Operation(
        operation_id="step-op",
        parent_id=None,
        name="step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    operations = [execution_op, step_op]
    execution = Execution("test-arn", start_input, operations)

    result = execution.get_assertable_operations()

    assert len(result) == 1
    assert result[0] == step_op


def test_has_pending_operations_with_pending_step() -> None:
    """Test has_pending_operations returns True for pending STEP operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.STEP,
            status=OperationStatus.PENDING,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations()

    assert result is True


def test_has_pending_operations_with_started_wait() -> None:
    """Test has_pending_operations returns True for started WAIT operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations()

    assert result is True


def test_has_pending_operations_with_started_callback() -> None:
    """Test has_pending_operations returns True for started CALLBACK operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.CALLBACK,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations()

    assert result is True


def test_has_pending_operations_with_started_invoke() -> None:
    """Test has_pending_operations returns True for started INVOKE operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.CHAINED_INVOKE,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations()

    assert result is True


def test_has_pending_operations_no_pending() -> None:
    """Test has_pending_operations returns False when no pending operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations()

    assert result is False


@no_type_check
def test_complete_success_with_string_result() -> None:
    """Test complete_success method with string result."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])

    execution.complete_success("success result")

    assert execution.is_complete is True
    assert execution.result.status == InvocationStatus.SUCCEEDED
    assert execution.result.result == "success result"


@no_type_check
def test_complete_success_with_none_result() -> None:
    """Test complete_success method with None result."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])

    execution.complete_success(None)

    assert execution.is_complete is True
    assert execution.result.status == InvocationStatus.SUCCEEDED
    assert execution.result.result is None


@no_type_check
def test_complete_fail() -> None:
    """Test complete_fail method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])
    error = ErrorObject.from_message("Test error message")

    execution.complete_fail(error)

    assert execution.is_complete is True
    assert execution.result.status == InvocationStatus.FAILED
    assert execution.result.error == error


def test_find_operation_exists() -> None:
    """Test find_operation method when operation exists."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="test-op-id",
        parent_id=None,
        name="test",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    index, found_operation = execution.find_operation("test-op-id")

    assert index == 0
    assert found_operation == operation


def test_find_operation_not_exists() -> None:
    """Test find_operation method when operation doesn't exist."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [])

    with pytest.raises(
        IllegalStateException, match="Attempting to update state of an Operation"
    ):
        execution.find_operation("non-existent-id")


@patch("async_durable_execution._runner.local.execution.datetime")
def test_complete_wait_success(mock_datetime) -> None:
    """Test complete_wait method successful completion."""
    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = mock_now

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="wait-op-id",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    result = execution.complete_wait("wait-op-id")

    assert result.status == OperationStatus.SUCCEEDED
    assert result.end_timestamp == mock_now
    assert execution.token_sequence == 1
    assert execution.operations[0] == result


def test_complete_wait_wrong_status() -> None:
    """Test complete_wait method with wrong operation status."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="wait-op-id",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(
        IllegalStateException, match="Attempting to transition a Wait Operation"
    ):
        execution.complete_wait("wait-op-id")


def test_complete_wait_wrong_type() -> None:
    """Test complete_wait method with wrong operation type."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(IllegalStateException, match="Expected WAIT operation"):
        execution.complete_wait("step-op-id")


@no_type_check
def test_complete_retry_success() -> None:
    """Test complete_retry method successful completion."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    step_details = StepDetails(
        next_attempt_timestamp=str(datetime.now(timezone.utc)),
        attempt=1,
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=step_details,
    )
    execution = Execution("test-arn", start_input, [operation])

    result = execution.complete_retry("step-op-id")

    assert result.status == OperationStatus.READY
    assert result.step_details.next_attempt_timestamp is None
    assert execution.token_sequence == 1
    assert execution.operations[0] == result


def test_complete_retry_no_step_details() -> None:
    """Test complete_retry method with no step_details."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
    )
    execution = Execution("test-arn", start_input, [operation])

    result = execution.complete_retry("step-op-id")

    assert result.status == OperationStatus.READY
    assert result.step_details is None
    assert execution.token_sequence == 1


def test_complete_retry_wrong_status() -> None:
    """Test complete_retry method with wrong operation status."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(
        IllegalStateException, match="Attempting to transition a Step Operation"
    ):
        execution.complete_retry("step-op-id")


def test_complete_retry_wrong_type() -> None:
    """Test complete_retry method with wrong operation type."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="wait-op-id",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.WAIT,
        status=OperationStatus.PENDING,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(IllegalStateException, match="Expected STEP operation"):
        execution.complete_retry("wait-op-id")


def test_status_running() -> None:
    """Test status property returns RUNNING for incomplete execution."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [])

    assert execution.current_status().value == "RUNNING"


def test_status_succeeded() -> None:
    """Test status property returns SUCCEEDED for successful execution."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])
    execution.complete_success("success result")

    assert execution.current_status().value == "SUCCEEDED"


def test_status_failed() -> None:
    """Test status property returns FAILED for failed execution."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])
    error = ErrorObject.from_message("Test error")
    execution.complete_fail(error)

    assert execution.current_status().value == "FAILED"


def test_status_timed_out() -> None:
    """Test status property returns TIMED_OUT for timeout errors."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])
    error = ErrorObject(
        message="Execution timed out", type="TimeoutError", data=None, stack_trace=None
    )
    execution.complete_timeout(error)

    assert execution.current_status().value == "TIMED_OUT"


def test_status_stopped() -> None:
    """Test status property returns STOPPED for stop errors."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])
    error = ErrorObject(
        message="Execution stopped", type="StopError", data=None, stack_trace=None
    )
    execution.complete_stopped(error)

    assert execution.current_status().value == "STOPPED"


def test_status_no_result() -> None:
    """Test status property returns FAILED for completed execution with no result."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-id",
    )
    execution = Execution("test-arn", start_input, [])
    execution.is_complete = True
    execution.result = None
    with pytest.raises(
        IllegalStateException,
        match="close_status must be set when execution is complete",
    ):
        execution.current_status()


@no_type_check
def test_complete_retry_with_step_details() -> None:
    """Test complete_retry with operation that has step_details."""
    step_details = StepDetails(
        attempt=1, next_attempt_timestamp=datetime.now(timezone.utc)
    )
    step_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=step_details,
    )

    execution = Execution("test-arn", Mock(), [step_op])

    result = execution.complete_retry("op-1")
    assert result.status == OperationStatus.READY
    assert result.step_details.next_attempt_timestamp is None


def test_complete_retry_without_step_details() -> None:
    """Test complete_retry with operation that has no step_details."""
    step_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=None,  # No step details
    )

    execution = Execution("test-arn", Mock(), [step_op])

    result = execution.complete_retry("op-1")
    assert result.status == OperationStatus.READY
    assert result.step_details is None


def test_find_callback_operation_not_found() -> None:
    """Test find_callback_operation raises exception when callback not found."""
    execution = Execution("test-arn", Mock(), [])

    with pytest.raises(
        IllegalStateException,
        match="Callback operation with callback_id \\[nonexistent\\] not found",
    ):
        execution.find_callback_operation("nonexistent")


def test_complete_callback_success_not_started() -> None:
    """Test complete_callback_success raises exception when callback not in STARTED state."""
    # Create callback operation in wrong state
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,  # Wrong state
        callback_details=CallbackDetails(callback_id="test-id"),
    )

    execution = Execution("test-arn", Mock(), [callback_op])

    with pytest.raises(
        IllegalStateException,
        match="Callback operation \\[test-id\\] is not in STARTED state",
    ):
        execution.complete_callback_success("test-id")


def test_complete_callback_failure_not_started() -> None:
    """Test complete_callback_failure raises exception when callback not in STARTED state."""
    # Create callback operation in wrong state
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,  # Wrong state
        callback_details=CallbackDetails(callback_id="test-id"),
    )

    execution = Execution("test-arn", Mock(), [callback_op])
    error = ErrorObject.from_message("test error")

    with pytest.raises(
        IllegalStateException,
        match="Callback operation \\[test-id\\] is not in STARTED state",
    ):
        execution.complete_callback_failure("test-id", error)


def test_complete_callback_success_no_callback_details() -> None:
    """Test complete_callback_success with operation that has no callback_details."""
    callback_details = CallbackDetails(callback_id="test-id")
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    execution = Execution("test-arn", Mock(), [callback_op])

    # Test with None result
    result = execution.complete_callback_success("test-id", None)
    assert result.status == OperationStatus.SUCCEEDED


def test_complete_callback_failure_no_callback_details() -> None:
    """Test complete_callback_failure with operation that has no callback_details."""
    callback_details = CallbackDetails(callback_id="test-id")
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    execution = Execution("test-arn", Mock(), [callback_op])
    error = ErrorObject.from_message("test error")

    # Test with actual callback details
    result = execution.complete_callback_failure("test-id", error)
    assert result.status == OperationStatus.FAILED


@no_type_check
def test_complete_callback_success_with_none_callback_details() -> None:
    """Test complete_callback_success when operation has None callback_details."""
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=None,  # None callback details
    )

    execution = Execution("test-arn", Mock(), [callback_op])

    # Mock find_callback_operation to return this operation
    execution.find_callback_operation = Mock(return_value=(0, callback_op))

    result = execution.complete_callback_success("test-id", b"result")
    assert result.status == OperationStatus.SUCCEEDED
    assert result.callback_details is None


@no_type_check
def test_complete_callback_failure_with_none_callback_details() -> None:
    """Test complete_callback_failure when operation has None callback_details."""
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=None,  # None callback details
    )

    execution = Execution("test-arn", Mock(), [callback_op])
    error = ErrorObject.from_message("test error")

    # Mock find_callback_operation to return this operation
    execution.find_callback_operation = Mock(return_value=(0, callback_op))

    result = execution.complete_callback_failure("test-id", error)
    assert result.status == OperationStatus.FAILED
    assert result.callback_details is None


@no_type_check
def test_complete_callback_success_with_bytes_result() -> None:
    """Test complete_callback_success with bytes result that gets decoded."""
    callback_details = CallbackDetails(callback_id="test-id")
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    execution = Execution("test-arn", Mock(), [callback_op])

    result = execution.complete_callback_success("test-id", b"test result")
    assert result.status == OperationStatus.SUCCEEDED
    assert result.callback_details.result == "test result"


@no_type_check
def test_complete_callback_success_with_none_result() -> None:
    """Test complete_callback_success with None result."""
    callback_details = CallbackDetails(callback_id="test-id")
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    execution = Execution("test-arn", Mock(), [callback_op])

    result = execution.complete_callback_success("test-id", None)
    assert result.status == OperationStatus.SUCCEEDED
    assert result.callback_details.result is None


def test_start_requires_invocation_id() -> None:
    """Test start rejects input without an invocation id."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id=None,
    )
    execution = Execution("test-arn", start_input, [])

    with pytest.raises(
        InvalidParameterValueException, match="invocation_id is required"
    ):
        execution.start()


@no_type_check
def test_complete_callback_timeout_success() -> None:
    """Test callback timeout updates operation details and token sequence."""
    callback_details = CallbackDetails(callback_id="test-id")
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    execution = Execution("test-arn", Mock(), [callback_op])
    error = ErrorObject.from_message("timed out")

    result = execution.complete_callback_timeout("test-id", error)

    assert result.status == OperationStatus.TIMED_OUT
    assert result.callback_details.error == error
    assert result.end_timestamp is not None
    assert execution.token_sequence == 1


def test_complete_callback_timeout_not_started() -> None:
    """Test callback timeout rejects callbacks outside STARTED state."""
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(callback_id="test-id"),
    )
    execution = Execution("test-arn", Mock(), [callback_op])

    with pytest.raises(
        IllegalStateException,
        match="Callback operation \\[test-id\\] is not in STARTED state",
    ):
        execution.complete_callback_timeout(
            "test-id", ErrorObject.from_message("timed out")
        )
