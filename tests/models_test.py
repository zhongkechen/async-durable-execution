"""Tests for model classes and serialization helpers."""

from collections.abc import Mapping
from typing import Any, cast, no_type_check

import datetime
from datetime import timezone
from unittest.mock import patch

import pytest

from async_durable_execution._core.exceptions import CallableRuntimeError
from async_durable_execution._core.execution import InitialExecutionState
from async_durable_execution._core.models import OperationIdentifier
from async_durable_execution._core.models import (
    CallbackDetails,
    CallbackOptions,
    ChainedInvokeDetails,
    ChainedInvokeOptions,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ContextDetails,
    ContextOptions,
    DurableExecutionInvocationOutput,
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
    StateOutput,
    StepDetails,
    StepOptions,
    TimestampConverter,
    WaitDetails,
    WaitOptions,
)


# =============================================================================
# Tests for Data Classes (ExecutionDetails, ContextDetails, ErrorObject, etc.)
# =============================================================================


@pytest.mark.parametrize(
    "model_type",
    [
        ErrorObject,
        DurableExecutionInvocationOutput,
        ExecutionDetails,
        ContextDetails,
        StepDetails,
        WaitDetails,
        CallbackDetails,
        ChainedInvokeDetails,
        StepOptions,
        WaitOptions,
        CallbackOptions,
        ChainedInvokeOptions,
        ContextOptions,
        OperationUpdate,
        Operation,
        CheckpointUpdatedExecutionState,
        CheckpointOutput,
        StateOutput,
    ],
)
def test_boto_models_only_expose_dict_serialization(model_type) -> None:
    assert hasattr(model_type, "from_dict")
    assert hasattr(model_type, "to_dict")
    assert not hasattr(model_type, "from_boto")
    assert not hasattr(model_type, "to_boto")
    assert not hasattr(model_type, "from_json_dict")
    assert not hasattr(model_type, "to_json_dict")
    assert not hasattr(model_type, "from_mapping")
    assert not hasattr(model_type, "to_mapping")


async def test_execution_details_from_dict() -> None:
    """Test ExecutionDetails.from_dict method."""
    data = {"InputPayload": "test_payload"}
    details = ExecutionDetails.from_dict(data)
    assert details.input_payload == "test_payload"


@no_type_check
async def test_execution_details_empty() -> None:
    """Test ExecutionDetails.from_dict with empty data."""
    data = {}
    details = ExecutionDetails.from_dict(data)
    assert details.input_payload is None


async def test_context_details_from_dict() -> None:
    """Test ContextDetails.from_dict method."""
    data = {"Result": "test_result"}
    details = ContextDetails.from_dict(data)
    assert details.result == "test_result"
    assert details.error is None


@no_type_check
async def test_context_details_with_error() -> None:
    """Test ContextDetails.from_dict with error."""
    error_data = {"ErrorMessage": "Context error", "ErrorType": "ContextError"}
    data = {"Result": "test_result", "Error": error_data}
    details = ContextDetails.from_dict(data)
    assert details.result == "test_result"
    assert details.error.message == "Context error"
    assert details.error.type == "ContextError"


@no_type_check
async def test_context_details_error_only() -> None:
    """Test ContextDetails.from_dict with only error."""
    error_data = {"ErrorMessage": "Context failed"}
    data = {"Error": error_data}
    details = ContextDetails.from_dict(data)
    assert details.result is None
    assert details.error.message == "Context failed"


@no_type_check
async def test_context_details_empty() -> None:
    """Test ContextDetails.from_dict with empty data."""
    data = {}
    details = ContextDetails.from_dict(data)
    assert details.replay_children is False
    assert details.result is None
    assert details.error is None


async def test_context_details_with_replay_children() -> None:
    """Test ContextDetails.from_dict with replay_children field."""
    data = {"ReplayChildren": True, "Result": "test_result"}
    details = ContextDetails.from_dict(data)
    assert details.replay_children is True
    assert details.result == "test_result"
    assert details.error is None


async def test_error_object_from_dict() -> None:
    """Test ErrorObject.from_dict method."""
    data = {
        "ErrorMessage": "Test error",
        "ErrorType": "TestError",
        "ErrorData": "test_data",
        "StackTrace": ["line1", "line2"],
    }
    error = ErrorObject.from_dict(data)
    assert error.message == "Test error"
    assert error.type == "TestError"
    assert error.data == "test_data"
    assert error.stack_trace == ["line1", "line2"]


async def test_error_object_from_exception() -> None:
    """Test ErrorObject.from_exception method."""
    exception = ValueError("Test value error")
    error = ErrorObject.from_exception(exception)
    assert error.message == "Test value error"
    assert error.type == "ValueError"
    assert error.data is None
    assert error.stack_trace is None


async def test_error_object_from_exception_runtime_error() -> None:
    """Test ErrorObject.from_exception with RuntimeError."""
    runtime_error = RuntimeError("Runtime issue")
    error = ErrorObject.from_exception(runtime_error)
    assert error.message == "Runtime issue"
    assert error.type == "RuntimeError"
    assert error.data is None
    assert error.stack_trace is None


async def test_error_object_from_exception_custom_error() -> None:
    """Test ErrorObject.from_exception with custom exception."""

    class CustomError(Exception):
        pass

    custom_error = CustomError("Custom message")
    error = ErrorObject.from_exception(custom_error)
    assert error.message == "Custom message"
    assert error.type == "CustomError"
    assert error.data is None
    assert error.stack_trace is None


async def test_error_object_from_exception_empty_message() -> None:
    """Test ErrorObject.from_exception with exception that has no message."""
    empty_error = ValueError()
    error = ErrorObject.from_exception(empty_error)
    assert not error.message
    assert error.type == "ValueError"
    assert error.data is None
    assert error.stack_trace is None


async def test_error_object_from_message_regular() -> None:
    """Test ErrorObject.from_message with regular message."""
    error = ErrorObject.from_message("Test error message")
    assert error.message == "Test error message"
    assert error.type is None
    assert error.data is None
    assert error.stack_trace is None


async def test_error_object_from_message_empty() -> None:
    """Test ErrorObject.from_message with empty message."""
    error = ErrorObject.from_message("")
    assert not error.message
    assert error.type is None
    assert error.data is None
    assert error.stack_trace is None


async def test_error_object_to_dict() -> None:
    """Test ErrorObject.to_dict method."""
    error = ErrorObject(
        message="Test error",
        type="TestError",
        data="test_data",
        stack_trace=["line1", "line2"],
    )
    result = error.to_dict()
    expected = {
        "ErrorMessage": "Test error",
        "ErrorType": "TestError",
        "ErrorData": "test_data",
        "StackTrace": ["line1", "line2"],
    }
    assert result == expected


async def test_error_object_to_dict_partial() -> None:
    """Test ErrorObject.to_dict with None values."""
    error = ErrorObject(message="Test error", type=None, data=None, stack_trace=None)
    result = error.to_dict()
    assert result == {"ErrorMessage": "Test error"}


async def test_error_object_to_dict_all_none() -> None:
    """Test ErrorObject.to_dict with all None values."""
    error = ErrorObject(message=None, type=None, data=None, stack_trace=None)
    result = error.to_dict()
    assert result == {}


async def test_callable_runtime_error_from_error_object() -> None:
    """Test CallableRuntimeError.from_error_object method."""
    error = ErrorObject(
        message="Test error",
        type="TestError",
        data="test_data",
        stack_trace=["line1"],
    )
    runtime_error = CallableRuntimeError.from_error_object(error)
    assert isinstance(runtime_error, CallableRuntimeError)
    assert runtime_error.message == "Test error"
    assert runtime_error.error_type == "TestError"
    assert runtime_error.data == "test_data"
    assert runtime_error.stack_trace == ["line1"]


@no_type_check
async def test_step_details_from_dict() -> None:
    """Test StepDetails.from_dict method."""
    error_data = {"ErrorMessage": "Step error"}
    data = {
        "Attempt": 2,
        "NextAttemptTimestamp": datetime.datetime(
            2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        ),
        "Result": "step_result",
        "Error": error_data,
    }
    details = StepDetails.from_dict(data)
    assert details.attempt == 2
    assert details.next_attempt_timestamp == datetime.datetime(
        2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
    )
    assert details.result == "step_result"
    assert details.error.message == "Step error"


@no_type_check
async def test_step_details_all_fields() -> None:
    """Test StepDetails.from_dict with all fields."""
    error_data = {"ErrorMessage": "Step failed", "ErrorType": "StepError"}
    data = {
        "Attempt": 3,
        "NextAttemptTimestamp": datetime.datetime(
            2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
        ),
        "Result": "step_success",
        "Error": error_data,
    }
    details = StepDetails.from_dict(data)
    assert details.attempt == 3
    assert details.next_attempt_timestamp == datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    )
    assert details.result == "step_success"
    assert details.error.message == "Step failed"
    assert details.error.type == "StepError"


@no_type_check
async def test_step_details_minimal() -> None:
    """Test StepDetails.from_dict with minimal data."""
    data = {}
    details = StepDetails.from_dict(data)
    assert details.attempt == 0
    assert details.next_attempt_timestamp is None
    assert details.result is None
    assert details.error is None


async def test_wait_details_from_dict() -> None:
    """Test WaitDetails.from_dict method."""
    timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    data = {"ScheduledEndTimestamp": timestamp}
    details = WaitDetails.from_dict(data)
    assert details.scheduled_end_timestamp == timestamp


@no_type_check
async def test_wait_details_from_dict_empty() -> None:
    """Test WaitDetails.from_dict with empty data."""
    data = {}
    details = WaitDetails.from_dict(data)
    assert details.scheduled_end_timestamp is None


@no_type_check
async def test_callback_details_from_dict() -> None:
    """Test CallbackDetails.from_dict method."""
    error_data = {"ErrorMessage": "Callback error"}
    data = {
        "CallbackId": "cb123",
        "Result": "callback_result",
        "Error": error_data,
    }
    details = CallbackDetails.from_dict(data)
    assert details.callback_id == "cb123"
    assert details.result == "callback_result"
    assert details.error.message == "Callback error"


@no_type_check
async def test_callback_details_all_fields() -> None:
    """Test CallbackDetails.from_dict with all fields."""
    error_data = {"ErrorMessage": "Callback failed", "ErrorType": "CallbackError"}
    data = {
        "CallbackId": "cb456",
        "Result": "callback_success",
        "Error": error_data,
    }
    details = CallbackDetails.from_dict(data)
    assert details.callback_id == "cb456"
    assert details.result == "callback_success"
    assert details.error.message == "Callback failed"
    assert details.error.type == "CallbackError"


async def test_callback_details_minimal() -> None:
    """Test CallbackDetails.from_dict with minimal required data."""
    data = {"CallbackId": "cb789"}
    details = CallbackDetails.from_dict(data)
    assert details.callback_id == "cb789"
    assert details.result is None
    assert details.error is None


@no_type_check
async def test_invoke_details_from_dict() -> None:
    """Test ChainedInvokeDetails.from_dict method."""
    error_data = {"ErrorMessage": "Invoke error"}
    data = {
        "Result": "invoke_result",
        "Error": error_data,
    }
    details = ChainedInvokeDetails.from_dict(data)
    assert details.result == "invoke_result"
    assert details.error.message == "Invoke error"


@no_type_check
async def test_invoke_details_all_fields() -> None:
    """Test ChainedInvokeDetails.from_dict with all fields."""
    error_data = {"ErrorMessage": "Invoke failed", "ErrorType": "InvokeError"}
    data = {
        "Result": "invoke_success",
        "Error": error_data,
    }
    details = ChainedInvokeDetails.from_dict(data)
    assert details.result == "invoke_success"
    assert details.error.message == "Invoke failed"
    assert details.error.type == "InvokeError"


async def test_invoke_details_minimal() -> None:
    """Test ChainedInvokeDetails.from_dict with minimal required data."""
    data = {"DurableExecutionArn": "arn:minimal"}
    details = ChainedInvokeDetails.from_dict(data)
    assert hasattr(details, "durable_execution_arn") is False
    assert details.result is None
    assert details.error is None


# =============================================================================
# Tests for Options Classes (StepOptions, WaitOptions, etc.)
# =============================================================================


async def test_step_options_from_dict() -> None:
    """Test StepOptions.from_dict method."""
    data = {"NextAttemptDelaySeconds": 30}
    options = StepOptions.from_dict(data)
    assert options.next_attempt_delay_seconds == 30


async def test_step_options_from_dict_empty() -> None:
    """Test StepOptions.from_dict with empty dict."""
    options = StepOptions.from_dict({})
    assert options.next_attempt_delay_seconds == 0


async def test_callback_options_from_dict() -> None:
    """Test CallbackOptions.from_dict method."""
    data = {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60}
    options = CallbackOptions.from_dict(data)
    assert options.timeout_seconds == 300
    assert options.heartbeat_timeout_seconds == 60


async def test_callback_options_from_dict_partial() -> None:
    """Test CallbackOptions.from_dict with partial data."""
    data = {"TimeoutSeconds": 300}
    options = CallbackOptions.from_dict(data)
    assert options.timeout_seconds == 300
    assert options.heartbeat_timeout_seconds == 0


async def test_invoke_options_from_dict() -> None:
    """Test ChainedInvokeOptions.from_dict method."""
    data = {"FunctionName": "test-function", "TenantId": "test-tenant"}
    options = ChainedInvokeOptions.from_dict(data)
    assert options.function_name == "test-function"
    assert options.tenant_id == "test-tenant"


async def test_invoke_options_from_dict_required_only() -> None:
    """Test ChainedInvokeOptions.from_dict with only required field."""
    data = {"FunctionName": "test-function"}
    options = ChainedInvokeOptions.from_dict(data)
    assert options.function_name == "test-function"
    assert options.tenant_id is None


async def test_invoke_options_from_dict_with_none_tenant() -> None:
    """Test ChainedInvokeOptions.from_dict with explicit None tenant_id."""
    data = {"FunctionName": "test-function", "TenantId": None}
    options = ChainedInvokeOptions.from_dict(data)
    assert options.function_name == "test-function"
    assert options.tenant_id is None


async def test_context_options_from_dict() -> None:
    """Test ContextOptions.from_dict method."""
    data = {"ReplayChildren": True}
    options = ContextOptions.from_dict(data)
    assert options.replay_children is True


async def test_context_options_from_dict_empty() -> None:
    """Test ContextOptions.from_dict with empty dict."""
    options = ContextOptions.from_dict({})
    assert options.replay_children is False


async def test_step_options_roundtrip() -> None:
    """Test StepOptions to_dict -> from_dict roundtrip."""
    original = StepOptions(next_attempt_delay_seconds=45)
    data = original.to_dict()
    restored = StepOptions.from_dict(data)
    assert restored == original


async def test_callback_options_roundtrip() -> None:
    """Test CallbackOptions to_dict -> from_dict roundtrip."""
    original = CallbackOptions(timeout_seconds=300, heartbeat_timeout_seconds=60)
    data = original.to_dict()
    restored = CallbackOptions.from_dict(data)
    assert restored == original


async def test_invoke_options_roundtrip() -> None:
    """Test ChainedInvokeOptions to_dict -> from_dict roundtrip."""
    original = ChainedInvokeOptions(function_name="test-func")
    data = original.to_dict()
    restored = ChainedInvokeOptions.from_dict(data)
    assert restored == original


async def test_context_options_roundtrip() -> None:
    """Test ContextOptions to_dict -> from_dict roundtrip."""
    original = ContextOptions(replay_children=True)
    data = original.to_dict()
    restored = ContextOptions.from_dict(data)
    assert restored == original


async def test_wait_options_from_dict() -> None:
    """Test WaitOptions.from_dict method."""
    data = {"WaitSeconds": 30}
    options = WaitOptions.from_dict(data)
    assert options.wait_seconds == 30


async def test_step_options_to_dict() -> None:
    """Test StepOptions.to_dict method."""
    options = StepOptions(next_attempt_delay_seconds=30)
    result = options.to_dict()
    assert result == {"NextAttemptDelaySeconds": 30}


async def test_wait_options_to_dict() -> None:
    """Test WaitOptions.to_dict method."""
    options = WaitOptions(wait_seconds=60)
    result = options.to_dict()
    assert result == {"WaitSeconds": 60}


async def test_callback_options_to_dict() -> None:
    """Test CallbackOptions.to_dict method."""
    options = CallbackOptions(timeout_seconds=300, heartbeat_timeout_seconds=60)
    result = options.to_dict()
    assert result == {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60}


async def test_callback_options_all_fields() -> None:
    """Test CallbackOptions with all fields."""
    options = CallbackOptions(timeout_seconds=300, heartbeat_timeout_seconds=60)
    result = options.to_dict()
    assert result["TimeoutSeconds"] == 300
    assert result["HeartbeatTimeoutSeconds"] == 60


async def test_invoke_options_to_dict() -> None:
    """Test ChainedInvokeOptions.to_dict method."""
    options = ChainedInvokeOptions(
        function_name="test_function",
    )
    result = options.to_dict()
    expected = {
        "FunctionName": "test_function",
    }
    assert result == expected


async def test_invoke_options_to_dict_minimal() -> None:
    """Test ChainedInvokeOptions.to_dict with minimal fields."""
    options = ChainedInvokeOptions(function_name="test_function")
    result = options.to_dict()
    assert result == {"FunctionName": "test_function"}


async def test_context_options_to_dict() -> None:
    """Test ContextOptions.to_dict method."""
    options = ContextOptions(replay_children=True)
    result = options.to_dict()
    assert result == {"ReplayChildren": True}


async def test_context_options_to_dict_default() -> None:
    """Test ContextOptions.to_dict with default value."""
    options = ContextOptions()
    result = options.to_dict()
    assert result == {"ReplayChildren": False}


async def test_context_options_to_dict_false() -> None:
    """Test ContextOptions.to_dict with replay_children=False."""
    options = ContextOptions(replay_children=False)
    result = options.to_dict()
    assert result == {"ReplayChildren": False}


async def test_invoke_options_from_dict_missing_function_name() -> None:
    """Test ChainedInvokeOptions.from_dict with missing required FunctionName."""
    data = {"TimeoutSeconds": 60}

    with pytest.raises(KeyError):
        ChainedInvokeOptions.from_dict(data)


async def test_invoke_options_to_dict_complete() -> None:
    """Test ChainedInvokeOptions.to_dict with all fields."""
    options = ChainedInvokeOptions(function_name="test_func")

    result = options.to_dict()

    assert result["FunctionName"] == "test_func"


# =============================================================================
# Tests for OperationUpdate Class
# =============================================================================


async def test_operation_update_create_invoke_start() -> None:
    """Test OperationUpdate.create_invoke_start method to cover line 545."""
    identifier = OperationIdentifier(
        "test-id", OperationSubType.CHAINED_INVOKE, "parent-id"
    )
    invoke_options = ChainedInvokeOptions("test-func")
    update = OperationUpdate.create_invoke_start(identifier, "payload", invoke_options)
    assert update.operation_id == "test-id"


async def test_operation_update_to_dict() -> None:
    """Test OperationUpdate.to_dict method."""
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    step_options = StepOptions(next_attempt_delay_seconds=30)

    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        parent_id="parent1",
        name="test_step",
        payload="test_payload",
        error=error,
        step_options=step_options,
    )

    result = update.to_dict()
    expected = {
        "Id": "op1",
        "Type": "STEP",
        "Action": "RETRY",
        "ParentId": "parent1",
        "Name": "test_step",
        "Payload": "test_payload",
        "Error": {"ErrorMessage": "Test error", "ErrorType": "TestError"},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
    }
    assert result == expected


async def test_operation_update_to_dict_includes_empty_payload() -> None:
    """Operation updates preserve an empty-string payload."""
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        payload="",
    )

    assert update.to_dict()["Payload"] == ""


async def test_operation_update_to_dict_complete() -> None:
    """Test OperationUpdate.to_dict with all optional fields."""
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    step_options = StepOptions(next_attempt_delay_seconds=30)
    wait_options = WaitOptions(wait_seconds=60)
    callback_options = CallbackOptions(
        timeout_seconds=300, heartbeat_timeout_seconds=60
    )
    chained_invoke_options = ChainedInvokeOptions(function_name="test_func")

    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        parent_id="parent1",
        name="test_step",
        payload="test_payload",
        error=error,
        step_options=step_options,
        wait_options=wait_options,
        callback_options=callback_options,
        chained_invoke_options=chained_invoke_options,
    )

    result = update.to_dict()
    expected = {
        "Id": "op1",
        "Type": "STEP",
        "Action": "RETRY",
        "ParentId": "parent1",
        "Name": "test_step",
        "Payload": "test_payload",
        "Error": {"ErrorMessage": "Test error", "ErrorType": "TestError"},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
        "WaitOptions": {"WaitSeconds": 60},
        "CallbackOptions": {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60},
        "ChainedInvokeOptions": {"FunctionName": "test_func"},
    }
    assert result == expected


async def test_operation_update_minimal() -> None:
    """Test OperationUpdate.to_dict with minimal required fields."""
    update = OperationUpdate(
        operation_id="minimal_op",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.START,
    )
    result = update.to_dict()
    expected = {
        "Id": "minimal_op",
        "Type": "EXECUTION",
        "Action": "START",
    }
    assert result == expected


async def test_operation_update_create_callback() -> None:
    """Test OperationUpdate.create_callback factory method."""
    callback_options = CallbackOptions(timeout_seconds=300)
    update = OperationUpdate.create_callback(
        OperationIdentifier("cb1", OperationSubType.CALLBACK, None, "test_callback"),
        callback_options,
    )
    assert update.operation_id == "cb1"
    assert update.operation_type is OperationType.CALLBACK
    assert update.action is OperationAction.START
    assert update.name == "test_callback"
    assert update.callback_options == callback_options
    assert update.sub_type is OperationSubType.CALLBACK


async def test_operation_update_create_wait_start() -> None:
    """Test OperationUpdate.create_wait_start factory method."""
    wait_options = WaitOptions(wait_seconds=30)
    update = OperationUpdate.create_wait_start(
        OperationIdentifier("wait1", OperationSubType.WAIT, "parent1", "test_wait"),
        wait_options,
    )
    assert update.operation_id == "wait1"
    assert update.parent_id == "parent1"
    assert update.operation_type is OperationType.WAIT
    assert update.action is OperationAction.START
    assert update.name == "test_wait"
    assert update.wait_options == wait_options
    assert update.sub_type is OperationSubType.WAIT


@patch("async_durable_execution._core.models.datetime")
async def test_operation_update_create_execution_succeed(mock_datetime) -> None:
    """Test OperationUpdate.create_execution_succeed factory method."""

    mock_datetime.datetime.now.return_value = datetime.datetime.fromtimestamp(
        1672531200.0, tz=datetime.timezone.utc
    )
    update = OperationUpdate.create_execution_succeed("success_payload")
    assert update.operation_id == "execution-result-1672531200000"
    assert update.operation_type == OperationType.EXECUTION
    assert update.action == OperationAction.SUCCEED
    assert update.payload == "success_payload"


async def test_operation_update_create_step_succeed() -> None:
    """Test OperationUpdate.create_step_succeed factory method."""
    update = OperationUpdate.create_step_succeed(
        OperationIdentifier("step1", OperationSubType.STEP, None, "test_step"),
        "step_payload",
    )
    assert update.operation_id == "step1"
    assert update.operation_type is OperationType.STEP
    assert update.action is OperationAction.SUCCEED
    assert update.name == "test_step"
    assert update.payload == "step_payload"
    assert update.sub_type is OperationSubType.STEP


@no_type_check
async def test_operation_update_factory_methods() -> None:
    """Test all OperationUpdate factory methods."""
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )

    # Test create_context_start
    update = OperationUpdate.create_context_start(
        OperationIdentifier(
            "ctx1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_context"
        ),
        OperationSubType.RUN_IN_CHILD_CONTEXT,
    )
    assert update.operation_type is OperationType.CONTEXT
    assert update.action is OperationAction.START
    assert update.sub_type is OperationSubType.RUN_IN_CHILD_CONTEXT

    # Test create_context_succeed
    update = OperationUpdate.create_context_succeed(
        OperationIdentifier(
            "ctx1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_context"
        ),
        "payload",
        OperationSubType.RUN_IN_CHILD_CONTEXT,
    )
    assert update.action is OperationAction.SUCCEED
    assert update.payload == "payload"
    assert update.sub_type is OperationSubType.RUN_IN_CHILD_CONTEXT

    # Test create_context_fail
    update = OperationUpdate.create_context_fail(
        OperationIdentifier(
            "ctx1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_context"
        ),
        error,
        OperationSubType.RUN_IN_CHILD_CONTEXT,
    )
    assert update.action is OperationAction.FAIL
    assert update.error == error
    assert update.sub_type is OperationSubType.RUN_IN_CHILD_CONTEXT

    # Test create_execution_fail
    update = OperationUpdate.create_execution_fail(error)
    assert update.operation_type is OperationType.EXECUTION
    assert update.action is OperationAction.FAIL

    # Test create_step_fail
    update = OperationUpdate.create_step_fail(
        OperationIdentifier("step1", OperationSubType.STEP, None, "test_step"), error
    )
    assert update.operation_type is OperationType.STEP
    assert update.action is OperationAction.FAIL
    assert update.sub_type is OperationSubType.STEP

    # Test create_step_start
    update = OperationUpdate.create_step_start(
        OperationIdentifier("step1", OperationSubType.STEP, None, "test_step")
    )
    assert update.action is OperationAction.START
    assert update.sub_type is OperationSubType.STEP

    # Test create_step_retry
    update = OperationUpdate.create_step_retry(
        OperationIdentifier("step1", OperationSubType.STEP, None, "test_step"),
        error,
        30,
    )
    assert update.action is OperationAction.RETRY
    assert update.step_options.next_attempt_delay_seconds == 30
    assert update.sub_type is OperationSubType.STEP


async def test_operation_update_with_parent_id() -> None:
    """Test OperationUpdate with parent_id field."""
    update = OperationUpdate(
        operation_id="child_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_op",
        name="child_step",
    )

    result = update.to_dict()
    assert result["ParentId"] == "parent_op"


async def test_operation_update_wait_and_invoke_types() -> None:
    """Test OperationUpdate with WAIT and INVOKE operation types."""
    # Test WAIT operation
    wait_options = WaitOptions(wait_seconds=30)
    wait_update = OperationUpdate(
        operation_id="wait_op",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        wait_options=wait_options,
    )

    result = wait_update.to_dict()
    assert result["Type"] == "WAIT"
    assert result["WaitOptions"]["WaitSeconds"] == 30

    # Test INVOKE operation
    chained_invoke_options = ChainedInvokeOptions(function_name="test_func")
    invoke_update = OperationUpdate(
        operation_id="invoke_op",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
        chained_invoke_options=chained_invoke_options,
    )

    result = invoke_update.to_dict()
    assert result["Type"] == "CHAINED_INVOKE"
    assert result["ChainedInvokeOptions"]["FunctionName"] == "test_func"


async def test_operation_update_create_wait() -> None:
    """Test OperationUpdate factory method for WAIT operations."""
    wait_options = WaitOptions(wait_seconds=30)
    update = OperationUpdate(
        operation_id="wait1",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        wait_options=wait_options,
    )

    assert update.operation_type == OperationType.WAIT
    assert update.wait_options == wait_options


async def test_operation_update_create_invoke() -> None:
    """Test OperationUpdate factory method for INVOKE operations."""
    chained_invoke_options = ChainedInvokeOptions(function_name="test-function")
    update = OperationUpdate(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
        chained_invoke_options=chained_invoke_options,
    )

    assert update.operation_type == OperationType.CHAINED_INVOKE
    assert update.chained_invoke_options == chained_invoke_options


async def test_operation_update_with_sub_type() -> None:
    """Test OperationUpdate with sub_type field."""
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        sub_type=OperationSubType.STEP,
    )
    result = update.to_dict()
    assert result["SubType"] == "Step"


async def test_operation_update_with_context_options() -> None:
    """Test OperationUpdate with context_options field."""
    context_options = ContextOptions(replay_children=True)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        context_options=context_options,
    )
    result = update.to_dict()
    assert result["ContextOptions"] == {"ReplayChildren": True}


async def test_operation_update_complete_with_new_fields() -> None:
    """Test OperationUpdate.to_dict with all fields including new ones."""
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    context_options = ContextOptions(replay_children=True)
    step_options = StepOptions(next_attempt_delay_seconds=30)
    wait_options = WaitOptions(wait_seconds=60)
    callback_options = CallbackOptions(
        timeout_seconds=300, heartbeat_timeout_seconds=60
    )
    chained_invoke_options = ChainedInvokeOptions(function_name="test_func")

    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.RETRY,
        parent_id="parent1",
        name="test_context",
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
        payload="test_payload",
        error=error,
        context_options=context_options,
        step_options=step_options,
        wait_options=wait_options,
        callback_options=callback_options,
        chained_invoke_options=chained_invoke_options,
    )

    result = update.to_dict()
    expected = {
        "Id": "op1",
        "Type": "CONTEXT",
        "Action": "RETRY",
        "ParentId": "parent1",
        "Name": "test_context",
        "SubType": "RunInChildContext",
        "Payload": "test_payload",
        "Error": {"ErrorMessage": "Test error", "ErrorType": "TestError"},
        "ContextOptions": {"ReplayChildren": True},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
        "WaitOptions": {"WaitSeconds": 60},
        "CallbackOptions": {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60},
        "ChainedInvokeOptions": {"FunctionName": "test_func"},
    }
    assert result == expected


# =============================================================================
# Tests for new wait-for-condition factory methods
# =============================================================================


async def test_operation_update_create_wait_for_condition_start() -> None:
    """Test OperationUpdate.create_wait_for_condition_start factory method."""
    identifier = OperationIdentifier(
        "wait_cond_1",
        OperationSubType.WAIT_FOR_CONDITION,
        "parent1",
        "test_wait_condition",
    )
    update = OperationUpdate.create_wait_for_condition_start(identifier)

    assert update.operation_id == "wait_cond_1"
    assert update.parent_id == "parent1"
    assert update.operation_type == OperationType.STEP
    assert update.sub_type == OperationSubType.WAIT_FOR_CONDITION
    assert update.action == OperationAction.START
    assert update.name == "test_wait_condition"


async def test_operation_update_create_wait_for_condition_succeed() -> None:
    """Test OperationUpdate.create_wait_for_condition_succeed factory method."""
    identifier = OperationIdentifier(
        "wait_cond_1",
        OperationSubType.WAIT_FOR_CONDITION,
        "parent1",
        "test_wait_condition",
    )
    update = OperationUpdate.create_wait_for_condition_succeed(
        identifier, "success_payload"
    )

    assert update.operation_id == "wait_cond_1"
    assert update.parent_id == "parent1"
    assert update.operation_type == OperationType.STEP
    assert update.sub_type == OperationSubType.WAIT_FOR_CONDITION
    assert update.action == OperationAction.SUCCEED
    assert update.name == "test_wait_condition"
    assert update.payload == "success_payload"


@no_type_check
async def test_operation_update_create_wait_for_condition_retry() -> None:
    """Test OperationUpdate.create_wait_for_condition_retry factory method."""
    identifier = OperationIdentifier(
        "wait_cond_1",
        OperationSubType.WAIT_FOR_CONDITION,
        "parent1",
        "test_wait_condition",
    )
    update = OperationUpdate.create_wait_for_condition_retry(
        identifier, "retry_payload", 45
    )

    assert update.operation_id == "wait_cond_1"
    assert update.parent_id == "parent1"
    assert update.operation_type == OperationType.STEP
    assert update.sub_type == OperationSubType.WAIT_FOR_CONDITION
    assert update.action == OperationAction.RETRY
    assert update.name == "test_wait_condition"
    assert update.payload == "retry_payload"
    assert update.step_options.next_attempt_delay_seconds == 45


async def test_operation_update_create_wait_for_condition_fail() -> None:
    """Test OperationUpdate.create_wait_for_condition_fail factory method."""
    identifier = OperationIdentifier(
        "wait_cond_1",
        OperationSubType.WAIT_FOR_CONDITION,
        "parent1",
        "test_wait_condition",
    )
    error = ErrorObject(
        message="Condition failed", type="ConditionError", data=None, stack_trace=None
    )
    update = OperationUpdate.create_wait_for_condition_fail(identifier, error)

    assert update.operation_id == "wait_cond_1"
    assert update.parent_id == "parent1"
    assert update.operation_type == OperationType.STEP
    assert update.sub_type == OperationSubType.WAIT_FOR_CONDITION
    assert update.action == OperationAction.FAIL
    assert update.name == "test_wait_condition"
    assert update.error == error


# Tests for ContextOptions class


async def test_operation_update_to_dict_with_sub_type() -> None:
    """Test OperationUpdate.to_dict includes sub_type field when present."""
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        sub_type=OperationSubType.WAIT_FOR_CONDITION,
    )
    result = update.to_dict()
    assert result["SubType"] == "WaitForCondition"


async def test_operation_update_to_dict_without_sub_type() -> None:
    """Test OperationUpdate.to_dict excludes sub_type field when None."""
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    result = update.to_dict()
    assert "SubType" not in result


async def test_operation_update_with_all_none_values() -> None:
    """Test OperationUpdate.to_dict with None values for optional fields."""
    update = OperationUpdate(
        operation_id="test",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    result = update.to_dict()

    # Should only contain required fields
    assert result["Id"] == "test"
    assert result["Type"] == "STEP"
    assert result["Action"] == "START"
    assert "ParentId" not in result
    assert "Name" not in result
    assert "Payload" not in result


async def test_operation_update_from_dict_with_minimal_data() -> None:
    """Test OperationUpdate.from_dict with minimal required data."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Action": "START",
    }

    update = OperationUpdate.from_dict(data)
    assert update.operation_id == "test-id"
    assert update.operation_type == OperationType.STEP
    assert update.action == OperationAction.START
    assert update.parent_id is None
    assert update.name is None


async def test_operation_update_from_dict_with_error_only() -> None:
    """Test OperationUpdate.from_dict with Error field only."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Action": "FAIL",
        "Error": {"ErrorMessage": "Test error"},
    }

    update = OperationUpdate.from_dict(data)
    assert update.error is not None
    assert update.error.message == "Test error"


async def test_operation_update_from_dict_with_all_options() -> None:
    """Test OperationUpdate.from_dict with all option types."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Action": "START",
        "ContextOptions": {"ReplayChildren": True},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
        "WaitOptions": {"WaitSeconds": 60},
        "CallbackOptions": {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60},
        "ChainedInvokeOptions": {"FunctionName": "test_func", "TimeoutSeconds": 120},
    }

    update = OperationUpdate.from_dict(data)
    assert update.operation_id == "test-id"
    assert update.operation_type == OperationType.STEP
    assert update.action == OperationAction.START
    assert update.context_options is not None
    assert update.step_options is not None
    assert update.wait_options is not None
    assert update.callback_options is not None
    assert update.chained_invoke_options is not None


# =============================================================================
# Tests for Operation Class
# =============================================================================


async def test_operation_from_dict_with_all_options() -> None:
    """Test Operation.from_dict with all option types to cover lines 339-361."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "ParentId": "parent-id",
        "ContextOptions": {"ReplayChildren": True},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
        "WaitOptions": {"WaitSeconds": 60},
        "CallbackOptions": {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60},
        "ChainedInvokeOptions": {"FunctionName": "test-func", "TimeoutSeconds": 120},
    }
    operation = Operation.from_dict(data)
    assert operation.operation_id == "test-id"


async def test_operation_from_dict_no_options() -> None:
    """Test Operation.from_dict without options to cover None assignments."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
    }
    operation = Operation.from_dict(data)
    assert operation.operation_id == "test-id"


async def test_operation_from_dict_individual_options() -> None:
    """Test Operation.from_dict with each option type individually."""
    # Test with just ContextOptions
    data1 = {
        "Id": "test1",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "ContextOptions": {"ReplayChildren": True},
    }
    op1 = Operation.from_dict(data1)
    assert op1.operation_id == "test1"

    # Test with just StepOptions
    data2 = {
        "Id": "test2",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "StepOptions": {"NextAttemptDelaySeconds": 30},
    }
    op2 = Operation.from_dict(data2)
    assert op2.operation_id == "test2"

    # Test with just WaitOptions
    data3 = {
        "Id": "test3",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "WaitOptions": {"WaitSeconds": 60},
    }
    op3 = Operation.from_dict(data3)
    assert op3.operation_id == "test3"

    # Test with just CallbackOptions
    data4 = {
        "Id": "test4",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "CallbackOptions": {"TimeoutSeconds": 300},
    }
    op4 = Operation.from_dict(data4)
    assert op4.operation_id == "test4"

    # Test with just ChainedInvokeOptions
    data5 = {
        "Id": "test5",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "ChainedInvokeOptions": {"FunctionName": "test-func"},
    }
    op5 = Operation.from_dict(data5)
    assert op5.operation_id == "test5"


async def test_operation_from_dict_with_all_option_types() -> None:
    """Test Operation.from_dict with all option types present."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "ContextOptions": {"ReplayChildren": True},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
        "WaitOptions": {"WaitSeconds": 60},
        "CallbackOptions": {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60},
        "ChainedInvokeOptions": {"FunctionName": "test_func", "TimeoutSeconds": 120},
    }

    operation = Operation.from_dict(data)
    assert operation.operation_id == "test-id"
    assert operation.operation_type == OperationType.STEP
    assert operation.status == OperationStatus.SUCCEEDED


@no_type_check
async def test_operation_to_dict_with_all_details() -> None:
    """Test Operation.to_dict with all detail types."""
    execution_details = ExecutionDetails(input_payload="exec_payload")
    context_details = ContextDetails(
        result="context_result", error=None, replay_children=True
    )
    step_details = StepDetails(
        attempt=2, next_attempt_timestamp="2023-01-01", result="step_result", error=None
    )
    wait_details = WaitDetails(
        scheduled_end_timestamp=datetime.datetime(
            2023, 1, 1, tzinfo=datetime.timezone.utc
        )
    )
    callback_details = CallbackDetails(
        callback_id="cb123", result="callback_result", error=None
    )
    chained_invoke_details = ChainedInvokeDetails(result="invoke_result", error=None)

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent",
        name="test_op",
        start_timestamp=datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
        end_timestamp=datetime.datetime(2023, 1, 2, tzinfo=datetime.timezone.utc),
        execution_details=execution_details,
        context_details=context_details,
        step_details=step_details,
        wait_details=wait_details,
        callback_details=callback_details,
        chained_invoke_details=chained_invoke_details,
        sub_type=OperationSubType.STEP,
    )

    result = operation.to_dict()

    assert result["ExecutionDetails"]["InputPayload"] == "exec_payload"
    assert result["ContextDetails"]["Result"] == "context_result"
    assert result["StepDetails"]["Attempt"] == 2
    assert result["WaitDetails"]["ScheduledEndTimestamp"] == datetime.datetime(
        2023, 1, 1, tzinfo=datetime.timezone.utc
    )
    assert result["CallbackDetails"]["CallbackId"] == "cb123"
    assert result["ChainedInvokeDetails"]["Result"] == "invoke_result"


async def test_operation_to_dict_with_step_details_partial() -> None:
    """Test Operation.to_dict with step_details having some None fields."""
    step_details = StepDetails(
        attempt=1, next_attempt_timestamp=None, result=None, error=None
    )

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=step_details,
    )

    result = operation.to_dict()
    step_dict = result["StepDetails"]
    assert step_dict["Attempt"] == 1
    assert "NextAttemptTimestamp" not in step_dict
    assert "Result" not in step_dict
    assert "Error" not in step_dict


async def test_operation_to_dict_with_callback_details_partial() -> None:
    """Test Operation.to_dict with callback_details having some None fields."""
    callback_details = CallbackDetails(callback_id="cb123", result=None, error=None)

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.PENDING,
        callback_details=callback_details,
    )

    result = operation.to_dict()
    callback_dict = result["CallbackDetails"]
    assert callback_dict["CallbackId"] == "cb123"
    assert "Result" not in callback_dict
    assert "Error" not in callback_dict


async def test_operation_to_dict_with_invoke_details_partial() -> None:
    """Test Operation.to_dict with invoke_details having some None fields."""
    chained_invoke_details = ChainedInvokeDetails(result=None, error=None)

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.PENDING,
        chained_invoke_details=chained_invoke_details,
    )

    result = operation.to_dict()
    invoke_dict = result["ChainedInvokeDetails"]
    assert "Result" not in invoke_dict
    assert "Error" not in invoke_dict


async def test_operation_to_dict_with_context_details_complete() -> None:
    """Test Operation.to_dict with context_details having all fields."""
    error = ErrorObject(
        message="Context error", type="ContextError", data=None, stack_trace=None
    )
    context_details = ContextDetails(
        result="context_result", error=error, replay_children=True
    )

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.FAILED,
        context_details=context_details,
    )

    result = operation.to_dict()
    context_dict = result["ContextDetails"]
    assert context_dict["Result"] == "context_result"
    # Note: The current implementation only includes Result, not error or replay_children


async def test_operation_to_dict_with_execution_details_none() -> None:
    """Test Operation.to_dict with execution_details having None input_payload."""
    execution_details = ExecutionDetails(input_payload=None)

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        execution_details=execution_details,
    )

    result = operation.to_dict()
    exec_dict = result["ExecutionDetails"]
    assert exec_dict["InputPayload"] is None


async def test_operation_to_dict_with_step_details_error() -> None:
    """Test Operation.to_dict with step_details having error."""
    error = ErrorObject(
        message="Step failed", type="StepError", data=None, stack_trace=None
    )
    step_details = StepDetails(
        attempt=1, next_attempt_timestamp=None, result=None, error=error
    )

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=step_details,
    )

    result = operation.to_dict()
    step_dict = result["StepDetails"]
    assert step_dict["Error"]["ErrorMessage"] == "Step failed"
    assert step_dict["Error"]["ErrorType"] == "StepError"


async def test_operation_to_dict_with_callback_details_error() -> None:
    """Test Operation.to_dict with callback_details having error."""
    error = ErrorObject(
        message="Callback failed", type="CallbackError", data=None, stack_trace=None
    )
    callback_details = CallbackDetails(callback_id="cb123", result=None, error=error)

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=callback_details,
    )

    result = operation.to_dict()
    callback_dict = result["CallbackDetails"]
    assert callback_dict["Error"]["ErrorMessage"] == "Callback failed"
    assert callback_dict["Error"]["ErrorType"] == "CallbackError"


async def test_operation_to_dict_with_invoke_details_error() -> None:
    """Test Operation.to_dict with chained_invoke_details having error."""
    error = ErrorObject(
        message="Invoke failed", type="InvokeError", data=None, stack_trace=None
    )
    chained_invoke_details = ChainedInvokeDetails(result=None, error=error)

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.FAILED,
        chained_invoke_details=chained_invoke_details,
    )

    result = operation.to_dict()
    invoke_dict = result["ChainedInvokeDetails"]
    assert invoke_dict["Error"]["ErrorMessage"] == "Invoke failed"
    assert invoke_dict["Error"]["ErrorType"] == "InvokeError"


@no_type_check
async def test_operation_from_dict() -> None:
    """Test Operation.from_dict method."""
    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "ParentId": "parent1",
        "Name": "test_step",
        "StepDetails": {"Result": "step_result"},
    }

    operation = Operation.from_dict(data)
    assert operation.operation_id == "op1"
    assert operation.operation_type is OperationType.STEP
    assert operation.status is OperationStatus.SUCCEEDED
    assert operation.parent_id == "parent1"
    assert operation.name == "test_step"
    assert operation.step_details.result == "step_result"


async def test_operation_from_dict_with_subtype() -> None:
    """Test Operation.from_dict method with SubType field."""
    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "SubType": "Step",
    }

    operation = Operation.from_dict(data)
    assert operation.operation_id == "op1"
    assert operation.operation_type is OperationType.STEP
    assert operation.status is OperationStatus.SUCCEEDED
    assert operation.sub_type is OperationSubType.STEP


@no_type_check
async def test_operation_from_dict_complete() -> None:
    """Test Operation.from_dict with all fields."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)
    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "ParentId": "parent1",
        "Name": "test_step",
        "StartTimestamp": start_time,
        "EndTimestamp": end_time,
        "SubType": "Step",
        "ExecutionDetails": {"InputPayload": "exec_payload"},
        "ContextDetails": {"Result": "context_result"},
        "StepDetails": {"Result": "step_result", "Attempt": 1},
        "WaitDetails": {"ScheduledEndTimestamp": start_time},
        "CallbackDetails": {"CallbackId": "cb1", "Result": "callback_result"},
        "ChainedInvokeDetails": {
            "DurableExecutionArn": "arn:test",
            "Result": "invoke_result",
        },
    }
    operation = Operation.from_dict(data)
    assert operation.operation_id == "op1"
    assert operation.operation_type is OperationType.STEP
    assert operation.status is OperationStatus.SUCCEEDED
    assert operation.parent_id == "parent1"
    assert operation.name == "test_step"
    assert operation.start_timestamp == start_time
    assert operation.end_timestamp == end_time
    assert operation.sub_type is OperationSubType.STEP
    assert operation.execution_details.input_payload == "exec_payload"
    assert operation.context_details.result == "context_result"
    assert operation.step_details.result == "step_result"
    assert operation.wait_details.scheduled_end_timestamp == start_time
    assert operation.callback_details.callback_id == "cb1"
    assert operation.chained_invoke_details is not None
    assert operation.chained_invoke_details.result == "invoke_result"


async def test_operation_to_dict_with_subtype() -> None:
    """Test Operation.to_dict method includes SubType field."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        sub_type=OperationSubType.STEP,
    )
    result = operation.to_dict()
    assert result["SubType"] == "Step"


async def test_operation_to_dict_all_optional_fields() -> None:
    """Test Operation.to_dict with all optional fields."""

    operation = Operation(
        operation_id="test1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent1",
        name="test-step",
        start_timestamp=datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
        end_timestamp=datetime.datetime(2023, 1, 2, tzinfo=datetime.timezone.utc),
        sub_type=OperationSubType.STEP,
    )

    result = operation.to_dict()

    assert result["ParentId"] == "parent1"
    assert result["Name"] == "test-step"
    assert result["StartTimestamp"] == datetime.datetime(
        2023, 1, 1, tzinfo=datetime.timezone.utc
    )
    assert result["EndTimestamp"] == datetime.datetime(
        2023, 1, 2, tzinfo=datetime.timezone.utc
    )
    assert result["SubType"] == "Step"


# =============================================================================
# Tests for Checkpoint Classes
# =============================================================================


async def test_checkpoint_output_from_dict() -> None:
    """Test CheckpointOutput.from_dict method."""
    data = {
        "CheckpointToken": "token123",
        "NewExecutionState": {
            "Operations": [{"Id": "op1", "Type": "STEP", "Status": "SUCCEEDED"}],
            "NextMarker": "marker123",
        },
    }
    output = CheckpointOutput.from_dict(data)
    assert output.checkpoint_token == "token123"  # noqa: S105
    assert len(output.new_execution_state.operations) == 1
    assert output.new_execution_state.next_marker == "marker123"


@no_type_check
async def test_checkpoint_output_from_dict_empty() -> None:
    """Test CheckpointOutput.from_dict with empty data."""
    data = {}
    output = CheckpointOutput.from_dict(data)
    assert output.checkpoint_token is None
    assert len(output.new_execution_state.operations) == 0
    assert output.new_execution_state.next_marker is None
    assert "CheckpointToken" not in output.to_dict()


async def test_checkpoint_updated_execution_state_from_dict() -> None:
    """Test CheckpointUpdatedExecutionState.from_dict method."""
    data = {
        "Operations": [
            {"Id": "op1", "Type": "STEP", "Status": "SUCCEEDED"},
            {"Id": "op2", "Type": "WAIT", "Status": "PENDING"},
        ],
        "NextMarker": "marker456",
    }
    state = CheckpointUpdatedExecutionState.from_dict(data)
    assert len(state.operations) == 2
    assert state.next_marker == "marker456"
    assert state.operations[0].operation_id == "op1"
    assert state.operations[1].operation_id == "op2"


@no_type_check
async def test_checkpoint_updated_execution_state_from_dict_empty() -> None:
    """Test CheckpointUpdatedExecutionState.from_dict with empty data."""
    data = {}
    state = CheckpointUpdatedExecutionState.from_dict(data)
    assert len(state.operations) == 0
    assert state.next_marker is None


async def test_state_output_from_dict() -> None:
    """Test StateOutput.from_dict method."""
    data = {
        "Operations": [
            {"Id": "op1", "Type": "EXECUTION", "Status": "SUCCEEDED"},
        ],
        "NextMarker": "state_marker",
    }
    output = StateOutput.from_dict(data)
    assert len(output.operations) == 1
    assert output.next_marker == "state_marker"
    assert output.operations[0].operation_type is OperationType.EXECUTION


@no_type_check
async def test_state_output_from_dict_empty() -> None:
    """Test StateOutput.from_dict with empty data."""
    data = {}
    output = StateOutput.from_dict(data)
    assert len(output.operations) == 0
    assert output.next_marker is None


async def test_state_output_from_dict_empty_operations() -> None:
    """Test StateOutput.from_dict with no operations."""
    data = {"NextMarker": "marker123"}  # No Operations key

    output = StateOutput.from_dict(data)
    assert len(output.operations) == 0
    assert output.next_marker == "marker123"


async def test_checkpoint_output_from_dict_with_empty_operations() -> None:
    """Test CheckpointOutput.from_dict with empty operations list."""
    data = {
        "CheckpointToken": "token123",
        "NewExecutionState": {"Operations": [], "NextMarker": "marker123"},
    }

    output = CheckpointOutput.from_dict(data)
    assert output.checkpoint_token == "token123"  # noqa: S105
    assert len(output.new_execution_state.operations) == 0
    assert output.new_execution_state.next_marker == "marker123"


async def test_state_output_from_dict_with_next_marker_only() -> None:
    """Test StateOutput.from_dict with NextMarker but no operations."""
    data = {"NextMarker": "marker456"}

    output = StateOutput.from_dict(data)
    assert len(output.operations) == 0
    assert output.next_marker == "marker456"


async def test_checkpoint_updated_execution_state_from_dict_with_operations() -> None:
    """Test CheckpointUpdatedExecutionState.from_dict with operations."""
    data = {
        "Operations": [
            {"Id": "op1", "Type": "STEP", "Status": "SUCCEEDED"},
            {"Id": "op2", "Type": "WAIT", "Status": "PENDING"},
        ],
        "NextMarker": "marker123",
    }

    state = CheckpointUpdatedExecutionState.from_dict(data)
    assert len(state.operations) == 2
    assert state.operations[0].operation_id == "op1"
    assert state.operations[1].operation_id == "op2"
    assert state.next_marker == "marker123"


# Tests for nested Operation JSON serialization
# =============================================================================


def _operation_to_json_mapping(operation: Operation) -> dict[str, Any]:
    state = InitialExecutionState(operations=[operation])
    return cast("dict[str, Any]", state.to_dict()["Operations"][0])


def _operation_from_json_mapping(data: Mapping[str, Any]) -> Operation:
    state = InitialExecutionState.from_dict({"Operations": [data]})
    return state.operations[0]


async def test_operation_json_mapping_minimal() -> None:
    """Test nested Operation JSON mapping with minimal required fields."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )

    result = _operation_to_json_mapping(operation)
    expected = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
    }
    assert result == expected


async def test_operation_json_mapping_with_timestamps() -> None:
    """Test nested Operation JSON mapping converts datetimes to milliseconds."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime(2023, 1, 1, 11, 30, 0, tzinfo=datetime.timezone.utc)

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        start_timestamp=start_time,
        end_timestamp=end_time,
    )

    result = _operation_to_json_mapping(operation)

    # Convert expected timestamps to milliseconds
    expected_start_ms = int(start_time.timestamp() * 1000)  # 1672574400000
    expected_end_ms = int(end_time.timestamp() * 1000)  # 1672579800000

    assert result["StartTimestamp"] == expected_start_ms
    assert result["EndTimestamp"] == expected_end_ms
    assert result["Id"] == "op1"
    assert result["Type"] == "STEP"
    assert result["Status"] == "SUCCEEDED"


async def test_operation_json_mapping_with_step_details_timestamp() -> None:
    """Test nested StepDetails.NextAttemptTimestamp JSON conversion."""
    next_attempt_time = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    )
    step_details = StepDetails(
        attempt=2, next_attempt_timestamp=next_attempt_time, result="step_result"
    )

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=step_details,
    )

    result = _operation_to_json_mapping(operation)
    expected_ms = int(next_attempt_time.timestamp() * 1000)  # 1672581600000

    assert result["StepDetails"]["NextAttemptTimestamp"] == expected_ms
    assert result["StepDetails"]["Attempt"] == 2
    assert result["StepDetails"]["Result"] == "step_result"


async def test_operation_json_mapping_with_wait_details_timestamp() -> None:
    """Test nested WaitDetails.ScheduledEndTimestamp JSON conversion."""
    scheduled_end_time = datetime.datetime(
        2023, 1, 1, 15, 0, 0, tzinfo=datetime.timezone.utc
    )
    wait_details = WaitDetails(scheduled_end_timestamp=scheduled_end_time)

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.WAIT,
        status=OperationStatus.PENDING,
        wait_details=wait_details,
    )

    result = _operation_to_json_mapping(operation)
    expected_ms = int(scheduled_end_time.timestamp() * 1000)  # 1672592400000

    assert result["WaitDetails"]["ScheduledEndTimestamp"] == expected_ms


async def test_operation_json_mapping_with_all_timestamps() -> None:
    """Test nested Operation JSON mapping with all timestamp fields."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)
    next_attempt_time = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    )
    scheduled_end_time = datetime.datetime(
        2023, 1, 1, 13, 0, 0, tzinfo=datetime.timezone.utc
    )

    step_details = StepDetails(
        attempt=1, next_attempt_timestamp=next_attempt_time, result="step_result"
    )
    wait_details = WaitDetails(scheduled_end_timestamp=scheduled_end_time)

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        start_timestamp=start_time,
        end_timestamp=end_time,
        step_details=step_details,
        wait_details=wait_details,
    )

    result = _operation_to_json_mapping(operation)

    # Verify all timestamps are converted to milliseconds
    assert result["StartTimestamp"] == int(start_time.timestamp() * 1000)
    assert result["EndTimestamp"] == int(end_time.timestamp() * 1000)
    assert result["StepDetails"]["NextAttemptTimestamp"] == int(
        next_attempt_time.timestamp() * 1000
    )
    assert result["WaitDetails"]["ScheduledEndTimestamp"] == int(
        scheduled_end_time.timestamp() * 1000
    )


async def test_operation_json_mapping_with_none_timestamps() -> None:
    """Test nested Operation JSON mapping handles None timestamps."""
    step_details = StepDetails(
        attempt=1, next_attempt_timestamp=None, result="step_result"
    )
    wait_details = WaitDetails(scheduled_end_timestamp=None)

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        start_timestamp=None,
        end_timestamp=None,
        step_details=step_details,
        wait_details=wait_details,
    )

    result = _operation_to_json_mapping(operation)

    # None timestamps should not be present in the result
    assert "StartTimestamp" not in result
    assert "EndTimestamp" not in result
    assert "NextAttemptTimestamp" not in result["StepDetails"]
    assert result["WaitDetails"] == {}  # Empty dict when no scheduled end timestamp


async def test_operation_from_json_mapping_minimal() -> None:
    """Test nested Operation parsing from a minimal JSON mapping."""
    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
    }

    operation = _operation_from_json_mapping(data)
    assert operation.operation_id == "op1"
    assert operation.operation_type is OperationType.STEP
    assert operation.status is OperationStatus.SUCCEEDED
    assert operation.start_timestamp is None
    assert operation.end_timestamp is None


async def test_operation_from_json_mapping_with_timestamps() -> None:
    """Test nested Operation parsing converts milliseconds to datetimes."""
    start_ms = 1672574400000  # 2023-01-01 12:00:00 UTC
    end_ms = 1672579800000  # 2023-01-01 13:30:00 UTC

    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "StartTimestamp": start_ms,
        "EndTimestamp": end_ms,
    }

    operation = _operation_from_json_mapping(data)

    expected_start = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    )
    expected_end = datetime.datetime(
        2023, 1, 1, 13, 30, 0, tzinfo=datetime.timezone.utc
    )

    assert operation.start_timestamp == expected_start
    assert operation.end_timestamp == expected_end
    assert operation.operation_id == "op1"


@no_type_check
async def test_operation_from_json_mapping_with_step_details_timestamp() -> None:
    """Test nested StepDetails.NextAttemptTimestamp parsing."""
    next_attempt_ms = 1672581600000  # 2023-01-01 14:00:00 UTC

    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "PENDING",
        "StepDetails": {
            "Attempt": 2,
            "NextAttemptTimestamp": next_attempt_ms,
            "Result": "step_result",
        },
    }

    operation = _operation_from_json_mapping(data)
    expected_time = datetime.datetime(
        2023, 1, 1, 14, 0, 0, tzinfo=datetime.timezone.utc
    )

    assert operation.step_details.next_attempt_timestamp == expected_time
    assert operation.step_details.attempt == 2
    assert operation.step_details.result == "step_result"


@no_type_check
async def test_operation_from_json_mapping_with_wait_details_timestamp() -> None:
    """Test nested WaitDetails.ScheduledEndTimestamp parsing."""
    scheduled_end_ms = 1672592400000  # 2023-01-01 17:00:00 UTC

    data = {
        "Id": "op1",
        "Type": "WAIT",
        "Status": "PENDING",
        "WaitDetails": {"ScheduledEndTimestamp": scheduled_end_ms},
    }

    operation = _operation_from_json_mapping(data)
    expected_time = datetime.datetime(
        2023, 1, 1, 17, 0, 0, tzinfo=datetime.timezone.utc
    )

    assert operation.wait_details.scheduled_end_timestamp == expected_time


@no_type_check
async def test_operation_from_json_mapping_with_all_timestamps() -> None:
    """Test nested Operation parsing with all timestamp fields."""
    start_ms = 1672574400000  # 2023-01-01 120:00:00 UTC
    end_ms = 1672578000000  # 2023-01-01 13:00:00 UTC
    next_attempt_ms = 1672581600000  # 2023-01-01 14:00:00 UTC
    scheduled_end_ms = 1672585200000  # 2023-01-01 15:00:00 UTC

    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "PENDING",
        "StartTimestamp": start_ms,
        "EndTimestamp": end_ms,
        "StepDetails": {
            "Attempt": 1,
            "NextAttemptTimestamp": next_attempt_ms,
            "Result": "step_result",
        },
        "WaitDetails": {"ScheduledEndTimestamp": scheduled_end_ms},
    }

    operation = _operation_from_json_mapping(data)

    # Verify all timestamps are converted correctly
    assert operation.start_timestamp == datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    )
    assert operation.end_timestamp == datetime.datetime(
        2023, 1, 1, 13, 0, 0, tzinfo=datetime.timezone.utc
    )
    assert operation.step_details.next_attempt_timestamp == datetime.datetime(
        2023, 1, 1, 14, 0, 0, tzinfo=datetime.timezone.utc
    )
    assert operation.wait_details.scheduled_end_timestamp == datetime.datetime(
        2023, 1, 1, 15, 0, 0, tzinfo=datetime.timezone.utc
    )


@no_type_check
async def test_operation_from_json_mapping_with_none_timestamps() -> None:
    """Test nested Operation parsing handles None timestamps."""
    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "StartTimestamp": None,
        "EndTimestamp": None,
        "StepDetails": {
            "Attempt": 1,
            "NextAttemptTimestamp": None,
            "Result": "step_result",
        },
        "WaitDetails": {"ScheduledEndTimestamp": None},
    }

    operation = _operation_from_json_mapping(data)

    assert operation.start_timestamp is None
    assert operation.end_timestamp is None
    assert operation.step_details.next_attempt_timestamp is None
    assert operation.wait_details.scheduled_end_timestamp is None


@no_type_check
async def test_operation_json_roundtrip() -> None:
    """Test Operation JSON mapping roundtrip preserves all data."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)
    next_attempt_time = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    )
    scheduled_end_time = datetime.datetime(
        2023, 1, 1, 13, 0, 0, tzinfo=datetime.timezone.utc
    )

    error = ErrorObject(
        message="Test error",
        type="TestError",
        data="error_data",
        stack_trace=["line1", "line2"],
    )

    step_details = StepDetails(
        attempt=2,
        next_attempt_timestamp=next_attempt_time,
        result="step_result",
        error=error,
    )

    wait_details = WaitDetails(scheduled_end_timestamp=scheduled_end_time)

    callback_details = CallbackDetails(callback_id="cb123", result="callback_result")

    execution_details = ExecutionDetails(input_payload="exec_payload")

    original = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        parent_id="parent1",
        name="test_step",
        start_timestamp=start_time,
        end_timestamp=end_time,
        sub_type=OperationSubType.STEP,
        execution_details=execution_details,
        step_details=step_details,
        wait_details=wait_details,
        callback_details=callback_details,
    )

    # Convert to JSON dict and back
    json_data = _operation_to_json_mapping(original)
    restored = _operation_from_json_mapping(json_data)

    # Verify all fields are preserved
    assert restored.operation_id == original.operation_id
    assert restored.operation_type == original.operation_type
    assert restored.status == original.status
    assert restored.parent_id == original.parent_id
    assert restored.name == original.name
    assert restored.start_timestamp == original.start_timestamp
    assert restored.end_timestamp == original.end_timestamp
    assert restored.sub_type == original.sub_type
    assert (
        restored.execution_details.input_payload
        == original.execution_details.input_payload
    )
    assert restored.step_details.attempt == original.step_details.attempt
    assert (
        restored.step_details.next_attempt_timestamp
        == original.step_details.next_attempt_timestamp
    )
    assert restored.step_details.result == original.step_details.result
    assert restored.step_details.error.message == original.step_details.error.message
    assert (
        restored.wait_details.scheduled_end_timestamp
        == original.wait_details.scheduled_end_timestamp
    )
    assert (
        restored.callback_details.callback_id == original.callback_details.callback_id
    )


async def test_operation_json_dict_preserves_non_timestamp_fields() -> None:
    """Test nested JSON mapping preserves non-timestamp fields."""
    context_details = ContextDetails(replay_children=True, result="context_result")

    chained_invoke_details = ChainedInvokeDetails(result="invoke_result")

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent1",
        name="test_context",
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
        context_details=context_details,
        chained_invoke_details=chained_invoke_details,
    )

    result = _operation_to_json_mapping(operation)

    # Verify non-timestamp fields are unchanged
    assert result["Id"] == "op1"
    assert result["Type"] == "CONTEXT"
    assert result["Status"] == "SUCCEEDED"
    assert result["ParentId"] == "parent1"
    assert result["Name"] == "test_context"
    assert result["SubType"] == "RunInChildContext"
    assert result["ContextDetails"]["Result"] == "context_result"
    assert result["ChainedInvokeDetails"]["Result"] == "invoke_result"


async def test_timestamp_converter_to_unix_millis_valid_datetime() -> None:
    """Test converting valid datetime to Unix timestamp in milliseconds."""
    # Test epoch
    epoch = datetime.datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert TimestampConverter.to_unix_millis(epoch) == 0

    # Test specific datetime
    dt = datetime.datetime(2024, 1, 1, 12, 30, 45, 123456, tzinfo=timezone.utc)
    expected_ms = int(dt.timestamp() * 1000)
    assert TimestampConverter.to_unix_millis(dt) == expected_ms

    # Test current time
    now = datetime.datetime.now(timezone.utc)
    result = TimestampConverter.to_unix_millis(now)
    assert isinstance(result, int)
    assert result > 0


async def test_timestamp_converter_to_unix_millis_none() -> None:
    """Test converting None to Unix timestamp returns None."""
    assert TimestampConverter.to_unix_millis(None) is None


async def test_timestamp_converter_to_unix_millis_edge_cases() -> None:
    """Test edge cases for datetime to Unix timestamp conversion."""
    # Test year 2038 (Unix timestamp overflow boundary for 32-bit systems)
    dt_2038 = datetime.datetime(2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc)
    result = TimestampConverter.to_unix_millis(dt_2038)
    assert isinstance(result, int)
    assert result > 0

    # Test far future date
    far_future = datetime.datetime(2100, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    result = TimestampConverter.to_unix_millis(far_future)
    assert isinstance(result, int)
    assert result > 0

    # Test microseconds precision (should be truncated in milliseconds)
    dt_with_microseconds = datetime.datetime(
        2024, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc
    )
    result = TimestampConverter.to_unix_millis(dt_with_microseconds)
    # Verify milliseconds precision (microseconds should be truncated)
    expected = int(dt_with_microseconds.timestamp() * 1000)
    assert result == expected


async def test_timestamp_converter_from_unix_millis_valid_timestamp() -> None:
    """Test converting valid Unix timestamp in milliseconds to datetime."""
    # Test epoch
    assert TimestampConverter.from_unix_millis(0) == datetime.datetime(
        1970, 1, 1, tzinfo=timezone.utc
    )

    # Test specific timestamp
    ms = 1704110445123  # 2024-01-01 12:30:45.123 UTC
    result = TimestampConverter.from_unix_millis(ms)
    expected = datetime.datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    assert result == expected
    assert result.tzinfo == timezone.utc

    # Test positive timestamp
    ms = 1609459200000  # 2021-01-01 00:00:00 UTC
    result = TimestampConverter.from_unix_millis(ms)
    assert result == datetime.datetime(2021, 1, 1, tzinfo=timezone.utc)


async def test_timestamp_converter_from_unix_millis_none() -> None:
    """Test converting None timestamp returns None."""
    assert TimestampConverter.from_unix_millis(None) is None


async def test_timestamp_converter_from_unix_millis_zero() -> None:
    """Test converting zero timestamp returns epoch."""
    result = TimestampConverter.from_unix_millis(0)
    assert result == datetime.datetime(1970, 1, 1, tzinfo=timezone.utc)


async def test_timestamp_converter_from_unix_millis_negative() -> None:
    """Test converting negative timestamp (before epoch)."""
    # Test negative timestamp (before 1970)
    ms = -86400000  # 1969-12-31 00:00:00 UTC
    result = TimestampConverter.from_unix_millis(ms)
    expected = datetime.datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    assert result == expected
    assert result.year == 1969


async def test_timestamp_converter_from_unix_millis_large_timestamp() -> None:
    """Test converting large timestamp values."""
    # Test year 2038 boundary
    ms = 2147483647000  # 2038-01-19 03:14:07 UTC
    result = TimestampConverter.from_unix_millis(ms)
    expected = datetime.datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    assert result == expected

    # Test far future
    ms = 4102444800000  # 2100-01-01 00:00:00 UTC
    result = TimestampConverter.from_unix_millis(ms)
    expected = datetime.datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    assert result == expected


@no_type_check
async def test_timestamp_converter_roundtrip_conversion() -> None:
    """Test roundtrip conversion: datetime -> millis -> datetime."""
    original_datetimes = [
        datetime.datetime(1970, 1, 1, tzinfo=timezone.utc),  # Epoch
        datetime.datetime(2024, 1, 1, 12, 30, 45, tzinfo=timezone.utc),  # Specific date
        datetime.datetime(
            2024, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc
        ),  # End of year with millis
        datetime.datetime.now(timezone.utc),  # Current time
        datetime.datetime(2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc),  # 2038 boundary
        datetime.datetime(
            1969, 12, 31, 23, 59, 59, tzinfo=timezone.utc
        ),  # Before epoch
    ]

    for original in original_datetimes:
        # Convert to milliseconds and back
        millis = TimestampConverter.to_unix_millis(original)
        converted_back = TimestampConverter.from_unix_millis(millis)

        # Should be equal within millisecond precision
        # (microseconds may be lost due to integer conversion)
        assert abs((converted_back - original).total_seconds()) < 0.001


async def test_timestamp_converter_roundtrip_with_none() -> None:
    """Test roundtrip conversion with None values."""
    # None -> None -> None
    millis = TimestampConverter.to_unix_millis(None)
    assert millis is None

    converted_back = TimestampConverter.from_unix_millis(millis)
    assert converted_back is None


@no_type_check
async def test_timestamp_converter_precision_handling() -> None:
    """Test precision handling in timestamp conversions."""
    # Test that microseconds are properly handled in millisecond conversion
    dt_with_microseconds = datetime.datetime(
        2024, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc
    )

    # Convert to milliseconds (should truncate microseconds to nearest millisecond)
    millis = TimestampConverter.to_unix_millis(dt_with_microseconds)

    # Convert back
    converted_back = TimestampConverter.from_unix_millis(millis)

    # The difference should be less than 1 millisecond
    time_diff = abs((converted_back - dt_with_microseconds).total_seconds())
    assert time_diff < 0.001


@no_type_check
async def test_timestamp_converter_timezone_handling() -> None:
    """Test that converted datetimes always have UTC timezone."""
    test_timestamps = [0, 1704110445123, -86400000, 2147483647000]

    for ms in test_timestamps:
        result = TimestampConverter.from_unix_millis(ms)
        assert result.tzinfo == timezone.utc


@no_type_check
async def test_timestamp_converter_type_validation() -> None:
    """Test that methods return correct types."""
    # Test to_unix_millis return type
    dt = datetime.datetime(2024, 1, 1, tzinfo=timezone.utc)
    result = TimestampConverter.to_unix_millis(dt)
    assert isinstance(result, int)

    result_none = TimestampConverter.to_unix_millis(None)
    assert result_none is None

    # Test from_unix_millis return type
    ms = 1704110445123
    result = TimestampConverter.from_unix_millis(ms)
    assert isinstance(result, datetime.datetime)

    result_none = TimestampConverter.from_unix_millis(None)
    assert result_none is None


async def test_timestamp_converter_static_methods() -> None:
    """Test that TimestampConverter methods are static and can be called without instance."""
    # Should be able to call without creating instance
    dt = datetime.datetime(2024, 1, 1, tzinfo=timezone.utc)

    # Call as static methods
    millis = TimestampConverter.to_unix_millis(dt)
    converted_back = TimestampConverter.from_unix_millis(millis)

    assert isinstance(millis, int)
    assert isinstance(converted_back, datetime.datetime)
    assert converted_back.tzinfo == timezone.utc


@no_type_check
async def test_timestamp_converter_millisecond_boundaries() -> None:
    """Test conversion at millisecond boundaries."""
    # Test exact millisecond values
    test_cases = [
        (datetime.datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc), 1704067200000),
        (
            datetime.datetime(2024, 1, 1, 0, 0, 0, 500000, tzinfo=timezone.utc),
            1704067200500,
        ),  # 500ms
        (
            datetime.datetime(2024, 1, 1, 0, 0, 0, 999000, tzinfo=timezone.utc),
            1704067200999,
        ),  # 999ms
    ]

    for dt, expected_ms in test_cases:
        result_ms = TimestampConverter.to_unix_millis(dt)
        assert result_ms == expected_ms

        # Convert back and verify
        result_dt = TimestampConverter.from_unix_millis(result_ms)
        # Should be equal within millisecond precision
        assert abs((result_dt - dt).total_seconds()) < 0.001


def test_operation_models_round_trip_custom_subtype_strings() -> None:
    operation = Operation(
        operation_id="custom",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        sub_type="AcmeStep",
    )
    update = OperationUpdate(
        operation_id="custom",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        sub_type="AcmeStep",
    )

    assert Operation.from_dict(operation.to_dict()).sub_type == "AcmeStep"
    assert OperationUpdate.from_dict(update.to_dict()).sub_type == "AcmeStep"


def test_operation_identifier_requires_operation_id_for_non_execution_operations() -> (
    None
):
    identifier = OperationIdentifier(None, sub_type=OperationSubType.STEP)

    with pytest.raises(ValueError, match="operation_id is required"):
        identifier.require_operation_id()


def test_operation_identifier_create_execution_op_builds_root_identifier() -> None:
    identifier = OperationIdentifier.create_execution_op()

    assert identifier.operation_id is None
    assert identifier.sub_type is OperationSubType.EXECUTION
    assert identifier.parent_id is None
    assert identifier.name is None
