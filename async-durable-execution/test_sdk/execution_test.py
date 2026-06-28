"""Tests for execution."""

import asyncio
import datetime
import json
import logging
import os
import time
from datetime import timedelta
from functools import partial
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import pytest

from async_durable_execution.context import get_current_context
from async_durable_execution import (
    DurableContext,
    create_callback,
    durable_callable,
    invoke,
    run_in_child_context,
    step,
    wait,
    wait_for_callback,
)
from async_durable_execution.exceptions import (
    BotoClientError,
    CheckpointError,
    CheckpointErrorCategory,
    DurableApiErrorCategory,
    ExecutionError,
    GetExecutionStateError,
    InvocationError,
    SuspendExecution,
)
from async_durable_execution.execution import (
    DurableExecutionInvocationInput,
    InitialExecutionState,
    InvocationStatus,
    _bind_service_client_to_handler,
    durable_execution,
)
from async_durable_execution.primitive.step import StepSemantics

from async_durable_execution.models import (
    CallbackDetails,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ContextDetails,
    DurableExecutionInvocationOutput,
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StateOutput,
    StepDetails,
    WaitDetails,
)
from async_durable_execution.client import DurableServiceClient
from .test_helpers import operation_id_sequence


LARGE_RESULT = "large_success" * 1024 * 1024
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


async def run_handler(handler, event, lambda_context, service_client=None):
    if service_client is not None:
        handler = _bind_service_client_to_handler(handler, service_client)
    if isinstance(event, DurableExecutionInvocationInput):
        event = event.to_json_dict()
    return await asyncio.to_thread(handler, event, lambda_context)


async def test_durable_execution_invocation_input_from_dict():
    """Test that DurableExecutionInvocationInput.from_dict works correctly"""
    input_dict = {
        "DurableExecutionArn": "9692ca80-399d-4f52-8d0a-41acc9cd0492/9692ca80-399d-4f52-8d0a-41acc9cd0492",
        "CheckpointToken": "9692ca80-399d-4f52-8d0a-41acc9cd0492",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "9692ca80-399d-4f52-8d0a-41acc9cd0492",
                    "ParentId": None,
                    "Name": None,
                    "Type": "EXECUTION",
                    "StartTimestamp": 1751414445.691,
                    "Status": "STARTED",
                    "ExecutionDetails": {"inputPayload": "{}"},
                }
            ],
            "NextMarker": "",
        },
    }

    result = DurableExecutionInvocationInput.from_dict(input_dict)

    assert (
        result.durable_execution_arn
        == "9692ca80-399d-4f52-8d0a-41acc9cd0492/9692ca80-399d-4f52-8d0a-41acc9cd0492"
    )
    assert result.checkpoint_token == "9692ca80-399d-4f52-8d0a-41acc9cd0492"  # noqa: S105
    assert isinstance(result.initial_execution_state, InitialExecutionState)
    assert len(result.initial_execution_state.operations) == 1
    assert not result.initial_execution_state.next_marker
    assert (
        result.initial_execution_state.operations[0].operation_id
        == "9692ca80-399d-4f52-8d0a-41acc9cd0492"
    )


async def test_initial_execution_state_from_dict_minimal():
    """Test that InitialExecutionState.from_dict works correctly"""
    input_dict = {
        "Operations": [
            {
                "Id": "9692ca80-399d-4f52-8d0a-41acc9cd0492",
                "Type": "EXECUTION",
                "Status": "STARTED",
            }
        ],
        "NextMarker": "test-marker",
    }

    result = InitialExecutionState.from_dict(input_dict)

    assert len(result.operations) == 1
    assert result.next_marker == "test-marker"
    assert result.operations[0].operation_id == "9692ca80-399d-4f52-8d0a-41acc9cd0492"


async def test_initial_execution_state_from_dict_no_operations():
    """Test that InitialExecutionState.from_dict handles missing Operations key."""
    input_dict = {"NextMarker": "test-marker"}

    result = InitialExecutionState.from_dict(input_dict)

    assert len(result.operations) == 0
    assert result.next_marker == "test-marker"


async def test_initial_execution_state_from_dict_empty_operations():
    """Test that InitialExecutionState.from_dict handles empty Operations list."""
    input_dict = {"Operations": [], "NextMarker": "test-marker"}

    result = InitialExecutionState.from_dict(input_dict)

    assert len(result.operations) == 0
    assert result.next_marker == "test-marker"


async def test_initial_execution_state_to_dict():
    """Test InitialExecutionState.to_dict method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="test_payload"),
    )

    state = InitialExecutionState(operations=[operation], next_marker="marker123")

    result = state.to_dict()
    expected = {"Operations": [operation.to_dict()], "NextMarker": "marker123"}

    assert result == expected


async def test_initial_execution_state_to_dict_empty():
    """Test InitialExecutionState.to_dict with empty operations."""
    state = InitialExecutionState(operations=[], next_marker="")

    result = state.to_dict()
    expected = {"Operations": [], "NextMarker": ""}

    assert result == expected


async def test_durable_execution_invocation_input_to_dict():
    """Test DurableExecutionInvocationInput.to_dict method."""
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )

    initial_state = InitialExecutionState(
        operations=[operation], next_marker="test_marker"
    )

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution/exec1",
        "CheckpointToken": "token123",
        "InitialExecutionState": initial_state.to_dict(),
    }

    assert result == expected


async def test_durable_execution_invocation_input_to_dict_not_local():
    initial_state = InitialExecutionState(operations=[], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution/exec1",
        "CheckpointToken": "token123",
        "InitialExecutionState": initial_state.to_dict(),
    }

    assert result == expected


async def test_operation_to_dict_complete():
    """Test Operation.to_dict with all fields populated."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent1",
        name="test_step",
        start_timestamp=start_time,
        end_timestamp=end_time,
        execution_details=ExecutionDetails(input_payload="exec_payload"),
    )

    result = operation.to_dict()
    expected = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "ParentId": "parent1",
        "Name": "test_step",
        "StartTimestamp": start_time,
        "EndTimestamp": end_time,
        "ExecutionDetails": {"InputPayload": "exec_payload"},
    }

    assert result == expected


async def test_operation_to_dict_minimal():
    """Test Operation.to_dict with minimal required fields."""
    operation = Operation(
        operation_id="minimal_op",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )

    result = operation.to_dict()
    expected = {
        "Id": "minimal_op",
        "Type": "EXECUTION",
        "Status": "STARTED",
    }

    assert result == expected


async def test_durable_execution_invocation_output_from_dict():
    """Test DurableExecutionInvocationOutput.from_dict method."""
    data = {
        "Status": "SUCCEEDED",
        "Result": '{"key": "value"}',
        "Error": {"ErrorType": "ValueError", "ErrorMessage": "Test error"},
    }

    result = DurableExecutionInvocationOutput.from_dict(data)

    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == '{"key": "value"}'
    assert result.error is not None
    assert result.error.type == "ValueError"
    assert result.error.message == "Test error"


async def test_durable_execution_invocation_output_from_dict_no_error():
    """Test DurableExecutionInvocationOutput.from_dict without error."""
    data = {"Status": "SUCCEEDED", "Result": '{"key": "value"}'}

    result = DurableExecutionInvocationOutput.from_dict(data)

    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == '{"key": "value"}'
    assert result.error is None


async def test_durable_execution_invocation_output_from_dict_no_result():
    """Test DurableExecutionInvocationOutput.from_dict without result."""
    data = {"Status": "PENDING"}

    result = DurableExecutionInvocationOutput.from_dict(data)

    assert result.status == InvocationStatus.PENDING
    assert result.result is None
    assert result.error is None


async def test_durable_execution_client_selection_env_normal_result():
    """Test durable_execution selects correct client from environment."""
    mock_lambda_api_client = Mock()
    with (
        patch(
            "async_durable_execution.execution.ThreadedSyncLambdaClient"
        ) as mock_lambda_client,
        patch(
            "async_durable_execution.execution.create_default_client",
            return_value=mock_lambda_api_client,
        ),
    ):
        mock_client = Mock(spec=DurableServiceClient)
        mock_lambda_client.return_value = mock_client

        # Mock successful checkpoint
        mock_output = CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )
        mock_client.checkpoint.return_value = mock_output

        @durable_execution
        async def test_handler(event: Any) -> dict:
            return {"result": "success"}

        # Create regular event with LocalRunner=False
        event = {
            "DurableExecutionArn": "arn:test:execution/exec1",
            "CheckpointToken": "token123",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "exec1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
        lambda_context.invoked_function_arn = None
        lambda_context.tenant_id = None

        result = await run_handler(test_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '{"result": "success"}'
        mock_lambda_client.assert_called_once_with(client=mock_lambda_api_client)
        mock_client.checkpoint.assert_not_called()


async def test_durable_execution_defers_default_client_until_invocation():
    """Decorating a handler must not require AWS environment configuration."""
    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_lambda_client:

        @durable_execution
        async def test_handler(event: Any) -> dict:
            return {"result": "success"}

        assert callable(test_handler)
        mock_lambda_client.assert_not_called()


async def test_durable_execution_client_selection_env_large_result():
    """Test durable_execution selects correct client from environment."""
    mock_lambda_api_client = Mock()
    with (
        patch(
            "async_durable_execution.execution.ThreadedSyncLambdaClient"
        ) as mock_lambda_client,
        patch(
            "async_durable_execution.execution.create_default_client",
            return_value=mock_lambda_api_client,
        ),
    ):
        mock_client = Mock(spec=DurableServiceClient)
        mock_lambda_client.return_value = mock_client

        # Mock successful checkpoint
        mock_output = CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )
        mock_client.checkpoint.return_value = mock_output

        @durable_execution
        async def test_handler(event: Any) -> dict:
            return {"result": LARGE_RESULT}

        # Create regular event with LocalRunner=False
        event = {
            "DurableExecutionArn": "arn:test:execution/exec1",
            "CheckpointToken": "token123",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "exec1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
        lambda_context.invoked_function_arn = None
        lambda_context.tenant_id = None

        result = await run_handler(test_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert not result["Result"]
        mock_lambda_client.assert_called_once_with(client=mock_lambda_api_client)
        mock_client.checkpoint.assert_called_once()


async def test_durable_execution_with_injected_client_success_normal_result():
    """Test durable_execution uses injected DurableServiceClient for successful execution."""
    mock_client = Mock(spec=DurableServiceClient)

    # Mock successful checkpoint
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    @durable_execution
    async def test_handler(event: Any) -> dict:
        return {"result": "success"}

    # Create execution input with injected client
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload='{"input": "test"}'),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert result["Result"] == '{"result": "success"}'
    mock_client.checkpoint.assert_not_called()


async def test_durable_execution_with_injected_client_success_large_result():
    """Test durable_execution uses injected DurableServiceClient for successful execution."""
    mock_client = Mock(spec=DurableServiceClient)

    # Mock successful checkpoint
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    @durable_execution
    async def test_handler(event: Any) -> dict:
        return {"result": LARGE_RESULT}

    # Create execution input with injected client
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload='{"input": "test"}'),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert not result.get("Result")
    mock_client.checkpoint.assert_called_once()

    # Verify the checkpoint call was for execution success
    call_args = mock_client.checkpoint.call_args
    updates = call_args[1]["updates"]
    assert len(updates) == 1
    assert updates[0].operation_type == OperationType.EXECUTION
    assert updates[0].action.value == "SUCCEED"
    assert json.loads(updates[0].payload) == {"result": LARGE_RESULT}


async def test_durable_execution_with_injected_client_failure():
    """Test durable_execution uses injected DurableServiceClient for failed execution."""
    mock_client = Mock(spec=DurableServiceClient)

    # Mock successful checkpoint for failure
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    @durable_execution
    async def test_handler(event: Any) -> dict:
        msg = "Test error"
        raise ValueError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )

    # small error, should not call checkpoint
    assert result["Status"] == InvocationStatus.FAILED.value
    assert result["Error"] == {"ErrorMessage": "Test error", "ErrorType": "ValueError"}

    assert not mock_client.checkpoint.called


async def test_durable_execution_with_large_error_payload():
    """Test that large error payloads trigger checkpoint."""
    mock_client = Mock(spec=DurableServiceClient)
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    @durable_execution
    async def test_handler(event: Any) -> dict:
        raise ValueError(LARGE_RESULT)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )

    assert result["Status"] == InvocationStatus.FAILED.value
    assert "Error" not in result
    mock_client.checkpoint.assert_called_once()

    call_args = mock_client.checkpoint.call_args
    updates = call_args[1]["updates"]
    assert len(updates) == 1
    assert updates[0].operation_type == OperationType.EXECUTION
    assert updates[0].action.value == "FAIL"
    assert updates[0].error.message == LARGE_RESULT


async def test_durable_execution_fatal_error_handling():
    """Test durable_execution handles FatalError correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        msg = "Retriable invocation error occurred"
        raise InvocationError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # expect raise; backend will retry
    with pytest.raises(InvocationError, match="Retriable invocation error occurred"):
        await run_handler(
            test_handler, invocation_input, lambda_context, service_client=mock_client
        )


async def test_durable_execution_execution_error_handling():
    """Test durable_execution handles InvocationError correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        msg = "Retriable invocation error occurred"
        raise ExecutionError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # ExecutionError should return FAILED status with ErrorObject in result field
    result = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert result["Status"] == InvocationStatus.FAILED.value

    # Parse the ErrorObject from the result field
    error_data = result["Error"]

    assert error_data["ErrorMessage"] == "Retriable invocation error occurred"
    assert error_data["ErrorType"] == "ExecutionError"


async def test_durable_execution_client_selection_default():
    """Test durable_execution selects correct client using default initialization."""
    mock_lambda_api_client = Mock()
    with (
        patch(
            "async_durable_execution.execution.ThreadedSyncLambdaClient"
        ) as mock_lambda_client,
        patch(
            "async_durable_execution.execution.create_default_client",
            return_value=mock_lambda_api_client,
        ),
    ):
        mock_client = Mock(spec=DurableServiceClient)
        mock_lambda_client.return_value = mock_client

        # Mock successful checkpoint
        mock_output = CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )
        mock_client.checkpoint.return_value = mock_output

        @durable_execution
        async def test_handler(event: Any) -> dict:
            return {"result": "success"}

        # Create a regular event dict instead of a durable invocation input object
        event = {
            "DurableExecutionArn": "arn:test:execution/exec1",
            "CheckpointToken": "token123",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "exec1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
        lambda_context.invoked_function_arn = None
        lambda_context.tenant_id = None

        result = await run_handler(test_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        mock_lambda_client.assert_called_once_with(client=mock_lambda_api_client)


async def test_durable_execution_default_async_client_is_invocation_scoped():
    """Test default async clients are not cached across asyncio.run event loops."""

    class StubAsyncLambdaApiClient:
        async def checkpoint_durable_execution(self, **_kwargs):
            return {}

        async def get_durable_execution_state(self, **_kwargs):
            return {}

    class ClosingServiceClient:
        def __init__(self, checkpoint_token: str) -> None:
            self.checkpoint_token = checkpoint_token
            self.closed = False

        async def checkpoint(
            self,
            _durable_execution_arn: str,
            _checkpoint_token: str,
            _updates: list[OperationUpdate],
            _client_token: str | None,
        ) -> CheckpointOutput:
            return CheckpointOutput(
                checkpoint_token=self.checkpoint_token,
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        async def get_execution_state(
            self,
            _durable_execution_arn: str,
            _checkpoint_token: str,
            _next_marker: str,
            _max_items: int = 1000,
        ) -> StateOutput:
            return StateOutput(operations=[], next_marker="")

        async def aclose(self) -> None:
            self.closed = True

    lambda_api_client_1 = StubAsyncLambdaApiClient()
    lambda_api_client_2 = StubAsyncLambdaApiClient()
    service_client_1 = ClosingServiceClient("new_token_1")  # noqa: S106
    service_client_2 = ClosingServiceClient("new_token_2")  # noqa: S106

    with (
        patch(
            "async_durable_execution.execution.AsyncLambdaClient",
            side_effect=[service_client_1, service_client_2],
        ) as mock_async_lambda_client,
        patch(
            "async_durable_execution.execution.create_default_client",
            side_effect=[lambda_api_client_1, lambda_api_client_2],
        ) as mock_create_default_client,
    ):

        @durable_execution
        async def test_handler(event: Any) -> dict:
            return {"result": "success"}

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
        lambda_context.invoked_function_arn = None
        lambda_context.tenant_id = None

        event = {
            "DurableExecutionArn": "arn:test:execution/exec1",
            "CheckpointToken": "token123",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "exec1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
        }

        result_1 = await test_handler._async_handler(event, lambda_context)  # noqa: SLF001
        result_2 = await test_handler._async_handler(event, lambda_context)  # noqa: SLF001

        assert result_1["Status"] == InvocationStatus.SUCCEEDED.value
        assert result_2["Status"] == InvocationStatus.SUCCEEDED.value
        assert mock_create_default_client.call_count == 2
        assert mock_async_lambda_client.call_count == 2
        assert service_client_1.closed
        assert service_client_2.closed


async def test_durable_handler_empty_input_payload():
    """Test durable_handler handles empty input payload correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        return {"result": "success"}

    # Create execution input with empty input payload
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload=""),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert result["Result"] == '{"result": "success"}'


async def test_durable_handler_whitespace_input_payload():
    """Test durable_handler handles whitespace-only input payload correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        return {"result": "success"}

    # Create execution input with whitespace-only input payload
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="   "),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert result["Result"] == '{"result": "success"}'


async def test_durable_handler_invalid_json_input_payload():
    """Test invalid JSON input payloads fail the invocation with a decode error."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        return {"result": "success"}

    # Create execution input with invalid JSON
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{invalid json}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )

    assert result["Status"] == InvocationStatus.FAILED.value
    assert result["Error"]["ErrorType"] == "JSONDecodeError"


async def test_durable_handler_background_thread_failure():
    """Test durable_handler returns FAILED when checkpointing fails."""
    mock_client = Mock(spec=DurableServiceClient)

    # Make checkpoint_batches_forever raise an error immediately
    def failing_checkpoint(*args, **kwargs):
        msg = "Background checkpoint failed"
        raise RuntimeError(msg)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        async def step_result() -> str:
            return "step_result"

        # Call a checkpoint operation so background thread error can propagate
        await step(step_result)
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail
    mock_client.checkpoint.side_effect = failing_checkpoint

    response = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert response["Status"] == InvocationStatus.FAILED.value
    assert response["Error"]["ErrorMessage"] == "Background checkpoint failed"
    assert response["Error"]["ErrorType"] == "RuntimeError"


async def test_durable_execution_suspend_execution():
    """Test durable_execution handles SuspendExecution correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        msg = "Suspending for callback"
        raise SuspendExecution(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )

    assert result["Status"] == InvocationStatus.PENDING.value
    assert "Result" not in result
    assert "Error" not in result


async def test_durable_execution_checkpoint_error_in_background_thread():
    """Test durable_execution propagates CheckpointError from background thread.

    This test simulates a CheckpointError occurring in the background checkpointing
    thread, which should interrupt user code execution and propagate the error.
    """
    mock_client = Mock(spec=DurableServiceClient)

    # Make the background checkpoint thread fail immediately
    def failing_checkpoint(*args, **kwargs):
        msg = "Background checkpoint failed"
        raise CheckpointError(msg, error_category=CheckpointErrorCategory.EXECUTION)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        async def step_result() -> str:
            return "step_result"

        # Call a checkpoint operation so background thread error can propagate
        await step(step_result)
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail with CheckpointError
    mock_client.checkpoint.side_effect = failing_checkpoint

    response = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert response["Status"] == InvocationStatus.FAILED.value
    assert response["Error"]["ErrorType"] == "CheckpointError"


async def test_durable_execution_checkpoint_execution_error_stops_background():
    """Test that CheckpointError handler stops background checkpointing.

    When user code raises CheckpointError, the handler should stop the background
    thread before re-raising to terminate the Lambda.
    """
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        # Directly raise CheckpointError to simulate checkpoint failure
        msg = "Checkpoint system failed"
        raise CheckpointError(msg, CheckpointErrorCategory.EXECUTION)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    response = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert response["Status"] == InvocationStatus.FAILED.value
    assert response["Error"]["ErrorType"] == "CheckpointError"


async def test_durable_execution_checkpoint_invocation_error_retries():
    """Test that CheckpointError with INVOCATION category re-raises to trigger Lambda retry."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        # Directly raise CheckpointError to simulate checkpoint failure
        msg = "Checkpoint system failed"
        raise CheckpointError(msg, CheckpointErrorCategory.INVOCATION)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    with pytest.raises(CheckpointError, match="Checkpoint system failed"):
        await run_handler(
            test_handler, invocation_input, lambda_context, service_client=mock_client
        )


async def test_durable_execution_background_thread_execution_error_returns_failed():
    """Test that background thread Execution errors return FAILED (permanent, no retry)."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_checkpoint(*args, **kwargs):
        msg = "Background checkpoint failed"
        raise CheckpointError(msg, error_category=CheckpointErrorCategory.EXECUTION)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        async def step_result() -> str:
            return "step_result"

        await step(step_result)
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_checkpoint

    response = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert response["Status"] == InvocationStatus.FAILED.value
    assert response["Error"]["ErrorType"] == "CheckpointError"


async def test_durable_execution_background_thread_invocation_error_retries():
    """Test that background thread Invocation errors re-raise to trigger Lambda retry."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_checkpoint(*args, **kwargs):
        msg = "Background checkpoint failed"
        raise CheckpointError(msg, error_category=CheckpointErrorCategory.INVOCATION)

    @durable_execution
    async def test_handler(event: Any) -> dict:
        async def step_result() -> str:
            return "step_result"

        await step(step_result)
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_checkpoint

    with pytest.raises(CheckpointError, match="Background checkpoint failed"):
        await run_handler(
            test_handler, invocation_input, lambda_context, service_client=mock_client
        )


async def test_durable_execution_final_success_checkpoint_execution_error_returns_failed():
    """Test that execution errors on final success checkpoint return FAILED (permanent, no retry)."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_final_checkpoint(*args, **kwargs):
        raise CheckpointError(  # noqa TRY003
            "Final checkpoint failed",  # noqa EM101
            error_category=CheckpointErrorCategory.EXECUTION,
        )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        # Return large result to trigger final checkpoint (>6MB)
        return {"result": "x" * (7 * 1024 * 1024)}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )
    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_final_checkpoint

    response = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert response["Status"] == InvocationStatus.FAILED.value
    assert response["Error"]["ErrorType"] == "CheckpointError"


async def test_durable_execution_final_success_checkpoint_invocation_error_retries():
    """Test that invocation errors on final success checkpoint re-raise to trigger Lambda retry."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_final_checkpoint(*args, **kwargs):
        raise CheckpointError(  # noqa TRY003
            "Final checkpoint failed",  # noqa EM101
            error_category=CheckpointErrorCategory.INVOCATION,
        )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        # Return large result to trigger final checkpoint (>6MB)
        return {"result": "x" * (7 * 1024 * 1024)}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_final_checkpoint

    with pytest.raises(CheckpointError, match="Final checkpoint failed"):
        await run_handler(
            test_handler, invocation_input, lambda_context, service_client=mock_client
        )


async def test_durable_execution_final_failure_checkpoint_execution_error_returns_failed():
    """Test that execution errors on final failure checkpoint return FAILED (permanent, no retry)."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_final_checkpoint(*args, **kwargs):
        raise CheckpointError(  # noqa TRY003
            "Final checkpoint failed",  # noqa EM101
            error_category=CheckpointErrorCategory.EXECUTION,
        )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        # Raise error with large message to trigger final checkpoint (>6MB)
        msg = "x" * (7 * 1024 * 1024)
        raise ValueError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_final_checkpoint

    response = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert response["Status"] == InvocationStatus.FAILED.value
    assert response["Error"]["ErrorType"] == "CheckpointError"


async def test_durable_execution_final_failure_checkpoint_invocation_error_retries():
    """Test that invocation errors on final failure checkpoint re-raise to trigger Lambda retry."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_final_checkpoint(*args, **kwargs):
        raise CheckpointError(  # noqa TRY003
            "Final checkpoint failed",  # noqa EM101
            error_category=CheckpointErrorCategory.INVOCATION,
        )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        # Raise error with large message to trigger final checkpoint (>6MB)
        msg = "x" * (7 * 1024 * 1024)
        raise ValueError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_final_checkpoint

    with pytest.raises(CheckpointError, match="Final checkpoint failed"):
        await run_handler(
            test_handler, invocation_input, lambda_context, service_client=mock_client
        )


async def test_durable_handler_background_thread_failure_on_succeed_checkpoint():
    """Test durable_handler handles background thread failure on SUCCEED checkpoint.

    This test allows the START checkpoint to succeed but fails on the SUCCEED checkpoint,
    which is the second checkpoint that occurs at the end of the step operation.
    """
    mock_client = Mock(spec=DurableServiceClient)

    def selective_failing_checkpoint(
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # Check if any update is a SUCCEED action for a STEP operation
        # The batch will contain both START and SUCCEED updates
        for update in updates:
            if (
                update.operation_type is OperationType.STEP
                and update.action is OperationAction.SUCCEED
            ):
                msg = "Background checkpoint failed on SUCCEED"
                raise RuntimeError(msg)

        # Allow other checkpoints to succeed
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        async def step_result() -> str:
            return "step_result"

        # Call a step operation which will trigger START and SUCCEED checkpoints
        await step(step_result)
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail selectively
    mock_client.checkpoint.side_effect = selective_failing_checkpoint

    response = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert response["Status"] == InvocationStatus.FAILED.value
    assert (
        response["Error"]["ErrorMessage"] == "Background checkpoint failed on SUCCEED"
    )
    assert response["Error"]["ErrorType"] == "RuntimeError"

    # Verify that checkpoint was called exactly once with a batch containing both updates:
    # The batch contains: STEP START and STEP SUCCEED (fails on SUCCEED)
    assert mock_client.checkpoint.call_count == 1

    # Verify the checkpoint call contained both START and SUCCEED updates
    call_args = mock_client.checkpoint.call_args
    updates = call_args[1]["updates"]
    assert len(updates) == 2

    # First update should be STEP START
    start_update = updates[0]
    assert start_update.operation_type is OperationType.STEP
    assert start_update.action is OperationAction.START

    # Second update should be STEP SUCCEED (the one that failed)
    succeed_update = updates[1]
    assert succeed_update.operation_type is OperationType.STEP
    assert succeed_update.action is OperationAction.SUCCEED


async def test_durable_handler_background_thread_failure_on_start_checkpoint():
    """Test durable_handler handles background thread failure on START checkpoint.

    This test fails on the START checkpoint, which should prevent the step from executing
    and therefore no SUCCEED checkpoint should be attempted.
    """
    mock_client = Mock(spec=DurableServiceClient)

    def selective_failing_checkpoint(
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # Check if any update is a START action for a STEP operation
        for update in updates:
            if (
                update.operation_type is OperationType.STEP
                and update.action is OperationAction.START
            ):
                msg = "Background checkpoint failed on START"
                raise RuntimeError(msg)

        # Allow other checkpoints to succeed
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        async def first_step_result() -> str:
            return "first_step_result"

        async def second_step_result() -> str:
            return "second_step_result"

        # First step with AT_MOST_ONCE_PER_RETRY (synchronous START checkpoint)
        # This should fail on START checkpoint and prevent execution
        await step(
            first_step_result,
            step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        )

        # Second step should never be reached if first step's START checkpoint fails
        await step(second_step_result)
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail selectively
    mock_client.checkpoint.side_effect = selective_failing_checkpoint

    response = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert response["Status"] == InvocationStatus.FAILED.value
    assert response["Error"]["ErrorMessage"] == "Background checkpoint failed on START"
    assert response["Error"]["ErrorType"] == "RuntimeError"

    # Verify that checkpoint was called exactly once with only the START update:
    # With AT_MOST_ONCE_PER_RETRY, START checkpoint is synchronous and blocks execution
    assert mock_client.checkpoint.call_count == 1

    # Verify the checkpoint call contained only the first step's START update
    call_args = mock_client.checkpoint.call_args
    updates = call_args[1]["updates"]
    assert len(updates) == 1

    # The single update should be STEP START (the one that fails)
    start_update = updates[0]
    assert start_update.operation_type is OperationType.STEP
    assert start_update.action is OperationAction.START

    # Verify no SUCCEED update was created (step execution was blocked)
    succeed_updates = [u for u in updates if u.action is OperationAction.SUCCEED]
    assert len(succeed_updates) == 0


async def test_durable_handler_background_thread_failure_on_large_result_checkpoint():
    """Test durable_handler handles background thread failure on large result checkpoint.

    This test verifies that when a large result checkpoint fails due to background thread
    error, the original error is properly unwrapped and raised.
    """
    mock_client = Mock(spec=DurableServiceClient)

    def failing_checkpoint(
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # Check if any update is a SUCCEED action for EXECUTION operation (large result)
        for update in updates:
            if (
                update.operation_type is OperationType.EXECUTION
                and update.action is OperationAction.SUCCEED
            ):
                msg = "Background checkpoint failed on large result"
                raise RuntimeError(msg)

        # Allow other checkpoints to succeed
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    @durable_execution
    async def test_handler(event: Any) -> str:
        # Return a large result that will trigger checkpoint
        return LARGE_RESULT

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail on large result
    mock_client.checkpoint.side_effect = failing_checkpoint

    response = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert response["Status"] == InvocationStatus.FAILED.value
    assert (
        response["Error"]["ErrorMessage"]
        == "Background checkpoint failed on large result"
    )
    assert response["Error"]["ErrorType"] == "RuntimeError"


async def test_durable_handler_background_thread_failure_on_error_checkpoint():
    """Test durable_handler handles background thread failure on error checkpoint.

    This test verifies that when an error checkpoint fails due to background thread
    error, the original checkpoint error is properly unwrapped and raised (not the
    user error that triggered the checkpoint).
    """
    mock_client = Mock(spec=DurableServiceClient)

    def failing_checkpoint(
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # Check if any update is a FAIL action for EXECUTION operation (error handling)
        for update in updates:
            if (
                update.operation_type is OperationType.EXECUTION
                and update.action is OperationAction.FAIL
            ):
                msg = "Background checkpoint failed on error handling"
                raise RuntimeError(msg)

        # Allow other checkpoints to succeed
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    @durable_execution
    async def test_handler(event: Any) -> str:
        # Raise an error that will trigger error checkpoint
        msg = "User function error"
        raise ValueError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail on error handling
    mock_client.checkpoint.side_effect = failing_checkpoint

    # Verify that errors are not raised, but returned because response is small
    resp = await run_handler(
        test_handler, invocation_input, lambda_context, service_client=mock_client
    )
    assert resp["Error"]["ErrorMessage"] == "User function error"
    assert resp["Error"]["ErrorType"] == "ValueError"
    assert resp["Status"] == InvocationStatus.FAILED.value


async def test_durable_execution_logs_checkpoint_error_extras_from_background_thread():
    """Test that CheckpointError extras are logged when raised from background thread."""
    mock_client = Mock(spec=DurableServiceClient)
    mock_logger = Mock()

    error_obj = {"Code": "TestError", "Message": "Test checkpoint error"}
    metadata_obj = {"RequestId": "test-request-id"}

    def failing_checkpoint(*args, **kwargs):
        raise CheckpointError(  # noqa TRY003
            "Checkpoint failed",  # noqa EM101
            error_category=CheckpointErrorCategory.EXECUTION,
            error=error_obj,
            response_metadata=metadata_obj,  # EM101
        )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        async def step_result() -> str:
            return "step_result"

        await step(step_result)
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_checkpoint

    with patch("async_durable_execution.execution.logger", mock_logger):
        response = await run_handler(
            test_handler, invocation_input, lambda_context, service_client=mock_client
        )
        assert response["Status"] == InvocationStatus.FAILED.value
        assert response["Error"]["ErrorType"] == "CheckpointError"

    mock_logger.exception.assert_called()
    # Background checkpoint failures are surfaced through the durable handler.
    first_call = mock_logger.exception.call_args_list[0]
    assert "Checkpoint system failed" in first_call[0][0]
    assert first_call[1]["extra"]["Error"] == error_obj
    assert first_call[1]["extra"]["ResponseMetadata"] == metadata_obj


async def test_durable_execution_logs_boto_client_error_extras_from_background_thread():
    """Test that BotoClientError extras are logged when raised from background thread."""

    mock_client = Mock(spec=DurableServiceClient)
    mock_logger = Mock()

    error_obj = {"Code": "ServiceError", "Message": "Boto3 service error"}
    metadata_obj = {"RequestId": "boto-request-id"}

    def failing_checkpoint(*args, **kwargs):
        raise BotoClientError(  # noqa TRY003
            "Boto3 error",  # noqa EM101
            error=error_obj,
            response_metadata=metadata_obj,  # EM101
        )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        async def step_result() -> str:
            return "step_result"

        await step(step_result)
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_checkpoint

    with patch("async_durable_execution.execution.logger", mock_logger):
        with pytest.raises(BotoClientError):
            await run_handler(
                test_handler,
                invocation_input,
                lambda_context,
                service_client=mock_client,
            )

    mock_logger.exception.assert_called_once()
    call_args = mock_logger.exception.call_args
    assert (
        "Invocation error. Must terminate." in call_args[0][0]
        or "Non-retryable Durable API error." in call_args[0][0]
    )
    assert "extra" not in call_args[1]


async def test_durable_execution_logs_checkpoint_error_extras_from_user_code():
    """Test that CheckpointError extras are logged when raised directly from user code."""
    mock_client = Mock(spec=DurableServiceClient)
    mock_logger = Mock()

    error_obj = {
        "Code": "UserCheckpointError",
        "Message": "User raised checkpoint error",
    }
    metadata_obj = {"RequestId": "user-request-id"}

    @durable_execution
    async def test_handler(event: Any) -> dict:
        raise CheckpointError(  # noqa TRY003
            "User checkpoint error",  # noqa EM101
            error_category=CheckpointErrorCategory.EXECUTION,
            error=error_obj,
            response_metadata=metadata_obj,  # EM101
        )

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    with patch("async_durable_execution.execution.logger", mock_logger):
        response = await run_handler(
            test_handler, invocation_input, lambda_context, service_client=mock_client
        )
        assert response["Status"] == InvocationStatus.FAILED.value
        assert response["Error"]["ErrorType"] == "CheckpointError"

    mock_logger.exception.assert_called_once()
    call_args = mock_logger.exception.call_args
    assert call_args[0][0] == "Checkpoint system failed"
    assert call_args[1]["extra"]["Error"] == error_obj
    assert call_args[1]["extra"]["ResponseMetadata"] == metadata_obj


async def test_durable_execution_with_boto3_client_parameter():
    """Test durable_execution decorator accepts boto3_client parameter."""
    # GIVEN a custom boto3 Lambda client
    mock_boto3_client = Mock()
    mock_boto3_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": [], "NextMarker": ""},
    }
    mock_boto3_client.get_durable_execution_state.return_value = {
        "Operations": [],
        "NextMarker": "",
    }

    # GIVEN a durable function decorated with the custom client
    @durable_execution(boto3_client=mock_boto3_client)
    async def test_handler(event: Any) -> dict:
        return {"result": "success"}

    event = {
        "DurableExecutionArn": "arn:test:execution/exec1",
        "CheckpointToken": "token123",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "exec1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": '{"input": "test"}'},
                }
            ],
            "NextMarker": "",
        },
    }

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # WHEN the handler is invoked
    result = await run_handler(test_handler, event, lambda_context)

    # THEN the execution succeeds using the custom client
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert result["Result"] == '{"result": "success"}'


async def test_durable_execution_with_service_client_parameter():
    """Test durable_execution decorator accepts service_client parameter."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution(service_client=mock_client)
    async def test_handler(event: Any) -> dict:
        durable_context = cast(DurableContext, get_current_context())
        assert event == {"input": "test"}
        assert durable_context.execution_state._service_client is mock_client  # noqa: SLF001
        return {"result": "success"}

    event = {
        "DurableExecutionArn": "arn:test:execution/exec1",
        "CheckpointToken": "token123",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "exec1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": '{"input": "test"}'},
                }
            ],
            "NextMarker": "",
        },
    }

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = await run_handler(test_handler, event, lambda_context)

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert result["Result"] == '{"result": "success"}'


async def test_durable_execution_with_non_durable_payload_raises_error():
    """Test that invoking a durable function with a regular event raises a helpful error."""

    # GIVEN a durable function
    @durable_execution
    async def test_handler(event: Any) -> dict:
        return {"result": "success"}

    # GIVEN a regular Lambda event (not a durable execution payload)
    regular_event = {"key": "value", "data": "test"}

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # WHEN the handler is invoked with a non-durable payload
    # THEN it raises a ValueError with a helpful message
    with pytest.raises(
        ExecutionError,
        match=(
            "Unexpected payload provided to start the durable execution. "
            "Check your resource configurations to confirm the durability is set."
        ),
    ):
        await run_handler(test_handler, regular_event, lambda_context)


async def test_durable_execution_with_non_dict_event_raises_error():
    """Test that invoking a durable function with a non-dict event raises a helpful error."""

    # GIVEN a durable function
    @durable_execution
    async def test_handler(event: Any) -> dict:
        return {"result": "success"}

    # GIVEN a non-dict event
    non_dict_event = "not a dict"

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # WHEN the handler is invoked with a non-dict event
    # THEN it raises a ValueError with a helpful message
    with pytest.raises(
        ExecutionError,
        match=(
            "Unexpected payload provided to start the durable execution. "
            "Check your resource configurations to confirm the durability is set."
        ),
    ):
        await run_handler(test_handler, non_dict_event, lambda_context)


# =============================================================================
# Tests for JSON Serialization Methods
# =============================================================================


async def test_initial_execution_state_to_json_dict_minimal():
    """Test InitialExecutionState.to_json_dict with minimal data."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )

    state = InitialExecutionState(operations=[operation], next_marker="marker123")

    result = state.to_json_dict()
    expected = {"Operations": [operation.to_json_dict()], "NextMarker": "marker123"}

    assert result == expected


async def test_initial_execution_state_to_json_dict_with_timestamps():
    """Test InitialExecutionState.to_json_dict converts datetime objects to millisecond timestamps."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        start_timestamp=start_time,
        end_timestamp=end_time,
        execution_details=ExecutionDetails(input_payload="test_payload"),
    )

    state = InitialExecutionState(operations=[operation], next_marker="marker123")

    result = state.to_json_dict()

    # Verify that timestamps are converted to milliseconds in the operation
    operation_result = result["Operations"][0]
    expected_start_ms = int(start_time.timestamp() * 1000)
    expected_end_ms = int(end_time.timestamp() * 1000)

    assert operation_result["StartTimestamp"] == expected_start_ms
    assert operation_result["EndTimestamp"] == expected_end_ms
    assert result["NextMarker"] == "marker123"


async def test_initial_execution_state_to_json_dict_empty():
    """Test InitialExecutionState.to_json_dict with empty operations."""
    state = InitialExecutionState(operations=[], next_marker="")

    result = state.to_json_dict()
    expected = {"Operations": [], "NextMarker": ""}

    assert result == expected


async def test_initial_execution_state_from_json_dict_minimal():
    """Test InitialExecutionState.from_json_dict with minimal data."""
    data = {
        "Operations": [
            {
                "Id": "op1",
                "Type": "EXECUTION",
                "Status": "STARTED",
            }
        ],
        "NextMarker": "test-marker",
    }

    result = InitialExecutionState.from_json_dict(data)

    assert len(result.operations) == 1
    assert result.next_marker == "test-marker"
    assert result.operations[0].operation_id == "op1"
    assert result.operations[0].operation_type is OperationType.EXECUTION
    assert result.operations[0].status is OperationStatus.STARTED


async def test_initial_execution_state_from_json_dict_with_timestamps():
    """Test InitialExecutionState.from_json_dict converts millisecond timestamps to datetime objects."""
    start_ms = 1672574400000  # 2023-01-01 12:00:00 UTC
    end_ms = 1672578000000  # 2023-01-01 13:00:00 UTC

    data = {
        "Operations": [
            {
                "Id": "op1",
                "Type": "EXECUTION",
                "Status": "STARTED",
                "StartTimestamp": start_ms,
                "EndTimestamp": end_ms,
                "ExecutionDetails": {"InputPayload": "test_payload"},
            }
        ],
        "NextMarker": "test-marker",
    }

    result = InitialExecutionState.from_json_dict(data)

    expected_start = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    )
    expected_end = datetime.datetime(2023, 1, 1, 13, 0, 0, tzinfo=datetime.timezone.utc)

    assert len(result.operations) == 1
    operation = result.operations[0]
    assert operation.start_timestamp == expected_start
    assert operation.end_timestamp == expected_end
    assert operation.execution_details.input_payload == "test_payload"


async def test_initial_execution_state_from_json_dict_no_operations():
    """Test InitialExecutionState.from_json_dict handles missing Operations key."""
    data = {"NextMarker": "test-marker"}

    result = InitialExecutionState.from_json_dict(data)

    assert len(result.operations) == 0
    assert result.next_marker == "test-marker"


async def test_initial_execution_state_from_json_dict_empty_operations():
    """Test InitialExecutionState.from_json_dict handles empty Operations list."""
    data = {"Operations": [], "NextMarker": "test-marker"}

    result = InitialExecutionState.from_json_dict(data)

    assert len(result.operations) == 0
    assert result.next_marker == "test-marker"


async def test_initial_execution_state_json_roundtrip():
    """Test InitialExecutionState to_json_dict -> from_json_dict roundtrip preserves all data."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    next_attempt_time = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
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

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        parent_id="parent1",
        name="test_step",
        start_timestamp=start_time,
        step_details=step_details,
    )

    original = InitialExecutionState(operations=[operation], next_marker="marker123")

    # Convert to JSON dict and back
    json_data = original.to_json_dict()
    restored = InitialExecutionState.from_json_dict(json_data)

    # Verify all fields are preserved
    assert len(restored.operations) == len(original.operations)
    assert restored.next_marker == original.next_marker

    restored_op = restored.operations[0]
    original_op = original.operations[0]

    assert restored_op.operation_id == original_op.operation_id
    assert restored_op.operation_type == original_op.operation_type
    assert restored_op.status == original_op.status
    assert restored_op.parent_id == original_op.parent_id
    assert restored_op.name == original_op.name
    assert restored_op.start_timestamp == original_op.start_timestamp
    assert restored_op.step_details.attempt == original_op.step_details.attempt
    assert (
        restored_op.step_details.next_attempt_timestamp
        == original_op.step_details.next_attempt_timestamp
    )
    assert restored_op.step_details.result == original_op.step_details.result
    assert (
        restored_op.step_details.error.message == original_op.step_details.error.message
    )


async def test_durable_execution_invocation_input_to_json_dict_minimal():
    """Test DurableExecutionInvocationInput.to_json_dict with minimal data."""
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )

    initial_state = InitialExecutionState(
        operations=[operation], next_marker="test_marker"
    )

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_json_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution/exec1",
        "CheckpointToken": "token123",
        "InitialExecutionState": initial_state.to_json_dict(),
    }

    assert result == expected


async def test_durable_execution_invocation_input_to_json_dict_with_timestamps():
    """Test DurableExecutionInvocationInput.to_json_dict converts datetime objects to millisecond timestamps."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        start_timestamp=start_time,
        end_timestamp=end_time,
        execution_details=ExecutionDetails(input_payload="test_payload"),
    )

    initial_state = InitialExecutionState(
        operations=[operation], next_marker="test_marker"
    )

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_json_dict()

    # Verify that timestamps are converted to milliseconds in nested operations
    operation_result = result["InitialExecutionState"]["Operations"][0]
    expected_start_ms = int(start_time.timestamp() * 1000)
    expected_end_ms = int(end_time.timestamp() * 1000)

    assert operation_result["StartTimestamp"] == expected_start_ms
    assert operation_result["EndTimestamp"] == expected_end_ms
    assert result["DurableExecutionArn"] == "arn:test:execution/exec1"
    assert result["CheckpointToken"] == "token123"


async def test_durable_execution_invocation_input_to_json_dict_empty_operations():
    """Test DurableExecutionInvocationInput.to_json_dict with empty operations."""
    initial_state = InitialExecutionState(operations=[], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_json_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution/exec1",
        "CheckpointToken": "token123",
        "InitialExecutionState": {"Operations": [], "NextMarker": ""},
    }

    assert result == expected


async def test_durable_execution_invocation_input_from_json_dict_minimal():
    """Test DurableExecutionInvocationInput.from_json_dict with minimal data."""
    data = {
        "DurableExecutionArn": "arn:test:execution/exec1",
        "CheckpointToken": "token123",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "exec1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                }
            ],
            "NextMarker": "test_marker",
        },
    }

    result = DurableExecutionInvocationInput.from_json_dict(data)

    assert result.durable_execution_arn == "arn:test:execution/exec1"
    assert result.checkpoint_token == "token123"  # noqa: S105
    assert isinstance(result.initial_execution_state, InitialExecutionState)
    assert len(result.initial_execution_state.operations) == 1
    assert result.initial_execution_state.next_marker == "test_marker"
    assert result.initial_execution_state.operations[0].operation_id == "exec1"


async def test_durable_execution_invocation_input_from_json_dict_with_timestamps():
    """Test DurableExecutionInvocationInput.from_json_dict converts millisecond timestamps to datetime objects."""
    start_ms = 1672574400000  # 2023-01-01 12:00:00 UTC
    end_ms = 1672578000000  # 2023-01-01 13:00:00 UTC

    data = {
        "DurableExecutionArn": "arn:test:execution/exec1",
        "CheckpointToken": "token123",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "exec1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "StartTimestamp": start_ms,
                    "EndTimestamp": end_ms,
                    "ExecutionDetails": {"InputPayload": "test_payload"},
                }
            ],
            "NextMarker": "test_marker",
        },
    }

    result = DurableExecutionInvocationInput.from_json_dict(data)

    expected_start = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
    )
    expected_end = datetime.datetime(2023, 1, 1, 13, 0, 0, tzinfo=datetime.timezone.utc)

    operation = result.initial_execution_state.operations[0]
    assert operation.start_timestamp == expected_start
    assert operation.end_timestamp == expected_end
    assert operation.execution_details.input_payload == "test_payload"


async def test_durable_execution_invocation_input_from_json_dict_empty_initial_state():
    """Test DurableExecutionInvocationInput.from_json_dict handles missing InitialExecutionState."""
    data = {
        "DurableExecutionArn": "arn:test:execution/exec1",
        "CheckpointToken": "token123",
    }

    result = DurableExecutionInvocationInput.from_json_dict(data)

    assert result.durable_execution_arn == "arn:test:execution/exec1"
    assert result.checkpoint_token == "token123"  # noqa: S105
    assert isinstance(result.initial_execution_state, InitialExecutionState)
    assert len(result.initial_execution_state.operations) == 0
    assert not result.initial_execution_state.next_marker


async def test_durable_execution_invocation_input_json_roundtrip():
    """Test DurableExecutionInvocationInput to_json_dict -> from_json_dict roundtrip preserves all data."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)
    next_attempt_time = datetime.datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
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

    wait_details = WaitDetails(scheduled_end_timestamp=next_attempt_time)

    execution_operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        start_timestamp=start_time,
        end_timestamp=end_time,
        execution_details=ExecutionDetails(input_payload="test_payload"),
    )

    step_operation = Operation(
        operation_id="step1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        parent_id="exec1",
        name="test_step",
        start_timestamp=start_time,
        step_details=step_details,
        wait_details=wait_details,
    )

    initial_state = InitialExecutionState(
        operations=[execution_operation, step_operation], next_marker="marker123"
    )

    original = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution:12345",
        checkpoint_token="token123456",  # noqa: S106
        initial_execution_state=initial_state,
    )

    # Convert to JSON dict and back
    json_data = original.to_json_dict()
    restored = DurableExecutionInvocationInput.from_json_dict(json_data)

    # Verify all top-level fields are preserved
    assert restored.durable_execution_arn == original.durable_execution_arn
    assert restored.checkpoint_token == original.checkpoint_token

    # Verify initial execution state is preserved
    assert len(restored.initial_execution_state.operations) == len(
        original.initial_execution_state.operations
    )
    assert (
        restored.initial_execution_state.next_marker
        == original.initial_execution_state.next_marker
    )

    # Verify execution operation is preserved
    restored_exec_op = restored.initial_execution_state.operations[0]
    original_exec_op = original.initial_execution_state.operations[0]

    assert restored_exec_op.operation_id == original_exec_op.operation_id
    assert restored_exec_op.operation_type == original_exec_op.operation_type
    assert restored_exec_op.status == original_exec_op.status
    assert restored_exec_op.start_timestamp == original_exec_op.start_timestamp
    assert restored_exec_op.end_timestamp == original_exec_op.end_timestamp
    assert (
        restored_exec_op.execution_details.input_payload
        == original_exec_op.execution_details.input_payload
    )

    # Verify step operation is preserved
    restored_step_op = restored.initial_execution_state.operations[1]
    original_step_op = original.initial_execution_state.operations[1]

    assert restored_step_op.operation_id == original_step_op.operation_id
    assert restored_step_op.operation_type == original_step_op.operation_type
    assert restored_step_op.status == original_step_op.status
    assert restored_step_op.parent_id == original_step_op.parent_id
    assert restored_step_op.name == original_step_op.name
    assert restored_step_op.start_timestamp == original_step_op.start_timestamp
    assert (
        restored_step_op.step_details.attempt == original_step_op.step_details.attempt
    )
    assert (
        restored_step_op.step_details.next_attempt_timestamp
        == original_step_op.step_details.next_attempt_timestamp
    )
    assert restored_step_op.step_details.result == original_step_op.step_details.result
    assert (
        restored_step_op.step_details.error.message
        == original_step_op.step_details.error.message
    )
    assert (
        restored_step_op.wait_details.scheduled_end_timestamp
        == original_step_op.wait_details.scheduled_end_timestamp
    )


async def test_durable_execution_invocation_input_json_dict_preserves_non_timestamp_fields():
    """Test that to_json_dict preserves all non-timestamp fields unchanged."""

    context_details = ContextDetails(replay_children=True, result="context_result")

    callback_details = CallbackDetails(callback_id="cb123", result="callback_result")

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent1",
        name="test_context",
        context_details=context_details,
        callback_details=callback_details,
    )

    initial_state = InitialExecutionState(
        operations=[operation], next_marker="marker123"
    )

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_json_dict()

    # Verify non-timestamp fields are unchanged
    operation_result = result["InitialExecutionState"]["Operations"][0]
    assert operation_result["Id"] == "op1"
    assert operation_result["Type"] == "CONTEXT"
    assert operation_result["Status"] == "SUCCEEDED"
    assert operation_result["ParentId"] == "parent1"
    assert operation_result["Name"] == "test_context"
    assert operation_result["ContextDetails"]["Result"] == "context_result"
    assert operation_result["CallbackDetails"]["CallbackId"] == "cb123"
    assert operation_result["CallbackDetails"]["Result"] == "callback_result"

    assert result["DurableExecutionArn"] == "arn:test:execution/exec1"
    assert result["CheckpointToken"] == "token123"
    assert result["InitialExecutionState"]["NextMarker"] == "marker123"


async def test_event_parsing_with_unix_millis_timestamps():
    """Test that event parsing converts Unix millis timestamps to datetime objects.

    This reproduces the production bug where NextAttemptTimestamp was sent as
    Unix milliseconds (integer) and caused TypeError when comparing with datetime.now().

    Regression test for: TypeError: '<' not supported between instances of 'int' and 'datetime.datetime'

    Tests all timestamp fields handled by from_json_dict:
    - StartTimestamp
    - EndTimestamp
    - StepDetails.NextAttemptTimestamp
    - WaitDetails.ScheduledEndTimestamp
    """
    # Real event structure from Lambda backend with Unix millis timestamps
    event = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789:function:test:$LATEST/durable-execution/e/o",
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "exec-op",
                    "Type": "EXECUTION",
                    "StartTimestamp": 1769481309631,  # Unix millis (int)
                    "EndTimestamp": 1769481319631,  # Unix millis (int)
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": "{}"},
                },
                {
                    "Id": "step-with-retry",
                    "Type": "STEP",
                    "SubType": "WaitForCondition",
                    "StartTimestamp": 1769481309631,  # Unix millis (int)
                    "Status": "PENDING",
                    "StepDetails": {
                        "Attempt": 1,
                        "NextAttemptTimestamp": 1769481369631,  # Unix millis (int) - THE BUG!
                    },
                },
                {
                    "Id": "wait-op",
                    "Type": "WAIT",
                    "StartTimestamp": 1769481309631,  # Unix millis (int)
                    "Status": "PENDING",
                    "WaitDetails": {
                        "ScheduledEndTimestamp": 1769481399631  # Unix millis (int)
                    },
                },
            ]
        },
    }

    # Parse using from_json_dict (the fix)
    invocation_input = DurableExecutionInvocationInput.from_json_dict(event)
    operations = invocation_input.initial_execution_state.operations

    # Verify EXECUTION operation timestamps
    assert isinstance(operations[0].start_timestamp, datetime.datetime)
    assert isinstance(operations[0].end_timestamp, datetime.datetime)
    assert operations[0].start_timestamp.tzinfo == datetime.timezone.utc
    assert operations[0].end_timestamp.tzinfo == datetime.timezone.utc

    # Verify STEP operation with NextAttemptTimestamp (the critical one!)
    assert operations[1].step_details is not None
    next_attempt = operations[1].step_details.next_attempt_timestamp
    assert isinstance(next_attempt, datetime.datetime)
    assert next_attempt.tzinfo == datetime.timezone.utc

    # Verify WAIT operation with ScheduledEndTimestamp
    assert operations[2].wait_details is not None
    scheduled_end = operations[2].wait_details.scheduled_end_timestamp
    assert isinstance(scheduled_end, datetime.datetime)
    assert scheduled_end.tzinfo == datetime.timezone.utc

    # Verify timestamps can be compared with datetime.now() without TypeError
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    assert isinstance(next_attempt < now or next_attempt >= now, bool)
    assert isinstance(scheduled_end < now or scheduled_end >= now, bool)


async def test_from_dict_leaves_timestamps_as_integers():
    """Test that from_dict (the bug) leaves timestamps as integers.

    This demonstrates the bug behavior for documentation purposes.
    """
    event = {
        "DurableExecutionArn": "arn:test",
        "CheckpointToken": "token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "step-id",
                    "Type": "STEP",
                    "SubType": "WaitForCondition",
                    "StartTimestamp": 1769481309631,
                    "EndTimestamp": 1769481319631,
                    "Status": "PENDING",
                    "StepDetails": {
                        "Attempt": 1,
                        "NextAttemptTimestamp": 1769481369631,  # Unix millis (int)
                    },
                },
                {
                    "Id": "wait-id",
                    "Type": "WAIT",
                    "StartTimestamp": 1769481309631,
                    "Status": "PENDING",
                    "WaitDetails": {
                        "ScheduledEndTimestamp": 1769481399631  # Unix millis (int)
                    },
                },
            ]
        },
    }

    # Using from_dict leaves timestamps as integers
    invocation_input = DurableExecutionInvocationInput.from_dict(event)
    operations = invocation_input.initial_execution_state.operations

    # All timestamps remain as integers (the bug)
    assert isinstance(operations[0].start_timestamp, int)
    assert isinstance(operations[0].end_timestamp, int)
    assert isinstance(operations[0].step_details.next_attempt_timestamp, int)
    assert isinstance(operations[1].wait_details.scheduled_end_timestamp, int)

    # These comparisons would cause TypeError
    with pytest.raises(
        TypeError,
        match="'<' not supported between instances of 'int' and 'datetime.datetime'",
    ):
        _ = operations[0].step_details.next_attempt_timestamp < datetime.datetime.now(
            tz=datetime.timezone.utc
        )

    with pytest.raises(
        TypeError,
        match="'<' not supported between instances of 'int' and 'datetime.datetime'",
    ):
        _ = operations[1].wait_details.scheduled_end_timestamp < datetime.datetime.now(
            tz=datetime.timezone.utc
        )


# =============================================================================
# Non-retryable Durable API error handling tests
# =============================================================================


def _make_invocation_input(next_marker=""):
    """Helper to create a standard test invocation input."""
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )
    return DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=InitialExecutionState(
            operations=[operation], next_marker=next_marker
        ),
    )


def _make_lambda_context():
    """Helper to create a standard mock Lambda context."""
    ctx = Mock()
    ctx.aws_request_id = "test-request"
    ctx.client_context = None
    ctx.identity = None
    ctx._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    ctx.invoked_function_arn = None
    ctx.tenant_id = None
    return ctx


async def test_durable_execution_replays_when_paginated_state_has_prior_operations():
    """Test paginated execution state starts in replay mode when prior operations exist."""
    mock_client = Mock(spec=DurableServiceClient)
    step_operation = Operation(
        operation_id="step1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    mock_client.get_execution_state.return_value = StateOutput(
        operations=[step_operation],
        next_marker=None,
    )

    invocation_input = _make_invocation_input(next_marker="page2")

    @durable_execution
    async def test_handler(event: Any) -> dict:
        del event
        durable_context = cast(DurableContext, get_current_context())
        return {"is_replaying": durable_context.is_replaying()}

    result = await run_handler(
        test_handler,
        invocation_input,
        _make_lambda_context(),
        service_client=mock_client,
    )

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert json.loads(result["Result"]) == {"is_replaying": True}
    mock_client.get_execution_state.assert_called_once_with(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",
        next_marker="page2",
    )


async def test_durable_execution_non_retryable_invocation_error_returns_failed():
    """Test that non-retryable InvocationError returns FAILED instead of retrying."""
    mock_client = Mock(spec=DurableServiceClient)
    non_retryable_error = GetExecutionStateError(
        message="KMS access denied",
        error_category=DurableApiErrorCategory.EXECUTION,
        error={"Code": "KMSAccessDeniedException", "Message": "KMS access denied"},
        response_metadata={"HTTPStatusCode": 502},
    )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        raise non_retryable_error

    result = await run_handler(
        test_handler,
        _make_invocation_input(),
        _make_lambda_context(),
        service_client=mock_client,
    )
    assert result["Status"] == InvocationStatus.FAILED.value
    assert result["Error"]["ErrorType"] == "GetExecutionStateError"


async def test_durable_execution_retryable_invocation_error_raises():
    """Test that retryable InvocationError raises to trigger Lambda retry."""
    mock_client = Mock(spec=DurableServiceClient)
    retryable_error = GetExecutionStateError(
        message="Service error",
        error={"Code": "ServiceException", "Message": "Internal error"},
        response_metadata={"HTTPStatusCode": 500},
    )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        raise retryable_error

    with pytest.raises(GetExecutionStateError, match="Service error"):
        await run_handler(
            test_handler,
            _make_invocation_input(),
            _make_lambda_context(),
            service_client=mock_client,
        )


async def test_durable_execution_non_retryable_background_thread_error_returns_failed():
    """Test that non-retryable error from background thread returns FAILED."""
    mock_client = Mock(spec=DurableServiceClient)
    non_retryable_error = GetExecutionStateError(
        message="KMS key disabled",
        error_category=DurableApiErrorCategory.EXECUTION,
        error={"Code": "KMSDisabledException", "Message": "KMS key disabled"},
        response_metadata={"HTTPStatusCode": 502},
    )
    mock_client.checkpoint.side_effect = lambda *a, **kw: (_ for _ in ()).throw(
        non_retryable_error
    )

    @durable_execution
    async def test_handler(event: Any) -> dict:
        async def step_result() -> str:
            return "step_result"

        await step(step_result)
        return {"result": "success"}

    result = await run_handler(
        test_handler,
        _make_invocation_input(),
        _make_lambda_context(),
        service_client=mock_client,
    )
    assert result["Status"] == InvocationStatus.FAILED.value
    assert result["Error"]["ErrorType"] == "GetExecutionStateError"


@pytest.mark.parametrize(
    ("error_code", "status_code", "error_category"),
    [
        ("KMSAccessDeniedException", 502, DurableApiErrorCategory.EXECUTION),
        ("ValidationException", 400, DurableApiErrorCategory.EXECUTION),
    ],
)
async def test_durable_execution_non_retryable_initial_pagination_error_returns_failed(
    error_code: str, status_code: int, error_category: DurableApiErrorCategory
):
    """Test that non-retryable errors during initial pagination return FAILED."""
    mock_client = Mock(spec=DurableServiceClient)
    non_retryable_error = GetExecutionStateError(
        message=f"{error_code} error",
        error_category=error_category,
        error={"Code": error_code, "Message": f"{error_code} error"},
        response_metadata={"HTTPStatusCode": status_code},
    )
    mock_client.get_execution_state.side_effect = non_retryable_error

    @durable_execution
    async def test_handler(event: Any) -> dict:
        return {"result": "success"}

    result = await run_handler(
        test_handler,
        _make_invocation_input(next_marker="next-page-marker"),
        _make_lambda_context(),
        service_client=mock_client,
    )
    assert result["Status"] == InvocationStatus.FAILED.value
    assert result["Error"]["ErrorType"] == "GetExecutionStateError"


async def test_durable_execution_retryable_initial_pagination_error_raises():
    """Test that retryable error during initial pagination raises to trigger Lambda retry."""
    mock_client = Mock(spec=DurableServiceClient)
    retryable_error = GetExecutionStateError(
        message="Service error",
        error={"Code": "ServiceException", "Message": "Internal error"},
        response_metadata={"HTTPStatusCode": 500},
    )
    mock_client.get_execution_state.side_effect = retryable_error

    @durable_execution
    async def test_handler(event: Any) -> dict:
        return {"result": "success"}

    with pytest.raises(GetExecutionStateError, match="Service error"):
        await run_handler(
            test_handler,
            _make_invocation_input(next_marker="next-page-marker"),
            _make_lambda_context(),
            service_client=mock_client,
        )


async def test_durable_execution_supports_async_handler():
    mock_client = Mock(spec=DurableServiceClient)
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    @durable_execution
    async def test_handler(event: Any) -> dict:
        await asyncio.sleep(0)
        logging.getLogger(__name__).info("handled async invocation")
        return {"result": "async-success"}

    result = await run_handler(
        test_handler,
        _make_invocation_input(),
        _make_lambda_context(),
        service_client=mock_client,
    )

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert json.loads(result["Result"]) == {"result": "async-success"}


async def test_durable_execution_handler_can_use_get_current_context_without_parameter():
    mock_client = Mock(spec=DurableServiceClient)
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    @durable_execution
    async def test_handler(event: Any) -> dict:
        durable_context = cast(DurableContext, get_current_context())
        assert event == {}

        async def load_value() -> str:
            await asyncio.sleep(0)
            return "from-context"

        result = await step(load_value)
        return {"value": result, "has_logger": hasattr(durable_context, "logger")}

    result = await run_handler(
        test_handler,
        _make_invocation_input(),
        _make_lambda_context(),
        service_client=mock_client,
    )

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert json.loads(result["Result"]) == {
        "value": "from-context",
        "has_logger": False,
    }


async def test_durable_execution_handler_can_access_lambda_context_from_current_context():
    mock_client = Mock(spec=DurableServiceClient)
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    lambda_context = _make_lambda_context()

    @durable_execution
    async def test_handler(event: Any) -> dict:
        durable_context = cast(DurableContext, get_current_context())
        assert durable_context.lambda_context is not None
        return {
            "event": event,
            "request_id": durable_context.lambda_context.aws_request_id,
            "has_durable_context": durable_context is not None,
        }

    result = await run_handler(
        test_handler,
        _make_invocation_input(),
        lambda_context,
        service_client=mock_client,
    )

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert json.loads(result["Result"]) == {
        "event": {},
        "request_id": lambda_context.aws_request_id,
        "has_durable_context": True,
    }


async def test_durable_execution_supports_async_steps_inside_async_handler():
    mock_client = Mock(spec=DurableServiceClient)
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    async def async_step() -> str:
        await asyncio.sleep(0)
        return "async-step-success"

    @durable_execution
    async def test_handler(event: Any) -> dict:
        await asyncio.sleep(0)
        step_result = await step(async_step, name="async-step")
        return {"step_result": step_result}

    result = await run_handler(
        test_handler,
        _make_invocation_input(),
        _make_lambda_context(),
        service_client=mock_client,
    )

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert json.loads(result["Result"]) == {"step_result": "async-step-success"}


def create_mock_checkpoint_with_operations():
    """Create a mock checkpoint function that properly tracks operations.

    Returns a tuple of (mock_checkpoint_function, checkpoint_calls_list).
    The mock properly maintains an operations list that gets updated with each checkpoint.
    """
    checkpoint_calls = []
    operations = [
        Operation(
            operation_id="execution-1",
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
        )
    ]

    async def mock_checkpoint(
        durable_execution_arn,
        checkpoint_token,
        updates,
        client_token="token",  # noqa: S107
    ):
        checkpoint_calls.append(updates)

        # Convert updates to Operation objects and add to operations list
        for update in updates:
            op = Operation(
                operation_id=update.operation_id,
                operation_type=update.operation_type,
                status=OperationStatus.STARTED,  # New operations start as STARTED
                parent_id=update.parent_id,
            )
            operations.append(op)

        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=operations.copy()
            ),
        )

    return mock_checkpoint, checkpoint_calls


async def test_step_different_ways_to_pass_args():
    async def step_plain() -> str:
        return "from step plain"

    async def step_no_args() -> str:
        return "from step no args"

    async def step_with_args(a: int, b: str) -> str:
        return f"from step {a} {b}"

    @durable_execution
    async def my_handler(event) -> list[str]:
        del event
        results: list[str] = []
        result: str = await step(partial(step_with_args, a=123, b="str"))
        assert result == "from step 123 str"
        results.append(result)

        result = await step(step_no_args)
        assert result == "from step no args"
        results.append(result)

        # note this won't work:
        # result: str = step(step_no_args)

        result = await step(step_plain)
        assert result == "from step plain"
        results.append(result)

        return results

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock the checkpoint method to track calls
        checkpoint_calls = []

        async def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        # Create test event
        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        # Create mock lambda context
        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Execute the handler
        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert (
            result["Result"]
            == '["from step 123 str", "from step no args", "from step plain"]'
        )

        # 3 START checkpoint, 3 SUCCEED checkpoint (batched together)
        # Flatten all operations from all batches
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 6

        # Check the last operation
        last_checkpoint = all_operations[-1]
        assert last_checkpoint.operation_type is OperationType.STEP
        assert last_checkpoint.action is OperationAction.SUCCEED
        assert last_checkpoint.payload == '"from step plain"'


async def test_durable_callable_decorator_creates_step_operation():
    @durable_callable
    async def decorated_step(status_code: int) -> str:
        assert get_current_context() is not None
        logging.getLogger(__name__).info("status=%s", status_code)
        return f"status:{status_code}"

    @durable_execution
    async def my_handler(event) -> str:
        del event
        return await step(decorated_step(200))

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        checkpoint_calls = []

        async def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '"status:200"'

        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2
        assert all_operations[0].name == "decorated_step"
        assert all_operations[1].name == "decorated_step"


async def test_step_with_logger():
    async def mystep(a: int, b: str) -> str:
        assert get_current_context() is not None
        logging.getLogger(__name__).info("from step %s %s", a, b)
        return "result"

    @durable_execution
    async def my_handler(event):
        del event
        result: str = await step(partial(mystep, a=123, b="str"))
        assert result == "result"

    with (
        patch(
            "async_durable_execution.execution.ThreadedSyncLambdaClient"
        ) as mock_client_class,
        patch.object(logging.getLogger(__name__), "info") as mock_info,
    ):
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock the checkpoint method to track calls
        checkpoint_calls = []

        async def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        # Create test event
        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        # Create mock lambda context
        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Execute the handler
        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value

        # 1 START checkpoint, 1 SUCCEED checkpoint (batched together)
        # Flatten all operations from all batches
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2

        mock_info.assert_called_once_with("from step %s %s", 123, "str")

        # Check the START operation
        start_op = all_operations[0]
        assert start_op.operation_type == OperationType.STEP
        assert start_op.action == OperationAction.START
        # Check the SUCCEED operation
        succeed_op = all_operations[1]
        assert succeed_op.operation_type == OperationType.STEP
        assert succeed_op.action == OperationAction.SUCCEED
        assert succeed_op.operation_id == start_op.operation_id


async def test_wait_inside_run_in_childcontext():
    """A wait inside a child context should suspend the execution."""

    mock_inside_child = Mock()

    async def func(a: int, b: int):
        child_context = cast(DurableContext, get_current_context())
        mock_inside_child(a, b)
        await wait(timedelta(seconds=1))

    @durable_execution
    async def my_handler(event):
        del event
        await run_in_child_context(partial(func, 10, 20), name="func")

    # Mock the lambda client
    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Use helper to create mock that properly tracks operations
        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        # Create test event
        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        # Create mock lambda context
        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Execute the handler
        result = await run_handler(my_handler, event, lambda_context)

        # Assert the execution returns PENDING status
        assert result["Status"] == InvocationStatus.PENDING.value

        # Assert that checkpoints were created (may be batched together)
        # Flatten all operations from all batches
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2  # One for child context start, one for wait

        expected_parent_id = next(operation_id_sequence())
        expected_child_id = next(operation_id_sequence(expected_parent_id))

        # Check first operation (child context start)
        first_checkpoint = all_operations[0]
        assert first_checkpoint.operation_type is OperationType.CONTEXT
        assert first_checkpoint.action is OperationAction.START
        assert first_checkpoint.operation_id == expected_parent_id

        # Check second operation (wait operation)
        second_checkpoint = all_operations[1]
        assert second_checkpoint.operation_type is OperationType.WAIT
        assert second_checkpoint.action is OperationAction.START
        assert second_checkpoint.operation_id == expected_child_id
        assert second_checkpoint.wait_options.wait_seconds == 1

        assert second_checkpoint.operation_id != first_checkpoint.operation_id

        mock_inside_child.assert_called_once_with(10, 20)


class CustomError(Exception):
    """Custom exception for testing."""


async def test_step_checkpoint_failure_propagates_error():
    """Test that errors during checkpoint invocation propagate correctly from background thread.

    This test demonstrates a bug: when a checkpoint fails in the background thread,
    the user code thread is blocked waiting on completion_event.wait() with no timeout.
    The background thread exception is raised, but the user thread never completes,
    causing the execution to hang indefinitely.
    """

    async def failing_step() -> str:
        return "this should checkpoint but fail"

    @durable_execution
    async def my_handler(event):
        del event
        # This step will trigger a checkpoint that fails
        result: str = await step(failing_step)
        return result

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock the checkpoint method to raise an error (using RuntimeError as a generic exception)
        async def mock_checkpoint_failure(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            # Simulate a failure during checkpoint invocation
            msg = "Checkpoint service unavailable"
            raise RuntimeError(msg)

        mock_client.checkpoint = mock_checkpoint_failure

        # Create test event
        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        # Create mock lambda context
        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Execute the handler - local runner surfaces execution failure in the
        # invocation payload rather than re-raising to the caller.
        result = await run_handler(my_handler, event, lambda_context)
        assert result["Status"] == InvocationStatus.FAILED.value
        assert result["Error"]["ErrorMessage"] == "Checkpoint service unavailable"


async def test_wait_not_caught_by_exception():
    """Do not catch Suspend exceptions."""

    @durable_execution
    async def my_handler(event: Any):
        del event
        try:
            await wait(timedelta(seconds=1))
        except Exception as err:
            msg = "This should not be caught"
            raise CustomError(msg) from err

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Use helper to create mock that properly tracks operations
        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        # Create test event
        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        # Create mock lambda context
        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Execute the handler
        result = await run_handler(my_handler, event, lambda_context)
        operation_ids = operation_id_sequence()

        # Assert the execution returns PENDING status
        assert result["Status"] == InvocationStatus.PENDING.value

        # Assert that only 1 checkpoint was created for the wait operation
        assert len(checkpoint_calls) == 1

        # Check the wait checkpoint
        checkpoint = checkpoint_calls[0][0]
        assert checkpoint.operation_type is OperationType.WAIT
        assert checkpoint.action is OperationAction.START
        assert checkpoint.operation_id == next(operation_ids)
        assert checkpoint.wait_options.wait_seconds == 1


async def test_durable_callable_wait_for_callback_submitter():
    """Test durable_callable submitter uses callback_id from current context."""

    mock_submitter = Mock()

    @durable_callable
    async def submit_to_external_system(task_name, priority):
        callback_context = get_current_context()
        callback_id = callback_context.callback_id
        mock_submitter(callback_id, task_name, priority)
        logging.getLogger(__name__).info(
            "Submitting %s with callback %s", task_name, callback_id
        )

    @durable_execution
    async def my_handler(event):
        del event
        await wait_for_callback(submit_to_external_system("my_task", priority=5))

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        checkpoint_calls = []

        async def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            # For CALLBACK operations, return the operation with callback details
            operations = [
                Operation(
                    operation_id=update.operation_id,
                    operation_type=OperationType.CALLBACK,
                    status=OperationStatus.STARTED,
                    callback_details=CallbackDetails(
                        callback_id=f"callback-{update.operation_id[:8]}"
                    ),
                )
                for update in updates
                if update.operation_type == OperationType.CALLBACK
            ]

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(
                    operations=operations, next_marker=None
                ),
            )

        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.PENDING.value

        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 4

        # First: CONTEXT START
        first_checkpoint = all_operations[0]
        assert first_checkpoint.operation_type is OperationType.CONTEXT
        assert first_checkpoint.action is OperationAction.START
        assert first_checkpoint.name == "submit_to_external_system"

        # Second: CALLBACK START
        second_checkpoint = all_operations[1]
        assert second_checkpoint.operation_type is OperationType.CALLBACK
        assert second_checkpoint.action is OperationAction.START
        assert second_checkpoint.parent_id == first_checkpoint.operation_id
        assert second_checkpoint.name == "submit_to_external_system-callback"

        # Third: STEP START
        third_checkpoint = all_operations[2]
        assert third_checkpoint.operation_type is OperationType.STEP
        assert third_checkpoint.action is OperationAction.START
        assert third_checkpoint.parent_id == first_checkpoint.operation_id
        assert third_checkpoint.name == "submit_to_external_system-submitter"

        # Fourth: STEP SUCCEED
        fourth_checkpoint = all_operations[3]
        assert fourth_checkpoint.operation_type is OperationType.STEP
        assert fourth_checkpoint.action is OperationAction.SUCCEED
        assert fourth_checkpoint.operation_id == third_checkpoint.operation_id

        mock_submitter.assert_called_once()
        call_args = mock_submitter.call_args[0]
        assert call_args[1] == "my_task"
        assert call_args[2] == 5


async def test_end_to_end_step_operation_with_double_check():
    """Test end-to-end step operation execution with double-check pattern.

    Verifies that the step executor re-checks state after creating a synchronous
    START checkpoint, enabling immediate response handling.
    """

    async def my_step() -> str:
        return "step_result"

    @durable_execution
    async def my_handler(event) -> str:
        result: str = await step(my_step)
        return result

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '"step_result"'

        # Verify checkpoints were created (START + SUCCEED)
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2


async def test_end_to_end_multiple_operations_execute_sequentially():
    """Test end-to-end execution with multiple operations.

    Verifies that multiple operations in a workflow execute correctly
    with the immediate response handling pattern.
    """

    async def step1() -> str:
        return "result1"

    async def step2() -> str:
        return "result2"

    @durable_execution
    async def my_handler(event) -> list[str]:
        return [await step(step1), await step(step2)]

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '["result1", "result2"]'

        # Verify all checkpoints were created (2 START + 2 SUCCEED)
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 4


async def test_end_to_end_wait_operation_with_double_check():
    """Test end-to-end wait operation execution with double-check pattern.

    Verifies that wait operations properly use the double-check pattern
    for immediate response handling.
    """

    @durable_execution
    async def my_handler(event) -> str:
        await wait(timedelta(seconds=5))
        return "completed"

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Wait will suspend, so we expect PENDING status
        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.PENDING.value

        # Verify wait checkpoint was created
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) >= 1


async def test_end_to_end_checkpoint_synchronization_with_operations_list():
    """Test that synchronous checkpoints properly update operations list.

    Verifies that when is_sync=True, the operations list is updated
    before the second status check occurs.
    """

    async def my_step() -> str:
        return "result"

    @durable_execution
    async def my_handler(event) -> str:
        return await step(my_step)

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value

        # Verify operations list was properly maintained
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) >= 2  # At least START and SUCCEED


async def test_callback_deferred_error_handling_to_result():
    """Test callback deferred error handling pattern.

    Verifies that callback operations properly return callback_id through
    the immediate response handling pattern, enabling deferred error handling.
    """

    async def step_after_callback() -> str:
        return "code_executed_after_callback"

    @durable_execution
    async def my_handler(event) -> str:
        # Create callback
        callback = await create_callback(name="test_callback")

        # This code executes even if callback will eventually fail
        # This is the deferred error handling pattern
        result = await step(step_after_callback)

        return f"{callback.callback_id}:{result}"

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        checkpoint_calls = []
        operations = [
            Operation(
                operation_id="execution-1",
                operation_type=OperationType.EXECUTION,
                status=OperationStatus.STARTED,
            )
        ]

        async def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            # Add operations with proper details
            for update in updates:
                if update.operation_type == OperationType.CALLBACK:
                    op = Operation(
                        operation_id=update.operation_id,
                        operation_type=update.operation_type,
                        status=OperationStatus.STARTED,
                        parent_id=update.parent_id,
                        callback_details=CallbackDetails(
                            callback_id=f"cb-{update.operation_id[:8]}"
                        ),
                    )
                else:
                    op = Operation(
                        operation_id=update.operation_id,
                        operation_type=update.operation_type,
                        status=OperationStatus.STARTED,
                        parent_id=update.parent_id,
                    )
                operations.append(op)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(
                    operations=operations.copy()
                ),
            )

        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        # Verify execution succeeded and code after callback executed
        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert "code_executed_after_callback" in result["Result"]


async def test_end_to_end_invoke_operation_with_double_check():
    """Test end-to-end invoke operation execution with double-check pattern.

    Verifies that invoke operations properly use the double-check pattern
    for immediate response handling.
    """

    @durable_execution
    async def my_handler(event):
        await invoke("my-function", {"data": "test"})

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Invoke will suspend, so we expect PENDING status
        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.PENDING.value

        # Verify invoke checkpoint was created
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) >= 1


async def test_end_to_end_child_context_with_async_checkpoint():
    """Test end-to-end child context execution with async checkpoint.

    Verifies that child context operations use async checkpoint (is_sync=False)
    and execute correctly without waiting for immediate response.
    """

    async def child_function() -> str:
        _ = cast(DurableContext, get_current_context())
        return "child_result"

    @durable_execution
    async def my_handler(event) -> str:
        result: str = await run_in_child_context(child_function)
        return result

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '"child_result"'

        # Verify checkpoints were created (START + SUCCEED)
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2


async def test_end_to_end_child_context_replay_children_mode():
    """Test end-to-end child context with large payload and ReplayChildren mode.

    Verifies that child context with large result (>256KB) triggers replay_children mode,
    uses summary generator if provided, and re-executes function on replay.
    """
    execution_count = {"count": 0}

    async def child_function_with_large_result() -> str:
        _ = cast(DurableContext, get_current_context())
        execution_count["count"] += 1
        return "large" * 256 * 1024

    def summary_generator(result: str) -> str:
        return f"summary_of_{len(result)}_bytes"

    @durable_execution
    async def my_handler(event) -> str:
        await run_in_child_context(
            child_function_with_large_result,
            summary_generator=summary_generator,
        )
        return f"executed_{execution_count['count']}_times"

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        checkpoint_calls = []
        operations = [
            Operation(
                operation_id="execution-1",
                operation_type=OperationType.EXECUTION,
                status=OperationStatus.STARTED,
            )
        ]

        async def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            for update in updates:
                op = Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                )
                operations.append(op)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(
                    operations=operations.copy()
                ),
            )

        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        # Function executed once during initial execution
        assert execution_count["count"] == 1

        # Verify replay_children was set in SUCCEED checkpoint
        all_operations = [op for batch in checkpoint_calls for op in batch]
        succeed_updates = [
            op
            for op in all_operations
            if hasattr(op, "action") and op.action.value == "SUCCEED"
        ]
        assert len(succeed_updates) == 1
        assert succeed_updates[0].context_options.replay_children is True


async def test_end_to_end_child_context_error_handling():
    """Test end-to-end child context error handling.

    Verifies that child context that raises exception creates FAIL checkpoint
    and error is wrapped as CallableRuntimeError.
    """

    async def child_function_that_fails() -> str:
        _ = cast(DurableContext, get_current_context())
        msg = "Child function error"
        raise ValueError(msg)

    @durable_execution
    async def my_handler(event) -> str:
        result: str = await run_in_child_context(child_function_that_fails)
        return result

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        # Verify execution failed
        assert result["Status"] == InvocationStatus.FAILED.value

        # Verify FAIL checkpoint was created
        all_operations = [op for batch in checkpoint_calls for op in batch]
        fail_updates = [
            op
            for op in all_operations
            if hasattr(op, "action") and op.action.value == "FAIL"
        ]
        assert len(fail_updates) == 1


async def test_end_to_end_child_context_invocation_error_reraised():
    """Test end-to-end child context InvocationError re-raising.

    Verifies that child context that raises InvocationError creates FAIL checkpoint
    and re-raises InvocationError (not wrapped) to enable retry at execution handler level.
    """

    async def child_function_with_invocation_error() -> str:
        _ = cast(DurableContext, get_current_context())
        msg = "Invocation failed in child"
        raise InvocationError(msg)

    @durable_execution
    async def my_handler(event) -> str:
        result: str = await run_in_child_context(child_function_with_invocation_error)
        return result

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # InvocationError should be re-raised (not wrapped) to trigger Lambda retry
        with pytest.raises(InvocationError, match="Invocation failed in child"):
            await run_handler(my_handler, event, lambda_context)

        # Verify FAIL checkpoint was created before re-raising
        all_operations = [op for batch in checkpoint_calls for op in batch]
        fail_updates = [
            op
            for op in all_operations
            if hasattr(op, "action") and op.action.value == "FAIL"
        ]
        assert len(fail_updates) == 1
