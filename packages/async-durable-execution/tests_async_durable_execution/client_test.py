"""Tests for the service module."""

import asyncio
import datetime
from datetime import timezone
from unittest.mock import Mock, patch

import pytest

from async_durable_execution.__about__ import __version__
from async_durable_execution.exceptions import (
    CallableRuntimeError,
    CheckpointError,
    GetExecutionStateError,
)
from async_durable_execution.models import OperationIdentifier
from async_durable_execution.models import (
    CallbackDetails,
    CallbackOptions,
    ChainedInvokeDetails,
    ChainedInvokeOptions,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ContextDetails,
    ContextOptions,
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
from async_durable_execution.client import (
    ThreadedSyncLambdaClient,
    create_default_client,
)
from async_durable_execution.types import DurableServiceClient


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def reset_lambda_client_cache():
    """Reset the class-level boto3 client cache before and after each test."""
    ThreadedSyncLambdaClient._cached_boto_client = None  # noqa: SLF001
    yield
    ThreadedSyncLambdaClient._cached_boto_client = None  # noqa: SLF001


@patch("async_durable_execution.client.boto3")
async def test_lambda_client_checkpoint(mock_boto3):
    """Test ThreadedSyncLambdaClient.checkpoint method."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = ThreadedSyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = await lambda_client.checkpoint("arn123", "token123", [update], None)

    mock_client.checkpoint_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


async def test_lambda_client_checkpoint_with_client_token():
    """Test ThreadedSyncLambdaClient.checkpoint method with client_token."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = ThreadedSyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = await lambda_client.checkpoint(
        "arn123", "token123", [update], "client-token-123"
    )

    mock_client.checkpoint_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
        ClientToken="client-token-123",
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


async def test_lambda_client_checkpoint_with_explicit_none_client_token():
    """Test ThreadedSyncLambdaClient.checkpoint method with explicit None client_token - should not pass ClientToken."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = ThreadedSyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = await lambda_client.checkpoint("arn123", "token123", [update], None)

    mock_client.checkpoint_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


async def test_lambda_client_checkpoint_with_empty_string_client_token():
    """Test ThreadedSyncLambdaClient.checkpoint method with empty string client_token - should pass empty string."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = ThreadedSyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = await lambda_client.checkpoint("arn123", "token123", [update], "")

    mock_client.checkpoint_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
        ClientToken="",
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


async def test_lambda_client_checkpoint_with_string_value_client_token():
    """Test ThreadedSyncLambdaClient.checkpoint method with string value client_token - should pass the value."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = ThreadedSyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = await lambda_client.checkpoint(
        "arn123", "token123", [update], "my-client-token"
    )

    mock_client.checkpoint_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
        ClientToken="my-client-token",
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


async def test_lambda_client_checkpoint_with_exception():
    """Test ThreadedSyncLambdaClient.checkpoint method with exception."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.side_effect = Exception("API Error")

    lambda_client = ThreadedSyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    with pytest.raises(CheckpointError):
        await lambda_client.checkpoint("arn123", "token123", [update], None)


@patch("async_durable_execution.client.logger")
async def test_lambda_client_checkpoint_logs_response_metadata(mock_logger):
    """Test ThreadedSyncLambdaClient.checkpoint logs ResponseMetadata from boto3 exception."""
    mock_client = Mock()
    boto_error = Exception("API Error")
    boto_error.response = {
        "ResponseMetadata": {
            "RequestId": "test-request-id-123",
            "HTTPStatusCode": 500,
            "RetryAttempts": 2,
        }
    }
    mock_client.checkpoint_durable_execution.side_effect = boto_error

    lambda_client = ThreadedSyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    with pytest.raises(CheckpointError):
        await lambda_client.checkpoint("arn123", "token123", [update], None)

    mock_logger.exception.assert_called_once_with(
        "Failed to checkpoint.",
        extra={
            "ResponseMetadata": {
                "RequestId": "test-request-id-123",
                "HTTPStatusCode": 500,
                "RetryAttempts": 2,
            },
        },
    )


@patch("async_durable_execution.client.logger")
async def test_lambda_client_get_execution_state_logs_response_metadata(mock_logger):
    """Test ThreadedSyncLambdaClient.get_execution_state logs ResponseMetadata from boto3 exception."""
    mock_client = Mock()
    boto_error = Exception("API Error")
    boto_error.response = {
        "ResponseMetadata": {
            "RequestId": "test-request-id-456",
            "HTTPStatusCode": 503,
            "RetryAttempts": 1,
        }
    }
    mock_client.get_durable_execution_state.side_effect = boto_error

    lambda_client = ThreadedSyncLambdaClient(mock_client)

    with pytest.raises(GetExecutionStateError) as exc_info:
        await lambda_client.get_execution_state("arn123", "token123", "", 1000)

    assert exc_info.value.error is None
    assert exc_info.value.response_metadata == {
        "RequestId": "test-request-id-456",
        "HTTPStatusCode": 503,
        "RetryAttempts": 1,
    }

    mock_logger.exception.assert_called_once_with(
        "Failed to get execution state.",
        extra={
            "ResponseMetadata": {
                "RequestId": "test-request-id-456",
                "HTTPStatusCode": 503,
                "RetryAttempts": 1,
            },
        },
    )


async def test_durable_service_client_protocol_checkpoint():
    """Test DurableServiceClient protocol checkpoint method signature."""
    mock_client = Mock(spec=DurableServiceClient)
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    updates = [
        OperationUpdate(
            operation_id="test", operation_type=OperationType.STEP, action="START"
        )
    ]

    result = await mock_client.checkpoint("arn123", "token", updates, "client_token")

    mock_client.checkpoint.assert_called_once_with(
        "arn123", "token", updates, "client_token"
    )
    assert result == mock_output


# =============================================================================
# Tests for Client Classes (DurableServiceClient, ThreadedSyncLambdaClient)
# =============================================================================


async def test_lambda_client_constructor():
    """Test ThreadedSyncLambdaClient constructor to cover lines 931-945."""
    mock_client = Mock()
    client = ThreadedSyncLambdaClient(mock_client)
    assert isinstance(client, ThreadedSyncLambdaClient)


@patch.dict("os.environ", {}, clear=True)
@patch("async_durable_execution.client.boto3.client")
async def test_create_default_client_builds_lambda_client_with_expected_config(
    mock_boto_client, reset_lambda_client_cache
):
    """Test create_default_client builds a lambda boto client with expected config."""
    mock_client = Mock()
    mock_boto_client.return_value = mock_client

    client = create_default_client()

    mock_boto_client.assert_called_once()
    call_args = mock_boto_client.call_args
    assert call_args[0][0] == "lambda"
    assert "config" in call_args[1]
    config = call_args[1]["config"]
    assert config.connect_timeout == 5
    assert config.read_timeout == 50
    assert config.user_agent_extra == f"async-durable-execution/{__version__}-async"
    assert client is mock_client


@patch.dict("os.environ", {"AWS_ENDPOINT_URL_LAMBDA": "http://localhost:3000"})
@patch("async_durable_execution.client.boto3.client")
async def test_create_default_client_defers_endpoint_handling_to_boto3(
    mock_boto_client, reset_lambda_client_cache
):
    """Test create_default_client still delegates endpoint handling to boto3."""
    mock_client = Mock()
    mock_boto_client.return_value = mock_client

    client = create_default_client()

    mock_boto_client.assert_called_once()
    call_args = mock_boto_client.call_args
    assert call_args[0][0] == "lambda"
    assert "config" in call_args[1]
    config = call_args[1]["config"]
    assert config.connect_timeout == 5
    assert config.read_timeout == 50
    assert config.user_agent_extra == f"async-durable-execution/{__version__}-async"
    assert client is mock_client


async def test_lambda_client_get_execution_state():
    """Test ThreadedSyncLambdaClient.get_execution_state method."""
    mock_client = Mock()
    mock_client.get_durable_execution_state.return_value = {
        "Operations": [{"Id": "op1", "Type": "STEP", "Status": "SUCCEEDED"}]
    }

    lambda_client = ThreadedSyncLambdaClient(mock_client)
    result = await lambda_client.get_execution_state(
        "arn123", "token123", "marker", 500
    )

    mock_client.get_durable_execution_state.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Marker="marker",
        MaxItems=500,
    )
    assert len(result.operations) == 1


async def test_durable_service_client_protocol_get_execution_state():
    """Test DurableServiceClient protocol get_execution_state method signature."""
    mock_client = Mock(spec=DurableServiceClient)
    mock_output = StateOutput(operations=[], next_marker="marker")
    mock_client.get_execution_state.return_value = mock_output

    result = await mock_client.get_execution_state("arn123", "token", "marker", 1000)

    mock_client.get_execution_state.assert_called_once_with(
        "arn123", "token", "marker", 1000
    )
    assert result == mock_output


@patch("async_durable_execution.client.create_default_client")
async def test_lambda_client_constructor_uses_default_factory_when_client_is_none(
    mock_create_default_client,
):
    """Test constructor uses create_default_client when no client is provided."""
    mock_client = Mock()
    mock_create_default_client.return_value = mock_client

    client = ThreadedSyncLambdaClient(None)

    mock_create_default_client.assert_called_once_with()
    assert client.client is mock_client


async def test_checkpoint_error_handling():
    """Test CheckpointError exception handling in ThreadedSyncLambdaClient.checkpoint."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.side_effect = Exception("API Error")

    lambda_client = ThreadedSyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="test",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    with pytest.raises(CheckpointError):
        await lambda_client.checkpoint("arn:test", "token", [update], None)


@patch("async_durable_execution.client.create_default_client")
async def test_lambda_client_constructor_uses_provided_client_without_default_factory(
    mock_create_default_client,
):
    """Test constructor keeps a provided client and skips create_default_client."""
    mock_client = Mock()

    client = ThreadedSyncLambdaClient(mock_client)

    mock_create_default_client.assert_not_called()
    assert client.client is mock_client


async def test_lambda_client_checkpoint_with_non_none_client_token():
    """Test ThreadedSyncLambdaClient.checkpoint with non-None client_token."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = ThreadedSyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="test",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = await lambda_client.checkpoint(
        "arn:test", "token", [update], "client_token_123"
    )

    # Verify ClientToken was passed
    mock_client.checkpoint_durable_execution.assert_called_once()
    call_args = mock_client.checkpoint_durable_execution.call_args[1]
    assert call_args["ClientToken"] == "client_token_123"
    assert result.checkpoint_token == "new_token"  # noqa: S105


@patch("async_durable_execution.client.create_default_client")
async def test_lambda_client_constructor_calls_default_factory_per_instance(
    mock_create_default_client,
):
    """Test each client(None) construction uses the default client factory."""
    mock_client_1 = Mock()
    mock_client_2 = Mock()
    mock_create_default_client.side_effect = [mock_client_1, mock_client_2]

    client1 = ThreadedSyncLambdaClient(None)
    client2 = ThreadedSyncLambdaClient(None)

    assert mock_create_default_client.call_count == 2
    assert client1.client is mock_client_1
    assert client2.client is mock_client_2
