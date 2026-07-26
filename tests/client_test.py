"""Tests for the service module."""

import asyncio
import datetime
from datetime import timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from async_durable_execution.__about__ import __version__
from async_durable_execution.core.exceptions import (
    CallableRuntimeError,
    CheckpointError,
    GetExecutionStateError,
)
from async_durable_execution.core.models import OperationIdentifier
from async_durable_execution.core.models import (
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
from async_durable_execution.core.client import (
    AsyncLambdaClient,
    ThreadedSyncLambdaClient,
    _AiobotocoreLambdaApiClient,
    aioboto_is_installed,
    create_default_async_client,
    create_default_client,
    create_default_service_client,
    create_default_sync_client,
    lambda_api_client_is_async,
)
from async_durable_execution.core.client import DurableServiceClient


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def reset_lambda_client_cache():
    """Reset the class-level botocore client cache before and after each test."""
    ThreadedSyncLambdaClient._cached_boto_client = None  # noqa: SLF001
    yield
    ThreadedSyncLambdaClient._cached_boto_client = None  # noqa: SLF001


@patch("async_durable_execution.core.client.get_session")
async def test_lambda_client_checkpoint(_mock_get_session):
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


@pytest.mark.parametrize("token", [None, ""])
async def test_lambda_client_checkpoint_rejects_missing_token(token):
    """Sync-backed checkpoint calls reject missing tokens before the API call."""
    mock_client = Mock()
    lambda_client = ThreadedSyncLambdaClient(mock_client)

    with pytest.raises(
        CheckpointError, match="Cannot checkpoint without a checkpoint token"
    ):
        await lambda_client.checkpoint("arn123", token, [], None)

    mock_client.checkpoint_durable_execution.assert_not_called()


@pytest.mark.parametrize("token", [None, ""])
async def test_lambda_client_get_execution_state_rejects_missing_token(token):
    """Sync-backed state calls reject missing tokens before the API call."""
    mock_client = Mock()
    lambda_client = ThreadedSyncLambdaClient(mock_client)

    with pytest.raises(
        GetExecutionStateError,
        match="Cannot get execution state without a checkpoint token",
    ):
        await lambda_client.get_execution_state("arn123", token, "marker")

    mock_client.get_durable_execution_state.assert_not_called()


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


@patch("async_durable_execution.core.client.logger")
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


@patch("async_durable_execution.core.client.logger")
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


async def test_async_lambda_client_checkpoint():
    """Test AsyncLambdaClient.checkpoint method."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution = AsyncMock(
        return_value={
            "CheckpointToken": "new_token",
            "NewExecutionState": {"Operations": []},
        }
    )
    lambda_client = AsyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = await lambda_client.checkpoint("arn123", "token123", [update], None)

    mock_client.checkpoint_durable_execution.assert_awaited_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


async def test_async_lambda_client_get_execution_state():
    """Test AsyncLambdaClient.get_execution_state method."""
    mock_client = Mock()
    mock_client.get_durable_execution_state = AsyncMock(
        return_value={"Operations": [], "CheckpointToken": "new_token"}
    )
    lambda_client = AsyncLambdaClient(mock_client)

    result = await lambda_client.get_execution_state("arn123", "token123", "marker")

    mock_client.get_durable_execution_state.assert_awaited_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Marker="marker",
        MaxItems=1000,
    )
    assert isinstance(result, StateOutput)
    assert result.operations == []


@pytest.mark.parametrize("token", [None, ""])
async def test_async_lambda_client_checkpoint_rejects_missing_token(token):
    """Async checkpoint calls reject missing tokens before the API call."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution = AsyncMock()
    lambda_client = AsyncLambdaClient(mock_client)

    with pytest.raises(
        CheckpointError, match="Cannot checkpoint without a checkpoint token"
    ):
        await lambda_client.checkpoint("arn123", token, [], None)

    mock_client.checkpoint_durable_execution.assert_not_awaited()


@pytest.mark.parametrize("token", [None, ""])
async def test_async_lambda_client_get_execution_state_rejects_missing_token(token):
    """Async state calls reject missing tokens before the API call."""
    mock_client = Mock()
    mock_client.get_durable_execution_state = AsyncMock()
    lambda_client = AsyncLambdaClient(mock_client)

    with pytest.raises(
        GetExecutionStateError,
        match="Cannot get execution state without a checkpoint token",
    ):
        await lambda_client.get_execution_state("arn123", token, "marker")

    mock_client.get_durable_execution_state.assert_not_awaited()


@patch.dict("os.environ", {}, clear=True)
@patch("async_durable_execution.core.client.get_session")
async def test_create_default_sync_client_builds_lambda_client_with_expected_config(
    mock_get_session, reset_lambda_client_cache
):
    """Test create_default_sync_client builds a lambda botocore client with expected config."""
    mock_client = Mock()
    mock_get_session.return_value.create_client.return_value = mock_client

    client = create_default_sync_client()

    mock_get_session.assert_called_once_with()
    mock_get_session.return_value.create_client.assert_called_once()
    call_args = mock_get_session.return_value.create_client.call_args
    assert call_args[0][0] == "lambda"
    assert "config" in call_args[1]
    config = call_args[1]["config"]
    assert config.connect_timeout == 5
    assert config.read_timeout == 50
    assert (
        config.user_agent_extra == f"durable-execution-sdk-python/{__version__}-async"
    )
    assert client is mock_client


@patch("async_durable_execution.core.client.create_default_async_client")
@patch("async_durable_execution.core.client.aioboto_is_installed", return_value=True)
async def test_create_default_client_uses_async_client_when_aioboto_is_installed(
    _mock_aioboto_is_installed,
    mock_create_default_async_client,
):
    """Test create_default_client prefers the async Lambda client when available."""
    mock_client = Mock()
    mock_create_default_async_client.return_value = mock_client

    client = create_default_client()

    assert client is mock_client
    mock_create_default_async_client.assert_called_once_with()


@patch("async_durable_execution.core.client.create_default_sync_client")
@patch("async_durable_execution.core.client.aioboto_is_installed", return_value=False)
async def test_create_default_client_uses_sync_client_when_aioboto_is_missing(
    _mock_aioboto_is_installed,
    mock_create_default_sync_client,
):
    """Test create_default_client falls back to the sync Lambda client."""
    mock_client = Mock()
    mock_create_default_sync_client.return_value = mock_client

    client = create_default_client()

    assert client is mock_client
    mock_create_default_sync_client.assert_called_once_with()


@patch("async_durable_execution.core.client.importlib.import_module")
async def test_create_default_async_client_builds_lambda_client_with_expected_config(
    mock_import_module,
):
    """Test create_default_async_client builds a lambda aioboto client."""
    mock_client = Mock()
    mock_session = Mock()
    mock_aiobotocore_session = Mock()
    mock_aiobotocore_session.get_session.return_value = mock_session
    mock_session.create_client.return_value = mock_client
    mock_import_module.return_value = mock_aiobotocore_session

    client = create_default_async_client()

    mock_import_module.assert_called_once_with("aiobotocore.session")
    mock_aiobotocore_session.get_session.assert_called_once_with()
    mock_session.create_client.assert_called_once()
    call_args = mock_session.create_client.call_args
    assert call_args[0][0] == "lambda"
    assert "config" in call_args[1]
    config = call_args[1]["config"]
    assert config.connect_timeout == 5
    assert config.read_timeout == 50
    assert (
        config.user_agent_extra == f"durable-execution-sdk-python/{__version__}-async"
    )
    assert client._client_context is mock_client  # noqa: SLF001


async def test_aiobotocore_lambda_api_client_closes_entered_context():
    """Test aiobotocore client context is exited after async client use."""
    entered_client = Mock()
    entered_client.checkpoint_durable_execution = AsyncMock(
        return_value={
            "CheckpointToken": "new-token",
            "NewExecutionState": {"Operations": []},
        }
    )
    client_context = Mock()
    client_context.__aenter__ = AsyncMock(return_value=entered_client)
    client_context.__aexit__ = AsyncMock()
    client = _AiobotocoreLambdaApiClient(client_context)

    await client.checkpoint_durable_execution()
    await client.aclose()

    client_context.__aenter__.assert_awaited_once_with()
    client_context.__aexit__.assert_awaited_once_with(None, None, None)
    assert client._client is None  # noqa: SLF001


async def test_aiobotocore_lambda_api_client_reuses_context_for_state_requests():
    entered_client = Mock()
    entered_client.get_durable_execution_state = AsyncMock(
        return_value={"Operations": []}
    )
    client_context = Mock()
    client_context.__aenter__ = AsyncMock(return_value=entered_client)
    client = _AiobotocoreLambdaApiClient(client_context)

    result = await client.get_durable_execution_state(CheckpointToken="token")

    assert result == {"Operations": []}
    entered_client.get_durable_execution_state.assert_awaited_once_with(
        CheckpointToken="token"
    )
    client_context.__aenter__.assert_awaited_once_with()


async def test_aiobotocore_lambda_api_client_close_before_enter_is_noop():
    client_context = Mock()
    client_context.__aexit__ = AsyncMock()
    client = _AiobotocoreLambdaApiClient(client_context)

    await client.aclose()

    client_context.__aexit__.assert_not_called()


async def test_async_lambda_client_closes_wrapped_client():
    """Test AsyncLambdaClient closes wrapped SDK-owned async clients."""
    wrapped_client = Mock()
    wrapped_client.aclose = AsyncMock()
    client = AsyncLambdaClient(wrapped_client)

    await client.aclose()

    wrapped_client.aclose.assert_awaited_once_with()


async def test_async_lambda_client_aclose_without_close_method_is_noop():
    wrapped_client = Mock(spec=[])
    client = AsyncLambdaClient(wrapped_client)

    await client.aclose()


async def test_async_lambda_client_checkpoint_passes_client_token():
    mock_client = Mock()
    mock_client.checkpoint_durable_execution = AsyncMock(
        return_value={
            "CheckpointToken": "new_token",
            "NewExecutionState": {"Operations": []},
        }
    )
    lambda_client = AsyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = await lambda_client.checkpoint(
        "arn123", "token123", [update], "client-token-123"
    )

    mock_client.checkpoint_durable_execution.assert_awaited_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
        ClientToken="client-token-123",
    )
    assert result.checkpoint_token == "new_token"  # noqa: S105


async def test_async_lambda_client_checkpoint_wraps_errors():
    mock_client = Mock()
    mock_client.checkpoint_durable_execution = AsyncMock(
        side_effect=RuntimeError("bad")
    )
    lambda_client = AsyncLambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    with pytest.raises(CheckpointError):
        await lambda_client.checkpoint("arn123", "token123", [update], None)


async def test_async_lambda_client_get_execution_state_wraps_errors():
    mock_client = Mock()
    mock_client.get_durable_execution_state = AsyncMock(side_effect=RuntimeError("bad"))
    lambda_client = AsyncLambdaClient(mock_client)

    with pytest.raises(GetExecutionStateError):
        await lambda_client.get_execution_state("arn123", "token123", "marker")


def test_lambda_api_client_is_async_detects_sync_and_async_methods():
    sync_client = Mock()

    async_client = Mock()
    async_client.checkpoint_durable_execution = AsyncMock()

    assert lambda_api_client_is_async(sync_client) is False
    assert lambda_api_client_is_async(async_client) is True


@pytest.mark.parametrize(
    ("find_spec_result", "expected"), [(object(), True), (None, False)]
)
@patch("async_durable_execution.core.client.importlib.util.find_spec")
def test_aioboto_is_installed_checks_for_aiobotocore(
    mock_find_spec,
    find_spec_result,
    expected,
):
    mock_find_spec.return_value = find_spec_result

    assert aioboto_is_installed() is expected
    mock_find_spec.assert_called_once_with("aiobotocore")


@patch("async_durable_execution.core.client.create_default_client")
@patch("async_durable_execution.core.client.aioboto_is_installed", return_value=True)
async def test_create_default_service_client_uses_aioboto_when_installed(
    _mock_aioboto_is_installed,
    mock_create_default_client,
):
    """Test create_default_service_client uses aioboto by default when installed."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution = AsyncMock()
    mock_create_default_client.return_value = mock_client

    service_client = create_default_service_client()

    assert isinstance(service_client, AsyncLambdaClient)
    assert service_client.client is mock_client
    mock_create_default_client.assert_called_once_with()


@patch("async_durable_execution.core.client.aioboto_is_installed", return_value=True)
async def test_create_default_service_client_uses_explicit_botocore_client(
    _mock_aioboto_is_installed,
):
    """Test explicit botocore clients keep using the sync adapter."""
    mock_client = Mock()

    service_client = create_default_service_client(mock_client)

    assert isinstance(service_client, ThreadedSyncLambdaClient)
    assert service_client.client is mock_client


@patch("async_durable_execution.core.client.create_default_client")
@patch("async_durable_execution.core.client.aioboto_is_installed", return_value=False)
async def test_create_default_service_client_uses_sync_client_when_aioboto_missing(
    _mock_aioboto_is_installed,
    mock_create_default_client,
):
    """Test default service client falls back to botocore when aioboto is missing."""
    mock_client = Mock()
    mock_create_default_client.return_value = mock_client

    service_client = create_default_service_client()

    assert isinstance(service_client, ThreadedSyncLambdaClient)
    assert service_client.client is mock_client
    mock_create_default_client.assert_called_once_with()


@patch("async_durable_execution.core.client.aioboto_is_installed", return_value=False)
async def test_create_default_service_client_uses_explicit_async_client(
    _mock_aioboto_is_installed,
):
    """Test explicit async Lambda clients use the async adapter."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution = AsyncMock()

    service_client = create_default_service_client(mock_client)

    assert isinstance(service_client, AsyncLambdaClient)
    assert service_client.client is mock_client


@patch.dict("os.environ", {"AWS_ENDPOINT_URL_LAMBDA": "http://localhost:3000"})
@patch("async_durable_execution.core.client.get_session")
async def test_create_default_sync_client_builds_botocore_client_with_lambda_endpoint_env(
    mock_get_session, reset_lambda_client_cache
):
    """Test create_default_sync_client delegates endpoint handling to botocore."""
    mock_client = Mock()
    mock_get_session.return_value.create_client.return_value = mock_client

    client = create_default_sync_client()

    mock_get_session.assert_called_once_with()
    mock_get_session.return_value.create_client.assert_called_once()
    call_args = mock_get_session.return_value.create_client.call_args
    assert call_args[0][0] == "lambda"
    assert "config" in call_args[1]
    config = call_args[1]["config"]
    assert config.connect_timeout == 5
    assert config.read_timeout == 50
    assert (
        config.user_agent_extra == f"durable-execution-sdk-python/{__version__}-async"
    )
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


@patch("async_durable_execution.core.client.create_default_sync_client")
async def test_lambda_client_constructor_uses_default_factory_when_client_is_none(
    mock_create_default_sync_client,
):
    """Test constructor uses create_default_sync_client when no client is provided."""
    mock_client = Mock()
    mock_create_default_sync_client.return_value = mock_client

    client = ThreadedSyncLambdaClient(None)

    mock_create_default_sync_client.assert_called_once_with()
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


@patch("async_durable_execution.core.client.create_default_sync_client")
async def test_lambda_client_constructor_uses_provided_client_without_default_factory(
    mock_create_default_sync_client,
):
    """Test constructor keeps a provided client and skips create_default_sync_client."""
    mock_client = Mock()

    client = ThreadedSyncLambdaClient(mock_client)

    mock_create_default_sync_client.assert_not_called()
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


@patch("async_durable_execution.core.client.create_default_sync_client")
async def test_lambda_client_constructor_calls_default_factory_per_instance(
    mock_create_default_sync_client,
):
    """Test each client(None) construction uses the default client factory."""
    mock_client_1 = Mock()
    mock_client_2 = Mock()
    mock_create_default_sync_client.side_effect = [mock_client_1, mock_client_2]

    client1 = ThreadedSyncLambdaClient(None)
    client2 = ThreadedSyncLambdaClient(None)

    assert mock_create_default_sync_client.call_count == 2
    assert client1.client is mock_client_1
    assert client2.client is mock_client_2
