"""Tests for invoker module."""

import json
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest

from async_durable_execution import (
    DurableContext,
    durable_execution,
)
from async_durable_execution.context import get_current_context
from async_durable_execution.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationOutput,
    InitialExecutionState,
    InvocationStatus,
)
from async_durable_execution.models import (
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
)
from async_durable_execution_runner.execution import Execution
from async_durable_execution_runner.invoker import (
    _LAMBDA_CLIENT_CONFIG,
    InProcessInvoker,
    LambdaInvoker,
    create_test_lambda_context,
)
from async_durable_execution_runner.model import (
    LambdaContext,
    StartDurableExecutionInput,
)


def test_create_test_lambda_context():
    """Test creating a test lambda context."""
    context = create_test_lambda_context()

    assert (
        context.invoked_function_arn
        == "arn:aws:lambda:us-west-2:123456789012:function:test-function"
    )
    assert context.tenant_id == "test-tenant-789"
    assert context.client_context is not None


def test_in_process_invoker_init():
    """Test InProcessInvoker initialization."""
    handler = Mock()
    service_client = Mock()

    invoker = InProcessInvoker(handler, service_client)

    assert invoker.handler is handler
    assert invoker.service_client is service_client


def test_in_process_invoker_create_invocation_input():
    """Test creating invocation input for in-process invoker."""
    handler = Mock()
    service_client = Mock()
    invoker = InProcessInvoker(handler, service_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)

    invocation_input = invoker.create_invocation_input(execution)

    assert isinstance(invocation_input, DurableExecutionInvocationInput)
    assert invocation_input.durable_execution_arn == execution.durable_execution_arn
    assert invocation_input.checkpoint_token is not None
    assert isinstance(invocation_input.initial_execution_state, InitialExecutionState)


async def test_in_process_invoker_invoke():
    """Test invoking function with in-process invoker."""
    # Mock handler that returns a valid response
    handler = Mock()
    handler.return_value = {"Status": "SUCCEEDED", "Result": "test-result"}

    service_client = Mock()
    invoker = InProcessInvoker(handler, service_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",  # noqa: S106
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    response = await invoker.invoke("test-function", input_data)

    assert isinstance(response.invocation_output, DurableExecutionInvocationOutput)
    assert response.invocation_output.status == InvocationStatus.SUCCEEDED
    assert response.invocation_output.result == "test-result"
    assert isinstance(response.request_id, str)

    # Verify handler was called with correct arguments
    handler.assert_called_once()
    call_args = handler.call_args[0]
    assert call_args[0] == input_data.to_json_dict()
    assert isinstance(call_args[1], LambdaContext)


async def test_in_process_invoker_binds_service_client_to_decorated_handler():
    """Test in-process invoker rebinds decorated handlers to the runner client."""
    service_client = Mock()

    @durable_execution
    async def handler(event: Any) -> dict:
        durable_context = cast(DurableContext, get_current_context())
        assert event == {"hello": "world"}
        assert durable_context.execution_state._service_client is service_client  # noqa: SLF001
        return {"result": "test-result"}

    invoker = InProcessInvoker(handler, service_client)

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation",
        input='{"hello": "world"}',
    )
    execution = Execution.new(start_input)
    execution.start()
    input_data = invoker.create_invocation_input(execution)

    response = await invoker.invoke("test-function", input_data)

    assert response.invocation_output.status == InvocationStatus.SUCCEEDED
    assert response.invocation_output.result == '{"result": "test-result"}'


def test_lambda_invoker_init():
    """Test LambdaInvoker initialization."""
    lambda_client = Mock()

    invoker = LambdaInvoker(lambda_client)

    assert invoker.lambda_client is lambda_client


def test_lambda_invoker_create():
    """Test creating LambdaInvoker with boto3 client."""
    with patch("async_durable_execution_runner.invoker.boto3") as mock_boto3:
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        invoker = LambdaInvoker.create("http://localhost:3001", "us-west-2")

        assert isinstance(invoker, LambdaInvoker)
        assert invoker.lambda_client is mock_client
        mock_boto3.client.assert_called_once_with(
            "lambda",
            endpoint_url="http://localhost:3001",
            region_name="us-west-2",
            config=_LAMBDA_CLIENT_CONFIG,
        )


def test_lambda_invoker_create_invocation_input():
    """Test creating invocation input for lambda invoker."""
    lambda_client = Mock()
    invoker = LambdaInvoker(lambda_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation",
    )
    execution = Execution.new(input_data)

    invocation_input = invoker.create_invocation_input(execution)

    assert isinstance(invocation_input, DurableExecutionInvocationInput)
    assert invocation_input.durable_execution_arn == execution.durable_execution_arn
    assert invocation_input.checkpoint_token is not None
    assert isinstance(invocation_input.initial_execution_state, InitialExecutionState)


async def test_lambda_invoker_invoke_success():
    """Test successful lambda invocation."""
    lambda_client = Mock()

    # Mock successful response
    mock_payload = Mock()
    mock_payload.read.return_value = json.dumps(
        {"Status": "SUCCEEDED", "Result": "lambda-result"}
    ).encode("utf-8")

    lambda_client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": mock_payload,
        "ResponseMetadata": {"HTTPHeaders": {"x-amzn-RequestId": "test-request-id"}},
    }

    invoker = LambdaInvoker(lambda_client)

    mock_operation = Operation(
        operation_id="op-1",
        parent_id=None,
        name="test-execution",
        start_timestamp=datetime.now(timezone.utc),
        end_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        execution_details=ExecutionDetails(input_payload='{"test": "data"}'),
    )

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",  # noqa: S106
        initial_execution_state=InitialExecutionState(
            operations=[mock_operation], next_marker=""
        ),
    )

    response = await invoker.invoke("test-function", input_data)

    assert isinstance(response.invocation_output, DurableExecutionInvocationOutput)
    assert response.invocation_output.status == InvocationStatus.SUCCEEDED
    assert response.invocation_output.result == "lambda-result"
    assert response.request_id == "test-request-id"

    # Verify lambda client was called correctly
    lambda_client.invoke.assert_called_once_with(
        FunctionName="test-function",
        InvocationType="RequestResponse",
        Payload=json.dumps(input_data.to_json_dict()),
    )


async def test_lambda_invoker_invoke_failure():
    """Test lambda invocation failure."""
    from async_durable_execution_runner.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Mock failed response
    mock_payload = Mock()
    lambda_client.invoke.return_value = {
        "StatusCode": 500,
        "Payload": mock_payload,
    }

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",  # noqa: S106
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        DurableFunctionsTestError,
        match="Lambda invocation failed with status code: 500",
    ):
        await invoker.invoke("test-function", input_data)


async def test_in_process_invoker_invoke_with_execution_operations():
    """Test in-process invoker with execution that has operations."""
    handler = Mock()
    handler.return_value = {"Status": "SUCCEEDED", "Result": None}

    service_client = Mock()
    invoker = InProcessInvoker(handler, service_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation",
    )
    execution = Execution.new(input_data)
    execution.start()  # This adds operations

    invocation_input = invoker.create_invocation_input(execution)
    response = await invoker.invoke("test-function", invocation_input)

    assert isinstance(response.invocation_output, DurableExecutionInvocationOutput)
    assert isinstance(response.request_id, str)
    assert response.invocation_output.status == InvocationStatus.SUCCEEDED
    assert len(invocation_input.initial_execution_state.operations) > 0


def test_lambda_invoker_create_invocation_input_with_operations():
    """Test lambda invoker creating input with execution operations."""
    lambda_client = Mock()
    invoker = LambdaInvoker(lambda_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation",
    )
    execution = Execution.new(input_data)
    execution.start()  # This adds operations

    invocation_input = invoker.create_invocation_input(execution)

    assert isinstance(invocation_input, DurableExecutionInvocationInput)
    assert len(invocation_input.initial_execution_state.operations) > 0
    assert invocation_input.initial_execution_state.next_marker == ""


async def test_lambda_invoker_invoke_empty_function_name():
    """Test lambda invocation with empty function name."""
    from async_durable_execution_runner.exceptions import (
        InvalidParameterValueException,
    )

    lambda_client = Mock()
    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        InvalidParameterValueException, match="Function name is required"
    ):
        await invoker.invoke("", input_data)


async def test_lambda_invoker_invoke_whitespace_function_name():
    """Test lambda invocation with whitespace-only function name."""
    from async_durable_execution_runner.exceptions import (
        InvalidParameterValueException,
    )

    lambda_client = Mock()
    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        InvalidParameterValueException, match="Function name is required"
    ):
        await invoker.invoke("   ", input_data)


async def test_lambda_invoker_invoke_status_202():
    """Test lambda invocation with status code 202."""
    lambda_client = Mock()

    mock_payload = Mock()
    mock_payload.read.return_value = json.dumps(
        {"Status": "SUCCEEDED", "Result": "async-result"}
    ).encode("utf-8")

    lambda_client.invoke.return_value = {
        "StatusCode": 202,
        "Payload": mock_payload,
        "ResponseMetadata": {
            "HTTPHeaders": {"x-amzn-RequestId": "test-request-id-202"}
        },
    }

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    response = await invoker.invoke("test-function", input_data)
    assert isinstance(response.invocation_output, DurableExecutionInvocationOutput)
    assert response.request_id == "test-request-id-202"


async def test_lambda_invoker_invoke_function_error():
    """Test lambda invocation with function error."""
    from async_durable_execution_runner.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    mock_payload = Mock()
    mock_payload.read.return_value = b'{"errorMessage": "Function failed"}'

    lambda_client.invoke.return_value = {
        "StatusCode": 200,
        "FunctionError": "Unhandled",
        "Payload": mock_payload,
    }

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Lambda invocation failed with status 200"
    ):
        await invoker.invoke("test-function", input_data)


def _create_mock_lambda_client_with_exceptions():
    """Helper to create mock lambda client with all exception types."""
    lambda_client = Mock()

    class MockException(Exception):
        pass

    exceptions_mock = Mock()
    for exc_name in [
        "ResourceNotFoundException",
        "InvalidParameterValueException",
        "TooManyRequestsException",
        "ServiceException",
        "ResourceConflictException",
        "InvalidRequestContentException",
        "RequestTooLargeException",
        "UnsupportedMediaTypeException",
        "InvalidRuntimeException",
        "InvalidZipFileException",
        "ResourceNotReadyException",
        "SnapStartTimeoutException",
        "SnapStartNotReadyException",
        "SnapStartException",
        "RecursiveInvocationException",
        "InvalidSecurityGroupIDException",
        "EC2ThrottledException",
        "EFSMountConnectivityException",
        "SubnetIPAddressLimitReachedException",
        "EC2UnexpectedException",
        "InvalidSubnetIDException",
        "EC2AccessDeniedException",
        "EFSIOException",
        "ENILimitReachedException",
        "EFSMountTimeoutException",
        "EFSMountFailureException",
        "KMSAccessDeniedException",
        "KMSDisabledException",
        "KMSNotFoundException",
        "KMSInvalidStateException",
    ]:
        setattr(exceptions_mock, exc_name, MockException)

    lambda_client.exceptions = exceptions_mock
    return lambda_client, MockException


async def test_lambda_invoker_invoke_resource_not_found():
    """Test lambda invocation with ResourceNotFoundException."""
    from async_durable_execution_runner.exceptions import (
        ResourceNotFoundException,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Create specific exception for ResourceNotFoundException
    class MockResourceNotFoundException(Exception):
        pass

    lambda_client.exceptions.ResourceNotFoundException = MockResourceNotFoundException

    lambda_client.invoke.side_effect = MockResourceNotFoundException(
        "Function not found"
    )

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        ResourceNotFoundException, match="Function not found: test-function"
    ):
        await invoker.invoke("test-function", input_data)


async def test_lambda_invoker_invoke_invalid_parameter():
    """Test lambda invocation with InvalidParameterValueException."""
    from async_durable_execution_runner.exceptions import (
        InvalidParameterValueException,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Override specific exception for this test
    class MockInvalidParameterValueException(Exception):
        pass

    lambda_client.exceptions.InvalidParameterValueException = (
        MockInvalidParameterValueException
    )

    lambda_client.invoke.side_effect = MockInvalidParameterValueException(
        "Invalid param"
    )

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(InvalidParameterValueException, match="Invalid parameter"):
        await invoker.invoke("test-function", input_data)


async def test_lambda_invoker_invoke_service_exception():
    """Test lambda invocation with ServiceException."""
    from async_durable_execution_runner.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Create specific exception for ServiceException
    class MockServiceException(Exception):
        pass

    lambda_client.exceptions.ServiceException = MockServiceException

    lambda_client.invoke.side_effect = MockServiceException("Service error")

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(DurableFunctionsTestError, match="Lambda invocation failed"):
        await invoker.invoke("test-function", input_data)


async def test_lambda_invoker_invoke_ec2_exception():
    """Test lambda invocation with EC2 exception."""
    from async_durable_execution_runner.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Create specific exception for EC2AccessDeniedException
    class MockEC2Exception(Exception):
        pass

    lambda_client.exceptions.EC2AccessDeniedException = MockEC2Exception

    lambda_client.invoke.side_effect = MockEC2Exception("Access denied")

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(DurableFunctionsTestError, match="Lambda infrastructure error"):
        await invoker.invoke("test-function", input_data)


async def test_lambda_invoker_invoke_kms_exception():
    """Test lambda invocation with KMS exception."""
    from async_durable_execution_runner.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Create specific exception for KMSAccessDeniedException
    class MockKMSException(Exception):
        pass

    lambda_client.exceptions.KMSAccessDeniedException = MockKMSException

    lambda_client.invoke.side_effect = MockKMSException("KMS access denied")

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(DurableFunctionsTestError, match="Lambda KMS error"):
        await invoker.invoke("test-function", input_data)


async def test_lambda_invoker_invoke_durable_execution_already_started():
    """Test lambda invocation with DurableExecutionAlreadyStartedException."""
    from async_durable_execution_runner.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    class MockDurableExecutionAlreadyStartedException(Exception):
        pass

    MockDurableExecutionAlreadyStartedException.__name__ = (
        "DurableExecutionAlreadyStartedException"
    )

    lambda_client.invoke.side_effect = MockDurableExecutionAlreadyStartedException(
        "Already started"
    )

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Durable execution already started"
    ):
        await invoker.invoke("test-function", input_data)


async def test_lambda_invoker_invoke_unexpected_exception():
    """Test lambda invocation with unexpected exception."""
    from async_durable_execution_runner.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()
    lambda_client.invoke.side_effect = RuntimeError("Unexpected error")

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Unexpected error during Lambda invocation"
    ):
        await invoker.invoke("test-function", input_data)
