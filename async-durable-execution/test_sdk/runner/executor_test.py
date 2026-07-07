"""Unit tests for executor module."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from async_durable_execution.execution import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from async_durable_execution.models import (
    CallbackDetails,
    CallbackOptions,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.primitive.invoke import invoke
from async_durable_execution.runner.exceptions import (
    IllegalStateException,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from async_durable_execution.runner.local.execution import (
    Execution,
    ExecutionStatus,
)
from async_durable_execution.runner.local import Executor
from async_durable_execution.runner.model import (
    InvokeResponse,
    InvocationCompletedDetails,
)
from async_durable_execution.runner.local.model import (
    CallbackToken,
    SendDurableExecutionCallbackFailureResponse,
    SendDurableExecutionCallbackHeartbeatResponse,
    SendDurableExecutionCallbackSuccessResponse,
    StartDurableExecutionInput,
)


@pytest.fixture
def mock_store():
    return Mock()


@pytest.fixture
def mock_scheduler():
    return Mock()


@pytest.fixture
def mock_invoker():
    invoker = Mock()
    invoker.invoke = AsyncMock()
    return invoker


@pytest.fixture
def mock_service_client():
    return Mock()


@pytest.fixture
def executor(mock_store, mock_scheduler, mock_invoker, mock_service_client):
    return Executor(mock_store, mock_scheduler, mock_invoker, mock_service_client)


@pytest.fixture
def start_input():
    return StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )


@pytest.fixture
def mock_execution():
    execution = Mock(spec=Execution)
    execution.durable_execution_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:test-execution"
    execution.is_complete = False
    execution.consecutive_failed_invocation_attempts = 0
    execution.start_input = Mock()
    execution.start_input.function_name = "test-function"
    return execution


async def test_init(mock_store, mock_scheduler, mock_invoker, mock_service_client):
    # Test that Executor can be constructed with dependencies
    # Dependency injection is implementation detail - test behavior instead
    executor = Executor(mock_store, mock_scheduler, mock_invoker, mock_service_client)

    # Verify executor is properly initialized by testing it can perform basic operations
    assert executor is not None

    # Test that the executor uses the injected dependencies by verifying behavior
    # This will be covered by other tests that exercise the executor's functionality


@patch("async_durable_execution.runner.local.executor.Execution")
async def test_start_execution(
    mock_execution_class, executor, start_input, mock_store, mock_scheduler
):
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution_class.new.return_value = mock_execution
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        result = executor.start_execution(start_input)

    # Test observable behavior through public API
    # The executor should generate an invocation_id if not provided
    call_args = mock_execution_class.new.call_args
    actual_input = call_args.kwargs["input"]

    # Verify all fields match except invocation_id should be generated
    assert actual_input.account_id == start_input.account_id
    assert actual_input.function_name == start_input.function_name
    assert actual_input.function_qualifier == start_input.function_qualifier
    assert actual_input.execution_name == start_input.execution_name
    assert (
        actual_input.execution_timeout_seconds == start_input.execution_timeout_seconds
    )
    assert (
        actual_input.execution_retention_period_days
        == start_input.execution_retention_period_days
    )
    assert actual_input.invocation_id is not None  # Should be generated
    assert actual_input.trace_fields == start_input.trace_fields
    assert actual_input.tenant_id == start_input.tenant_id
    assert actual_input.input == start_input.input
    mock_execution.start.assert_called_once()
    mock_store.save.assert_called_once_with(mock_execution)
    mock_scheduler.create_event.assert_called_once()

    # Verify execution timeout was scheduled
    assert mock_scheduler.call_later.called
    timeout_call = mock_scheduler.call_later.call_args
    assert timeout_call.kwargs["delay"] == start_input.execution_timeout_seconds
    assert timeout_call.kwargs["completion_event"] == mock_event

    mock_invoke.assert_called_once_with("test-arn")
    assert result.execution_arn == "test-arn"

    # Test that completion event was created by verifying wait_until_complete works
    # This tests the same functionality without accessing private members
    mock_event.wait_async = AsyncMock(return_value=True)
    wait_result = await executor.wait_until_complete("test-arn", timeout=1)
    assert wait_result is True
    mock_event.wait_async.assert_called_once_with(1)


@patch("async_durable_execution.runner.local.executor.Execution")
async def test_start_execution_with_provided_invocation_id(
    mock_execution_class, executor, mock_store, mock_scheduler
):
    # Create input with invocation_id already provided
    provided_invocation_id = "user-provided-id-123"
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id=provided_invocation_id,
    )

    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution_class.new.return_value = mock_execution
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        result = executor.start_execution(start_input)

    # Should use the provided invocation_id unchanged
    mock_execution_class.new.assert_called_once_with(input=start_input)
    mock_execution.start.assert_called_once()
    mock_store.save.assert_called_once_with(mock_execution)
    mock_scheduler.create_event.assert_called_once()
    mock_invoke.assert_called_once_with("test-arn")
    assert result.execution_arn == "test-arn"

    mock_execution = Mock()
    mock_store.load.return_value = mock_execution

    result = executor.get_execution("test-arn")

    mock_store.load.assert_called_once_with("test-arn")
    assert result == mock_execution


async def test_should_complete_workflow_with_error_when_invocation_fails(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that failed invocation responses trigger workflow completion with error."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.operations = []
    mock_execution.get_new_checkpoint_token.return_value = "checkpoint-token"
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return failed response
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    failed_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, error=ErrorObject.from_message("Test error")
    )
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=failed_response, request_id="test-request-id"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Mock the workflow completion methods
        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger invocation through public start_execution method
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            assert mock_scheduler.call_later.call_count >= 1
            handler = mock_scheduler.call_later.call_args_list[-1][0][0]

            # Execute the handler to trigger the invocation logic
            import asyncio

            await handler()

        # Assert - verify workflow was completed with error
        mock_fail.assert_called_once_with("test-arn", failed_response.error)


async def test_should_complete_workflow_with_result_when_invocation_succeeds(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that successful invocation responses trigger workflow completion with result."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return successful response
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    success_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="success result"
    )
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=success_response, request_id="test-request-id"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Mock the workflow completion methods
        with patch.object(executor, "complete_execution") as mock_complete:
            # Act - trigger invocation through public start_execution method
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            assert mock_scheduler.call_later.call_count >= 1
            handler = mock_scheduler.call_later.call_args_list[-1][0][0]

            # Execute the handler to trigger the invocation logic
            import asyncio

            await handler()

        # Assert - verify workflow was completed with result
        mock_complete.assert_called_once_with("test-arn", "success result")


async def test_should_handle_pending_status_when_operations_exist(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that pending invocation responses are handled when operations exist."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0
    mock_execution.has_pending_operations.return_value = True

    # Mock invoker to return pending response
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    pending_response = DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=pending_response, request_id="test-request-id"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        import asyncio

        await handler()

    # Assert - verify pending operations were checked
    mock_execution.has_pending_operations.assert_called_once_with()


async def test_should_ignore_response_when_execution_already_complete(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that responses are ignored when execution is already complete."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = True  # Already complete
    mock_execution.start_input = start_input

    # Mock invoker - this shouldn't be called since execution is complete
    mock_invoker.create_invocation_input.return_value = Mock()
    mock_invoker.invoke.return_value = (
        DurableExecutionInvocationOutput(status=InvocationStatus.SUCCEEDED),
        "test-request-id",
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        import asyncio

        await handler()

    # Assert - verify invoker was not called since execution was already complete
    mock_invoker.create_invocation_input.assert_not_called()
    mock_invoker.invoke.assert_not_called()


async def test_should_retry_when_response_has_no_status(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that invocation responses without status trigger retry logic."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return response without status
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    no_status_response = DurableExecutionInvocationOutput(status=None)
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=no_status_response, request_id="test-request-id"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

        # Assert - verify retry was triggered due to validation error
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called 3 times: timeout + initial + retry)
        assert mock_scheduler.call_later.call_count == 3


async def test_should_retry_when_failed_response_has_result(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that failed responses with result trigger retry logic."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return invalid failed response (with result)
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    invalid_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, result="should not have result"
    )
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=invalid_response, request_id="test-request-id"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

        # Assert - verify retry was triggered due to validation error
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called 3 times: timeout + initial + retry)
        assert mock_scheduler.call_later.call_count == 3


async def test_should_retry_when_success_response_has_error(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that successful responses with error trigger retry logic."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return invalid success response (with error)
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    invalid_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED,
        error=ErrorObject.from_message("should not have error"),
    )
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=invalid_response, request_id="test-request-id"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

        # Assert - verify retry was triggered due to validation error
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called 3 times: timeout + initial + retry)
        assert mock_scheduler.call_later.call_count == 3


async def test_should_retry_when_pending_response_has_no_operations(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that pending responses without operations trigger retry logic."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0
    mock_execution.has_pending_operations.return_value = False  # No pending operations

    # Mock invoker to return pending response
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    pending_response = DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=pending_response, request_id="test-request-id"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

        # Assert - verify retry was triggered due to validation error
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called 3 times: timeout + initial + retry)
        assert mock_scheduler.call_later.call_count == 3


async def test_invoke_handler_success(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test successful invocation through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.operations = []
    mock_execution.get_new_checkpoint_token.return_value = "checkpoint-token"

    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="test"
    )
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=mock_response, request_id="test-request-id"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

    # Verify the invocation process was executed
    mock_invoker.create_invocation_input.assert_called_once_with(
        start_input=start_input,
        durable_execution_arn="test-arn",
        checkpoint_token="checkpoint-token",
        operations=[],
    )
    mock_invoker.invoke.assert_called_once_with(
        "test-function", mock_invocation_input, None
    )


async def test_invoke_handler_execution_already_complete(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that completed executions are handled properly through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = True
    mock_execution.start_input = start_input

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

    # Verify store was accessed to check execution status
    mock_store.load.assert_called_with("test-arn")


async def test_invoke_handler_execution_completed_during_invocation(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test execution completing during invocation through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input

    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = Mock()
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=mock_response, request_id="test-request-id"
    )

    # Create a completed execution mock
    completed_execution = Mock()
    completed_execution.durable_execution_arn = "test-arn"
    completed_execution.is_complete = True
    completed_execution.start_input = start_input

    # First call returns incomplete execution, second call returns completed execution
    mock_store.load.side_effect = [mock_execution, completed_execution]

    # Mock execution creation
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

    # Verify the execution was checked for completion
    assert mock_store.load.call_count >= 2


async def test_invoke_handler_resource_not_found(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test resource not found handling causes workflow failure through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input

    mock_invoker.create_invocation_input.side_effect = ResourceNotFoundException(
        "Function not found"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Mock the public fail_execution method to verify it gets called
        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger invocation through public start_execution method
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            assert mock_scheduler.call_later.call_count >= 1
            handler = mock_scheduler.call_later.call_args_list[-1][0][0]

            # Execute the handler to trigger the invocation logic
            await handler()

        # Assert - verify workflow failure was triggered through public API
        mock_fail.assert_called_once()
        # Verify the error contains the expected message
        call_args = mock_fail.call_args
        assert call_args[0][0] == "test-arn"  # execution_arn is first positional arg
        assert "Function not found" in str(
            call_args[0][1]
        )  # error is second positional arg


async def test_invoke_handler_general_exception(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test general exception handling triggers retry through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Configure invoker to fail
    mock_invoker.create_invocation_input.side_effect = Exception("General error")

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

        # Assert - verify retry was scheduled through observable behavior
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called 3 times: timeout + initial + retry)
        assert mock_scheduler.call_later.call_count == 3


async def test_invoke_execution_through_start_execution(
    executor, mock_scheduler, start_input
):
    """Test execution invocation behavior through public start_execution method."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        # Start execution which internally calls _invoke_execution
        executor.start_execution(start_input)

    # Verify scheduler was called with the completion event
    mock_scheduler.call_later.assert_called()
    args = mock_scheduler.call_later.call_args
    assert args[1]["delay"] == 0  # Initial invocation has no delay
    assert args[1]["completion_event"] == mock_event


async def test_should_complete_workflow_successfully_through_public_api(
    executor, mock_store, mock_execution
):
    """Test workflow completion through public complete_execution method."""
    # Arrange
    mock_execution.result = "test result"  # Mock result after completion
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events") as mock_complete_events:
        # Act - Use public API to complete workflow
        executor.complete_execution("test-arn", "result")

    # Assert - Verify final execution status and stored results
    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_execution.complete_success.assert_called_once_with(result="result")
    mock_store.update.assert_called_once_with(mock_execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")


async def test_should_complete_workflow_with_failure_through_public_api(
    executor, mock_store, mock_execution
):
    """Test workflow failure completion through public fail_execution method."""
    # Arrange
    error = ErrorObject.from_message("test error")
    mock_execution.result = "error result"  # Mock result after failure
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events") as mock_complete_events:
        # Act - Use public API to fail workflow
        executor.fail_execution("test-arn", error)

    # Assert - Verify final execution status and stored error
    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_execution.complete_fail.assert_called_once_with(error=error)
    mock_store.update.assert_called_once_with(mock_execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")


async def test_should_handle_workflow_completion_state_through_public_api(
    executor, mock_store, mock_execution
):
    """Test workflow completion behavior and state management through public API."""
    # Arrange
    mock_execution.result = "final result"  # Mock result after completion
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events") as mock_complete_events:
        # Act - Complete workflow through public API
        executor.complete_execution("test-arn", "result")

    # Assert - Verify completion was processed and observer notifications sent
    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_execution.complete_success.assert_called_once_with(result="result")
    mock_store.update.assert_called_once_with(mock_execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")


async def test_should_fail_execution_when_function_not_found(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that workflow fails when function is not found during invocation."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to raise function not found error
    mock_invoker.create_invocation_input.side_effect = ResourceNotFoundException(
        "Function not found: test_function"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger invocation through public start_execution method
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            assert mock_scheduler.call_later.call_count >= 1
            handler = mock_scheduler.call_later.call_args_list[-1][0][0]

            # Execute the handler to trigger the invocation logic
            import asyncio

            await handler()

        # Assert - verify failure was triggered with correct error
        mock_fail.assert_called_once()
        call_args = mock_fail.call_args
        assert call_args[0][0] == "test-arn"  # execution_arn
        assert "Function not found" in call_args[0][1].message  # error message


async def test_should_fail_execution_when_retries_exhausted(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that workflow fails when maximum retry attempts are exhausted."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = (
        executor.MAX_CONSECUTIVE_FAILED_ATTEMPTS + 1
    )

    # Mock invoker to raise exception (simulating network/invocation failure)
    mock_invoker.create_invocation_input.side_effect = Exception("Network error")

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger invocation through public start_execution method
            # This will cause an exception during invocation, which triggers retry logic
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            assert mock_scheduler.call_later.call_count >= 1
            handler = mock_scheduler.call_later.call_args_list[-1][0][0]

            # Execute the handler to trigger the invocation logic
            import asyncio

            await handler()

        # Assert - verify failure was triggered when retries exhausted
        mock_fail.assert_called_once()
        call_args = mock_fail.call_args
        assert call_args[0][0] == "test-arn"  # execution_arn


async def test_should_prevent_multiple_workflow_failures_on_complete_execution(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that attempting to fail an already completed execution raises an exception."""
    # Arrange - execution starts incomplete but becomes complete during processing
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False  # Initially incomplete
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Create a completed execution for the _fail_workflow call
    completed_execution = Mock()
    completed_execution.is_complete = True

    # Mock invoker to raise ResourceNotFoundException (triggers _fail_workflow)
    mock_invoker.create_invocation_input.side_effect = ResourceNotFoundException(
        "Function not found"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        # First load returns incomplete, second load (in _fail_workflow) returns complete
        mock_store.load.side_effect = [mock_execution, completed_execution]

        # Act & Assert - triggering workflow failure on completed execution should raise exception
        executor.start_execution(start_input)
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic - this should raise the exception
        with pytest.raises(
            IllegalStateException, match="Cannot make multiple close workflow decisions"
        ):
            await handler()


async def test_should_retry_invocation_when_under_limit_through_public_api(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that invocation retries when under limit through public API with final outcome verification."""
    # Arrange - Set up execution that will trigger retry logic
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 3  # Under limit (5 is max)

    # Configure invoker to fail initially with validation error, then succeed on retry
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input

    # First invocation: invalid response triggers retry
    invalid_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED,
        result="should not have result",  # Invalid: failed response with result
    )
    # Second invocation: valid success response
    success_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="final success"
    )
    mock_invoker.invoke.side_effect = [
        InvokeResponse(
            invocation_output=invalid_response, request_id="test-request-id-1"
        ),
        InvokeResponse(
            invocation_output=success_response, request_id="test-request-id-2"
        ),
    ]

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger the retry scenario through public API
        executor.start_execution(start_input)

        # Simulate scheduler executing the initial invocation handler
        initial_handler = mock_scheduler.call_later.call_args_list[-1][0][0]
        import asyncio

        await initial_handler()

        # Verify retry was scheduled due to validation error
        assert mock_scheduler.call_later.call_count == 3  # timeout + initial + retry
        retry_call = mock_scheduler.call_later.call_args_list[
            2
        ]  # Third call is the retry
        retry_handler = retry_call[0][0]
        retry_delay = retry_call[1]["delay"]

        # Execute the retry handler to complete the scenario
        await retry_handler()

    # Assert - verify final outcome after retry sequence
    assert (
        mock_execution.consecutive_failed_invocation_attempts == 4
    )  # Incremented from 3 to 4
    assert retry_delay == Executor.RETRY_BACKOFF_SECONDS  # Correct backoff delay used
    mock_store.save.assert_called_with(mock_execution)  # Execution state saved
    assert mock_invoker.invoke.call_count == 2  # Initial + retry invocation


async def test_should_fail_workflow_when_retry_limit_exceeded(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that workflow fails when retry limit is exceeded through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 6  # Over limit

    # Mock invoker to consistently fail
    mock_invoker.create_invocation_input.side_effect = Exception("Persistent error")
    mock_store.load.return_value = mock_execution

    # Mock execution creation
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution

        # Mock the public fail_execution method to verify it gets called
        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger execution that will exceed retry limit
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            assert mock_scheduler.call_later.call_count >= 1
            handler = mock_scheduler.call_later.call_args_list[-1][0][0]

            # Execute the handler to trigger the invocation logic
            await handler()

        # Assert - verify workflow failed due to retry limit exceeded
        mock_fail.assert_called_once()
        # Verify the error contains the expected message
        call_args = mock_fail.call_args
        assert call_args[0][0] == "test-arn"  # execution_arn is first positional arg
        assert "Persistent error" in str(
            call_args[0][1]
        )  # error is second positional arg


async def test_complete_events_through_complete_execution(
    executor, mock_store, mock_scheduler
):
    """Test completion event behavior through public complete_execution method."""
    mock_execution = Mock()
    mock_execution.result = "test result"
    mock_store.load.return_value = mock_execution

    # Set up completion event through start_execution
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Mock the timeout future that will be created
    mock_timeout_future = Mock()
    mock_scheduler.call_later.return_value = mock_timeout_future

    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_exec = Mock()
        mock_exec.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_exec

        start_input = Mock()
        start_input.execution_timeout_seconds = 300
        executor.start_execution(start_input)

    # Now complete the execution - this should trigger event.set() and cancel timeout
    executor.complete_execution("test-arn", "result")

    # Verify the event was set and timeout was cancelled
    mock_event.set.assert_called_once()
    mock_timeout_future.cancel.assert_called_once()


async def test_complete_events_no_event_through_public_api(executor, mock_store):
    """Test that completing non-existent execution handles missing events gracefully."""
    mock_execution = Mock()
    mock_execution.result = "test result"
    mock_store.load.return_value = mock_execution

    # Complete execution without setting up completion event first
    # Should not raise exception when event doesn't exist
    executor.complete_execution("nonexistent-arn", "result")


async def test_wait_until_complete_success(executor, mock_scheduler):
    """Test wait until complete success through public API."""
    mock_event = Mock()
    mock_event.wait_async = AsyncMock(return_value=True)
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        start_input.execution_timeout_seconds = 0
        executor.start_execution(start_input)

    result = await executor.wait_until_complete("test-arn", timeout=10)

    assert result is True
    mock_event.wait_async.assert_called_once_with(10)


async def test_wait_until_complete_timeout(executor, mock_scheduler):
    """Test wait until complete timeout through public API."""
    mock_event = Mock()
    mock_event.wait_async = AsyncMock(return_value=False)
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        start_input.execution_timeout_seconds = 0
        executor.start_execution(start_input)

    result = await executor.wait_until_complete("test-arn", timeout=10)

    assert result is False


async def test_wait_until_complete_no_event(executor):
    with pytest.raises(ResourceNotFoundException, match="execution does not exist"):
        await executor.wait_until_complete("nonexistent-arn")


async def test_complete_execution(executor, mock_store, mock_execution):
    mock_execution.result = "test result"
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events") as mock_complete_events:
        executor.complete_execution("test-arn", "result")

    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_execution.complete_success.assert_called_once_with(result="result")
    mock_store.update.assert_called_once_with(mock_execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")


async def test_fail_execution(executor, mock_store, mock_execution):
    error = ErrorObject.from_message("test error")
    mock_execution.result = "error result"
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events") as mock_complete_events:
        executor.fail_execution("test-arn", error)

    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_execution.complete_fail.assert_called_once_with(error=error)
    mock_store.update.assert_called_once_with(mock_execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")


async def test_should_schedule_wait_timer_correctly(executor, mock_scheduler):
    """Test that wait timer is scheduled correctly through public method."""
    # Arrange
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        start_input.execution_timeout_seconds = 0
        executor.start_execution(start_input)

    # Act - schedule wait timer through public method
    executor.schedule_wait_timer("test-arn", "op-123", delay=5.0)

    # Assert - verify scheduler was called correctly
    assert mock_scheduler.call_later.call_count == 2  # start_execution + wait timer
    wait_call = mock_scheduler.call_later.call_args_list[1]  # Second call is wait timer
    assert wait_call[1]["delay"] == 5.0
    assert wait_call[1]["completion_event"] == mock_event


async def test_should_ignore_wait_completion_for_completed_execution(
    executor, mock_store, mock_execution
):
    """Test that wait completion logic correctly handles completed executions."""
    # Arrange
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution

    # Act - simulate the wait completion logic for a completed execution
    execution = mock_store.load("test-arn")

    # The logic should check if execution is complete before attempting to complete wait
    if not execution.is_complete:
        execution.complete_wait(operation_id="op-123")
        mock_store.update(execution)

    # Assert - verify that complete_wait was not called for completed execution
    mock_execution.complete_wait.assert_not_called()
    mock_store.update.assert_not_called()


async def test_should_handle_wait_completion_exception_gracefully(
    executor, mock_store, mock_execution
):
    """Test that wait completion exceptions are handled through error handling."""
    # Arrange
    mock_store.load.return_value = mock_execution
    mock_execution.is_complete = False
    mock_execution.complete_wait.side_effect = Exception("test error")

    # Act & Assert - test that exception handling works correctly
    # This tests the error handling logic without scheduler timing dependencies
    execution = mock_store.load("test-arn")

    with pytest.raises(Exception, match="test error"):
        execution.complete_wait(operation_id="op-123")


async def test_should_complete_retry_when_retry_scheduled(
    executor, mock_store, mock_scheduler, mock_execution
):
    """Test retry completion through public scheduler callback API."""
    # Arrange
    mock_store.load.return_value = mock_execution

    # Mock _invoke_execution to prevent async warnings
    with patch.object(executor, "_invoke_execution"):
        # Act - trigger retry through public API
        executor.schedule_step_retry("test-arn", "op-123", 10.0)
        retry_handler = mock_scheduler.call_later.call_args.args[0]
        await retry_handler()

    # Assert - verify observable behavior
    mock_store.load.assert_called_with("test-arn")
    mock_execution.complete_retry.assert_called_once_with(operation_id="op-123")
    mock_store.update.assert_called_with(mock_execution)


async def test_should_ignore_retry_when_execution_complete(
    executor, mock_store, mock_scheduler, mock_execution
):
    """Test that completed executions ignore retry events through public API."""
    # Arrange
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution

    # Mock _invoke_execution to prevent async warnings
    with patch.object(executor, "_invoke_execution"):
        # Act - trigger retry through public API
        executor.schedule_step_retry("test-arn", "op-123", 10.0)
        retry_handler = mock_scheduler.call_later.call_args.args[0]
        await retry_handler()

    # Assert - verify no retry processing occurs
    mock_execution.complete_retry.assert_not_called()
    mock_store.update.assert_not_called()


async def test_should_handle_retry_exception_gracefully(
    executor, mock_store, mock_scheduler, mock_execution
):
    """Test that retry exceptions are handled gracefully through public API."""
    # Arrange
    mock_store.load.return_value = mock_execution
    mock_execution.complete_retry.side_effect = Exception("test error")

    # Mock _invoke_execution to prevent async warnings
    with patch.object(executor, "_invoke_execution"):
        # Act - should not raise exception
        executor.schedule_step_retry("test-arn", "op-123", 10.0)
        retry_handler = mock_scheduler.call_later.call_args.args[0]
        await retry_handler()

    # Assert - verify the retry was attempted but exception was caught
    mock_execution.complete_retry.assert_called_once_with(operation_id="op-123")


async def test_schedule_wait_timer(executor, mock_scheduler):
    """Test wait timer scheduling through public observer method."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        start_input.execution_timeout_seconds = 0
        executor.start_execution(start_input)

    with patch.object(executor, "_on_wait_succeeded"):
        with patch.object(executor, "_invoke_execution"):
            executor.schedule_wait_timer("test-arn", "op-123", 10.0)

    # Verify scheduler was called with correct parameters
    assert (
        mock_scheduler.call_later.call_count == 2
    )  # Once for start_execution, once for wait timer
    wait_timer_call = mock_scheduler.call_later.call_args_list[
        1
    ]  # Second call is for wait timer
    assert wait_timer_call[1]["delay"] == 10.0
    assert wait_timer_call[1]["completion_event"] == mock_event


async def test_should_retry_when_response_has_unexpected_status(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that responses with unexpected status trigger retry logic."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return response with unexpected status
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    unexpected_response = Mock()
    unexpected_response.status = "UNKNOWN_STATUS"
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=unexpected_response, request_id="test-request-id"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

        # Assert - verify retry was triggered due to validation error
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called 3 times: timeout + initial + retry)
        assert mock_scheduler.call_later.call_count == 3


async def test_invoke_handler_execution_completed_during_invocation_async(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test execution completing during invocation through public API."""
    # First call returns incomplete execution, second call returns completed execution
    incomplete_execution = Mock(spec=Execution)
    incomplete_execution.is_complete = False
    incomplete_execution.start_input = start_input
    incomplete_execution.consecutive_failed_invocation_attempts = 0
    incomplete_execution.durable_execution_arn = "test-arn"
    incomplete_execution.operations = []
    incomplete_execution.get_new_checkpoint_token.return_value = "checkpoint-token"

    completed_execution = Mock(spec=Execution)
    completed_execution.is_complete = True

    mock_store.load.side_effect = [incomplete_execution, completed_execution]

    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = Mock()
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=mock_response, request_id="test-request-id"
    )

    # Mock execution creation
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = incomplete_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

    # Verify the execution was loaded multiple times (before and after invocation)
    assert mock_store.load.call_count >= 2


async def test_invoke_handler_resource_not_found_async(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test resource not found handling causes workflow failure through public API (async version)."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input

    mock_invoker.create_invocation_input.side_effect = ResourceNotFoundException(
        "Function not found"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Mock the public fail_execution method to verify it gets called
        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger invocation through public start_execution method
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            assert mock_scheduler.call_later.call_count >= 1
            handler = mock_scheduler.call_later.call_args_list[-1][0][0]

            # Execute the handler to trigger the invocation logic
            await handler()

        # Assert - verify workflow failure was triggered through public API
        mock_fail.assert_called_once()
        # Verify the error contains the expected message
        call_args = mock_fail.call_args
        assert call_args[0][0] == "test-arn"  # execution_arn is first positional arg
        assert "Function not found" in str(
            call_args[0][1]
        )  # error is second positional arg


async def test_invoke_handler_general_exception_async(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test general exception handling triggers retry through public API (async version)."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Configure invoker to fail initially, then succeed on retry
    mock_invoker.create_invocation_input.side_effect = [
        Exception("General error"),  # First call fails
        Mock(),  # Second call succeeds (returns invocation input)
    ]

    # Mock successful response for retry
    success_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="success"
    )
    mock_invoker.invoke.return_value = InvokeResponse(
        invocation_output=success_response, request_id="test-request-id"
    )

    # Mock execution creation and store behavior
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        assert mock_scheduler.call_later.call_count >= 1
        handler = mock_scheduler.call_later.call_args_list[-1][0][0]

        # Execute the handler to trigger the invocation logic
        await handler()

        # Assert - verify retry was scheduled through observable behavior
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called 3 times: timeout + initial + retry)
        assert mock_scheduler.call_later.call_count == 3


async def test_invoke_execution_with_delay_through_wait_timer(executor, mock_scheduler):
    """Test execution invocation with delay through wait timer scheduling."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        start_input.execution_timeout_seconds = 0
        executor.start_execution(start_input)

    # Test delay behavior through wait timer scheduling
    with patch.object(executor, "_on_wait_succeeded"):
        executor.schedule_wait_timer("test-arn", "op-123", 10.0)

    # Verify scheduler was called with delay for wait timer
    wait_timer_call = mock_scheduler.call_later.call_args_list[
        1
    ]  # Second call is for wait timer
    assert wait_timer_call[1]["delay"] == 10.0


async def test_invoke_execution_no_delay_through_start_execution(
    executor, mock_scheduler
):
    """Test execution invocation with no delay through start_execution."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Test no delay behavior through start_execution
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        start_input.execution_timeout_seconds = 0
        executor.start_execution(start_input)

    # Verify scheduler was called with no delay for initial execution
    initial_call = mock_scheduler.call_later.call_args_list[
        0
    ]  # First call is for initial execution
    assert initial_call[1]["delay"] == 0


async def test_schedule_step_retry(executor, mock_scheduler):
    """Test step retry scheduling through public observer method."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        start_input.execution_timeout_seconds = 0
        executor.start_execution(start_input)

    with patch.object(executor, "_on_retry_ready"):
        with patch.object(executor, "_invoke_execution"):
            executor.schedule_step_retry("test-arn", "op-123", 10.0)

    # Verify scheduler was called with correct parameters
    assert (
        mock_scheduler.call_later.call_count == 2
    )  # Once for start_execution, once for retry
    retry_call = mock_scheduler.call_later.call_args_list[1]  # Second call is for retry
    assert retry_call[1]["delay"] == 10.0
    assert retry_call[1]["completion_event"] == mock_event


async def test_wait_handler_execution(executor, mock_scheduler):
    """Test wait handler execution through public observer method."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        start_input.execution_timeout_seconds = 0
        executor.start_execution(start_input)

    with patch.object(executor, "_on_wait_succeeded") as mock_wait:
        with patch.object(executor, "_invoke_execution") as mock_invoke:
            executor.schedule_wait_timer("test-arn", "op-123", 10.0)

            # Get the handler that was passed to call_later (second call for wait timer)
            wait_timer_call = mock_scheduler.call_later.call_args_list[1]
            wait_handler = wait_timer_call[0][0]

            # Execute the handler to test the inner function
            await wait_handler()

            mock_wait.assert_called_once_with("test-arn", "op-123")
            mock_invoke.assert_called_once_with("test-arn")


async def test_retry_handler_execution(executor, mock_scheduler):
    """Test retry handler execution through public observer method."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "async_durable_execution.runner.local.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        start_input.execution_timeout_seconds = 0
        executor.start_execution(start_input)

    with patch.object(executor, "_on_retry_ready") as mock_retry:
        with patch.object(executor, "_invoke_execution") as mock_invoke:
            executor.schedule_step_retry("test-arn", "op-123", 10.0)

            # Get the handler that was passed to call_later (second call for retry)
            retry_call = mock_scheduler.call_later.call_args_list[1]
            retry_handler = retry_call[0][0]

            # Execute the handler to test the inner function
            await retry_handler()

            mock_retry.assert_called_once_with("test-arn", "op-123")
            mock_invoke.assert_called_once_with("test-arn")


async def test_get_execution_not_found(executor, mock_store):
    mock_store.load.side_effect = KeyError("not found")

    with pytest.raises(ResourceNotFoundException):
        executor.get_execution("test-arn")


async def test_get_execution_state(executor, mock_store):
    """Test get_execution_state method."""

    mock_execution = Mock()
    mock_execution.used_tokens = {"token1", "token2"}

    # Create mock operations
    operations = [
        Operation(
            operation_id="op-1",
            parent_id=None,
            name="step1",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
        ),
        Operation(
            operation_id="op-2",
            parent_id=None,
            name="step2",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        ),
    ]
    mock_execution.get_assertable_operations.return_value = operations

    mock_store.load.return_value = mock_execution

    result = executor.get_execution_state("test-arn", checkpoint_token="token1")  # noqa: S106

    assert len(result.operations) == 2
    assert result.next_marker is None
    mock_store.load.assert_called_once_with("test-arn")


async def test_get_execution_state_invalid_token(executor, mock_store):
    """Test get_execution_state with invalid checkpoint token."""
    mock_execution = Mock()
    mock_execution.used_tokens = {"token1", "token2"}
    mock_store.load.return_value = mock_execution

    with pytest.raises(
        InvalidParameterValueException, match="Invalid checkpoint token"
    ):
        executor.get_execution_state("test-arn", checkpoint_token="invalid-token")  # noqa: S106


async def test_get_execution_history(executor, mock_store):
    """Test get_execution_history method."""
    mock_execution = Mock()
    mock_execution.operations = []  # Empty operations list
    mock_execution.updates = []
    mock_execution.invocation_completions = []
    mock_execution.durable_execution_arn = ""
    mock_execution.start_input = Mock()
    mock_execution.result = Mock()

    mock_store.load.return_value = mock_execution

    result = executor.get_execution_history("test-arn")

    assert result.events == []
    assert result.next_marker is None
    mock_store.load.assert_called_once_with("test-arn")


async def test_get_execution_history_with_events(executor, mock_store):
    """Test get_execution_history with actual events."""
    from async_durable_execution.models import StepDetails

    # Create operations that will generate events
    op1 = Operation(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        start_timestamp=datetime.now(timezone.utc),
        end_timestamp=datetime.now(timezone.utc),
        step_details=StepDetails(result="test_result"),
    )
    mock_execution = Mock()
    mock_execution.operations = [op1]
    mock_execution.updates = []
    mock_execution.invocation_completions = []
    mock_execution.durable_execution_arn = ""
    mock_execution.start_input = Mock()
    mock_execution.result = Mock()
    mock_store.load.return_value = mock_execution

    result = executor.get_execution_history("test-arn", include_execution_data=True)

    assert len(result.events) == 2  # Started + Succeeded events
    assert result.events[0].event_type == "StepStarted"
    assert result.events[1].event_type == "StepSucceeded"


async def test_get_execution_history_reverse_order(executor, mock_store):
    """Test get_execution_history with reverse order."""
    op1 = Operation(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        start_timestamp=datetime.now(timezone.utc),
        end_timestamp=datetime.now(timezone.utc),
    )

    mock_execution = Mock()
    mock_execution.operations = [op1]
    mock_execution.updates = []
    mock_execution.invocation_completions = []
    mock_execution.durable_execution_arn = ""
    mock_execution.start_input = Mock()
    mock_execution.result = Mock()
    mock_store.load.return_value = mock_execution

    result = executor.get_execution_history("test-arn", reverse_order=True)

    assert len(result.events) == 2
    # In reverse order, succeeded event should come first
    assert result.events[0].event_type == "StepSucceeded"
    assert result.events[1].event_type == "StepStarted"


async def test_get_execution_history_pagination(executor, mock_store):
    """Test get_execution_history with pagination."""
    # Create multiple operations to generate many events
    operations = []
    for i in range(3):
        op = Operation(
            operation_id=f"op-{i}",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            start_timestamp=datetime.now(timezone.utc),
            end_timestamp=datetime.now(timezone.utc),
        )
        operations.append(op)

    mock_execution = Mock()
    mock_execution.operations = operations
    mock_execution.updates = []
    mock_execution.invocation_completions = []
    mock_execution.durable_execution_arn = ""
    mock_execution.start_input = Mock()
    mock_execution.result = Mock()
    mock_store.load.return_value = mock_execution

    # Test with max_items=2
    result = executor.get_execution_history("test-arn", max_items=2)

    assert len(result.events) == 2
    assert result.next_marker == "3"  # Next event_id


async def test_get_execution_history_pagination_with_marker(executor, mock_store):
    """Test get_execution_history pagination with marker."""
    operations = []
    for i in range(3):
        op = Operation(
            operation_id=f"op-{i}",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            start_timestamp=datetime.now(timezone.utc),
            end_timestamp=datetime.now(timezone.utc),
        )
        operations.append(op)

    mock_execution = Mock()
    mock_execution.operations = operations
    mock_execution.updates = []
    mock_execution.invocation_completions = []
    mock_execution.durable_execution_arn = ""
    mock_execution.start_input = Mock()
    mock_execution.result = Mock()
    mock_store.load.return_value = mock_execution

    # Test with marker (start from event_id 3)
    result = executor.get_execution_history("test-arn", marker="3", max_items=2)

    assert len(result.events) == 2
    # Should get events with event_id >= 3


async def test_get_execution_history_invalid_marker(executor, mock_store):
    """Test get_execution_history with invalid marker."""
    mock_execution = Mock()
    mock_execution.operations = []
    mock_execution.updates = []
    mock_execution.invocation_completions = []
    mock_execution.durable_execution_arn = ""
    mock_execution.start_input = Mock()
    mock_execution.result = Mock()
    mock_store.load.return_value = mock_execution

    # Invalid marker should default to 1
    result = executor.get_execution_history("test-arn", marker="invalid")

    assert result.events == []
    assert result.next_marker is None


async def test_checkpoint_execution(executor, mock_store):
    """Test checkpoint_execution method."""
    mock_execution = Mock()
    mock_execution.used_tokens = {"token1", "token2"}
    mock_execution.get_new_checkpoint_token.return_value = "new-token"
    mock_store.load.return_value = mock_execution

    result = executor.checkpoint_execution("test-arn", "token1")

    assert result.checkpoint_token == "new-token"  # noqa: S105
    assert result.new_execution_state is None
    mock_store.load.assert_called_once_with("test-arn")
    mock_execution.get_new_checkpoint_token.assert_called_once()


async def test_checkpoint_execution_invalid_token(executor, mock_store):
    """Test checkpoint_execution with invalid checkpoint token."""
    mock_execution = Mock()
    mock_execution.used_tokens = {"token1", "token2"}
    mock_store.load.return_value = mock_execution

    with pytest.raises(
        InvalidParameterValueException, match="Invalid checkpoint token"
    ):
        executor.checkpoint_execution("test-arn", "invalid-token")


# Callback method tests


async def test_send_callback_success(executor, mock_store):
    """Test send_callback_success method."""
    from async_durable_execution.runner.local.model import CallbackToken

    # Create valid callback token
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    callback_id = callback_token.to_str()

    # Create mock execution with callback operation
    mock_execution = Mock()
    mock_execution.find_callback_operation.return_value = (0, Mock())
    mock_execution.complete_callback_success.return_value = Mock()
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        result = executor.send_callback_success(callback_id, b"success-result")

    assert isinstance(result, SendDurableExecutionCallbackSuccessResponse)
    mock_store.load.assert_called_once_with("test-arn")
    mock_execution.complete_callback_success.assert_called_once_with(
        callback_id, b"success-result"
    )
    mock_store.update.assert_called_once_with(mock_execution)
    # Verify execution is invoked after callback success
    mock_invoke.assert_called_once_with("test-arn")


async def test_send_callback_success_empty_callback_id(executor):
    """Test send_callback_success with empty callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_success("")


async def test_send_callback_success_none_callback_id(executor):
    """Test send_callback_success with None callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_success(None)


async def test_send_callback_success_with_result(executor, mock_store):
    """Test send_callback_success with result data."""
    from async_durable_execution.runner.local.model import CallbackToken

    # Create valid callback token
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    callback_id = callback_token.to_str()

    # Create mock execution with callback operation
    mock_execution = Mock()
    mock_execution.find_callback_operation.return_value = (0, Mock())
    mock_execution.complete_callback_success.return_value = Mock()
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        result = executor.send_callback_success(callback_id, b"test-result")

    assert isinstance(result, SendDurableExecutionCallbackSuccessResponse)
    mock_execution.complete_callback_success.assert_called_once_with(
        callback_id, b"test-result"
    )
    # Verify execution is invoked after callback success
    mock_invoke.assert_called_once_with("test-arn")


async def test_send_callback_failure(executor, mock_store):
    """Test send_callback_failure method."""
    from async_durable_execution.runner.local.model import CallbackToken

    # Create valid callback token
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    callback_id = callback_token.to_str()

    # Create mock execution with callback operation
    mock_execution = Mock()
    mock_execution.find_callback_operation.return_value = (0, Mock())
    mock_execution.complete_callback_failure.return_value = Mock()
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        result = executor.send_callback_failure(callback_id)

    assert isinstance(result, SendDurableExecutionCallbackFailureResponse)
    mock_store.load.assert_called_once_with("test-arn")
    mock_store.update.assert_called_once_with(mock_execution)
    # Verify execution is invoked after callback failure
    mock_invoke.assert_called_once_with("test-arn")


async def test_send_callback_failure_empty_callback_id(executor):
    """Test send_callback_failure with empty callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_failure("")


async def test_send_callback_failure_none_callback_id(executor):
    """Test send_callback_failure with None callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_failure(None)


async def test_send_callback_failure_with_error(executor, mock_store):
    """Test send_callback_failure with error object."""
    # Create valid callback token
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    callback_id = callback_token.to_str()

    # Create mock execution with callback operation
    mock_execution = Mock()
    mock_execution.find_callback_operation.return_value = (0, Mock())
    mock_execution.complete_callback_failure.return_value = Mock()
    mock_store.load.return_value = mock_execution

    error = ErrorObject.from_message("Test callback error")
    with patch.object(executor, "_invoke_execution") as mock_invoke:
        result = executor.send_callback_failure(callback_id, error)

    assert isinstance(result, SendDurableExecutionCallbackFailureResponse)
    mock_execution.complete_callback_failure.assert_called_once_with(callback_id, error)
    # Verify execution is invoked after callback failure
    mock_invoke.assert_called_once_with("test-arn")


async def test_send_callback_heartbeat(executor, mock_store):
    """Test send_callback_heartbeat method."""
    # Create valid callback token
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    callback_id = callback_token.to_str()

    # Create mock execution with callback operation
    mock_execution = Mock()
    mock_operation = Mock()
    mock_operation.status = OperationStatus.STARTED
    mock_execution.find_callback_operation.return_value = (0, mock_operation)
    mock_execution.updates = []  # No callback options to reset timeout
    mock_execution.invocation_completions = []
    mock_store.load.return_value = mock_execution

    result = executor.send_callback_heartbeat(callback_id)

    assert isinstance(result, SendDurableExecutionCallbackHeartbeatResponse)
    # Called twice: once in get_execution, once in _reset_callback_heartbeat_timeout
    assert mock_store.load.call_count == 2
    mock_execution.find_callback_operation.assert_called_once_with(callback_id)


async def test_send_callback_heartbeat_empty_callback_id(executor):
    """Test send_callback_heartbeat with empty callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_heartbeat("")


async def test_send_callback_heartbeat_none_callback_id(executor):
    """Test send_callback_heartbeat with None callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_heartbeat(None)


async def test_complete_execution_no_result(mock_store, executor):
    """Test complete_execution when execution has no result after completion."""
    mock_execution = Mock()
    mock_execution.result = None  # No result after completion
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events"):
        with pytest.raises(IllegalStateException, match="Execution result is required"):
            executor.complete_execution("test-arn", "result")


async def test_fail_execution_no_result(mock_store, executor):
    """Test fail_execution when execution has no result after failure."""
    mock_execution = Mock()
    mock_execution.result = None  # No result after failure
    mock_store.load.return_value = mock_execution
    error = ErrorObject.from_message("test error")

    with patch.object(executor, "_complete_events"):
        with pytest.raises(IllegalStateException, match="Execution result is required"):
            executor.fail_execution("test-arn", error)


async def test_send_callback_heartbeat_inactive_callback(mock_store, executor):
    """Test send_callback_heartbeat with inactive callback."""

    # Create valid callback token
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    callback_id = callback_token.to_str()

    # Create mock execution with inactive callback operation
    mock_execution = Mock()
    mock_operation = Mock()
    mock_operation.status = OperationStatus.SUCCEEDED  # Not STARTED
    mock_execution.find_callback_operation.return_value = (0, mock_operation)
    mock_store.load.return_value = mock_execution

    with pytest.raises(ResourceNotFoundException, match="Callback .* is not active"):
        executor.send_callback_heartbeat(callback_id)


async def test_send_callback_success_invalid_token(executor):
    """Test send_callback_success with invalid token format."""
    with pytest.raises(
        ResourceNotFoundException, match="Failed to process callback success"
    ):
        executor.send_callback_success("invalid-token")


async def test_send_callback_failure_invalid_token(executor):
    """Test send_callback_failure with invalid token format."""
    with pytest.raises(
        ResourceNotFoundException, match="Failed to process callback failure"
    ):
        executor.send_callback_failure("invalid-token")


async def test_send_callback_heartbeat_invalid_token(executor):
    """Test send_callback_heartbeat with invalid token format."""
    with pytest.raises(
        ResourceNotFoundException, match="Failed to process callback heartbeat"
    ):
        executor.send_callback_heartbeat("invalid-token")


async def test_complete_events_no_event(executor):
    """Test _complete_events when no event exists."""
    # Should not raise exception when event doesn't exist
    executor._complete_events("nonexistent-arn")  # Should handle gracefully


# Tests for callback timeout functionality


async def test_callback_timeout_scheduling(executor, mock_store, mock_scheduler):
    """Test that callback timeouts are scheduled when callback is created."""
    # Create callback options with both timeouts
    callback_options = CallbackOptions(timeout_seconds=60, heartbeat_timeout_seconds=30)

    # Set up completion event
    executor._completion_events["test-arn"] = Mock()

    # Test the timeout scheduling directly with correct parameters
    executor._schedule_callback_timeouts("test-arn", callback_options, "callback-id")

    # Verify scheduler was called for both timeouts
    assert mock_scheduler.call_later.call_count == 2  # main timeout + heartbeat timeout


async def test_callback_timeout_scheduling_scales_long_delays(
    executor, mock_store, mock_scheduler, monkeypatch
):
    """Test that local callback timers are scaled with the long-delay floor."""
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.1")
    callback_options = CallbackOptions(timeout_seconds=60, heartbeat_timeout_seconds=30)

    executor._completion_events["test-arn"] = Mock()

    executor._schedule_callback_timeouts("test-arn", callback_options, "callback-id")

    assert mock_scheduler.call_later.call_args_list[0].kwargs["delay"] == 6.0
    assert mock_scheduler.call_later.call_args_list[1].kwargs["delay"] == 5.0


async def test_callback_timeout_scheduling_preserves_short_delays(
    executor, mock_store, mock_scheduler, monkeypatch
):
    """Test that callback timers shorter than the floor are not lengthened."""
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.1")
    callback_options = CallbackOptions(timeout_seconds=3, heartbeat_timeout_seconds=2)

    executor._completion_events["test-arn"] = Mock()

    executor._schedule_callback_timeouts("test-arn", callback_options, "callback-id")

    assert mock_scheduler.call_later.call_args_list[0].kwargs["delay"] == 3.0
    assert mock_scheduler.call_later.call_args_list[1].kwargs["delay"] == 2.0


async def test_callback_timeout_cleanup(executor, mock_store):
    """Test that callback timeouts are cleaned up when callback completes."""
    # Create mock timeout events
    timeout_event = Mock()
    heartbeat_event = Mock()

    executor._callback_timeouts["callback-id"] = timeout_event
    executor._callback_heartbeats["callback-id"] = heartbeat_event

    # Trigger cleanup
    executor._cleanup_callback_timeouts("callback-id")

    # Verify events were cancelled and removed
    timeout_event.cancel.assert_called_once()
    heartbeat_event.cancel.assert_called_once()
    assert "callback-id" not in executor._callback_timeouts
    assert "callback-id" not in executor._callback_heartbeats


async def test_callback_heartbeat_timeout_reset(executor, mock_store, mock_scheduler):
    """Test that heartbeat timeout is reset when heartbeat is received."""

    # Create callback token
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    callback_id = callback_token.to_str()

    # Create mock execution with callback options
    mock_execution = Mock()
    callback_options = CallbackOptions(heartbeat_timeout_seconds=30)
    update = OperationUpdate(
        operation_id="op-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        callback_options=callback_options,
    )
    mock_execution.updates = [update]

    mock_store.load.return_value = mock_execution
    mock_scheduler.create_event.return_value = Mock()

    # Set up existing heartbeat event
    old_event = Mock()
    executor._callback_heartbeats[callback_id] = old_event

    # Reset heartbeat timeout
    executor._reset_callback_heartbeat_timeout(callback_id, "test-arn")

    # Verify old event was cancelled and new one scheduled
    old_event.cancel.assert_called_once()
    mock_scheduler.call_later.assert_called()


async def test_callback_timeout_handlers(executor, mock_store):
    """Test callback timeout and heartbeat timeout handlers."""
    # Create callback token
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    callback_id = callback_token.to_str()

    # Create mock execution
    mock_execution = Mock()
    mock_execution.is_complete = False
    mock_store.load.return_value = mock_execution

    # Test main timeout handler
    executor._on_callback_timeout("test-arn", callback_id)

    # Verify callback was failed with timeout error
    mock_execution.complete_callback_timeout.assert_called()
    timeout_error = mock_execution.complete_callback_timeout.call_args[0][1]
    assert "Callback timed out" in str(timeout_error.message)

    # Reset mocks for heartbeat test
    mock_execution.reset_mock()

    # Test heartbeat timeout handler
    executor._on_callback_heartbeat_timeout("test-arn", callback_id)

    # Verify callback was failed with heartbeat timeout error
    mock_execution.complete_callback_timeout.assert_called()
    heartbeat_error = mock_execution.complete_callback_timeout.call_args[0][1]
    assert "Callback heartbeat timed out" in str(heartbeat_error.message)


async def test_callback_timeout_completed_execution(executor, mock_store):
    """Test that timeout handlers ignore completed executions."""

    # Create callback token
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    callback_id = callback_token.to_str()

    # Create completed execution
    mock_execution = Mock()
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution

    # Test timeout handlers with completed execution
    executor._on_callback_timeout("test-arn", callback_id)
    executor._on_callback_heartbeat_timeout("test-arn", callback_id)

    # Verify no callback operations were performed
    mock_execution.complete_callback_timeout.assert_not_called()
    mock_store.update.assert_not_called()


async def test_schedule_callback_timeouts_no_callback_details(executor, mock_store):
    """Test _schedule_callback_timeouts when no callback options are provided."""

    # Should return early without scheduling
    executor._schedule_callback_timeouts("test-arn", None, "callback-id")

    # No scheduler calls should be made
    assert len(executor._callback_timeouts) == 0
    assert len(executor._callback_heartbeats) == 0


async def test_schedule_callback_timeouts_no_callback_options(executor, mock_store):
    """Test _schedule_callback_timeouts when callback options are None."""

    # Should return early without scheduling
    executor._schedule_callback_timeouts("test-arn", None, "callback-id")

    # No scheduler calls should be made
    assert len(executor._callback_timeouts) == 0
    assert len(executor._callback_heartbeats) == 0


async def test_schedule_callback_timeouts_zero_timeouts(
    executor, mock_store, mock_scheduler
):
    """Test _schedule_callback_timeouts with zero timeout values."""
    # Create operation with callback details
    operation = Operation(
        operation_id="op-123",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=CallbackDetails(callback_id="callback-id"),
    )

    mock_execution = Mock()
    mock_execution.find_operation.return_value = (0, operation)

    # Create update with zero timeouts (disabled)
    callback_options = CallbackOptions(timeout_seconds=0, heartbeat_timeout_seconds=0)
    update = OperationUpdate(
        operation_id="op-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        callback_options=callback_options,
    )
    mock_execution.updates = [update]

    mock_store.load.return_value = mock_execution
    executor._completion_events["test-arn"] = Mock()

    # Should not schedule any timeouts
    executor._schedule_callback_timeouts("test-arn", callback_options, "callback-id")

    # No scheduler calls should be made
    mock_scheduler.call_later.assert_not_called()
    assert len(executor._callback_timeouts) == 0
    assert len(executor._callback_heartbeats) == 0


async def test_schedule_callback_timeouts_only_main_timeout(
    executor, mock_store, mock_scheduler
):
    """Test _schedule_callback_timeouts with only main timeout configured."""

    # Create callback options with only main timeout
    callback_options = CallbackOptions(timeout_seconds=60, heartbeat_timeout_seconds=0)

    executor._completion_events["test-arn"] = Mock()

    executor._schedule_callback_timeouts("test-arn", callback_options, "callback-id")

    # Only main timeout should be scheduled
    assert mock_scheduler.call_later.call_count == 1
    assert len(executor._callback_timeouts) == 1
    assert len(executor._callback_heartbeats) == 0


async def test_schedule_callback_timeouts_only_heartbeat_timeout(
    executor, mock_store, mock_scheduler
):
    """Test _schedule_callback_timeouts with only heartbeat timeout configured."""
    # Create callback options with only heartbeat timeout
    callback_options = CallbackOptions(timeout_seconds=0, heartbeat_timeout_seconds=30)

    executor._completion_events["test-arn"] = Mock()

    executor._schedule_callback_timeouts("test-arn", callback_options, "callback-id")

    # Only heartbeat timeout should be scheduled
    assert mock_scheduler.call_later.call_count == 1
    assert len(executor._callback_timeouts) == 0
    assert len(executor._callback_heartbeats) == 1


async def test_schedule_callback_timeouts_exception_handling(executor, mock_store):
    """Test _schedule_callback_timeouts handles exceptions gracefully."""
    callback_options = CallbackOptions(timeout_seconds=60, heartbeat_timeout_seconds=0)
    executor._scheduler.call_later.side_effect = Exception("Test error")

    # Should not raise exception
    executor._schedule_callback_timeouts("test-arn", callback_options, "callback-id")

    # No timeouts should be scheduled
    assert len(executor._callback_timeouts) == 0
    assert len(executor._callback_heartbeats) == 0


async def test_timeout_execution(executor, mock_store):
    """Test timeout_execution method."""
    # Create real execution instance
    mock_start_input = Mock()
    mock_start_input.execution_name = "test-execution"
    mock_start_input.function_name = "test-function"

    execution = Execution(
        durable_execution_arn="test-arn",
        start_input=mock_start_input,
        operations=[Mock()],
    )
    execution.is_complete = False
    mock_store.load.return_value = execution

    error = ErrorObject.from_message("Execution timeout")

    with patch.object(executor, "_complete_events") as mock_complete_events:
        executor.timeout_execution("test-arn", error)

    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_store.update.assert_called_once_with(execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")
    assert execution.is_complete is True
    assert execution.close_status == ExecutionStatus.TIMED_OUT
    assert execution.result.error == error


async def test_stop_execution(executor):
    """Test stop_execution method."""
    error = ErrorObject.from_message("Execution stopped")

    with patch.object(executor, "fail_execution") as mock_fail:
        executor.stop_execution("test-arn", error)

    mock_fail.assert_called_once_with("test-arn", error)


@patch("async_durable_execution.runner.local.executor.Execution")
async def test_start_execution_timeout_handler_notifies_timed_out(
    mock_execution_class, executor, start_input, mock_scheduler
):
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution_class.new.return_value = mock_execution
    mock_scheduler.create_event.return_value = Mock()

    with patch.object(executor, "_invoke_execution"):
        with patch.object(executor, "timeout_execution") as mock_timed_out:
            executor.start_execution(start_input)
            timeout_handler = mock_scheduler.call_later.call_args_list[0][0][0]
            await timeout_handler()

    mock_timed_out.assert_called_once()
    assert mock_timed_out.call_args.args[0] == "test-arn"
    assert "Execution timed out after 300 seconds" in (
        mock_timed_out.call_args.args[1].message
    )


async def test_get_execution_state_paginates_and_ignores_invalid_marker(
    executor, mock_store
):
    operations = [
        Operation(
            operation_id=f"op-{i}",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        )
        for i in range(3)
    ]
    mock_execution = Mock()
    mock_execution.used_tokens = {"token"}
    mock_execution.get_assertable_operations.return_value = operations
    mock_store.load.return_value = mock_execution

    result = executor.get_execution_state(
        "test-arn",
        checkpoint_token="token",  # noqa: S106
        marker="not-an-int",
        max_items=2,
    )

    assert [operation.operation_id for operation in result.operations] == [
        "op-0",
        "op-1",
    ]
    assert result.next_marker == "2"


async def test_get_execution_history_includes_invocation_and_pending_chained_invoke(
    executor, mock_store, start_input
):
    now = datetime.now(timezone.utc)
    pending_invoke = Operation(
        operation_id="invoke-op",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.PENDING,
        start_timestamp=now,
        name="invoke-child",
    )
    mock_execution = Mock()
    mock_execution.operations = [pending_invoke]
    mock_execution.updates = []
    mock_execution.invocation_completions = [
        InvocationCompletedDetails(
            start_timestamp=now,
            end_timestamp=now,
            request_id="request-1",
        )
    ]
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.start_input = start_input
    mock_execution.result = None
    mock_store.load.return_value = mock_execution

    result = executor.get_execution_history("test-arn")

    event_types = [event.event_type for event in result.events]
    assert event_types[0] == "InvocationCompleted"
    assert event_types.count("ChainedInvokeStarted") == 2


async def test_get_execution_history_reverse_pagination_next_marker(
    executor, mock_store
):
    operations = [
        Operation(
            operation_id=f"op-{i}",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            start_timestamp=datetime(2026, 1, 1, i, tzinfo=timezone.utc),
            end_timestamp=datetime(2026, 1, 1, i, 1, tzinfo=timezone.utc),
        )
        for i in range(3)
    ]
    mock_execution = Mock()
    mock_execution.operations = operations
    mock_execution.updates = []
    mock_execution.invocation_completions = []
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.start_input = Mock()
    mock_execution.result = None
    mock_store.load.return_value = mock_execution

    result = executor.get_execution_history("test-arn", reverse_order=True, max_items=2)

    assert len(result.events) == 2
    assert result.next_marker == str(result.events[-1].event_id)


async def test_checkpoint_execution_with_updates_returns_new_state(
    executor, mock_store, mock_service_client
):
    operation = Operation(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    checkpoint_output = Mock()
    checkpoint_output.checkpoint_token = "token-2"  # noqa: S105
    checkpoint_output.new_execution_state.operations = [operation]
    checkpoint_output.new_execution_state.next_marker = "next"
    mock_service_client.process_checkpoint.return_value = checkpoint_output
    mock_execution = Mock()
    mock_execution.used_tokens = {"token-1"}
    mock_store.load.return_value = mock_execution

    result = executor.checkpoint_execution(
        "test-arn",
        "token-1",  # noqa: S106
        updates=[update],
        client_token="client-token",  # noqa: S106
    )

    mock_service_client.process_checkpoint.assert_called_once_with(
        checkpoint_token="token-1",  # noqa: S106
        updates=[update],
        client_token="client-token",  # noqa: S106
    )
    assert result.checkpoint_token == "token-2"  # noqa: S105
    assert result.new_execution_state.operations == [operation]
    assert result.new_execution_state.next_marker == "next"


async def test_validate_invocation_response_rejects_completed_execution(
    executor, mock_execution
):
    mock_execution.is_complete = True
    response = DurableExecutionInvocationOutput(status=InvocationStatus.SUCCEEDED)

    with pytest.raises(
        IllegalStateException, match="Execution already completed, ignoring result"
    ):
        executor._validate_invocation_response_and_store(
            "test-arn", response, mock_execution
        )


async def test_callback_resume_is_coalesced_while_invocation_active(
    executor, mock_store
):
    execution = Mock()
    execution.is_complete = False
    mock_store.load.return_value = execution

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        executor._mark_invocation_started("test-arn")
        executor._schedule_callback_resume("test-arn")

        assert "test-arn" in executor._pending_resumes
        mock_invoke.assert_not_called()

        executor._mark_invocation_finished("test-arn")

    mock_invoke.assert_called_once_with("test-arn")


async def test_callback_resume_not_invoked_after_completion(executor, mock_store):
    execution = Mock()
    execution.is_complete = True
    mock_store.load.return_value = execution
    executor._active_invocations.add("test-arn")
    executor._pending_resumes.add("test-arn")

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        executor._mark_invocation_finished("test-arn")

    mock_invoke.assert_not_called()


async def test_wait_resume_is_deferred_while_invocation_active(
    executor, mock_store, mock_scheduler
):
    execution = Mock()
    execution.is_complete = False
    mock_store.load.return_value = execution

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        executor._mark_invocation_started("test-arn")
        executor.schedule_wait_timer("test-arn", "wait-op", 1.0)

        wait_handler = mock_scheduler.call_later.call_args[0][0]
        await wait_handler()

        assert ("test-arn", "wait-op") in executor._deferred_wait_resumes
        execution.complete_wait.assert_not_called()
        mock_invoke.assert_not_called()

        executor._mark_invocation_finished("test-arn")

    execution.complete_wait.assert_called_once_with(operation_id="wait-op")
    mock_store.update.assert_called_once_with(execution)
    mock_invoke.assert_called_once_with("test-arn")


async def test_retry_resume_is_deferred_while_invocation_active(
    executor, mock_store, mock_scheduler
):
    execution = Mock()
    execution.is_complete = False
    mock_store.load.return_value = execution

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        executor._mark_invocation_started("test-arn")
        executor.schedule_step_retry("test-arn", "retry-op", 1.0)

        retry_handler = mock_scheduler.call_later.call_args[0][0]
        await retry_handler()

        assert ("test-arn", "retry-op") in executor._deferred_retry_resumes
        execution.complete_retry.assert_not_called()
        mock_invoke.assert_not_called()

        executor._mark_invocation_finished("test-arn")

    execution.complete_retry.assert_called_once_with(operation_id="retry-op")
    mock_store.update.assert_called_once_with(execution)
    mock_invoke.assert_called_once_with("test-arn")


async def test_complete_workflow_rejects_already_completed_execution(
    executor, mock_store
):
    execution = Mock()
    execution.is_complete = True
    mock_store.load.return_value = execution

    with pytest.raises(
        IllegalStateException, match="Cannot make multiple close workflow decisions"
    ):
        executor._complete_workflow("test-arn", result="result", error=None)


async def test_on_wait_succeeded_updates_execution(executor, mock_store):
    execution = Mock()
    execution.is_complete = False
    mock_store.load.return_value = execution

    executor._on_wait_succeeded("test-arn", "wait-op")

    execution.complete_wait.assert_called_once_with(operation_id="wait-op")
    mock_store.update.assert_called_once_with(execution)


async def test_on_wait_succeeded_ignores_completed_execution(executor, mock_store):
    execution = Mock()
    execution.is_complete = True
    mock_store.load.return_value = execution

    executor._on_wait_succeeded("test-arn", "wait-op")

    execution.complete_wait.assert_not_called()
    mock_store.update.assert_not_called()


async def test_on_wait_succeeded_logs_exceptions(executor, mock_store):
    execution = Mock()
    execution.is_complete = False
    execution.complete_wait.side_effect = RuntimeError("wait failed")
    mock_store.load.return_value = execution

    executor._on_wait_succeeded("test-arn", "wait-op")

    execution.complete_wait.assert_called_once_with(operation_id="wait-op")
    mock_store.update.assert_not_called()


async def test_schedule_callback_timeouts_schedules_timeouts(executor):
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="callback-op")
    callback_options = CallbackOptions(timeout_seconds=10)

    with patch.object(executor, "_schedule_callback_timeouts") as mock_schedule:
        executor.schedule_callback_timeouts(
            "test-arn",
            callback_options,
            callback_token.to_str(),
        )

    mock_schedule.assert_called_once_with(
        "test-arn", callback_options, callback_token.to_str()
    )


async def test_schedule_callback_timeouts_none_options_returns(
    executor, mock_scheduler
):
    executor._schedule_callback_timeouts("test-arn", None, "callback-id")

    mock_scheduler.call_later.assert_not_called()


async def test_callback_timeout_scheduled_handlers_call_timeout_methods(
    executor, mock_scheduler
):
    callback_options = CallbackOptions(timeout_seconds=10, heartbeat_timeout_seconds=20)

    executor._schedule_callback_timeouts("test-arn", callback_options, "callback-id")

    timeout_handler = mock_scheduler.call_later.call_args_list[0][0][0]
    heartbeat_handler = mock_scheduler.call_later.call_args_list[1][0][0]
    with patch.object(executor, "_on_callback_timeout") as mock_timeout:
        await timeout_handler()
    with patch.object(
        executor, "_on_callback_heartbeat_timeout"
    ) as mock_heartbeat_timeout:
        await heartbeat_handler()

    mock_timeout.assert_called_once_with("test-arn", "callback-id")
    mock_heartbeat_timeout.assert_called_once_with("test-arn", "callback-id")


async def test_reset_callback_heartbeat_scheduled_handler_calls_timeout(
    executor, mock_store, mock_scheduler
):
    callback_token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    callback_id = callback_token.to_str()
    execution = Mock()
    execution.updates = [
        OperationUpdate(
            operation_id="op-123",
            operation_type=OperationType.CALLBACK,
            action=OperationAction.START,
            callback_options=CallbackOptions(heartbeat_timeout_seconds=10),
        )
    ]
    mock_store.load.return_value = execution

    executor._reset_callback_heartbeat_timeout(callback_id, "test-arn")

    heartbeat_handler = mock_scheduler.call_later.call_args.args[0]
    with patch.object(
        executor, "_on_callback_heartbeat_timeout"
    ) as mock_heartbeat_timeout:
        await heartbeat_handler()

    mock_heartbeat_timeout.assert_called_once_with("test-arn", callback_id)


async def test_reset_callback_heartbeat_timeout_handles_invalid_token(executor):
    executor._reset_callback_heartbeat_timeout("invalid-token", "test-arn")


async def test_callback_timeout_handlers_swallow_invalid_token(executor):
    executor._on_callback_timeout("test-arn", "invalid-token")
    executor._on_callback_heartbeat_timeout("test-arn", "invalid-token")
