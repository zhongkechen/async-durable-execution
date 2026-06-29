"""Unit tests for runner module."""

import asyncio
import datetime
import json
from unittest.mock import Mock, patch

import pytest

from async_durable_execution import InvocationStatus
from async_durable_execution.models import (
    OperationStatus,
    OperationType,
    StepDetails,
)
from async_durable_execution.models import Operation as SvcOperation
from async_durable_execution.runner.exceptions import (
    DurableFunctionsTestError,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from async_durable_execution.runner.execution import Execution
from async_durable_execution.runner.model import (
    GetDurableExecutionHistoryResponse,
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
)
from async_durable_execution.runner.runner import (
    DurableFunctionCloudTestRunner,
    DurableFunctionLocalTestRunner,
    DurableFunctionTestResult,
    create_runner,
)


async def test_durable_function_test_result_create():
    """Test DurableFunctionTestResult.create method."""
    # Create mock execution with operations
    execution = Mock(spec=Execution)

    # Create operations - one EXECUTION (should be filtered) and one STEP
    exec_op = SvcOperation(
        operation_id="exec-id",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )

    step_op = SvcOperation(
        operation_id="step-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        name="test-step",
        step_details=StepDetails(result=json.dumps("step-result")),
    )

    execution.operations = [exec_op, step_op]

    # Mock execution result
    execution.result = Mock()
    execution.result.status = InvocationStatus.SUCCEEDED
    execution.result.result = json.dumps("test-result")
    execution.result.error = None

    result = DurableFunctionTestResult.create(execution)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps("test-result")
    assert result.error is None
    assert len(result.operations) == 1  # EXECUTION operation filtered out
    assert isinstance(result.operations[0], SvcOperation)


async def test_durable_function_test_result_get_operation_by_name():
    """Test DurableFunctionTestResult get_operation_by_name method."""
    step_op = SvcOperation(
        operation_id="step-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        name="test-step",
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[step_op],
    )

    found_op = result.get_operation_by_name("test-step")
    assert found_op is step_op


async def test_durable_function_test_result_get_operation_by_name_not_found():
    """Test DurableFunctionTestResult get_operation_by_name raises error when not found."""
    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[],
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Operation with name 'missing' not found"
    ):
        result.get_operation_by_name("missing")


async def test_durable_function_test_result_get_step():
    """Test DurableFunctionTestResult get_step method."""
    step_op = SvcOperation(
        operation_id="step-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        name="test-step",
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[step_op],
    )

    found_step = result.get_step("test-step")
    assert found_step is step_op
    assert found_step.name == "test-step"


async def test_durable_function_test_result_get_wait():
    """Test DurableFunctionTestResult get_wait method."""
    wait_op = SvcOperation(
        operation_id="wait-id",
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
        name="test-wait",
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[wait_op],
    )

    found_wait = result.get_wait("test-wait")
    assert found_wait is wait_op
    assert found_wait.name == "test-wait"


async def test_durable_function_test_result_get_context():
    """Test DurableFunctionTestResult get_context method."""
    ctx_op = SvcOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        name="test-context",
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[ctx_op],
    )

    found_ctx = result.get_context("test-context")
    assert found_ctx is ctx_op
    assert found_ctx.name == "test-context"


async def test_durable_function_test_result_get_callback():
    """Test DurableFunctionTestResult get_callback method."""
    callback_op = SvcOperation(
        operation_id="callback-id",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        name="test-callback",
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[callback_op],
    )

    found_callback = result.get_callback("test-callback")
    assert found_callback is callback_op
    assert found_callback.name == "test-callback"


async def test_durable_function_test_result_get_invoke():
    """Test DurableFunctionTestResult get_invoke method."""
    invoke_op = SvcOperation(
        operation_id="invoke-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        name="test-invoke",
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[invoke_op],
    )

    found_invoke = result.get_invoke("test-invoke")
    assert found_invoke is invoke_op
    assert found_invoke.name == "test-invoke"


async def test_durable_function_test_result_get_execution():
    """Test DurableFunctionTestResult get_execution method."""
    exec_op = SvcOperation(
        operation_id="exec-id",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        name="test-execution",
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[exec_op],
    )

    found_exec = result.get_execution("test-execution")
    assert found_exec is exec_op
    assert found_exec.name == "test-execution"


async def test_durable_function_test_result_get_deserialized_result():
    """Test DurableFunctionTestResult deserializes the execution result."""
    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[],
        result=json.dumps({"status": "ok"}),
    )

    assert result.get_deserialized_result() == {"status": "ok"}


@patch("async_durable_execution.runner.runner.Scheduler")
@patch("async_durable_execution.runner.runner.InMemoryExecutionStore")
@patch("async_durable_execution.runner.runner.CheckpointProcessor")
@patch("async_durable_execution.runner.runner.InMemoryServiceClient")
@patch("async_durable_execution.runner.runner.InProcessInvoker")
@patch("async_durable_execution.runner.runner.Executor")
async def test_durable_function_test_runner_init(
    mock_executor, mock_invoker, mock_client, mock_processor, mock_store, mock_scheduler
):
    """Test DurableFunctionLocalTestRunner initialization."""
    handler = Mock()

    DurableFunctionLocalTestRunner(handler)

    # Verify all components are initialized
    mock_scheduler.assert_called_once()
    mock_scheduler.return_value.start.assert_called_once()
    mock_store.assert_called_once()
    mock_processor.assert_called_once()
    mock_client.assert_called_once()
    mock_invoker.assert_called_once_with(handler, mock_client.return_value)
    mock_executor.assert_called_once()

    # Verify observer pattern setup
    mock_processor.return_value.add_execution_observer.assert_called_once_with(
        mock_executor.return_value
    )


async def test_durable_function_test_runner_context_manager():
    """Test DurableFunctionLocalTestRunner context manager."""
    handler = Mock()

    with patch.object(DurableFunctionLocalTestRunner, "__init__", return_value=None):
        with patch.object(DurableFunctionLocalTestRunner, "close") as mock_close:
            runner = DurableFunctionLocalTestRunner(handler)

            with runner:
                pass

            mock_close.assert_called_once()


async def test_durable_function_cloud_test_runner_context_manager():
    """Test DurableFunctionCloudTestRunner context manager."""
    with patch.object(DurableFunctionCloudTestRunner, "__init__", return_value=None):
        with patch.object(DurableFunctionCloudTestRunner, "close") as mock_close:
            runner = DurableFunctionCloudTestRunner("test-function:$LATEST")

            with runner:
                pass

            mock_close.assert_called_once()


@patch("async_durable_execution.runner.runner.DurableFunctionLocalTestRunner")
async def test_create_runner_local_uses_configured_defaults(mock_local_runner_class):
    """Test create_runner builds a local runner with default run values."""
    handler = Mock()
    mock_local_runner = Mock()
    mock_local_runner_class.return_value = mock_local_runner

    runner = create_runner(
        mode="local",
        handler=handler,
        input={"hello": "world"},
        timeout=12,
        poll_interval=0.25,
    )

    assert runner is mock_local_runner
    mock_local_runner_class.assert_called_once_with(
        handler=handler,
        poll_interval=0.25,
        input={"hello": "world"},
        timeout=12,
    )


@patch("async_durable_execution.runner.runner.DurableFunctionCloudTestRunner")
async def test_create_runner_cloud_uses_configured_defaults(mock_cloud_runner_class):
    """Test create_runner builds a cloud runner with default async values."""
    mock_cloud_runner = Mock()
    mock_cloud_runner_class.return_value = mock_cloud_runner

    runner = create_runner(
        mode="cloud",
        function_name="hello-world:$LATEST",
        region="us-east-1",
        lambda_endpoint="https://example.com",
        input="payload",
        timeout=45,
        poll_interval=0.5,
    )

    assert runner is mock_cloud_runner
    mock_cloud_runner_class.assert_called_once_with(
        function_name="hello-world:$LATEST",
        region="us-east-1",
        lambda_endpoint="https://example.com",
        poll_interval=0.5,
        input="payload",
        timeout=45,
    )


async def test_create_runner_requires_handler_for_local_mode():
    """Test local mode validation for create_runner."""
    with pytest.raises(
        InvalidParameterValueException,
        match="handler is required when mode='local'",
    ):
        create_runner(mode="local")


async def test_create_runner_requires_function_name_for_cloud_mode():
    """Test cloud mode validation for create_runner."""
    with pytest.raises(
        InvalidParameterValueException,
        match="function_name is required when mode='cloud'",
    ):
        create_runner(mode="cloud")


async def test_create_runner_rejects_unknown_mode():
    """Test mode validation for create_runner."""
    with pytest.raises(
        InvalidParameterValueException,
        match="Unsupported runner mode: unsupported",
    ):
        create_runner(mode="unsupported")


@patch("async_durable_execution.runner.runner.Scheduler")
async def test_durable_function_test_runner_close(mock_scheduler):
    """Test DurableFunctionLocalTestRunner close method."""
    handler = Mock()

    # Let the constructor run normally with mocked dependencies
    mock_scheduler_instance = Mock()
    mock_scheduler.return_value = mock_scheduler_instance

    runner = DurableFunctionLocalTestRunner(handler)
    runner.close()

    # Verify scheduler.stop() was called
    mock_scheduler_instance.stop.assert_called_once()


@patch("async_durable_execution.runner.runner.Executor")
@patch("async_durable_execution.runner.runner.InMemoryExecutionStore")
async def test_durable_function_test_runner_run(mock_store_class, mock_executor_class):
    """Test DurableFunctionLocalTestRunner run method."""
    handler = Mock()

    # Mock the class instances
    mock_executor = Mock()
    mock_store = Mock()
    mock_executor_class.return_value = mock_executor
    mock_store_class.return_value = mock_store

    # Mock execution output
    output = StartDurableExecutionOutput(execution_arn="test-arn")
    mock_executor.start_execution.return_value = output
    mock_executor.wait_until_complete.return_value = True

    # Mock execution for result creation
    mock_execution = Mock(spec=Execution)
    mock_execution.operations = []
    mock_execution.result = Mock()
    mock_execution.result.status = InvocationStatus.SUCCEEDED
    mock_execution.result.result = json.dumps("test-result")
    mock_execution.result.error = None
    mock_store.load.return_value = mock_execution

    runner = DurableFunctionLocalTestRunner(handler, input="test-input")
    result = await runner.run()

    # Verify start_execution was called with correct input
    mock_executor.start_execution.assert_called_once()
    start_input = mock_executor.start_execution.call_args[0][0]
    assert isinstance(start_input, StartDurableExecutionInput)
    assert start_input.input == "test-input"
    assert start_input.function_name == "test-function"
    assert start_input.execution_name == "execution-name"
    assert start_input.account_id == "123456789012"

    # Verify wait_until_complete was called
    mock_executor.wait_until_complete.assert_called_once_with("test-arn", 900)

    # Verify store.load was called
    mock_store.load.assert_called_once_with("test-arn")

    # Verify result
    assert isinstance(result, DurableFunctionTestResult)
    assert result.status is InvocationStatus.SUCCEEDED


@patch("async_durable_execution.runner.runner.Executor")
@patch("async_durable_execution.runner.runner.InMemoryExecutionStore")
async def test_durable_function_test_runner_run_with_custom_params(
    mock_store_class, mock_executor_class
):
    """Test DurableFunctionLocalTestRunner run method with custom parameters."""
    handler = Mock()

    # Mock the class instances
    mock_executor = Mock()
    mock_store = Mock()
    mock_executor_class.return_value = mock_executor
    mock_store_class.return_value = mock_store

    # Mock execution output
    output = StartDurableExecutionOutput(execution_arn="test-arn")
    mock_executor.start_execution.return_value = output
    mock_executor.wait_until_complete.return_value = True

    # Mock execution for result creation
    mock_execution = Mock(spec=Execution)
    mock_execution.operations = []
    mock_execution.result = Mock()
    mock_execution.result.status = InvocationStatus.SUCCEEDED
    mock_execution.result.result = json.dumps("test-result")
    mock_execution.result.error = None
    mock_store.load.return_value = mock_execution

    runner = DurableFunctionLocalTestRunner(
        handler,
        input="custom-input",
        timeout=1800,
        function_name="custom-function",
        execution_name="custom-execution",
        account_id="987654321098",
    )
    result = await runner.run()

    # Verify start_execution was called with custom parameters
    start_input = mock_executor.start_execution.call_args[0][0]
    assert start_input.input == "custom-input"
    assert start_input.function_name == "custom-function"
    assert start_input.execution_name == "custom-execution"
    assert start_input.account_id == "987654321098"
    assert start_input.execution_timeout_seconds == 1800

    # Verify wait_until_complete was called with custom timeout
    mock_executor.wait_until_complete.assert_called_once_with("test-arn", 1800)

    assert result.status is InvocationStatus.SUCCEEDED


@patch("async_durable_execution.runner.runner.Executor")
async def test_durable_function_test_runner_run_timeout(mock_executor_class):
    """Test DurableFunctionLocalTestRunner run method with timeout."""
    handler = Mock()

    # Mock the class instance
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor

    # Mock execution output
    output = StartDurableExecutionOutput(execution_arn="test-arn")
    mock_executor.start_execution.return_value = output
    mock_executor.wait_until_complete.return_value = False  # Timeout

    runner = DurableFunctionLocalTestRunner(handler, input="test-input")

    with pytest.raises(TimeoutError, match="Execution did not complete within timeout"):
        await runner.run()


async def test_runner_run_methods_do_not_accept_call_time_overrides():
    """Test run APIs only use values configured when the runner is created."""
    local_runner = DurableFunctionLocalTestRunner(Mock())
    cloud_runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(TypeError):
        local_runner.run(input="override")

    with pytest.raises(TypeError):
        local_runner.run_async(timeout=10)

    with pytest.raises(TypeError):
        cloud_runner.run(input="override")

    with pytest.raises(TypeError):
        cloud_runner.run_async(timeout=10)


async def test_durable_function_test_result_create_with_parent_operations():
    """Test DurableFunctionTestResult.create with operations that have parent_id."""
    execution = Mock(spec=Execution)

    # Create operation with parent_id (should be filtered out)
    child_op = SvcOperation(
        operation_id="child-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent-id",
        step_details=StepDetails(result=json.dumps("child-result")),
    )

    # Create operation without parent_id (should be included)
    root_op = SvcOperation(
        operation_id="root-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        name="root-step",
        step_details=StepDetails(result=json.dumps("root-result")),
    )

    execution.operations = [child_op, root_op]
    execution.result = Mock()
    execution.result.status = InvocationStatus.SUCCEEDED
    execution.result.result = json.dumps("test-result")
    execution.result.error = None

    result = DurableFunctionTestResult.create(execution)

    assert len(result.operations) == 1  # Only root operation included


# Tests for DurableFunctionCloudTestRunner and from_execution_history


async def test_durable_function_test_result_from_execution_history():
    """Test DurableFunctionTestResult.from_execution_history factory method."""
    import datetime

    from async_durable_execution import InvocationStatus
    from async_durable_execution.runner.model import (
        Event,
        EventResult,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
        StepSucceededDetails,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="SUCCEEDED",
        start_timestamp=datetime.datetime(
            2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        ),
        end_timestamp=datetime.datetime(
            2023, 1, 1, 0, 1, 0, tzinfo=datetime.timezone.utc
        ),
        result="test-result",
        error=None,
    )

    history_response = GetDurableExecutionHistoryResponse(
        events=[
            Event(
                event_type="ExecutionStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                operation_id="exec-1",
            ),
            Event(
                event_type="StepStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 10, tzinfo=datetime.timezone.utc
                ),
                operation_id="step-1",
                name="test-step",
            ),
            Event(
                event_type="StepSucceeded",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 20, tzinfo=datetime.timezone.utc
                ),
                operation_id="step-1",
                step_succeeded_details=StepSucceededDetails(
                    result=EventResult(payload="step-result", truncated=False)
                ),
            ),
        ]
    )

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == "test-result"
    assert result.error is None
    assert len(result.operations) == 1
    assert isinstance(result.operations[0], SvcOperation)
    assert result.operations[0].name == "test-step"


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_init(mock_boto3):
    """Test DurableFunctionCloudTestRunner initialization."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        region="us-west-2",
        poll_interval=0.5,
    )

    assert runner.function_name == "test-function"
    assert runner.region == "us-west-2"
    assert runner.poll_interval == 0.5
    mock_boto3.return_value.create_client.assert_called_once()


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_run_success(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run with successful execution."""
    from async_durable_execution import InvocationStatus
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": Mock(read=lambda: b'{"result": "success"}'),
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
    }

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "SUCCEEDED",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
        "Result": "test-result",
    }

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "ExecutionStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "exec-1",
            }
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        poll_interval=0.01,
        input="test-input",
        timeout=10,
    )

    result = await runner.run()

    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == "test-result"
    mock_client.invoke.assert_called_once()


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_run_invoke_failure(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run with invoke failure."""
    from async_durable_execution.runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client
    mock_client.invoke.side_effect = Exception("Invoke failed")

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        input="test-input",
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to invoke Lambda function"
    ):
        await runner.run()


@patch("async_durable_execution.runner.runner.get_session")
@patch("async_durable_execution.runner.runner.time")
async def test_cloud_runner_wait_for_completion_timeout(mock_time, mock_boto3):
    """Test DurableFunctionCloudTestRunner._wait_for_completion with timeout."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client
    mock_time.time.side_effect = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "RUNNING",
        "StartTimestamp": "2023-01-01T00:00:00Z",
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    with pytest.raises(TimeoutError, match="Execution did not complete within"):
        runner._wait_for_completion("test-arn", timeout=2)


async def test_durable_function_test_result_from_execution_history_with_exception():
    """Test from_execution_history handles events_to_operations exception."""
    import datetime

    from async_durable_execution import InvocationStatus
    from async_durable_execution.runner.model import (
        Event,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="SUCCEEDED",
        start_timestamp=datetime.datetime(
            2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        ),
    )

    history_response = GetDurableExecutionHistoryResponse(
        events=[
            Event(
                event_type="StepStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                operation_id=None,
            )
        ]
    )

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert result.status == InvocationStatus.SUCCEEDED
    assert len(result.operations) == 0


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_completion_failed_status(mock_boto3):
    """Test DurableFunctionCloudTestRunner._wait_for_completion with FAILED status."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "FAILED",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
        "Error": {"ErrorMessage": "execution failed"},
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    result = runner._wait_for_completion("test-arn", timeout=10)

    assert result.status == "FAILED"


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_run_bad_status_code(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run with bad HTTP status code."""
    from async_durable_execution.runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 500,
        "Payload": Mock(read=lambda: b"Internal Server Error"),
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        input="test-input",
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Lambda invocation failed with status 500"
    ):
        await runner.run()


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_run_function_error(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run with function error."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 200,
        "FunctionError": "Unhandled",
        "Payload": Mock(read=lambda: b'{"errorMessage": "Function failed"}'),
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
    }

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "FAILED",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
        "Error": {"ErrorMessage": "execution failed"},
    }

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "ExecutionStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "exec-1",
            }
        ]
    }
    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        input="test-input",
    )
    result = await runner.run()
    assert result.status is InvocationStatus.FAILED


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_run_missing_execution_arn(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run with missing execution ARN."""
    from async_durable_execution.runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": Mock(read=lambda: b'{"result": "success"}'),
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        input="test-input",
    )

    with pytest.raises(
        DurableFunctionsTestError, match="No DurableExecutionArn in response"
    ):
        await runner.run()


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_completion_get_execution_failure(mock_boto3):
    """Test DurableFunctionCloudTestRunner._wait_for_completion with API failure."""
    from async_durable_execution.runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client
    mock_client.get_durable_execution.side_effect = Exception("API error")

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to get execution status"
    ):
        runner._wait_for_completion("test-arn", timeout=10)


async def test_durable_function_test_result_from_execution_history_filters_execution_type():
    """Test from_execution_history filters out EXECUTION type operations."""
    import datetime

    from async_durable_execution.runner.model import (
        Event,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="SUCCEEDED",
        start_timestamp=datetime.datetime(
            2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        ),
    )

    history_response = GetDurableExecutionHistoryResponse(
        events=[
            Event(
                event_type="ExecutionStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                operation_id="exec-1",
            ),
        ]
    )

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert len(result.operations) == 0


async def test_durable_function_test_result_from_execution_history_unknown_status():
    """Test from_execution_history with unknown status defaults to FAILED."""
    import datetime

    from async_durable_execution import InvocationStatus
    from async_durable_execution.runner.model import (
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="UNKNOWN_STATUS",
        start_timestamp=datetime.datetime(
            2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        ),
    )

    history_response = GetDurableExecutionHistoryResponse(events=[])

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert result.status == InvocationStatus.FAILED


async def test_durable_function_test_result_from_execution_history_with_parent_operations():
    """Test from_execution_history filters operations with parent_id."""
    import datetime

    from async_durable_execution.runner.model import (
        Event,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="SUCCEEDED",
        start_timestamp=datetime.datetime(
            2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        ),
    )

    history_response = GetDurableExecutionHistoryResponse(
        events=[
            Event(
                event_type="StepStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                operation_id="step-1",
                name="parent-step",
            ),
            Event(
                event_type="StepStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 10, tzinfo=datetime.timezone.utc
                ),
                operation_id="step-2",
                name="child-step",
                parent_id="step-1",
            ),
        ]
    )

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert len(result.operations) == 1
    assert result.operations[0].name == "parent-step"


async def test_durable_function_test_result_from_execution_history_failed():
    """Test from_execution_history with failed execution."""
    import datetime

    from async_durable_execution import InvocationStatus
    from async_durable_execution.models import ErrorObject
    from async_durable_execution.runner.model import (
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="FAILED",
        start_timestamp=datetime.datetime(
            2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        ),
        end_timestamp=datetime.datetime(
            2023, 1, 1, 0, 1, 0, tzinfo=datetime.timezone.utc
        ),
        error=ErrorObject(
            message="execution failed", type=None, data=None, stack_trace=None
        ),
    )

    history_response = GetDurableExecutionHistoryResponse(events=[])

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert result.status == InvocationStatus.FAILED
    assert result.error.message == "execution failed"


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_completion_timed_out_status(mock_boto3):
    """Test DurableFunctionCloudTestRunner._wait_for_completion with TIMED_OUT status."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "TIMED_OUT",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    result = runner._wait_for_completion("test-arn", timeout=10)

    assert result.status == "TIMED_OUT"


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_completion_aborted_status(mock_boto3):
    """Test DurableFunctionCloudTestRunner._wait_for_completion with ABORTED status."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "ABORTED",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    result = runner._wait_for_completion("test-arn", timeout=10)

    assert result.status == "ABORTED"


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_run_async_success(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run_async with successful invocation."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 202,
        "Payload": Mock(read=lambda: b'{"result": "success"}'),
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        input="test-input",
    )
    execution_arn = await runner.run_async()

    assert (
        execution_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1"
    )
    mock_client.invoke.assert_called_once_with(
        FunctionName="test-function",
        InvocationType="Event",
        Payload='"test-input"',
    )


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_run_async_with_400(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run_async with successful invocation."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 400,
        "Payload": Mock(read=lambda: b'{"result": "failed"}'),
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        input="test-input",
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Lambda invocation failed with status 400"
    ):
        await runner.run_async()


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_run_async_failure(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run_async with invocation failure."""
    from async_durable_execution.runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client
    mock_client.invoke.side_effect = Exception("Async invoke failed")

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        input="test-input",
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to invoke Lambda function"
    ):
        await runner.run_async()


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_send_callback_success(mock_boto3):
    """Test DurableFunctionCloudTestRunner.send_callback_success."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    await runner.send_callback_success("callback-123")

    mock_client.send_durable_execution_callback_success.assert_called_once_with(
        CallbackId="callback-123", Result=None
    )


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_send_callback_failure(mock_boto3):
    """Test DurableFunctionCloudTestRunner.send_callback_failure."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    await runner.send_callback_failure("callback-123")

    mock_client.send_durable_execution_callback_failure.assert_called_once_with(
        CallbackId="callback-123", Error=None
    )


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_send_callback_heartbeat(mock_boto3):
    """Test DurableFunctionCloudTestRunner.send_callback_heartbeat."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    await runner.send_callback_heartbeat("callback-123")

    mock_client.send_durable_execution_callback_heartbeat.assert_called_once_with(
        CallbackId="callback-123"
    )


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_send_callback_error(mock_boto3):
    """Test DurableFunctionCloudTestRunner callback methods with API errors."""
    from async_durable_execution.runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client
    mock_client.send_durable_execution_callback_success.side_effect = Exception(
        "API error"
    )

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to send callback success"
    ):
        await runner.send_callback_success("callback-123")


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_callback_success(mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback success."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
                "CallbackStartedDetails": {"CallbackId": "callback-123"},
            }
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )
    callback_id = await runner.wait_for_callback(
        "test-arn", name="test-callback", timeout=10
    )

    assert callback_id == "callback-123"


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_callback_none(mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback none."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
                "CallbackStartedDetails": {"CallbackId": "callback-123"},
            }
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    with pytest.raises(TimeoutError, match="Callback did not available within"):
        await runner.wait_for_callback("test-arn", name="test-callback1", timeout=2)


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_callback_success_without_name(mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback success."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
                "CallbackStartedDetails": {"CallbackId": "callback-123"},
            }
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )
    callback_id = await runner.wait_for_callback("test-arn")

    assert callback_id == "callback-123"


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_callback_all_done_without_name(mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback all_done_without_name."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
                "CallbackStartedDetails": {"CallbackId": "callback-123"},
            },
            {
                "EventType": "CallbackSucceeded",
                "EventTimestamp": "2023-01-01T00:05:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
            },
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )
    with pytest.raises(TimeoutError, match="Callback did not available within"):
        await runner.wait_for_callback("test-arn", timeout=2)


@patch("async_durable_execution.runner.runner.Executor")
async def test_local_runner_wait_for_callback_all_done_without_name(
    mock_executor_class,
):
    """Test DurableFunctionCloudTestRunner.wait_for_callback all_done_without_name."""
    handler = Mock()
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor
    mock_executor.get_execution_history.return_value = (
        GetDurableExecutionHistoryResponse.from_dict(
            {
                "Events": [
                    {
                        "EventType": "CallbackStarted",
                        "EventTimestamp": "2023-01-01T00:00:00Z",
                        "Id": "callback-event-1",
                        "Name": "test-callback",
                        "CallbackStartedDetails": {"CallbackId": "callback-123"},
                    },
                    {
                        "EventType": "CallbackSucceeded",
                        "EventTimestamp": "2023-01-01T00:05:00Z",
                        "Id": "callback-event-1",
                        "Name": "test-callback",
                    },
                ]
            }
        )
    )

    runner = DurableFunctionLocalTestRunner(handler)
    with pytest.raises(TimeoutError, match="Callback did not available within"):
        await runner.wait_for_callback("test-arn", timeout=2)


@patch("async_durable_execution.runner.runner.Executor")
async def test_local_runner_wait_for_callback_with_exception(mock_executor_class):
    """Test DurableFunctionLocalTestRunner.wait_for_callback with exception."""
    handler = Mock()
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor
    mock_executor.get_execution_history.side_effect = Exception("error")

    runner = DurableFunctionLocalTestRunner(handler)
    with pytest.raises(
        DurableFunctionsTestError, match="Failed to fetch execution history"
    ):
        await runner.wait_for_callback("test-arn", timeout=10)


@patch("async_durable_execution.runner.runner.Executor")
async def test_local_runner_wait_for_callback_with_resource_not_found_exception(
    mock_executor_class,
):
    """Test DurableFunctionLocalTestRunner.wait_for_callback with resource_not_found exception."""
    handler = Mock()
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor
    mock_executor.get_execution_history.side_effect = ResourceNotFoundException("error")

    runner = DurableFunctionLocalTestRunner(handler)
    with pytest.raises(TimeoutError, match="Callback did not available within"):
        await runner.wait_for_callback("test-arn", timeout=2)


@patch("async_durable_execution.runner.runner.get_session")
@patch("async_durable_execution.runner.runner.time")
async def test_cloud_runner_wait_for_callback_timeout(mock_time, mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback timeout."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client
    mock_time.time.side_effect = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]

    mock_client.get_durable_execution_history.return_value = {"Events": []}

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    with pytest.raises(TimeoutError, match="Callback did not available within"):
        await runner.wait_for_callback("test-arn", timeout=2)


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_callback_already_completed(mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback already completed."""
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
                "CallbackStartedDetails": {"CallbackId": "callback-123"},
            },
            {
                "EventType": "CallbackSucceeded",
                "EventTimestamp": "2023-01-01T00:05:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
            },
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Callback test-callback has already completed"
    ):
        await runner.wait_for_callback("test-arn", "test-callback", timeout=2)


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_callback_client_error_retryable(mock_boto3):
    """Test wait_for_callback with retryable ClientError."""
    from botocore.exceptions import ClientError

    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    # First call raises ResourceNotFoundException, second succeeds
    mock_client.get_durable_execution_history.side_effect = [
        ClientError(
            error_response={"Error": {"Code": "ResourceNotFoundException"}},
            operation_name="GetDurableExecutionHistory",
        ),
        {
            "Events": [
                {
                    "EventType": "CallbackStarted",
                    "EventTimestamp": "2023-01-01T00:00:00Z",
                    "Id": "callback-event-1",
                    "Name": "test-callback",
                    "CallbackStartedDetails": {"CallbackId": "callback-123"},
                }
            ]
        },
    ]

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )
    callback_id = await runner.wait_for_callback(
        "test-arn", name="test-callback", timeout=10
    )

    assert callback_id == "callback-123"


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_callback_client_error_non_retryable(
    mock_boto3,
):
    """Test wait_for_callback with non-retryable ClientError."""
    from botocore.exceptions import ClientError

    from async_durable_execution.runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.get_durable_execution_history.side_effect = ClientError(
        error_response={"Error": {"Code": "AccessDeniedException"}},
        operation_name="GetDurableExecutionHistory",
    )

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to fetch execution history"
    ):
        await runner.wait_for_callback("test-arn", timeout=10)


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_callback_generic_exception(mock_boto3):
    """Test wait_for_callback with generic Exception."""
    from async_durable_execution.runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    mock_client.get_durable_execution_history.side_effect = Exception("Network error")

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to fetch execution history"
    ):
        await runner.wait_for_callback("test-arn", timeout=10)


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_result_fetch_history_exception(mock_boto3):
    """Test wait_for_result with exception in _fetch_execution_history."""
    from async_durable_execution.runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    # Mock successful _wait_for_completion
    mock_execution_response = Mock()
    mock_execution_response.status = "SUCCEEDED"

    # Mock _fetch_execution_history to raise exception
    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    runner._wait_for_completion = Mock(return_value=mock_execution_response)
    runner._fetch_execution_history = Mock(
        side_effect=Exception("History fetch failed")
    )

    with pytest.raises(
        DurableFunctionsTestError,
        match="Failed to fetch execution history: History fetch failed",
    ):
        await runner.wait_for_result("test-arn", timeout=60)


@patch("async_durable_execution.runner.runner.get_session")
async def test_cloud_runner_wait_for_result_success(mock_boto3):
    """Test wait_for_result successful execution."""
    from async_durable_execution import InvocationStatus
    from async_durable_execution.runner.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value.create_client.return_value = mock_client

    # Mock successful responses
    mock_execution_response = Mock()
    mock_execution_response.status = "SUCCEEDED"
    mock_history_response = Mock()
    mock_history_response.events = []

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    runner._wait_for_completion = Mock(return_value=mock_execution_response)
    runner._fetch_execution_history = Mock(return_value=mock_history_response)

    # Mock the from_execution_history method
    with patch(
        "async_durable_execution.runner.runner.DurableFunctionTestResult.from_execution_history"
    ) as mock_from_history:
        mock_result = Mock()
        mock_result.status = InvocationStatus.SUCCEEDED
        mock_from_history.return_value = mock_result

        result = await runner.wait_for_result("test-arn", timeout=60)

        assert result.status == InvocationStatus.SUCCEEDED
        mock_from_history.assert_called_once_with(
            mock_execution_response, mock_history_response
        )
