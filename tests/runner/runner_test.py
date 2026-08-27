"""Unit tests for runner module."""

from typing import no_type_check

from typing import Any

import asyncio
import datetime
import json
from unittest.mock import AsyncMock, Mock, call, patch

import pytest


pytestmark = pytest.mark.httpx_installed(False)

from async_durable_execution import (
    DurableFunctionCloudTestRunner,
    DurableFunctionLocalTestRunner,
    DurableFunctionTestResult,
    InvocationStatus,
    create_cloud_runner,
    create_local_runner,
)
from async_durable_execution._core.models import (
    CallbackDetails,
    ChainedInvokeDetails,
    ContextDetails,
    OperationStatus,
    OperationType,
    StepDetails,
)
from async_durable_execution._core.models import Operation
from async_durable_execution._runner.exceptions import (
    DurableFunctionsTestError,
    ResourceNotFoundException,
)
from async_durable_execution._runner.cloud import ThreadedSyncCloudLambdaClient
from async_durable_execution._runner.local.execution import Execution
from async_durable_execution._runner.model import (
    GetDurableExecutionHistoryResponse,
)
from async_durable_execution._runner.local.model import (
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
)


class AsyncLambdaClientStub:
    def __init__(self) -> None:
        self.exceptions = type("Exceptions", (), {})()
        self.closed = False
        self.calls: list[tuple[str, dict]] = []

    async def invoke(self, **kwargs) -> Any:
        self.calls.append(("invoke", kwargs))
        return {"method": "invoke", "kwargs": kwargs}

    async def get_durable_execution(self, **kwargs) -> Any:
        self.calls.append(("get_durable_execution", kwargs))
        return {"method": "get_durable_execution", "kwargs": kwargs}

    async def get_durable_execution_history(self, **kwargs) -> Any:
        self.calls.append(("get_durable_execution_history", kwargs))
        return {"method": "get_durable_execution_history", "kwargs": kwargs}

    async def send_durable_execution_callback_success(self, **kwargs) -> Any:
        self.calls.append(("send_durable_execution_callback_success", kwargs))
        return {"method": "send_durable_execution_callback_success", "kwargs": kwargs}

    async def send_durable_execution_callback_failure(self, **kwargs) -> Any:
        self.calls.append(("send_durable_execution_callback_failure", kwargs))
        return {"method": "send_durable_execution_callback_failure", "kwargs": kwargs}

    async def send_durable_execution_callback_heartbeat(self, **kwargs) -> Any:
        self.calls.append(("send_durable_execution_callback_heartbeat", kwargs))
        return {
            "method": "send_durable_execution_callback_heartbeat",
            "kwargs": kwargs,
        }

    async def aclose(self) -> None:
        self.closed = True


class AsyncLambdaClientContextStub:
    def __init__(self, client) -> None:
        self.client = client
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> Any:
        self.entered = True
        return self.client

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Any:
        self.exited = True


async def test_durable_function_test_result_create() -> None:
    """Test DurableFunctionTestResult.create method."""
    # Create mock execution with operations
    execution = Mock(spec=Execution)

    # Create operations - one EXECUTION (should be filtered) and one STEP
    exec_op = Operation(
        operation_id="exec-id",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )

    step_op = Operation(
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
    assert isinstance(result.operations[0], Operation)


async def test_durable_function_test_result_get_operation_by_name() -> None:
    """Test DurableFunctionTestResult get_operation_by_name method."""
    step_op = Operation(
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


async def test_durable_function_test_result_get_operation_by_name_not_found() -> None:
    """Test DurableFunctionTestResult get_operation_by_name raises error when not found."""
    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[],
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Operation with name 'missing' not found"
    ):
        result.get_operation_by_name("missing")


async def test_durable_function_test_result_get_step() -> None:
    """Test DurableFunctionTestResult get_step method."""
    step_op = Operation(
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


async def test_durable_function_test_result_get_wait() -> None:
    """Test DurableFunctionTestResult get_wait method."""
    wait_op = Operation(
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


async def test_durable_function_test_result_get_context() -> None:
    """Test DurableFunctionTestResult get_context method."""
    ctx_op = Operation(
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


async def test_durable_function_test_result_get_callback() -> None:
    """Test DurableFunctionTestResult get_callback method."""
    callback_op = Operation(
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


async def test_durable_function_test_result_get_invoke() -> None:
    """Test DurableFunctionTestResult get_invoke method."""
    invoke_op = Operation(
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


async def test_durable_function_test_result_get_execution() -> None:
    """Test DurableFunctionTestResult get_execution method."""
    exec_op = Operation(
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


async def test_durable_function_test_result_get_deserialized_result() -> None:
    """Test DurableFunctionTestResult deserializes the execution result."""
    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[],
        result=json.dumps({"status": "ok"}),
    )

    assert result.get_deserialized_result() == {"status": "ok"}


@patch("async_durable_execution._runner.local.Scheduler")
@patch("async_durable_execution._runner.local.InMemoryServiceClient")
@patch("async_durable_execution._runner.local.InProcessInvoker")
@patch("async_durable_execution._runner.local.Executor")
async def test_durable_function_test_runner_init(
    mock_executor, mock_invoker, mock_client, mock_scheduler
) -> None:
    """Test DurableFunctionLocalTestRunner initialization."""
    handler = Mock()

    DurableFunctionLocalTestRunner(handler)

    # Verify all components are initialized
    mock_scheduler.assert_called_once()
    mock_scheduler.return_value.start.assert_not_called()
    mock_client.assert_called_once_with(scheduler=mock_scheduler.return_value)
    mock_invoker.assert_called_once_with(handler, mock_client.return_value)
    mock_executor.assert_called_once_with(
        scheduler=mock_scheduler.return_value,
        invoker=mock_invoker.return_value,
        service_client=mock_client.return_value,
    )

    # Verify service client binds directly to the single local executor.
    mock_client.return_value.bind_executor.assert_called_once_with(
        mock_executor.return_value
    )


async def test_durable_function_test_runner_context_manager() -> None:
    """Test DurableFunctionLocalTestRunner async context manager."""
    handler = Mock()

    with patch.object(DurableFunctionLocalTestRunner, "__init__", return_value=None):
        with patch.object(DurableFunctionLocalTestRunner, "close") as mock_close:
            runner = DurableFunctionLocalTestRunner(handler)

            async with runner:
                pass

            mock_close.assert_called_once()


async def test_durable_function_cloud_test_runner_context_manager() -> None:
    """Test DurableFunctionCloudTestRunner async context manager."""
    with patch.object(DurableFunctionCloudTestRunner, "__init__", return_value=None):
        with patch.object(DurableFunctionCloudTestRunner, "aclose") as mock_aclose:
            runner = DurableFunctionCloudTestRunner("test-function:$LATEST")

            async with runner:
                pass

            mock_aclose.assert_awaited_once()


@patch("async_durable_execution._runner.local.DurableFunctionLocalTestRunner")
async def test_create_local_runner_uses_configured_defaults(
    mock_local_runner_class,
) -> None:
    """Test create_local_runner builds a local runner with default run values."""
    handler = Mock()
    mock_local_runner = Mock()
    mock_local_runner_class.return_value = mock_local_runner

    runner = create_local_runner(
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


@patch("async_durable_execution._runner.cloud.DurableFunctionCloudTestRunner")
async def test_create_cloud_runner_uses_configured_defaults(
    mock_cloud_runner_class,
) -> None:
    """Test create_cloud_runner builds a cloud runner with default async values."""
    mock_cloud_runner = Mock()
    mock_cloud_runner_class.return_value = mock_cloud_runner

    runner = create_cloud_runner(
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


async def test_async_cloud_lambda_client_direct_client_delegates_all_methods() -> None:
    from async_durable_execution._runner.cloud import AsyncCloudLambdaClient

    raw_client = AsyncLambdaClientStub()
    client = AsyncCloudLambdaClient(raw_client)

    assert client.exceptions is raw_client.exceptions
    assert await client.invoke(FunctionName="fn") == {
        "method": "invoke",
        "kwargs": {"FunctionName": "fn"},
    }
    assert await client.get_durable_execution(DurableExecutionArn="arn") == {
        "method": "get_durable_execution",
        "kwargs": {"DurableExecutionArn": "arn"},
    }
    assert await client.get_durable_execution_history(DurableExecutionArn="arn") == {
        "method": "get_durable_execution_history",
        "kwargs": {"DurableExecutionArn": "arn"},
    }
    assert await client.send_durable_execution_callback_success(
        CallbackId="callback"
    ) == {
        "method": "send_durable_execution_callback_success",
        "kwargs": {"CallbackId": "callback"},
    }
    assert await client.send_durable_execution_callback_failure(
        CallbackId="callback"
    ) == {
        "method": "send_durable_execution_callback_failure",
        "kwargs": {"CallbackId": "callback"},
    }
    assert await client.send_durable_execution_callback_heartbeat(
        CallbackId="callback"
    ) == {
        "method": "send_durable_execution_callback_heartbeat",
        "kwargs": {"CallbackId": "callback"},
    }

    await client.aclose()

    assert raw_client.closed is True


async def test_async_cloud_lambda_client_enters_context_on_first_call() -> None:
    from async_durable_execution._runner.cloud import AsyncCloudLambdaClient

    raw_client = AsyncLambdaClientStub()
    context = AsyncLambdaClientContextStub(raw_client)
    client = AsyncCloudLambdaClient(context)

    with pytest.raises(AttributeError, match="not been initialized"):
        _ = client.exceptions

    assert await client.get_durable_execution(DurableExecutionArn="arn") == {
        "method": "get_durable_execution",
        "kwargs": {"DurableExecutionArn": "arn"},
    }
    assert context.entered is True
    assert client.exceptions is raw_client.exceptions

    await client.aclose()

    assert context.exited is True


async def test_read_payload_handles_sync_read_returning_awaitable() -> None:
    from async_durable_execution._runner.cloud import _read_payload

    class Payload:
        def read(self) -> Any:
            async def inner() -> bytes:
                return b"payload"

            return inner()

    assert await _read_payload(Payload()) == "payload"


def test_threaded_sync_cloud_lambda_client_close_delegates_to_client() -> None:
    from async_durable_execution._runner.cloud import ThreadedSyncCloudLambdaClient

    raw_client = Mock()
    client = ThreadedSyncCloudLambdaClient(raw_client)

    client.close()

    raw_client.close.assert_called_once()


@patch("async_durable_execution._runner.local.Scheduler")
async def test_durable_function_test_runner_close(mock_scheduler) -> None:
    """Test DurableFunctionLocalTestRunner close method."""
    handler = Mock()

    # Let the constructor run normally with mocked dependencies
    mock_scheduler_instance = Mock()
    mock_scheduler.return_value = mock_scheduler_instance

    runner = DurableFunctionLocalTestRunner(handler)
    runner.close()

    # Verify scheduler.stop() was called
    mock_scheduler_instance.stop.assert_called_once()


@patch("async_durable_execution._runner.local.Executor")
async def test_durable_function_test_runner_run(mock_executor_class) -> None:
    """Test DurableFunctionLocalTestRunner run method."""
    handler = Mock()

    # Mock the class instances
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor

    # Mock execution output
    output = StartDurableExecutionOutput(execution_arn="test-arn")
    mock_executor.start_execution.return_value = output
    mock_executor.wait_until_complete = AsyncMock(return_value=True)

    # Mock execution for result creation
    mock_execution = Mock(spec=Execution)
    mock_execution.operations = []
    mock_execution.result = Mock()
    mock_execution.result.status = InvocationStatus.SUCCEEDED
    mock_execution.result.result = json.dumps("test-result")
    mock_execution.result.error = None
    mock_executor.get_execution.return_value = mock_execution

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

    # Verify execution was read directly from executor
    mock_executor.get_execution.assert_called_once_with("test-arn")

    # Verify result
    assert isinstance(result, DurableFunctionTestResult)
    assert result.status is InvocationStatus.SUCCEEDED


@patch("async_durable_execution._runner.local.Executor")
async def test_durable_function_test_runner_run_with_custom_params(
    mock_executor_class,
) -> None:
    """Test DurableFunctionLocalTestRunner run method with custom parameters."""
    handler = Mock()

    # Mock the class instances
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor

    # Mock execution output
    output = StartDurableExecutionOutput(execution_arn="test-arn")
    mock_executor.start_execution.return_value = output
    mock_executor.wait_until_complete = AsyncMock(return_value=True)

    # Mock execution for result creation
    mock_execution = Mock(spec=Execution)
    mock_execution.operations = []
    mock_execution.result = Mock()
    mock_execution.result.status = InvocationStatus.SUCCEEDED
    mock_execution.result.result = json.dumps("test-result")
    mock_execution.result.error = None
    mock_executor.get_execution.return_value = mock_execution

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


@patch("async_durable_execution._runner.local.Executor")
async def test_durable_function_test_runner_run_timeout(mock_executor_class) -> None:
    """Test DurableFunctionLocalTestRunner run method with timeout."""
    handler = Mock()

    # Mock the class instance
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor

    # Mock execution output
    output = StartDurableExecutionOutput(execution_arn="test-arn")
    mock_executor.start_execution.return_value = output
    mock_executor.wait_until_complete = AsyncMock(return_value=False)  # Timeout

    runner = DurableFunctionLocalTestRunner(handler, input="test-input")

    with pytest.raises(TimeoutError, match="Execution did not complete within timeout"):
        await runner.run()


@no_type_check
async def test_runner_run_methods_do_not_accept_call_time_overrides() -> None:
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


async def test_durable_function_test_result_create_with_parent_operations() -> None:
    """Test DurableFunctionTestResult.create with operations that have parent_id."""
    execution = Mock(spec=Execution)

    # Create operation with parent_id (should be filtered out)
    child_op = Operation(
        operation_id="child-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent-id",
        step_details=StepDetails(result=json.dumps("child-result")),
    )

    # Create operation without parent_id (should be included)
    root_op = Operation(
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


async def test_durable_function_test_result_from_execution_history() -> None:
    """Test DurableFunctionTestResult.from_execution_history factory method."""
    import datetime

    from async_durable_execution import InvocationStatus
    from async_durable_execution._runner.model import (
        Event,
        EventResult,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
        StepSucceededDetails,
    )
    from async_durable_execution._runner.model import (
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
    assert isinstance(result.operations[0], Operation)
    assert result.operations[0].name == "test-step"


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_init(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner initialization."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        region="us-west-2",
        poll_interval=0.5,
    )

    assert runner.function_name == "test-function"
    assert runner.region == "us-west-2"
    assert runner.poll_interval == 0.5
    mock_boto3.assert_called_once_with(None, "us-west-2")


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_run_success(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.run with successful execution."""
    from async_durable_execution import InvocationStatus
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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
    mock_client.invoke.assert_called_once_with(
        FunctionName="test-function",
        InvocationType="RequestResponse",
        Payload='"test-input"',
    )
    mock_client.get_durable_execution.assert_called_once_with(
        DurableExecutionArn=(
            "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1"
        ),
        IncludeExecutionData=True,
    )


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_fetch_execution_history_paginates(mock_boto3) -> None:
    """Test cloud history fetching follows NextMarker until all events are loaded."""
    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
    mock_client.get_durable_execution_history.side_effect = [
        {
            "Events": [
                {
                    "EventType": "StepStarted",
                    "EventTimestamp": "2023-01-01T00:00:00Z",
                    "Id": "step-1",
                    "Name": "early-step",
                }
            ],
            "NextMarker": "page-2",
        },
        {
            "Events": [
                {
                    "EventType": "StepStarted",
                    "EventTimestamp": "2023-01-01T00:00:01Z",
                    "Id": "step-2",
                    "Name": "late-step",
                }
            ],
        },
    ]

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    history = await runner._fetch_execution_history("test-arn")

    assert [event.name for event in history.events] == ["early-step", "late-step"]
    assert mock_client.get_durable_execution_history.call_args_list[0].kwargs == {
        "DurableExecutionArn": "test-arn",
        "IncludeExecutionData": True,
    }
    assert mock_client.get_durable_execution_history.call_args_list[1].kwargs == {
        "DurableExecutionArn": "test-arn",
        "IncludeExecutionData": True,
        "Marker": "page-2",
    }


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_fetch_execution_history_rejects_repeated_marker(
    mock_boto3,
) -> None:
    """Test cloud history fetching fails if pagination does not advance."""
    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
    mock_client.get_durable_execution_history.return_value = {
        "Events": [],
        "NextMarker": "same-page",
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(DurableFunctionsTestError, match="repeated marker"):
        await runner._fetch_execution_history("test-arn")


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_run_invoke_failure(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.run with invoke failure."""
    from async_durable_execution._runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
    mock_client.invoke.side_effect = Exception("Invoke failed")

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        input="test-input",
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to invoke Lambda function"
    ):
        await runner.run()


@patch("async_durable_execution._runner.cloud.create_lambda_client")
@patch("async_durable_execution._runner.cloud.time")
async def test_cloud_runner_wait_for_completion_timeout(mock_time, mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner._wait_for_completion with timeout."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
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
        await runner._wait_for_completion("test-arn", timeout=2)


async def test_durable_function_test_result_from_execution_history_with_exception() -> (
    None
):
    """Test from_execution_history handles events_to_operations exception."""
    import datetime

    from async_durable_execution import InvocationStatus
    from async_durable_execution._runner.model import (
        Event,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from async_durable_execution._runner.model import (
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


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_completion_failed_status(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner._wait_for_completion with FAILED status."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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
    result = await runner._wait_for_completion("test-arn", timeout=10)

    assert result.status == "FAILED"


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_run_bad_status_code(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.run with bad HTTP status code."""
    from async_durable_execution._runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_run_failed_execution(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.run with a failed execution."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

    mock_client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": Mock(read=lambda: b'{"result": "started"}'),
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


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_run_missing_execution_arn(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.run with missing execution ARN."""
    from async_durable_execution._runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_completion_get_execution_failure(
    mock_boto3,
) -> None:
    """Test DurableFunctionCloudTestRunner._wait_for_completion with API failure."""
    from async_durable_execution._runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
    mock_client.get_durable_execution.side_effect = Exception("API error")

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to get execution status"
    ):
        await runner._wait_for_completion("test-arn", timeout=10)


@patch("async_durable_execution._runner.cloud.create_lambda_client")
@patch("async_durable_execution._runner.cloud.asyncio.sleep", new_callable=AsyncMock)
async def test_cloud_runner_wait_for_completion_retries_resource_not_found(
    mock_sleep, mock_boto3
) -> None:
    """Test _wait_for_completion retries until async execution is visible."""
    from botocore.exceptions import ClientError

    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
    mock_client.get_durable_execution.side_effect = [
        ClientError(
            error_response={"Error": {"Code": "ResourceNotFoundException"}},
            operation_name="GetDurableExecution",
        ),
        {
            "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
            "DurableExecutionName": "test-execution",
            "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
            "Status": "SUCCEEDED",
            "StartTimestamp": "2023-01-01T00:00:00Z",
            "EndTimestamp": "2023-01-01T00:01:00Z",
        },
    ]

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    result = await runner._wait_for_completion("test-arn", timeout=10)

    assert result.status == "SUCCEEDED"
    assert mock_client.get_durable_execution.call_count == 2
    assert mock_sleep.await_args_list.count(call(0.01)) == 1


async def test_durable_function_test_result_from_execution_history_filters_execution_type() -> (
    None
):
    """Test from_execution_history filters out EXECUTION type operations."""
    import datetime

    from async_durable_execution._runner.model import (
        Event,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from async_durable_execution._runner.model import (
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


async def test_durable_function_test_result_from_execution_history_unknown_status() -> (
    None
):
    """Test from_execution_history with unknown status defaults to FAILED."""
    import datetime

    from async_durable_execution import InvocationStatus
    from async_durable_execution._runner.model import (
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from async_durable_execution._runner.model import (
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


async def test_durable_function_test_result_from_execution_history_with_parent_operations() -> (
    None
):
    """Test from_execution_history filters operations with parent_id."""
    import datetime

    from async_durable_execution._runner.model import (
        Event,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from async_durable_execution._runner.model import (
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


@no_type_check
async def test_durable_function_test_result_from_execution_history_failed() -> None:
    """Test from_execution_history with failed execution."""
    import datetime

    from async_durable_execution import InvocationStatus
    from async_durable_execution._core.models import ErrorObject
    from async_durable_execution._runner.model import (
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from async_durable_execution._runner.model import (
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


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_completion_timed_out_status(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner._wait_for_completion with TIMED_OUT status."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "TIMED_OUT",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    result = await runner._wait_for_completion("test-arn", timeout=10)

    assert result.status == "TIMED_OUT"


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_completion_aborted_status(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner._wait_for_completion with ABORTED status."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "ABORTED",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    result = await runner._wait_for_completion("test-arn", timeout=10)

    assert result.status == "ABORTED"


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_run_async_success(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.run_async with successful invocation."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_run_async_with_400(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.run_async with successful invocation."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_run_async_failure(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.run_async with invocation failure."""
    from async_durable_execution._runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
    mock_client.invoke.side_effect = Exception("Async invoke failed")

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        input="test-input",
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to invoke Lambda function"
    ):
        await runner.run_async()


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_send_callback_success(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.send_callback_success."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    await runner.send_callback_success("callback-123")

    mock_client.send_durable_execution_callback_success.assert_called_once_with(
        CallbackId="callback-123", Result=None
    )


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_send_callback_failure(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.send_callback_failure."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    await runner.send_callback_failure("callback-123")

    mock_client.send_durable_execution_callback_failure.assert_called_once_with(
        CallbackId="callback-123", Error=None
    )


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_send_callback_heartbeat(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.send_callback_heartbeat."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    await runner.send_callback_heartbeat("callback-123")

    mock_client.send_durable_execution_callback_heartbeat.assert_called_once_with(
        CallbackId="callback-123"
    )


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_send_callback_error(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner callback methods with API errors."""
    from async_durable_execution._runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
    mock_client.send_durable_execution_callback_success.side_effect = Exception(
        "API error"
    )

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to send callback success"
    ):
        await runner.send_callback_success("callback-123")


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_callback_success(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.wait_for_callback success."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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
                "EventType": "InvocationCompleted",
                "EventTimestamp": "2023-01-01T00:00:01Z",
            },
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )
    callback_id = await runner.wait_for_callback(
        "test-arn", name="test-callback", timeout=10
    )

    assert callback_id == "callback-123"


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_callback_none(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.wait_for_callback none."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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
                "EventType": "InvocationCompleted",
                "EventTimestamp": "2023-01-01T00:00:01Z",
            },
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    with pytest.raises(TimeoutError, match="Callback was not available within"):
        await runner.wait_for_callback("test-arn", name="test-callback1", timeout=2)


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_callback_success_without_name(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.wait_for_callback success."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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
                "EventType": "InvocationCompleted",
                "EventTimestamp": "2023-01-01T00:00:01Z",
            },
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )
    callback_id = await runner.wait_for_callback("test-arn")

    assert callback_id == "callback-123"


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_callback_waits_for_creator_completion(
    mock_boto3,
) -> None:
    """Test that wait_for_callback waits until the callback creator settles."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

    callback_started_event = {
        "EventType": "CallbackStarted",
        "EventTimestamp": "2023-01-01T00:00:00Z",
        "Id": "callback-event-1",
        "Name": "test-callback",
        "CallbackStartedDetails": {"CallbackId": "callback-123"},
    }
    mock_client.get_durable_execution_history.side_effect = [
        {"Events": [callback_started_event]},
        {
            "Events": [
                callback_started_event,
                {
                    "EventType": "InvocationCompleted",
                    "EventTimestamp": "2023-01-01T00:00:01Z",
                },
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
    assert mock_client.get_durable_execution_history.call_count == 2


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_callback_all_done_without_name(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.wait_for_callback all_done_without_name."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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
    with pytest.raises(TimeoutError, match="Callback was not available within"):
        await runner.wait_for_callback("test-arn", timeout=2)


@patch("async_durable_execution._runner.local.Executor")
async def test_local_runner_wait_for_callback_all_done_without_name(
    mock_executor_class,
) -> None:
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
    with pytest.raises(TimeoutError, match="Callback was not available within"):
        await runner.wait_for_callback("test-arn", timeout=2)


@patch("async_durable_execution._runner.local.Executor")
async def test_local_runner_wait_for_callback_with_exception(
    mock_executor_class,
) -> None:
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


@patch("async_durable_execution._runner.local.Executor")
async def test_local_runner_wait_for_callback_with_resource_not_found_exception(
    mock_executor_class,
) -> None:
    """Test DurableFunctionLocalTestRunner.wait_for_callback with resource_not_found exception."""
    handler = Mock()
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor
    mock_executor.get_execution_history.side_effect = ResourceNotFoundException("error")

    runner = DurableFunctionLocalTestRunner(handler)
    with pytest.raises(TimeoutError, match="Callback was not available within"):
        await runner.wait_for_callback("test-arn", timeout=2)


@patch("async_durable_execution._runner.cloud.create_lambda_client")
@patch("async_durable_execution._runner.cloud.time")
async def test_cloud_runner_wait_for_callback_timeout(mock_time, mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.wait_for_callback timeout."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
    mock_time.time.side_effect = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]

    mock_client.get_durable_execution_history.return_value = {"Events": []}

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    with pytest.raises(TimeoutError, match="Callback was not available within"):
        await runner.wait_for_callback("test-arn", timeout=2)


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_callback_already_completed(mock_boto3) -> None:
    """Test DurableFunctionCloudTestRunner.wait_for_callback already completed."""
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_callback_client_error_retryable(
    mock_boto3,
) -> None:
    """Test wait_for_callback with retryable ClientError."""
    from botocore.exceptions import ClientError

    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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
                },
                {
                    "EventType": "InvocationCompleted",
                    "EventTimestamp": "2023-01-01T00:00:01Z",
                },
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


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_callback_client_error_non_retryable(
    mock_boto3,
) -> None:
    """Test wait_for_callback with non-retryable ClientError."""
    from botocore.exceptions import ClientError

    from async_durable_execution._runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

    mock_client.get_durable_execution_history.side_effect = ClientError(
        error_response={"Error": {"Code": "AccessDeniedException"}},
        operation_name="GetDurableExecutionHistory",
    )

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to fetch execution history"
    ):
        await runner.wait_for_callback("test-arn", timeout=10)


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_wait_for_callback_generic_exception(mock_boto3) -> None:
    """Test wait_for_callback with generic Exception."""
    from async_durable_execution._runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

    mock_client.get_durable_execution_history.side_effect = Exception("Network error")

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to fetch execution history"
    ):
        await runner.wait_for_callback("test-arn", timeout=10)


@patch("async_durable_execution._runner.cloud.create_lambda_client")
@no_type_check
async def test_cloud_runner_wait_for_result_fetch_history_exception(mock_boto3) -> None:
    """Test wait_for_result with exception in _fetch_execution_history."""
    from async_durable_execution._runner.exceptions import (
        DurableFunctionsTestError,
    )
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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


@patch("async_durable_execution._runner.cloud.create_lambda_client")
@no_type_check
async def test_cloud_runner_wait_for_result_success(mock_boto3) -> None:
    """Test wait_for_result successful execution."""
    from async_durable_execution import InvocationStatus
    from async_durable_execution._runner.cloud import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)

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
        "async_durable_execution._runner.model.DurableFunctionTestResult.from_execution_history"
    ) as mock_from_history:
        mock_result = Mock()
        mock_result.status = InvocationStatus.SUCCEEDED
        mock_from_history.return_value = mock_result

        result = await runner.wait_for_result("test-arn", timeout=60)

        assert result.status == InvocationStatus.SUCCEEDED
        mock_from_history.assert_called_once_with(
            mock_execution_response, mock_history_response
        )


def test_durable_function_test_result_create_requires_execution_result() -> None:
    """Test creating a result from an incomplete execution is rejected."""
    execution = Mock(spec=Execution)
    execution.operations = []
    execution.result = None

    with pytest.raises(
        DurableFunctionsTestError,
        match="Execution result must exist to create test result",
    ):
        DurableFunctionTestResult.create(execution)


def test_durable_function_test_result_operation_type_mismatch_raises() -> None:
    """Test typed accessors reject operations with the wrong type."""
    wait_op = Operation(
        operation_id="wait-id",
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
        name="shared-name",
    )
    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[wait_op],
    )

    with pytest.raises(
        DurableFunctionsTestError,
        match="expected OperationType.STEP",
    ):
        result.get_step("shared-name")


def test_durable_function_test_result_nested_operation_helpers() -> None:
    """Test nested operation helpers use the complete operation source."""
    parent_op = Operation(
        operation_id="parent-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        name="parent",
    )
    child_op = Operation(
        operation_id="child-id",
        parent_id="parent-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        name="child",
    )
    execution_op = Operation(
        operation_id="execution-id",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        name="execution",
    )
    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[parent_op],
        _all_operations=[execution_op, parent_op, child_op],
    )

    assert result.get_child_operations(parent_op) == [child_op]
    assert result.get_all_operations() == [parent_op, child_op]


def test_durable_function_test_result_deserializes_operation_payloads() -> None:
    """Test result payload extraction for supported operation types."""
    result = DurableFunctionTestResult(status=InvocationStatus.SUCCEEDED, operations=[])
    operations = [
        Operation(
            operation_id="context-id",
            operation_type=OperationType.CONTEXT,
            status=OperationStatus.SUCCEEDED,
            context_details=ContextDetails(result='{"kind": "context"}'),
        ),
        Operation(
            operation_id="step-id",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            step_details=StepDetails(result='{"kind": "step"}'),
        ),
        Operation(
            operation_id="callback-id",
            operation_type=OperationType.CALLBACK,
            status=OperationStatus.SUCCEEDED,
            callback_details=CallbackDetails(
                callback_id="callback-id",
                result='{"kind": "callback"}',
            ),
        ),
        Operation(
            operation_id="invoke-id",
            operation_type=OperationType.CHAINED_INVOKE,
            status=OperationStatus.SUCCEEDED,
            chained_invoke_details=ChainedInvokeDetails(result='{"kind": "invoke"}'),
        ),
        Operation(
            operation_id="wait-id",
            operation_type=OperationType.WAIT,
            status=OperationStatus.SUCCEEDED,
        ),
    ]

    assert result.get_operation_deserialized_result(operations[0]) == {
        "kind": "context"
    }
    assert result.get_operation_deserialized_result(operations[1]) == {"kind": "step"}
    assert result.get_operation_deserialized_result(operations[2]) == {
        "kind": "callback"
    }
    assert result.get_operation_deserialized_result(operations[3]) == {"kind": "invoke"}
    assert result.get_operation_deserialized_result(operations[4]) is None


async def test_local_runner_async_context_manager_closes() -> None:
    """Test local runner async context manager closes resources."""
    with patch.object(DurableFunctionLocalTestRunner, "__init__", return_value=None):
        with patch.object(DurableFunctionLocalTestRunner, "close") as mock_close:
            runner = DurableFunctionLocalTestRunner(Mock())

            async with runner:
                pass

            mock_close.assert_called_once()


async def test_local_runner_run_async_requires_execution_arn() -> None:
    """Test local run_async rejects missing execution ARN from executor."""
    with patch("async_durable_execution._runner.local.Executor") as mock_executor_class:
        mock_executor = Mock()
        mock_executor.start_execution.return_value = StartDurableExecutionOutput(
            execution_arn=None
        )
        mock_executor_class.return_value = mock_executor
        runner = DurableFunctionLocalTestRunner(Mock())

        with pytest.raises(
            DurableFunctionsTestError,
            match="Execution ARN must exist to run test",
        ):
            await runner.run_async()


@no_type_check
def test_local_runner_mock_invoke_result_delegates_to_service_client() -> None:
    """Test local invoke mocks are registered on the service client."""
    runner = DurableFunctionLocalTestRunner(Mock())

    try:
        runner.mock_invoke_result("child-function:$LATEST", {"child": "result"})

        service_client = runner._service_client
        child_processor = service_client._transformer.processors[
            OperationType.CHAINED_INVOKE
        ]
        assert child_processor._mock_results["child-function:$LATEST"] == (
            '{"child": "result"}'
        )
    finally:
        runner.close()


def test_cloud_runner_close_ignores_client_without_close() -> None:
    """Test close is a no-op for minimal clients without a close method."""
    with patch.object(DurableFunctionCloudTestRunner, "__init__", return_value=None):
        runner = DurableFunctionCloudTestRunner("test-function")
        runner.lambda_client = object()

        runner.close()


def test_cloud_runner_close_calls_client_close() -> None:
    """Test close delegates to clients that expose close."""
    with patch.object(DurableFunctionCloudTestRunner, "__init__", return_value=None):
        runner = DurableFunctionCloudTestRunner("test-function")
        runner.lambda_client = Mock()

        runner.close()

    runner.lambda_client.close.assert_called_once()


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_send_callback_failure_error(mock_boto3) -> None:
    """Test callback failure API errors are wrapped."""
    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
    mock_client.send_durable_execution_callback_failure.side_effect = Exception(
        "API error"
    )
    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to send callback failure"
    ):
        await runner.send_callback_failure("callback-123")


@patch("async_durable_execution._runner.cloud.create_lambda_client")
async def test_cloud_runner_send_callback_heartbeat_error(mock_boto3) -> None:
    """Test callback heartbeat API errors are wrapped."""
    mock_client = Mock()
    mock_boto3.return_value = ThreadedSyncCloudLambdaClient(mock_client)
    mock_client.send_durable_execution_callback_heartbeat.side_effect = Exception(
        "API error"
    )
    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to send callback heartbeat"
    ):
        await runner.send_callback_heartbeat("callback-123")
