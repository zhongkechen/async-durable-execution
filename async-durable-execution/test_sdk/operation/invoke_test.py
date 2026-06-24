"""Unit tests for invoke handler."""

from __future__ import annotations

import inspect
import json
from unittest.mock import Mock, patch

import pytest
from async_durable_execution.exceptions import (
    CallableRuntimeError,
    ExecutionError,
    SuspendExecution,
    TimedSuspendExecution,
    suspend_with_optional_resume_delay,
)
from async_durable_execution.models import OperationIdentifier
from async_durable_execution.models import (
    ChainedInvokeDetails,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from async_durable_execution.operation.invoke import (
    InvokeConfig,
    InvokeOperationExecutor,
    invoke,
)
from async_durable_execution.state import ExecutionState
from async_durable_execution.operation.base import CheckpointedResult

from ..serdes_test import CustomDictSerDes


# Test helper - maintains old handler signature for backward compatibility in tests
async def invoke_handler(function_name, payload, state, operation_identifier, config):
    """Test helper that wraps InvokeOperationExecutor with old handler signature."""
    if not config:
        config = InvokeConfig()
    executor = InvokeOperationExecutor(
        function_name=function_name,
        payload=payload,
        state=state,
        operation_identifier=operation_identifier,
        config=config,
    )
    return await executor.process()


def test_invoke_config_defaults():
    """InvokeConfig keeps its expected defaults."""
    config = InvokeConfig()

    assert config.serdes_payload is None
    assert config.serdes_result is None
    assert config.tenant_id is None


def test_invoke_config_with_tenant_id():
    """InvokeConfig stores explicit tenant ids."""
    config = InvokeConfig(tenant_id="test-tenant")

    assert config.tenant_id == "test-tenant"


def test_invoke_name_is_keyword_only():
    """invoke operation name must be passed as a keyword."""
    parameters = inspect.signature(invoke).parameters

    assert parameters["function_name"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["payload"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY


async def test_invoke_handler_already_succeeded():
    """Test invoke_handler when operation already succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=json.dumps("test_result")),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke1", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=None,
    )

    assert result == "test_result"
    mock_state.create_checkpoint.assert_not_called()


async def test_invoke_handler_already_succeeded_none_result():
    """Test invoke_handler when operation succeeded with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke2",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=None),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke2", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=None,
    )

    assert result is None


async def test_invoke_handler_already_succeeded_no_chained_invoke_details():
    """Test invoke_handler when operation succeeded but has no chained_invoke_details."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke3",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=None,
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke3", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=None,
    )

    assert result is None


@pytest.mark.parametrize(
    "kind", [OperationStatus.FAILED, OperationStatus.STOPPED, OperationStatus.TIMED_OUT]
)
async def test_invoke_handler_already_terminated(kind: OperationStatus):
    """Test invoke_handler when operation already failed."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="invoke4",
        operation_type=OperationType.CHAINED_INVOKE,
        status=kind,
        chained_invoke_details=ChainedInvokeDetails(error=error),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(CallableRuntimeError):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke4", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=None,
        )


async def test_invoke_handler_already_timed_out():
    """Test invoke_handler when operation already timed out."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    error = ErrorObject(
        message="Operation timed out", type="TimeoutError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="invoke5",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.TIMED_OUT,
        chained_invoke_details=ChainedInvokeDetails(error=error),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(CallableRuntimeError):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke5", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=None,
        )


@pytest.mark.parametrize("status", [OperationStatus.STARTED])
async def test_invoke_handler_already_started(status):
    """Test invoke_handler when operation is already started."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke6",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(
        SuspendExecution, match="Invoke invoke6 started, suspending for completion"
    ):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke6", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=None,
        )


@pytest.mark.parametrize("status", [OperationStatus.STARTED, OperationStatus.PENDING])
async def test_invoke_handler_already_started_with_timeout(status):
    """Test invoke_handler when operation is already started."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke7",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    config = InvokeConfig[str, str]()

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke7", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=config,
        )


async def test_invoke_handler_new_operation():
    """Test invoke_handler when starting a new operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started (no immediate response)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke8",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    config = InvokeConfig[str, str]()

    with pytest.raises(
        SuspendExecution, match="Invoke invoke8 started, suspending for completion"
    ):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke8", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=config,
        )

    # Verify checkpoint was created
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]

    assert operation_update.operation_id == "invoke8"
    assert operation_update.operation_type == OperationType.CHAINED_INVOKE
    assert operation_update.action == OperationAction.START
    assert operation_update.name == "test_invoke"
    assert operation_update.payload == json.dumps("test_input")
    assert operation_update.chained_invoke_options.function_name == "test_function"


async def test_invoke_handler_new_operation_with_config():
    """Test invoke_handler when starting a new operation with explicit config."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    config = InvokeConfig[str, str]()

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke9", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=config,
        )


async def test_invoke_handler_new_operation_default_config():
    """Test invoke_handler when starting a new operation with default config."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    config = InvokeConfig[str, str]()

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke10", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=config,
        )


async def test_invoke_handler_no_config():
    """Test invoke_handler when no config is provided."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke11", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=None,
        )

    # Verify default config was used
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    chained_invoke_options = operation_update.to_dict()["ChainedInvokeOptions"]
    assert chained_invoke_options["FunctionName"] == "test_function"
    # tenant_id should be None when not specified
    assert "TenantId" not in chained_invoke_options


async def test_invoke_handler_custom_serdes():
    """Test invoke_handler with custom serialization."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke12",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}',
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    config = InvokeConfig[dict, dict](
        serdes_payload=CustomDictSerDes(), serdes_result=CustomDictSerDes()
    )

    result = await invoke_handler(
        function_name="test_function",
        payload={"key": "value", "number": 42, "list": [1, 2, 3]},
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke12", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=config,
    )

    # CustomDictSerDes transforms the result back
    assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}


async def test_invoke_handler_custom_serdes_new_operation():
    """Test invoke_handler with custom serialization for new operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    config = InvokeConfig[dict, dict](
        serdes_payload=CustomDictSerDes(), serdes_result=CustomDictSerDes()
    )
    complex_payload = {"key": "value", "number": 42, "list": [1, 2, 3]}

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload=complex_payload,
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke13", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=config,
        )

    # Verify custom serialization was used
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    expected_serialized = '{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
    assert operation_update.payload == expected_serialized


async def test_suspend_with_optional_resume_delay_with_timeout():
    """Test suspend_with_optional_resume_delay with timeout."""
    with pytest.raises(TimedSuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", 30)

    assert "test message" in str(exc_info.value)


async def test_suspend_with_optional_resume_delay_no_timeout():
    """Test suspend_with_optional_resume_delay without timeout."""
    with pytest.raises(SuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", None)

    assert "test message" in str(exc_info.value)


async def test_suspend_with_optional_resume_delay_zero_timeout():
    """Test suspend_with_optional_resume_delay with zero timeout."""
    with pytest.raises(SuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", 0)

    assert "test message" in str(exc_info.value)


async def test_suspend_with_optional_resume_delay_negative_timeout():
    """Test suspend_with_optional_resume_delay with negative timeout."""
    with pytest.raises(SuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", -5)

    assert "test message" in str(exc_info.value)


@pytest.mark.parametrize("status", [OperationStatus.STARTED, OperationStatus.PENDING])
async def test_invoke_handler_with_operation_name(status: OperationStatus):
    """Test invoke_handler uses operation name in logs when available."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke14",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke14", OperationSubType.CHAINED_INVOKE, None, "named_invoke"
            ),
            config=None,
        )


@pytest.mark.parametrize("status", [OperationStatus.STARTED, OperationStatus.PENDING])
async def test_invoke_handler_without_operation_name(status: OperationStatus):
    """Test invoke_handler uses function name in logs when no operation name."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke15",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke15", OperationSubType.CHAINED_INVOKE, None, None
            ),
            config=None,
        )


async def test_invoke_handler_with_none_payload():
    """Test invoke_handler when payload is None."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload=None,
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke16", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=None,
        )

    # Verify checkpoint was created with None payload
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    assert operation_update.payload == "null"  # JSON serialization of None


async def test_invoke_handler_already_succeeded_with_none_payload():
    """Test invoke_handler when operation succeeded and original payload was None."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke17",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=json.dumps("test_result")),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    result = await invoke_handler(
        function_name="test_function",
        payload=None,
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke17", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=None,
    )

    assert result == "test_result"
    mock_state.create_checkpoint.assert_not_called()


@patch("async_durable_execution.operation.invoke.suspend_with_optional_resume_delay")
async def test_invoke_handler_suspend_does_not_raise(mock_suspend):
    """Test invoke_handler when suspend_with_optional_resume_delay doesn't raise an exception."""

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    # Mock suspend_with_optional_resume_delay to not raise an exception (which it should always do)
    mock_suspend.return_value = None

    with pytest.raises(
        ExecutionError,
        match="suspend_with_optional_resume_delay should have raised an exception, but did not.",
    ):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke18", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            config=None,
        )

    mock_suspend.assert_called_once()


async def test_invoke_handler_with_tenant_id():
    """Test invoke_handler passes tenant_id to checkpoint."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    config = InvokeConfig(tenant_id="test-tenant-123")

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke1", OperationSubType.CHAINED_INVOKE, None, None
            ),
            config=config,
        )

    # Verify checkpoint was called with tenant_id
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    chained_invoke_options = operation_update.to_dict()["ChainedInvokeOptions"]
    assert chained_invoke_options["FunctionName"] == "test_function"
    assert chained_invoke_options["TenantId"] == "test-tenant-123"


async def test_invoke_handler_without_tenant_id():
    """Test invoke_handler without tenant_id doesn't include it in checkpoint."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    config = InvokeConfig(tenant_id=None)

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke1", OperationSubType.CHAINED_INVOKE, None, None
            ),
            config=config,
        )

    # Verify checkpoint was called without tenant_id
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    chained_invoke_options = operation_update.to_dict()["ChainedInvokeOptions"]
    assert chained_invoke_options["FunctionName"] == "test_function"
    assert "TenantId" not in chained_invoke_options


async def test_invoke_handler_default_config_no_tenant_id():
    """Test invoke_handler with default config has no tenant_id."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke1", OperationSubType.CHAINED_INVOKE, None, None
            ),
            config=None,
        )

    # Verify checkpoint was called without tenant_id
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    chained_invoke_options = operation_update.to_dict()["ChainedInvokeOptions"]
    assert chained_invoke_options["FunctionName"] == "test_function"
    assert "TenantId" not in chained_invoke_options


async def test_invoke_handler_defaults_to_json_serdes():
    """Test invoke_handler uses DEFAULT_JSON_SERDES when config has no serdes."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    config = InvokeConfig[dict, dict](serdes_payload=None, serdes_result=None)
    payload = {"key": "value", "number": 42}

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload=payload,
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke_json", OperationSubType.CHAINED_INVOKE, None, None
            ),
            config=config,
        )

    # Verify JSON serialization was used (not extended types)
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    assert operation_update.payload == json.dumps(payload)


async def test_invoke_handler_result_defaults_to_json_serdes():
    """Test invoke_handler uses DEFAULT_JSON_SERDES for result deserialization."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    result_data = {"key": "value", "number": 42}
    operation = Operation(
        operation_id="invoke_result_json",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=json.dumps(result_data)),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    config = InvokeConfig[dict, dict](serdes_payload=None, serdes_result=None)

    result = await invoke_handler(
        function_name="test_function",
        payload={"input": "data"},
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke_result_json", OperationSubType.CHAINED_INVOKE, None, None
        ),
        config=config,
    )

    # Verify JSON deserialization was used (not extended types)
    assert result == result_data


# ============================================================================
# Immediate Response Handling Tests
# ============================================================================


async def test_invoke_immediate_response_get_checkpoint_result_called_twice():
    """Test that get_checkpoint_result is called twice when checkpoint is created."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started (no immediate response)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke_immediate_1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke_immediate_1",
                OperationSubType.CHAINED_INVOKE,
                None,
                "test_invoke",
            ),
            config=None,
        )

    # Verify get_checkpoint_result was called twice
    assert mock_state.operations.get.call_count == 2


async def test_invoke_immediate_response_create_checkpoint_with_is_sync_true():
    """Test that create_checkpoint is called with is_sync=True."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke_immediate_2",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke_immediate_2",
                OperationSubType.CHAINED_INVOKE,
                None,
                "test_invoke",
            ),
            config=None,
        )

    # Verify create_checkpoint was called with is_sync=True
    mock_state.create_checkpoint.assert_called_once()
    call_kwargs = mock_state.create_checkpoint.call_args[1]
    assert call_kwargs["is_sync"] is True


async def test_invoke_immediate_response_immediate_success():
    """Test immediate success: checkpoint returns SUCCEEDED on second check.

    When checkpoint returns SUCCEEDED on second check, operation returns result
    without suspend.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: succeeded (immediate response)
    not_found = CheckpointedResult.create_not_found()
    succeeded_op = Operation(
        operation_id="invoke_immediate_3",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(
            result=json.dumps("immediate_result")
        ),
    )
    succeeded = CheckpointedResult.create_from_operation(succeeded_op)
    mock_state.operations.get.side_effect = [not_found, succeeded]

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke_immediate_3", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=None,
    )

    # Verify result was returned without suspend
    assert result == "immediate_result"
    # Verify checkpoint was created
    mock_state.create_checkpoint.assert_called_once()
    # Verify get_checkpoint_result was called twice
    assert mock_state.operations.get.call_count == 2


async def test_invoke_immediate_response_immediate_success_with_none_result():
    """Test immediate success with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: succeeded with None result
    not_found = CheckpointedResult.create_not_found()
    succeeded_op = Operation(
        operation_id="invoke_immediate_4",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=None),
    )
    succeeded = CheckpointedResult.create_from_operation(succeeded_op)
    mock_state.operations.get.side_effect = [not_found, succeeded]

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke_immediate_4", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=None,
    )

    # Verify None result was returned without suspend
    assert result is None
    assert mock_state.operations.get.call_count == 2


@pytest.mark.parametrize(
    "status",
    [OperationStatus.FAILED, OperationStatus.TIMED_OUT, OperationStatus.STOPPED],
)
async def test_invoke_immediate_response_immediate_failure(status: OperationStatus):
    """Test immediate failure: checkpoint returns FAILED/TIMED_OUT/STOPPED on second check.

    When checkpoint returns a failure status on second check, operation raises error
    without suspend.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: failed (immediate response)
    not_found = CheckpointedResult.create_not_found()
    error = ErrorObject(
        message="Immediate failure", type="TestError", data=None, stack_trace=None
    )
    failed_op = Operation(
        operation_id="invoke_immediate_5",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(error=error),
    )
    failed = CheckpointedResult.create_from_operation(failed_op)
    mock_state.operations.get.side_effect = [not_found, failed]

    # Verify error is raised without suspend
    with pytest.raises(CallableRuntimeError):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke_immediate_5",
                OperationSubType.CHAINED_INVOKE,
                None,
                "test_invoke",
            ),
            config=None,
        )

    # Verify checkpoint was created
    mock_state.create_checkpoint.assert_called_once()
    # Verify get_checkpoint_result was called twice
    assert mock_state.operations.get.call_count == 2


async def test_invoke_immediate_response_no_immediate_response():
    """Test no immediate response: checkpoint returns STARTED on second check.

    When checkpoint returns STARTED on second check, operation suspends normally.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started (no immediate response)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke_immediate_6",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    # Verify operation suspends
    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke_immediate_6",
                OperationSubType.CHAINED_INVOKE,
                None,
                "test_invoke",
            ),
            config=None,
        )

    # Verify checkpoint was created
    mock_state.create_checkpoint.assert_called_once()
    # Verify get_checkpoint_result was called twice
    assert mock_state.operations.get.call_count == 2


async def test_invoke_immediate_response_already_completed():
    """Test already completed: checkpoint is already SUCCEEDED on first check.

    When checkpoint is already SUCCEEDED on first check, no checkpoint is created
    and result is returned immediately.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: already succeeded
    succeeded_op = Operation(
        operation_id="invoke_immediate_7",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(
            result=json.dumps("existing_result")
        ),
    )
    succeeded = CheckpointedResult.create_from_operation(succeeded_op)
    mock_state.operations.get.return_value = succeeded

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke_immediate_7", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=None,
    )

    # Verify result was returned
    assert result == "existing_result"
    # Verify no checkpoint was created
    mock_state.create_checkpoint.assert_not_called()
    # Verify get_checkpoint_result was called only once
    assert mock_state.operations.get.call_count == 1


async def test_invoke_immediate_response_with_config_immediate_success():
    """Test immediate success with explicit config."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: succeeded
    not_found = CheckpointedResult.create_not_found()
    succeeded_op = Operation(
        operation_id="invoke_immediate_8",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(
            result=json.dumps("timeout_result")
        ),
    )
    succeeded = CheckpointedResult.create_from_operation(succeeded_op)
    mock_state.operations.get.side_effect = [not_found, succeeded]

    config = InvokeConfig[str, str]()

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke_immediate_8", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=config,
    )

    # Verify result was returned without suspend
    assert result == "timeout_result"
    assert mock_state.operations.get.call_count == 2


async def test_invoke_immediate_response_with_config_no_immediate_response():
    """Test no immediate response with explicit config.

    When no immediate response, operation should suspend normally.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke_immediate_9",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    config = InvokeConfig[str, str]()

    # Verify operation suspends normally
    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke_immediate_9",
                OperationSubType.CHAINED_INVOKE,
                None,
                "test_invoke",
            ),
            config=config,
        )

    assert mock_state.operations.get.call_count == 2


async def test_invoke_immediate_response_with_custom_serdes():
    """Test immediate success with custom serialization."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: succeeded
    not_found = CheckpointedResult.create_not_found()
    succeeded_op = Operation(
        operation_id="invoke_immediate_10",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
        ),
    )
    succeeded = CheckpointedResult.create_from_operation(succeeded_op)
    mock_state.operations.get.side_effect = [not_found, succeeded]

    config = InvokeConfig[dict, dict](
        serdes_payload=CustomDictSerDes(), serdes_result=CustomDictSerDes()
    )

    result = await invoke_handler(
        function_name="test_function",
        payload={"key": "value", "number": 42, "list": [1, 2, 3]},
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke_immediate_10", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=config,
    )

    # Verify custom deserialization was used
    assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}
    assert mock_state.operations.get.call_count == 2


async def test_invoke_suspends_when_second_check_returns_started():
    """Test backward compatibility: when the second checkpoint check returns
    STARTED (not terminal), the invoke operation suspends normally.

    Validates: Requirements 8.1, 8.2
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: checkpoint doesn't exist
    # Second call: checkpoint returns STARTED (no immediate response)
    mock_state.operations.get.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(
            Operation(
                operation_id="invoke-1",
                operation_type=OperationType.STEP,
                status=OperationStatus.STARTED,
            )
        ),
    ]

    executor = InvokeOperationExecutor(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke-1", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        function_name="my-function",
        payload={"data": "test"},
        config=InvokeConfig(),
    )

    with pytest.raises(SuspendExecution):
        await executor.process()

    # Assert - behaves like "old way"
    assert mock_state.operations.get.call_count == 2  # Double-check happened
    mock_state.create_checkpoint.assert_called_once()  # START checkpoint created


async def test_invoke_suspends_when_second_check_returns_started_duplicate():
    """Test backward compatibility: when the second checkpoint check returns
    STARTED (not terminal), the invoke operation suspends normally.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: checkpoint doesn't exist
    # Second call: checkpoint returns STARTED (no immediate response)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="invoke-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.operations.get.side_effect = [not_found, started]

    executor = InvokeOperationExecutor(
        function_name="my-function",
        payload={"data": "test"},
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke-1", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        config=InvokeConfig(),
    )

    with pytest.raises(SuspendExecution):
        await executor.process()

    # Assert - behaves like "old way"
    assert mock_state.operations.get.call_count == 2  # Double-check happened
    mock_state.create_checkpoint.assert_called_once()  # START checkpoint created
