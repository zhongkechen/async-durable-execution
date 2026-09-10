"""Unit tests for invoke handler."""

from __future__ import annotations
from typing import Any

import inspect
import json
from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest
from async_durable_execution._core.context import (
    DurableContext,
    reset_current_context,
    set_current_context,
)
from async_durable_execution._core.exceptions import (
    CallableRuntimeError,
    ExecutionError,
    SuspendExecution,
    TimedSuspendExecution,
    ValidationError,
    suspend_with_optional_resume_delay,
)
from async_durable_execution._core.models import OperationIdentifier
from async_durable_execution._core.models import (
    ChainedInvokeDetails,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from async_durable_execution._operation.invoke import invoke
from async_durable_execution._primitive.invoke import InvokeOperationExecutor
from async_durable_execution._operation.recurse import recurse
from async_durable_execution._core.state import (
    RECURSIVE_LEVEL_INPUT_FIELD,
    ExecutionState,
)

from ..serdes_test import CustomDictSerDes


# Test helper - keeps executor setup concise in operation tests.
async def invoke_handler(
    function_name,
    payload,
    state,
    operation_identifier,
    serdes_payload=None,
    serdes_result=None,
    tenant_id=None,
) -> Any:
    """Test helper that wraps InvokeOperationExecutor."""
    executor = InvokeOperationExecutor(
        function_name=function_name,
        payload=payload,
        state=state,
        operation_identifier=operation_identifier,
        serdes_payload=serdes_payload,
        serdes_result=serdes_result,
        tenant_id=tenant_id,
    )
    return await executor.process()


def test_invoke_name_is_keyword_only() -> None:
    """invoke operation name must be passed as a keyword."""
    parameters = inspect.signature(invoke).parameters

    assert parameters["function_name"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["payload"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY


def test_recurse_name_and_function_name_are_keyword_only() -> None:
    """recurse resolves the target from context unless explicitly overridden."""
    parameters = inspect.signature(recurse).parameters

    assert parameters["payload"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["function_name"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["with_recursive_level"].kind is inspect.Parameter.KEYWORD_ONLY


def _recursive_level_from_input(input_event) -> int:
    if isinstance(input_event, dict) and isinstance(
        input_event.get(RECURSIVE_LEVEL_INPUT_FIELD), int
    ):
        return input_event[RECURSIVE_LEVEL_INPUT_FIELD]
    return 0


def create_recursive_test_context(lambda_context, input_event=None) -> DurableContext:
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.lambda_context = lambda_context
    input_event = {"current": "input"} if input_event is None else input_event
    mock_state.get_input_event.return_value = input_event
    mock_state.recursive_level = _recursive_level_from_input(input_event)
    return DurableContext(
        execution_state=mock_state,
        operation_identifier=OperationIdentifier.create_execution_op(),
    )


async def run_recurse_with_context(context: DurableContext, **kwargs) -> Any:
    executor = Mock()
    executor.process = AsyncMock(return_value="recursive-result")

    token = set_current_context(context)
    try:
        with patch(
            "async_durable_execution._primitive.invoke.InvokeOperationExecutor",
            return_value=executor,
        ) as mock_executor:
            result = await recurse(**kwargs)
    finally:
        reset_current_context(token)

    return result, mock_executor, executor


async def test_recurse_uses_current_invoked_function_arn() -> None:
    """recurse invokes the current qualified Lambda ARN by default."""
    lambda_context = Mock()
    lambda_context.invoked_function_arn = (
        "arn:aws:lambda:us-east-1:123456789012:function:test-function:prod"
    )
    lambda_context.function_version = "42"
    lambda_context.function_name = "test-function"
    lambda_context.tenant_id = "tenant-1"
    context = create_recursive_test_context(lambda_context)

    result, mock_executor, executor = await run_recurse_with_context(
        context,
        payload={"n": 4},
        name="recurse-left",
    )

    assert result == "recursive-result"
    mock_executor.assert_called_once_with(
        function_name=(
            "arn:aws:lambda:us-east-1:123456789012:function:test-function:prod"
        ),
        payload={"n": 4},
        state=context.execution_state,
        operation_identifier=ANY,
        serdes_payload=None,
        serdes_result=None,
        tenant_id="tenant-1",
    )
    executor.process.assert_awaited_once()


async def test_recurse_appends_function_version_to_unqualified_arn() -> None:
    """A local or unqualified ARN is made qualified when the context has a version."""
    lambda_context = Mock()
    lambda_context.invoked_function_arn = (
        "arn:aws:lambda:us-east-1:123456789012:function:test-function"
    )
    lambda_context.function_version = "$LATEST"
    lambda_context.function_name = "test-function"
    lambda_context.tenant_id = "tenant-1"
    context = create_recursive_test_context(lambda_context)

    _, mock_executor, _ = await run_recurse_with_context(
        context,
        payload={"n": 4},
        tenant_id="tenant-override",
    )

    assert (
        mock_executor.call_args.kwargs["function_name"]
        == "arn:aws:lambda:us-east-1:123456789012:function:test-function:$LATEST"
    )
    assert mock_executor.call_args.kwargs["tenant_id"] == "tenant-override"


async def test_recurse_falls_back_to_context_function_name() -> None:
    """Short function names are qualified with function_version when possible."""
    lambda_context = Mock()
    lambda_context.invoked_function_arn = None
    lambda_context.function_name = "test-function"
    lambda_context.function_version = "prod"
    lambda_context.tenant_id = None
    context = create_recursive_test_context(lambda_context)

    _, mock_executor, _ = await run_recurse_with_context(
        context,
        payload={"n": 4},
    )

    assert mock_executor.call_args.kwargs["function_name"] == "test-function:prod"


async def test_recurse_explicit_function_name_does_not_need_lambda_context() -> None:
    """Explicit function_name is an escape hatch for unusual runtimes and tests."""
    context = create_recursive_test_context(lambda_context=None)

    _, mock_executor, _ = await run_recurse_with_context(
        context,
        payload={"n": 4},
        function_name="test-function:prod",
    )

    assert mock_executor.call_args.kwargs["function_name"] == "test-function:prod"


async def test_recurse_adds_recursive_level_to_payload() -> None:
    """The first recursive call gets __recursive_level=1."""
    context = create_recursive_test_context(
        lambda_context=None,
        input_event={"n": 10},
    )

    _, mock_executor, _ = await run_recurse_with_context(
        context,
        payload={"n": 5},
        function_name="test-function:prod",
        with_recursive_level=True,
    )

    assert mock_executor.call_args.kwargs["payload"] == {
        "n": 5,
        RECURSIVE_LEVEL_INPUT_FIELD: 1,
    }


async def test_recurse_increments_existing_recursive_level() -> None:
    """Nested recursive calls increment from the current context level."""
    context = create_recursive_test_context(
        lambda_context=None,
        input_event={"n": 10, RECURSIVE_LEVEL_INPUT_FIELD: 2},
    )

    _, mock_executor, _ = await run_recurse_with_context(
        context,
        payload={"n": 5},
        function_name="test-function:prod",
        with_recursive_level=True,
    )

    assert mock_executor.call_args.kwargs["payload"] == {
        "n": 5,
        RECURSIVE_LEVEL_INPUT_FIELD: 3,
    }


async def test_recurse_rejects_recursive_level_for_non_dict_payload() -> None:
    """recursive_level can only be inserted into dict payloads."""
    context = create_recursive_test_context(
        lambda_context=None,
        input_event={"n": 10},
    )

    token = set_current_context(context)
    try:
        with pytest.raises(ValidationError, match="must be a dict"):
            recurse(
                [1, 2, 3],
                function_name="test-function:prod",
                with_recursive_level=True,
            )
    finally:
        reset_current_context(token)


async def test_recurse_rejects_same_payload_as_execution_input() -> None:
    """recurse must make progress by changing the payload it sends."""
    current_input = {"n": 4}
    context = create_recursive_test_context(
        lambda_context=None,
        input_event=current_input,
    )

    token = set_current_context(context)
    try:
        with pytest.raises(ValidationError, match="must differ"):
            recurse(current_input, function_name="test-function:prod")
    finally:
        reset_current_context(token)


async def test_recurse_requires_resolvable_function_name() -> None:
    """Missing Lambda function metadata produces a clear error."""
    lambda_context = Mock()
    lambda_context.invoked_function_arn = None
    lambda_context.function_name = None
    lambda_context.function_version = None
    lambda_context.tenant_id = None
    context = create_recursive_test_context(lambda_context)

    token = set_current_context(context)
    try:
        with pytest.raises(RuntimeError, match="could not determine"):
            recurse({"n": 4})
    finally:
        reset_current_context(token)


async def test_invoke_handler_already_succeeded() -> None:
    """Test invoke_handler when operation already succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=json.dumps("test_result")),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke1", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
    )

    assert result == "test_result"
    mock_state.create_checkpoint.assert_not_called()


async def test_invoke_handler_already_succeeded_none_result() -> None:
    """Test invoke_handler when operation succeeded with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke2",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=None),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke2", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
    )

    assert result is None


async def test_invoke_handler_already_succeeded_no_chained_invoke_details() -> None:
    """Test invoke_handler when operation succeeded but has no chained_invoke_details."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke3",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=None,
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke3", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
    )

    assert result is None


@pytest.mark.parametrize(
    "kind", [OperationStatus.FAILED, OperationStatus.STOPPED, OperationStatus.TIMED_OUT]
)
async def test_invoke_handler_already_terminated(kind: OperationStatus) -> None:
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(CallableRuntimeError):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke4", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
        )


async def test_invoke_handler_already_timed_out() -> None:
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(CallableRuntimeError):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke5", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
        )


async def test_invoke_handler_terminal_without_error_object() -> None:
    """Terminal invoke checkpoints without an ErrorObject surface an unknown error."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke_missing_error",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.FAILED,
        chained_invoke_details=None,
    )
    mock_state.operations.get.return_value = operation

    with pytest.raises(CallableRuntimeError, match="Unknown error"):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke_missing_error",
                OperationSubType.CHAINED_INVOKE,
                None,
                "test_invoke",
            ),
        )


@pytest.mark.parametrize("status", [OperationStatus.STARTED])
async def test_invoke_handler_already_started(status) -> None:
    """Test invoke_handler when operation is already started."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke6",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = operation
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
        )


@pytest.mark.parametrize("status", [OperationStatus.STARTED, OperationStatus.PENDING])
async def test_invoke_handler_already_started_with_timeout(status) -> None:
    """Test invoke_handler when operation is already started."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke7",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result
    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke7", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
        )


async def test_invoke_handler_new_operation() -> None:
    """Test invoke_handler when starting a new operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started (no immediate response)
    not_found = None
    started_op = Operation(
        operation_id="invoke8",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]
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


async def test_invoke_handler_new_operation_with_direct_defaults() -> None:
    """Test invoke_handler when starting a new operation with direct defaults."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = None
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]
    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke9", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
        )


async def test_invoke_handler_new_operation_default_fields() -> None:
    """Test invoke_handler when starting a new operation with default fields."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = None
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]
    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke10", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
        )


async def test_invoke_handler_no_optional_fields() -> None:
    """Test invoke_handler when no optional fields is provided."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = None
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke11", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
        )

    # Verify default fields was used
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    chained_invoke_options = operation_update.to_dict()["ChainedInvokeOptions"]
    assert chained_invoke_options["FunctionName"] == "test_function"
    # tenant_id should be None when not specified
    assert "TenantId" not in chained_invoke_options


async def test_invoke_handler_custom_serdes() -> None:
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    serdes_payload = CustomDictSerDes()
    serdes_result = CustomDictSerDes()

    result = await invoke_handler(
        function_name="test_function",
        payload={"key": "value", "number": 42, "list": [1, 2, 3]},
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke12", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
        serdes_payload=serdes_payload,
        serdes_result=serdes_result,
    )

    # CustomDictSerDes transforms the result back
    assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}


async def test_invoke_handler_custom_serdes_new_operation() -> None:
    """Test invoke_handler with custom serialization for new operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = None
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]

    serdes_payload = CustomDictSerDes()
    serdes_result = CustomDictSerDes()
    complex_payload = {"key": "value", "number": 42, "list": [1, 2, 3]}

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload=complex_payload,
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke13", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
            serdes_payload=serdes_payload,
            serdes_result=serdes_result,
        )

    # Verify custom serialization was used
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    expected_serialized = '{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
    assert operation_update.payload == expected_serialized


async def test_suspend_with_optional_resume_delay_with_timeout() -> None:
    """Test suspend_with_optional_resume_delay with timeout."""
    with pytest.raises(TimedSuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", 30)

    assert "test message" in str(exc_info.value)


async def test_suspend_with_optional_resume_delay_no_timeout() -> None:
    """Test suspend_with_optional_resume_delay without timeout."""
    with pytest.raises(SuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", None)

    assert "test message" in str(exc_info.value)


async def test_suspend_with_optional_resume_delay_zero_timeout() -> None:
    """Test suspend_with_optional_resume_delay with zero timeout."""
    with pytest.raises(SuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", 0)

    assert "test message" in str(exc_info.value)


async def test_suspend_with_optional_resume_delay_negative_timeout() -> None:
    """Test suspend_with_optional_resume_delay with negative timeout."""
    with pytest.raises(SuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", -5)

    assert "test message" in str(exc_info.value)


@pytest.mark.parametrize("status", [OperationStatus.STARTED, OperationStatus.PENDING])
async def test_invoke_handler_with_operation_name(status: OperationStatus) -> None:
    """Test invoke_handler uses operation name in logs when available."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke14",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke14", OperationSubType.CHAINED_INVOKE, None, "named_invoke"
            ),
        )


@pytest.mark.parametrize("status", [OperationStatus.STARTED, OperationStatus.PENDING])
async def test_invoke_handler_without_operation_name(status: OperationStatus) -> None:
    """Test invoke_handler uses function name in logs when no operation name."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke15",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke15", OperationSubType.CHAINED_INVOKE, None, None
            ),
        )


async def test_invoke_handler_with_none_payload() -> None:
    """Test invoke_handler when payload is None."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = None
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload=None,
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke16", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
            ),
        )

    # Verify checkpoint was created with None payload
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    assert operation_update.payload == "null"  # JSON serialization of None


async def test_invoke_handler_already_succeeded_with_none_payload() -> None:
    """Test invoke_handler when operation succeeded and original payload was None."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke17",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=json.dumps("test_result")),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    result = await invoke_handler(
        function_name="test_function",
        payload=None,
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke17", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
    )

    assert result == "test_result"
    mock_state.create_checkpoint.assert_not_called()


@patch("async_durable_execution._primitive.invoke.suspend_with_optional_resume_delay")
async def test_invoke_handler_suspend_does_not_raise(mock_suspend) -> None:
    """Test invoke_handler when suspend_with_optional_resume_delay doesn't raise an exception."""

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = None
    started_op = Operation(
        operation_id="invoke_test",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
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
        )

    mock_suspend.assert_called_once()


async def test_invoke_handler_with_tenant_id() -> None:
    """Test invoke_handler passes tenant_id to checkpoint."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = None
    started_op = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke1", OperationSubType.CHAINED_INVOKE, None, None
            ),
            tenant_id="test-tenant-123",
        )

    # Verify checkpoint was called with tenant_id
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    chained_invoke_options = operation_update.to_dict()["ChainedInvokeOptions"]
    assert chained_invoke_options["FunctionName"] == "test_function"
    assert chained_invoke_options["TenantId"] == "test-tenant-123"


async def test_invoke_handler_without_tenant_id() -> None:
    """Test invoke_handler without tenant_id doesn't include it in checkpoint."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = None
    started_op = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke1", OperationSubType.CHAINED_INVOKE, None, None
            ),
        )

    # Verify checkpoint was called without tenant_id
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    chained_invoke_options = operation_update.to_dict()["ChainedInvokeOptions"]
    assert chained_invoke_options["FunctionName"] == "test_function"
    assert "TenantId" not in chained_invoke_options


async def test_invoke_handler_default_fields_no_tenant_id() -> None:
    """Test invoke_handler with default fields has no tenant_id."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = None
    started_op = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke1", OperationSubType.CHAINED_INVOKE, None, None
            ),
        )

    # Verify checkpoint was called without tenant_id
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    chained_invoke_options = operation_update.to_dict()["ChainedInvokeOptions"]
    assert chained_invoke_options["FunctionName"] == "test_function"
    assert "TenantId" not in chained_invoke_options


async def test_invoke_handler_defaults_to_json_serdes() -> None:
    """Test invoke_handler uses DEFAULT_JSON_SERDES when no serdes fields are provided."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = None
    started_op = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]

    payload = {"key": "value", "number": 42}

    with pytest.raises(SuspendExecution):
        await invoke_handler(
            function_name="test_function",
            payload=payload,
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "invoke_json", OperationSubType.CHAINED_INVOKE, None, None
            ),
        )

    # Verify JSON serialization was used (not extended types)
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    assert operation_update.payload == json.dumps(payload)


async def test_invoke_handler_result_defaults_to_json_serdes() -> None:
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    result = await invoke_handler(
        function_name="test_function",
        payload={"input": "data"},
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke_result_json", OperationSubType.CHAINED_INVOKE, None, None
        ),
    )

    # Verify JSON deserialization was used (not extended types)
    assert result == result_data


# ============================================================================
# Start Handling Tests
# ============================================================================


async def test_invoke_start_direct_state_lookup_called_once() -> None:
    """Test start creates checkpoint then suspends without reloading it."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_state.operations.get.return_value = None

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
        )

    assert mock_state.operations.get.call_count == 1
    mock_state.create_checkpoint.assert_called_once()


async def test_invoke_start_create_checkpoint_with_is_sync_true() -> None:
    """Test that create_checkpoint is called with is_sync=True."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_state.operations.get.return_value = None

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
        )

    # Verify create_checkpoint was called with is_sync=True
    mock_state.create_checkpoint.assert_called_once()
    call_kwargs = mock_state.create_checkpoint.call_args[1]
    assert call_kwargs["is_sync"] is True


async def test_invoke_immediate_response_already_completed() -> None:
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
    succeeded = succeeded_op
    mock_state.operations.get.return_value = succeeded

    result = await invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "invoke_immediate_7", OperationSubType.CHAINED_INVOKE, None, "test_invoke"
        ),
    )

    # Verify result was returned
    assert result == "existing_result"
    # Verify no checkpoint was created
    mock_state.create_checkpoint.assert_not_called()
    # Verify direct state lookup was called only once
    assert mock_state.operations.get.call_count == 1


@pytest.mark.parametrize("status", [OperationStatus.STOPPED, OperationStatus.TIMED_OUT])
async def test_completed_flat_replay_preserves_matching_terminal_invoke(status) -> None:
    """The type guard still permits reading matching cached invoke errors."""
    from async_durable_execution._primitive.base import _completed_flat_replay

    state = Mock(spec=ExecutionState)
    state.create_checkpoint = AsyncMock()
    state.operations = {
        "invoke": Operation(
            operation_id="invoke",
            operation_type=OperationType.CHAINED_INVOKE,
            status=status,
            chained_invoke_details=ChainedInvokeDetails(
                error=ErrorObject(message="recorded failure", type="RemoteError")
            ),
        )
    }
    executor: InvokeOperationExecutor[str] = InvokeOperationExecutor(
        function_name="test_function",
        payload={},
        state=state,
        operation_identifier=OperationIdentifier(
            "invoke", OperationSubType.CHAINED_INVOKE
        ),
    )
    token = _completed_flat_replay.set(True)
    try:
        with pytest.raises(CallableRuntimeError, match="recorded failure") as raised:
            await executor.process()
        assert raised.value.error_type == "RemoteError"
    finally:
        _completed_flat_replay.reset(token)
    state.create_checkpoint.assert_not_called()
