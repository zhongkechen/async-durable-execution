"""Unit tests for callback handler."""

from typing import no_type_check

from collections.abc import Iterator
from contextlib import contextmanager
import inspect
import math
from datetime import timedelta
from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest
from async_durable_execution._core.context import (
    bind_current_context,
    reset_current_context,
    set_current_context,
    get_current_context,
)
from async_durable_execution._core.exceptions import (
    ValidationError,
    _encode_sdk_control_error_data,
    _restore_sdk_control_error,
)
from async_durable_execution._core.models import OperationIdentifier
from async_durable_execution._core.models import (
    CallbackDetails,
    CallbackOptions,
    CallbackTimeoutType,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
)
import async_durable_execution._operation.wait_for_callback as callback
from async_durable_execution._operation.callback import (
    Callback,
    CallbackError,
    create_callback,
)
from async_durable_execution._primitive.callback import (
    CallbackOperationExecutor,
)
from async_durable_execution._operation.wait_for_callback import (
    wait_for_callback,
    wait_for_callback_handler,
)
from async_durable_execution._core.serdes import SerDes
from async_durable_execution._core.state import ExecutionState
from async_durable_execution import (
    StepContext,
    WaitForCallbackContext,
    get_wait_for_callback_context,
)
from typing import Any, NoReturn


async def create_callback_handler(
    state,
    operation_identifier,
    timeout=None,
    heartbeat_timeout=None,
) -> Any:
    """Test helper that wraps CallbackOperationExecutor."""
    executor = CallbackOperationExecutor(
        state=state,
        operation_identifier=operation_identifier,
        timeout=timeout,
        heartbeat_timeout=heartbeat_timeout,
    )
    return await executor.process()


def mock_new_callback_checkpoint(mock_state, operation) -> None:
    """Configure state mocks for a missing callback that checkpoint creation returns."""
    mock_state.operations.get.return_value = None
    mock_state.create_checkpoint.return_value = operation


def test_create_callback_name_is_keyword_only() -> None:
    """create_callback operation name must be passed as a keyword."""
    parameters = inspect.signature(create_callback).parameters

    assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY


def test_callback_error_is_defined_by_callback_module() -> None:
    assert CallbackError.__module__ == "async_durable_execution._primitive.callback"


def test_callback_error_control_codec_preserves_callback_id() -> None:
    source = CallbackError("Callback failed", callback_id="callback-123")

    data = _encode_sdk_control_error_data(source)
    restored = _restore_sdk_control_error(
        str(source),
        type(source).__name__,
        data,
    )

    assert isinstance(restored, CallbackError)
    assert restored.callback_id == "callback-123"


def test_wait_for_callback_name_is_keyword_only() -> None:
    """wait_for_callback operation name must be passed as a keyword."""
    parameters = inspect.signature(wait_for_callback).parameters

    assert parameters["submitter"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY


def test_get_wait_for_callback_context_returns_bound_callback_context() -> None:
    context = WaitForCallbackContext(
        execution_state=Mock(spec=ExecutionState),
        operation_identifier=OperationIdentifier(
            "submitter-step",
            OperationSubType.STEP,
            None,
        ),
        callback_id="callback-123",
    )

    with bind_current_context(context):
        callback_context = get_wait_for_callback_context()

    assert callback_context is context
    assert callback_context.callback_id == "callback-123"


def test_get_wait_for_callback_context_rejects_non_callback_context() -> None:
    with (
        bind_current_context(
            StepContext(
                execution_state=Mock(spec=ExecutionState),
                operation_identifier=OperationIdentifier(
                    "step",
                    OperationSubType.STEP,
                    None,
                ),
            )
        ),
        pytest.raises(
            RuntimeError,
            match=r"get_wait_for_callback_context\(\) can only be used while a wait_for_callback submitter is executing\.",
        ),
    ):
        get_wait_for_callback_context()


async def execute_step_with_mock_context(func) -> Any:
    step_context = Mock(spec=StepContext)
    step_context.execution_state = Mock()
    step_context.operation_identifier = OperationIdentifier(
        "submitter-step",
        OperationSubType.STEP,
        None,
    )
    token = set_current_context(step_context)
    try:
        return await func()
    finally:
        reset_current_context(token)


@contextmanager
def patch_wait_for_callback_ops(
    mock_callback, *, step_side_effect=None
) -> Iterator[tuple[AsyncMock, AsyncMock]]:
    create_callback_mock = AsyncMock(return_value=mock_callback)
    step_mock = AsyncMock()
    if step_side_effect is not None:
        step_mock.side_effect = step_side_effect

    with (
        patch.object(callback, "create_callback", create_callback_mock),
        patch(
            "async_durable_execution._operation.wait_for_callback.step",
            step_mock,
        ),
    ):
        yield create_callback_mock, step_mock


async def run_wait_for_callback_handler(*args, **kwargs) -> Any:
    return await wait_for_callback_handler(*args, **kwargs)()


async def test_create_callback_handler_new_operation_with_config() -> None:
    """Test create_callback_handler creates new checkpoint when operation doesn't exist."""
    mock_state = Mock(spec=ExecutionState)

    # Initial lookup misses, and the sync checkpoint returns the created operation.
    callback_details = CallbackDetails(callback_id="cb123")
    operation = Operation(
        operation_id="callback1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, operation)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback1", OperationSubType.CALLBACK, None, "test_callback"
        ),
        timeout=timedelta(minutes=5),
        heartbeat_timeout=timedelta(minutes=1),
    )

    assert result == "cb123"
    expected_operation = OperationUpdate(
        operation_id="callback1",
        parent_id=None,
        operation_type=OperationType.CALLBACK,
        sub_type=OperationSubType.CALLBACK,
        action=OperationAction.START,
        name="test_callback",
        callback_options=CallbackOptions(
            timeout_seconds=300, heartbeat_timeout_seconds=60
        ),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
    )
    mock_state.operations.get.assert_called_once_with("callback1")


async def test_create_callback_handler_accepts_int_seconds_config() -> None:
    """Test create_callback_handler timeout fields accept integer seconds."""
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="cb123")
    operation = Operation(
        operation_id="callback1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, operation)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback1", OperationSubType.CALLBACK, None, "test_callback"
        ),
        timeout=300,
        heartbeat_timeout=60,
    )

    assert result == "cb123"
    expected_operation = OperationUpdate(
        operation_id="callback1",
        parent_id=None,
        operation_type=OperationType.CALLBACK,
        sub_type=OperationSubType.CALLBACK,
        action=OperationAction.START,
        name="test_callback",
        callback_options=CallbackOptions(
            timeout_seconds=300, heartbeat_timeout_seconds=60
        ),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
    )
    mock_state.operations.get.assert_called_once_with("callback1")


async def test_create_callback_handler_new_operation_without_config() -> None:
    """Test create_callback_handler creates new checkpoint without config."""
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="cb456")
    operation = Operation(
        operation_id="callback2",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, operation)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback2", OperationSubType.CALLBACK, None
        ),
    )

    assert result == "cb456"
    expected_operation = OperationUpdate(
        operation_id="callback2",
        parent_id=None,
        operation_type=OperationType.CALLBACK,
        sub_type=OperationSubType.CALLBACK,
        action=OperationAction.START,
        name=None,
        callback_options=CallbackOptions(),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
    )


async def test_create_callback_handler_existing_started_operation() -> None:
    """Test create_callback_handler returns existing callback_id for started operation."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="existing_cb123")
    operation = Operation(
        operation_id="callback3",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback3", OperationSubType.CALLBACK, None
        ),
    )

    assert result == "existing_cb123"
    # Should not create new checkpoint for existing operation
    mock_state.create_checkpoint.assert_not_called()
    mock_state.operations.get.assert_called_once_with("callback3")


async def test_create_callback_handler_existing_failed_operation() -> None:
    """Test create_callback_handler returns callback_id for failed operation (deferred error)."""
    # CRITICAL: create_callback_handler should NOT raise on FAILED
    # Errors are deferred to Callback.result() for deterministic replay
    mock_state = Mock(spec=ExecutionState)
    failed_op = Operation(
        operation_id="callback4",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=CallbackDetails(callback_id="failed_cb4"),
    )
    mock_result = failed_op
    mock_state.operations.get.return_value = mock_result

    # Should return callback_id without raising
    callback_id = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback4", OperationSubType.CALLBACK, None
        ),
    )

    assert callback_id == "failed_cb4"
    mock_state.create_checkpoint.assert_not_called()


async def test_create_callback_handler_existing_started_missing_callback_details() -> (
    None
):
    """Test create_callback_handler raises error when existing started operation has no callback details."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="callback5",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=None,
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(CallbackError, match="Missing callback details"):
        await create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "callback5", OperationSubType.CALLBACK, None
            ),
        )


async def test_create_callback_handler_new_operation_missing_callback_details_after_checkpoint() -> (
    None
):
    """Test create_callback_handler raises error when new operation has no callback details after checkpoint."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="callback6",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=None,
    )
    mock_new_callback_checkpoint(mock_state, operation)

    with pytest.raises(CallbackError, match="Missing callback details"):
        await create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "callback6", OperationSubType.CALLBACK, None
            ),
        )


async def test_create_callback_handler_new_operation_missing_checkpoint_result() -> (
    None
):
    """Test create_callback_handler raises when checkpoint creation returns no operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.operations.get.return_value = None
    mock_state.create_checkpoint.return_value = None

    with pytest.raises(CallbackError, match="Missing callback details"):
        await create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "callback_missing", OperationSubType.CALLBACK, None
            ),
        )


async def test_callback_executor_execute_without_operation_raises() -> None:
    mock_state = Mock(spec=ExecutionState)
    executor = CallbackOperationExecutor(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_execute_missing", OperationSubType.CALLBACK, None
        ),
    )

    with pytest.raises(CallbackError, match="Missing callback details"):
        await executor.execute(None)


async def test_create_callback_handler_existing_timed_out_operation() -> None:
    """Test create_callback_handler returns existing callback_id for timed out operation."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="timed_out_cb123")
    operation = Operation(
        operation_id="callback_timed_out",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.TIMED_OUT,
        callback_details=callback_details,
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_timed_out", OperationSubType.CALLBACK, None
        ),
    )

    assert result == "timed_out_cb123"
    mock_state.create_checkpoint.assert_not_called()


async def test_create_callback_handler_existing_timed_out_missing_callback_details() -> (
    None
):
    """Test create_callback_handler raises error when timed out operation has no callback details."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="callback_timed_out_no_details",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.TIMED_OUT,
        callback_details=None,
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(CallbackError, match="Missing callback details"):
        await create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "callback_timed_out_no_details", OperationSubType.CALLBACK, None
            ),
        )


async def test_wait_for_callback_handler_basic() -> None:
    """Test wait_for_callback_handler with basic parameters."""
    mock_callback = Mock()
    mock_callback.callback_id = "callback789"
    mock_callback.result = AsyncMock(return_value="callback_result")
    mock_submitter = AsyncMock(return_value=None)

    with patch_wait_for_callback_ops(mock_callback) as (_, step_mock):
        result = await run_wait_for_callback_handler(mock_submitter)

    assert result == "callback_result"
    step_mock.assert_called_once()
    mock_callback.result.assert_called_once()


async def test_wait_for_callback_handler_with_name_and_config() -> None:
    """Test wait_for_callback_handler with name and fields."""
    mock_callback = Mock()
    mock_callback.callback_id = "callback999"
    mock_callback.result = AsyncMock(return_value="named_callback_result")
    mock_submitter = AsyncMock(return_value=None)

    with patch_wait_for_callback_ops(mock_callback) as (
        create_callback_mock,
        step_mock,
    ):
        result = await run_wait_for_callback_handler(mock_submitter, "test_callback")

    assert result == "named_callback_result"
    create_callback_mock.assert_called_once_with(
        name="test_callback-callback",
        timeout=None,
        heartbeat_timeout=None,
        serdes=None,
    )
    step_mock.assert_called_once()


@no_type_check
async def test_wait_for_callback_handler_submitter_reads_callback_id_from_context() -> (
    None
):
    """Test wait_for_callback_handler exposes callback_id through current context."""
    mock_callback = Mock()
    mock_callback.callback_id = "callback_test_id"
    mock_callback.result = AsyncMock(return_value="test_result")

    captured_callback_id = None

    async def mock_submitter() -> None:
        nonlocal captured_callback_id
        captured_callback_id = get_current_context().callback_id

    async def capture_step_call(func, name, **_kwargs) -> None:
        # Execute the step callable to verify submitter is called correctly
        await execute_step_with_mock_context(func)

    with patch_wait_for_callback_ops(
        mock_callback,
        step_side_effect=capture_step_call,
    ):
        await run_wait_for_callback_handler(mock_submitter, "test")

    assert captured_callback_id == "callback_test_id"


async def test_create_callback_handler_with_none_operation_in_result() -> None:
    """Test create_callback_handler when callback details are missing."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="none_operation",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=None,
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(CallbackError, match="Missing callback details"):
        await create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "none_operation", OperationSubType.CALLBACK, None
            ),
        )


async def test_create_callback_handler_with_negative_timeouts() -> None:
    """Test create_callback_handler with negative timeout values."""
    with pytest.raises(ValidationError, match="timeout must be non-negative"):
        await create_callback_handler(
            state=Mock(spec=ExecutionState),
            operation_identifier=OperationIdentifier(
                "negative_timeout", OperationSubType.CALLBACK, None
            ),
            timeout=timedelta(seconds=-100),
            heartbeat_timeout=timedelta(seconds=-50),
        )


async def test_wait_for_callback_handler_with_none_callback_id() -> None:
    """Test wait_for_callback_handler when callback has None callback_id."""
    mock_callback = Mock()
    mock_callback.callback_id = None
    mock_callback.result = AsyncMock(return_value="result_with_none_id")

    mock_submitter = AsyncMock(return_value=None)

    async def execute_step(func, name, **_kwargs) -> Any:
        return await execute_step_with_mock_context(func)

    with patch_wait_for_callback_ops(
        mock_callback,
        step_side_effect=execute_step,
    ):
        result = await run_wait_for_callback_handler(mock_submitter, "test")

    assert result == "result_with_none_id"
    # Verify submitter was called without arguments.
    assert mock_submitter.call_count == 1
    call_args = mock_submitter.call_args[0]
    assert len(call_args) == 0


async def test_wait_for_callback_handler_with_empty_string_callback_id() -> None:
    """Test wait_for_callback_handler when callback has empty string callback_id."""
    mock_callback = Mock()
    mock_callback.callback_id = ""
    mock_callback.result = AsyncMock(return_value="result_with_empty_id")

    mock_submitter = AsyncMock(return_value=None)

    async def execute_step(func, name, **_kwargs) -> Any:
        return await execute_step_with_mock_context(func)

    with patch_wait_for_callback_ops(
        mock_callback,
        step_side_effect=execute_step,
    ):
        result = await run_wait_for_callback_handler(mock_submitter, "test")

    assert result == "result_with_empty_id"
    # Verify submitter was called without arguments.
    assert mock_submitter.call_count == 1
    call_args = mock_submitter.call_args[0]
    assert len(call_args) == 0


async def test_wait_for_callback_handler_with_large_data() -> None:
    """Test wait_for_callback_handler with large result data."""
    mock_callback = Mock()
    mock_callback.callback_id = "large_data_cb"

    large_result = {
        "data": ["item_" + str(i) for i in range(1000)],
        "metadata": {"size": 1000, "type": "large_dataset"},
    }
    mock_callback.result = AsyncMock(return_value=large_result)
    mock_submitter = AsyncMock(return_value=None)

    with patch_wait_for_callback_ops(mock_callback):
        result = await run_wait_for_callback_handler(mock_submitter, "large_data_test")

    assert result == large_result
    assert len(result["data"]) == 1000


async def test_wait_for_callback_handler_with_unicode_names() -> None:
    """Test wait_for_callback_handler with unicode characters in names."""
    unicode_names = ["测试回调", "コールバック", "🔄 callback test 🚀"]

    for name in unicode_names:
        mock_callback = Mock()
        mock_callback.callback_id = f"unicode_cb_{hash(name) % 1000}"
        mock_callback.result = AsyncMock(return_value=f"result_for_{name}")
        mock_submitter = AsyncMock(return_value=None)

        with patch_wait_for_callback_ops(mock_callback) as (_, step_mock):
            result = await run_wait_for_callback_handler(mock_submitter, name)

        assert result == f"result_for_{name}"
        expected_name = f"{name}-submitter"
        step_mock.assert_called_once_with(
            func=ANY,
            name=expected_name,
            retry_strategy=None,
            serdes=None,
        )


async def test_create_callback_handler_existing_succeeded_operation() -> None:
    """Test create_callback_handler returns existing callback_id for succeeded operation."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="succeeded_cb123")
    operation = Operation(
        operation_id="callback_succeeded",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=callback_details,
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_succeeded", OperationSubType.CALLBACK, None
        ),
    )

    assert result == "succeeded_cb123"
    mock_state.create_checkpoint.assert_not_called()


async def test_create_callback_handler_existing_succeeded_missing_callback_details() -> (
    None
):
    """Test create_callback_handler raises error when succeeded operation has no callback details."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="callback_succeeded_no_details",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=None,
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(CallbackError, match="Missing callback details"):
        await create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "callback_succeeded_no_details", OperationSubType.CALLBACK, None
            ),
        )


async def test_create_callback_handler_config_with_zero_timeouts() -> None:
    """Test create_callback_handler with config having zero timeout values."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="cb_zero_timeout")
    operation = Operation(
        operation_id="callback_zero",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, operation)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_zero", OperationSubType.CALLBACK, None
        ),
        timeout=timedelta(seconds=0),
        heartbeat_timeout=timedelta(seconds=0),
    )

    assert result == "cb_zero_timeout"
    expected_operation = OperationUpdate(
        operation_id="callback_zero",
        parent_id=None,
        operation_type=OperationType.CALLBACK,
        sub_type=OperationSubType.CALLBACK,
        action=OperationAction.START,
        name=None,
        callback_options=CallbackOptions(
            timeout_seconds=0, heartbeat_timeout_seconds=0
        ),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
    )


async def test_create_callback_handler_config_with_large_timeouts() -> None:
    """Test create_callback_handler with large timeout values."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="cb_large_timeout")
    operation = Operation(
        operation_id="callback_large",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, operation)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_large", OperationSubType.CALLBACK, None
        ),
        timeout=timedelta(days=1),
        heartbeat_timeout=timedelta(hours=1),
    )

    assert result == "cb_large_timeout"
    expected_operation = OperationUpdate(
        operation_id="callback_large",
        parent_id=None,
        operation_type=OperationType.CALLBACK,
        sub_type=OperationSubType.CALLBACK,
        action=OperationAction.START,
        name=None,
        callback_options=CallbackOptions(
            timeout_seconds=86400, heartbeat_timeout_seconds=3600
        ),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
    )


async def test_create_callback_handler_empty_operation_id() -> None:
    """Test create_callback_handler with empty operation_id."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="cb_empty_id")
    operation = Operation(
        operation_id="",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, operation)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("", OperationSubType.CALLBACK, None),
    )

    assert result == "cb_empty_id"


async def test_wait_for_callback_handler_submitter_exception_handling() -> None:
    """Test wait_for_callback_handler when submitter raises exception."""
    mock_callback = Mock()
    mock_callback.callback_id = "callback_exception"
    mock_callback.result = AsyncMock(return_value="exception_result")

    async def failing_submitter() -> NoReturn:
        msg = "Submitter failed"
        raise ValueError(msg)

    async def step_side_effect(func, name, **_kwargs) -> None:
        await execute_step_with_mock_context(func)

    with patch_wait_for_callback_ops(
        mock_callback,
        step_side_effect=step_side_effect,
    ):
        with pytest.raises(ValueError, match="Submitter failed"):
            await run_wait_for_callback_handler(failing_submitter, "test")


async def test_wait_for_callback_handler_callback_result_exception() -> None:
    """Test wait_for_callback_handler when callback.result() raises exception."""
    mock_callback = Mock()
    mock_callback.callback_id = "callback_result_exception"
    mock_callback.result = AsyncMock(side_effect=RuntimeError("Callback result failed"))
    mock_submitter = AsyncMock(return_value=None)

    with patch_wait_for_callback_ops(mock_callback):
        with pytest.raises(RuntimeError, match="Callback result failed"):
            await run_wait_for_callback_handler(mock_submitter, "test")


async def test_wait_for_callback_handler_empty_name_handling() -> None:
    """Test wait_for_callback_handler with empty string name."""
    mock_callback = Mock()
    mock_callback.callback_id = "callback_empty_name"
    mock_callback.result = AsyncMock(return_value="empty_name_result")
    mock_submitter = AsyncMock(return_value=None)

    with patch_wait_for_callback_ops(mock_callback) as (_, step_mock):
        result = await run_wait_for_callback_handler(mock_submitter, "")

    assert result == "empty_name_result"
    step_mock.assert_called_once()


async def test_wait_for_callback_handler_complex_callback_result() -> None:
    """Test wait_for_callback_handler with complex callback result."""
    mock_callback = Mock()
    mock_callback.callback_id = "callback_complex"
    complex_result = {
        "status": "success",
        "data": [1, 2, 3],
        "metadata": {"timestamp": 123456},
    }
    mock_callback.result = AsyncMock(return_value=complex_result)
    mock_submitter = AsyncMock(return_value=None)

    with patch_wait_for_callback_ops(mock_callback):
        result = await run_wait_for_callback_handler(mock_submitter, "complex_test")

    assert result == complex_result
    mock_callback.result.assert_called_once()


async def test_wait_for_callback_handler_step_name_formatting() -> None:
    """Test wait_for_callback_handler step name formatting with various inputs."""
    mock_callback = Mock()
    mock_callback.callback_id = "callback_name_format"
    mock_callback.result = AsyncMock(return_value="formatted_result")
    mock_submitter = AsyncMock(return_value=None)

    with patch_wait_for_callback_ops(mock_callback) as (_, step_mock):
        await run_wait_for_callback_handler(mock_submitter, "test with spaces")

    step_calls = step_mock.call_args_list
    assert len(step_calls) == 1
    _, kwargs = step_calls[0]
    assert kwargs["name"] == "test with spaces-submitter"


async def test_wait_for_callback_handler_config_propagation() -> None:
    """Test wait_for_callback_handler properly passes fields to create_callback."""
    mock_callback = Mock()
    mock_callback.callback_id = "callback_config_prop"
    mock_callback.result = AsyncMock(return_value="config_result")
    mock_submitter = AsyncMock(return_value=None)

    timeout = timedelta(minutes=2)
    heartbeat_timeout = timedelta(seconds=30)

    with patch_wait_for_callback_ops(mock_callback) as (create_callback_mock, _):
        result = await run_wait_for_callback_handler(
            mock_submitter,
            "config_test",
            timeout=timeout,
            heartbeat_timeout=heartbeat_timeout,
        )

    assert result == "config_result"
    create_callback_mock.assert_called_once_with(
        name="config_test-callback",
        timeout=timeout,
        heartbeat_timeout=heartbeat_timeout,
        serdes=None,
    )


async def test_wait_for_callback_handler_accepts_int_seconds_config() -> None:
    """Test wait_for_callback_handler timeout fields accept integer seconds."""
    mock_callback = Mock()
    mock_callback.callback_id = "callback_config_prop"
    mock_callback.result = AsyncMock(return_value="config_result")
    mock_submitter = AsyncMock(return_value=None)

    with patch_wait_for_callback_ops(mock_callback) as (create_callback_mock, _):
        result = await run_wait_for_callback_handler(
            mock_submitter,
            "config_test",
            timeout=120,
            heartbeat_timeout=30,
        )

    assert result == "config_result"
    create_callback_mock.assert_called_once_with(
        name="config_test-callback",
        timeout=120,
        heartbeat_timeout=30,
        serdes=None,
    )


async def test_wait_for_callback_handler_step_config_propagation() -> None:
    """Test wait_for_callback_handler properly passes retry_strategy and serdes to step config."""
    mock_callback = Mock()
    mock_callback.callback_id = "step_config_test"
    mock_callback.result = AsyncMock(return_value="step_config_result")
    mock_submitter = AsyncMock(return_value=None)

    def test_retry_strategy(exception, attempt) -> int:
        return 1

    mock_serdes = Mock(spec=SerDes)

    with patch_wait_for_callback_ops(mock_callback) as (_, step_mock):
        result = await run_wait_for_callback_handler(
            mock_submitter,
            "step_config_test",
            retry_strategy=test_retry_strategy,
            serdes=mock_serdes,
        )

    assert result == "step_config_result"

    # Verify step was called with the direct retry and serialization fields.
    step_mock.assert_called_once()
    call_args = step_mock.call_args

    assert call_args.kwargs["retry_strategy"] == test_retry_strategy
    assert call_args.kwargs["serdes"] == mock_serdes


@no_type_check
async def test_wait_for_callback_handler_with_various_result_types() -> None:
    """Test wait_for_callback_handler with various result types."""
    result_types = [None, True, False, 0, math.pi, "", "string", [], {"key": "value"}]

    for i, expected_result in enumerate(result_types):
        mock_callback = Mock()
        mock_callback.callback_id = f"type_test_cb_{i}"
        mock_callback.result = AsyncMock(return_value=expected_result)
        mock_submitter = AsyncMock(return_value=None)

        with patch_wait_for_callback_ops(mock_callback):
            result = await run_wait_for_callback_handler(
                mock_submitter, f"type_test_{i}"
            )

        assert result == expected_result
        assert type(result) is type(expected_result)


async def test_callback_lifecycle_complete_flow() -> None:
    """Test complete callback lifecycle from creation to completion."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="lifecycle_cb123")
    operation = Operation(
        operation_id="lifecycle_callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, operation)
    mock_callback = Mock()
    mock_callback.callback_id = "lifecycle_cb123"
    mock_callback.result = AsyncMock(
        return_value={"status": "completed", "data": "test_data"}
    )

    timeout = timedelta(minutes=5)
    heartbeat_timeout = timedelta(minutes=1)
    callback_id = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "lifecycle_callback", OperationSubType.CALLBACK, None
        ),
        timeout=timeout,
        heartbeat_timeout=heartbeat_timeout,
    )

    assert callback_id == "lifecycle_cb123"

    async def mock_submitter() -> str:
        callback_context = get_current_context()
        assert isinstance(callback_context, WaitForCallbackContext)
        assert callback_context.callback_id == "lifecycle_cb123"
        return "submitted"

    async def execute_step(func, name, **_kwargs) -> Any:
        return await execute_step_with_mock_context(func)

    with patch_wait_for_callback_ops(
        mock_callback,
        step_side_effect=execute_step,
    ):
        result = await run_wait_for_callback_handler(
            mock_submitter,
            "lifecycle_test",
            timeout=timeout,
            heartbeat_timeout=heartbeat_timeout,
            serdes=None,
        )

    assert result == {"status": "completed", "data": "test_data"}


async def test_callback_retry_scenario() -> None:
    """Test callback behavior during retry scenarios."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="retry_cb456")
    operation = Operation(
        operation_id="retry_callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    mock_state.operations.get.return_value = operation

    callback_id_1 = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "retry_callback", OperationSubType.CALLBACK, None
        ),
    )
    callback_id_2 = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "retry_callback", OperationSubType.CALLBACK, None
        ),
    )

    assert callback_id_1 == callback_id_2 == "retry_cb456"
    mock_state.create_checkpoint.assert_not_called()


async def test_callback_timeout_configuration() -> None:
    """Test callback with various timeout configurations."""
    test_cases = [(0, 0), (30, 10), (3600, 300), (86400, 3600)]

    for timeout_seconds, heartbeat_timeout_seconds in test_cases:
        mock_state = Mock(spec=ExecutionState)
        callback_details = CallbackDetails(callback_id=f"timeout_cb_{timeout_seconds}")
        operation = Operation(
            operation_id=f"timeout_callback_{timeout_seconds}",
            operation_type=OperationType.CALLBACK,
            status=OperationStatus.STARTED,
            callback_details=callback_details,
        )
        mock_new_callback_checkpoint(mock_state, operation)

        callback_id = await create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                f"timeout_callback_{timeout_seconds}", OperationSubType.CALLBACK, None
            ),
            timeout=timedelta(seconds=timeout_seconds),
            heartbeat_timeout=timedelta(seconds=heartbeat_timeout_seconds),
        )

        assert callback_id == f"timeout_cb_{timeout_seconds}"


async def test_callback_error_propagation() -> None:
    """Test error propagation through callback operations."""
    # CRITICAL: create_callback_handler should NOT raise on FAILED
    # Errors are deferred to Callback.result() for deterministic replay
    mock_state = Mock(spec=ExecutionState)
    failed_op = Operation(
        operation_id="error_callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=CallbackDetails(callback_id="failed_cb"),
    )
    mock_result = failed_op
    mock_state.operations.get.return_value = mock_result

    # Should return callback_id without raising
    callback_id = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "error_callback", OperationSubType.CALLBACK, None
        ),
    )
    assert callback_id == "failed_cb"

    with patch.object(
        callback,
        "create_callback",
        AsyncMock(side_effect=ValueError("Context creation failed")),
    ):
        with pytest.raises(ValueError, match="Context creation failed"):
            await run_wait_for_callback_handler(
                AsyncMock(return_value=None), "error_test"
            )


@no_type_check
async def test_callback_with_complex_submitter() -> None:
    """Test callback with complex submitter logic."""
    mock_callback = Mock()
    mock_callback.callback_id = "complex_cb789"
    mock_callback.result = AsyncMock(return_value="complex_result")

    submission_log = []

    async def complex_submitter() -> Any:
        callback_id = get_current_context().callback_id
        submission_log.append(f"received_id: {callback_id}")
        if callback_id == "complex_cb789":
            submission_log.append("api_call_success")
            return {"submitted": True, "callback_id": callback_id}

        submission_log.append("api_call_failed")
        msg = "Invalid callback ID"
        raise ValueError(msg)

    async def execute_step(func, name, **_kwargs) -> Any:
        return await execute_step_with_mock_context(func)

    with patch_wait_for_callback_ops(
        mock_callback,
        step_side_effect=execute_step,
    ):
        result = await run_wait_for_callback_handler(complex_submitter, "complex_test")

    assert result == "complex_result"
    assert submission_log == ["received_id: complex_cb789", "api_call_success"]


async def test_callback_state_consistency() -> None:
    """Test callback state consistency across multiple operations."""
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="consistent_cb")
    started_operation = Operation(
        operation_id="consistent_callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    succeeded_operation = Operation(
        operation_id="consistent_callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=callback_details,
    )

    mock_new_callback_checkpoint(mock_state, started_operation)

    callback_id_1 = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "consistent_callback", OperationSubType.CALLBACK, None
        ),
    )

    mock_state.operations.get.side_effect = None
    mock_state.operations.get.return_value = succeeded_operation

    callback_id_2 = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "consistent_callback", OperationSubType.CALLBACK, None
        ),
    )

    assert callback_id_1 == callback_id_2 == "consistent_cb"


async def test_callback_name_variations() -> None:
    """Test callback operations with various name formats."""
    name_test_cases = [
        None,
        "",
        "simple",
        "name with spaces",
        "name-with-dashes",
        "name_with_underscores",
        "name.with.dots",
        "name with special chars: !@#$%^&*()",
    ]

    for name in name_test_cases:
        mock_callback = Mock()
        mock_callback.callback_id = f"name_test_{hash(str(name)) % 1000}"
        mock_callback.result = AsyncMock(return_value=f"result_for_{name}")
        mock_submitter = AsyncMock(return_value=None)

        with patch_wait_for_callback_ops(mock_callback) as (_, step_mock):
            result = await run_wait_for_callback_handler(mock_submitter, name)

        assert result == f"result_for_{name}"
        expected_name = f"{name}-submitter" if name is not None else "submitter"
        step_mock.assert_called_once_with(
            func=ANY,
            name=expected_name,
            retry_strategy=None,
            serdes=None,
        )


@patch("async_durable_execution._primitive.callback.OperationUpdate")
async def test_callback_operation_update_creation(mock_operation_update) -> None:
    """Test that OperationUpdate.create_callback is called with correct parameters."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="update_test_cb")
    operation = Operation(
        operation_id="update_test",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    mock_new_callback_checkpoint(mock_state, operation)

    await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "update_test", OperationSubType.CALLBACK, None
        ),
        timeout=timedelta(minutes=10),
        heartbeat_timeout=timedelta(minutes=2),
    )

    mock_operation_update.create_callback.assert_called_once_with(
        identifier=OperationIdentifier("update_test", OperationSubType.CALLBACK, None),
        callback_options=CallbackOptions(
            timeout_seconds=600, heartbeat_timeout_seconds=120
        ),
    )


async def test_callback_immediate_response_uses_checkpoint_return_value() -> None:
    """Test that callback start uses the operation returned from checkpoint creation."""
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="cb_immediate_1")
    started_op = Operation(
        operation_id="callback_immediate_1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, started_op)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_immediate_1", OperationSubType.CALLBACK, None
        ),
    )

    # Verify callback_id was returned
    assert result == "cb_immediate_1"
    mock_state.operations.get.assert_called_once_with("callback_immediate_1")


async def test_callback_immediate_response_create_checkpoint_with_is_sync_true() -> (
    None
):
    """Test that create_checkpoint is called with is_sync=True."""
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="cb_immediate_2")
    started_op = Operation(
        operation_id="callback_immediate_2",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, started_op)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_immediate_2", OperationSubType.CALLBACK, None
        ),
    )

    # Verify callback_id was returned
    assert result == "cb_immediate_2"
    # Verify create_checkpoint was called with is_sync=True (default)
    mock_state.create_checkpoint.assert_called_once()
    # is_sync=True is the default, so it won't be in kwargs if not explicitly passed
    # We just verify the checkpoint was created


async def test_callback_immediate_response_immediate_success() -> None:
    """Test immediate success: checkpoint returns SUCCEEDED operation.

    When checkpoint creation returns SUCCEEDED, operation returns callback_id
    without raising.
    """
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="cb_immediate_success")
    succeeded_op = Operation(
        operation_id="callback_immediate_3",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, succeeded_op)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_immediate_3", OperationSubType.CALLBACK, None
        ),
    )

    # Verify callback_id was returned without raising
    assert result == "cb_immediate_success"
    # Verify checkpoint was created
    mock_state.create_checkpoint.assert_called_once()
    mock_state.operations.get.assert_called_once_with("callback_immediate_3")


async def test_callback_immediate_response_immediate_failure_deferred() -> None:
    """Test immediate failure deferred: checkpoint returns FAILED operation.

    CRITICAL: When checkpoint creation returns FAILED, create_callback()
    returns callback_id (does NOT raise). Errors are deferred to Callback.result()
    for deterministic replay.
    """
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="cb_immediate_failed")
    failed_op = Operation(
        operation_id="callback_immediate_4",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, failed_op)

    # CRITICAL: Should return callback_id without raising
    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_immediate_4", OperationSubType.CALLBACK, None
        ),
    )

    # Verify callback_id was returned (error deferred)
    assert result == "cb_immediate_failed"
    # Verify checkpoint was created
    mock_state.create_checkpoint.assert_called_once()
    mock_state.operations.get.assert_called_once_with("callback_immediate_4")


@no_type_check
async def test_callback_result_raises_error_for_failed_callbacks() -> None:
    """Test that Callback.result() raises error for FAILED callbacks (deferred error handling).

    This test verifies that errors are properly deferred to Callback.result() rather
    than being raised during create_callback(). This ensures deterministic replay:
    code between create_callback() and callback.result() always executes.
    """

    mock_state = Mock(spec=ExecutionState)

    # Create a FAILED callback operation
    error = ErrorObject(
        message="Callback failed", type="CallbackError", data=None, stack_trace=None
    )
    callback_details = CallbackDetails(
        callback_id="cb_failed_result", result=None, error=error
    )
    failed_op = Operation(
        operation_id="callback_failed_result",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=callback_details,
    )
    mock_state.operations.get.return_value = failed_op

    # Create Callback instance
    callback = Callback(
        callback_id="cb_failed_result",
        operation_id="callback_failed_result",
        state=mock_state,
        serdes=None,
    )

    # Verify that result() raises CallbackError
    with pytest.raises(CallbackError, match="Callback failed"):
        await callback.result()


@no_type_check
async def test_callback_result_failed_without_error_details_uses_default_message() -> (
    None
):
    mock_state = Mock(spec=ExecutionState)
    failed_op = Operation(
        operation_id="callback_failed_no_details",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=None,
    )
    mock_state.operations.get.return_value = failed_op

    callback = Callback(
        callback_id="cb_failed_no_details",
        operation_id="callback_failed_no_details",
        state=mock_state,
        serdes=None,
    )

    with pytest.raises(CallbackError, match="^Callback failed$"):
        await callback.result()


@no_type_check
async def test_callback_result_raises_error_for_timed_out_callbacks() -> None:
    """Test that Callback.result() raises error for TIMED_OUT callbacks."""

    mock_state = Mock(spec=ExecutionState)

    # Create a TIMED_OUT callback operation
    error = ErrorObject(
        message="Callback timed out",
        type="CallbackTimeoutError",
        data=None,
        stack_trace=None,
    )
    callback_details = CallbackDetails(
        callback_id="cb_timed_out_result", result=None, error=error
    )
    timed_out_op = Operation(
        operation_id="callback_timed_out_result",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.TIMED_OUT,
        callback_details=callback_details,
    )
    mock_state.operations.get.return_value = timed_out_op

    # Create Callback instance
    callback = Callback(
        callback_id="cb_timed_out_result",
        operation_id="callback_timed_out_result",
        state=mock_state,
        serdes=None,
    )

    # Verify that result() raises CallbackError
    with pytest.raises(CallbackError, match="Callback timed out"):
        await callback.result()


@no_type_check
async def test_callback_result_appends_timeout_type_from_error_metadata() -> None:
    """Test that timeout subtype is preserved when only ErrorType carries it."""

    mock_state = Mock(spec=ExecutionState)

    error = ErrorObject(
        message="Callback timed out",
        type=CallbackTimeoutType.TIMEOUT.value,
        data=None,
        stack_trace=None,
    )
    callback_details = CallbackDetails(
        callback_id="cb_timed_out_with_type", result=None, error=error
    )
    timed_out_op = Operation(
        operation_id="callback_timed_out_with_type",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.TIMED_OUT,
        callback_details=callback_details,
    )
    mock_state.operations.get.return_value = timed_out_op

    callback = Callback(
        callback_id="cb_timed_out_with_type",
        operation_id="callback_timed_out_with_type",
        state=mock_state,
        serdes=None,
    )

    with pytest.raises(CallbackError, match="Callback timed out: Callback.Timeout"):
        await callback.result()


@no_type_check
async def test_callback_result_does_not_duplicate_timeout_type_in_message() -> None:
    """Test that timeout subtype is not appended twice."""

    mock_state = Mock(spec=ExecutionState)

    error = ErrorObject(
        message="Callback timed out: Callback.Timeout",
        type=CallbackTimeoutType.TIMEOUT.value,
        data=None,
        stack_trace=None,
    )
    callback_details = CallbackDetails(
        callback_id="cb_timed_out_full_message", result=None, error=error
    )
    timed_out_op = Operation(
        operation_id="callback_timed_out_full_message",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.TIMED_OUT,
        callback_details=callback_details,
    )
    mock_state.operations.get.return_value = timed_out_op

    callback = Callback(
        callback_id="cb_timed_out_full_message",
        operation_id="callback_timed_out_full_message",
        state=mock_state,
        serdes=None,
    )

    with pytest.raises(CallbackError, match="^Callback timed out: Callback.Timeout$"):
        await callback.result()


async def test_callback_immediate_response_no_immediate_response() -> None:
    """Test no immediate response: checkpoint returns STARTED operation.

    When checkpoint creation returns STARTED, operation returns callback_id
    normally (callbacks don't suspend).
    """
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="cb_immediate_started")
    started_op = Operation(
        operation_id="callback_immediate_5",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, started_op)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_immediate_5", OperationSubType.CALLBACK, None
        ),
    )

    # Verify callback_id was returned
    assert result == "cb_immediate_started"
    # Verify checkpoint was created
    mock_state.create_checkpoint.assert_called_once()
    mock_state.operations.get.assert_called_once_with("callback_immediate_5")


async def test_callback_immediate_response_already_completed() -> None:
    """Test already completed: checkpoint exists on first check.

    When checkpoint is already SUCCEEDED on first check, no checkpoint is created
    and callback_id is returned immediately.
    """
    mock_state = Mock(spec=ExecutionState)

    # First call: already succeeded
    callback_details = CallbackDetails(callback_id="cb_already_completed")
    succeeded_op = Operation(
        operation_id="callback_immediate_6",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=callback_details,
    )
    succeeded = succeeded_op
    mock_state.operations.get.return_value = succeeded

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_immediate_6", OperationSubType.CALLBACK, None
        ),
    )

    # Verify callback_id was returned
    assert result == "cb_already_completed"
    # Verify no checkpoint was created (already exists)
    mock_state.create_checkpoint.assert_not_called()
    # Verify direct state lookup was called only once
    assert mock_state.operations.get.call_count == 1


async def test_callback_immediate_response_already_failed() -> None:
    """Test already failed: checkpoint is already FAILED on first check.

    When checkpoint is already FAILED on first check, no checkpoint is created
    and callback_id is returned (error deferred to Callback.result()).
    """
    mock_state = Mock(spec=ExecutionState)

    # First call: already failed
    callback_details = CallbackDetails(callback_id="cb_already_failed")
    failed_op = Operation(
        operation_id="callback_immediate_7",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=callback_details,
    )
    failed = failed_op
    mock_state.operations.get.return_value = failed

    # Should return callback_id without raising
    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_immediate_7", OperationSubType.CALLBACK, None
        ),
    )

    # Verify callback_id was returned (error deferred)
    assert result == "cb_already_failed"
    # Verify no checkpoint was created (already exists)
    mock_state.create_checkpoint.assert_not_called()
    # Verify direct state lookup was called only once
    assert mock_state.operations.get.call_count == 1


@no_type_check
async def test_callback_deferred_error_handling_code_execution_between_create_and_result() -> (
    None
):
    """Test callback deferred error handling with code execution between create_callback() and callback.result().

    This test verifies that code between create_callback() and callback.result() executes
    even when the callback is FAILED. This ensures deterministic replay.
    """

    mock_state = Mock(spec=ExecutionState)

    # Setup: callback is already FAILED
    error = ErrorObject(
        message="Callback failed", type="CallbackError", data=None, stack_trace=None
    )
    callback_details = CallbackDetails(
        callback_id="cb_deferred_error", result=None, error=error
    )
    failed_op = Operation(
        operation_id="callback_deferred_error",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=callback_details,
    )
    mock_state.operations.get.return_value = failed_op

    # Step 1: create_callback() returns callback_id without raising
    callback_id = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_deferred_error", OperationSubType.CALLBACK, None
        ),
    )
    assert callback_id == "cb_deferred_error"

    # Step 2: Code executes between create_callback() and callback.result()
    execution_log = [
        "code_executed_after_create_callback",
        f"callback_id: {callback_id}",
    ]

    # Step 3: Callback.result() raises the error
    callback = Callback(
        callback_id=callback_id,
        operation_id="callback_deferred_error",
        state=mock_state,
        serdes=None,
    )

    with pytest.raises(CallbackError, match="Callback failed"):
        await callback.result()

    # Verify code between create_callback() and callback.result() executed
    assert execution_log == [
        "code_executed_after_create_callback",
        "callback_id: cb_deferred_error",
    ]


async def test_callback_immediate_response_with_config() -> None:
    """Test immediate response with callback configuration."""
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="cb_with_config")
    succeeded_op = Operation(
        operation_id="callback_with_config",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=callback_details,
    )
    mock_new_callback_checkpoint(mock_state, succeeded_op)

    result = await create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback_with_config", OperationSubType.CALLBACK, None
        ),
        timeout=timedelta(minutes=5),
        heartbeat_timeout=timedelta(minutes=1),
    )

    # Verify callback_id was returned
    assert result == "cb_with_config"
    # Verify checkpoint was created with config
    mock_state.create_checkpoint.assert_called_once()
    call_args = mock_state.create_checkpoint.call_args[1]
    operation_update = call_args["operation_update"]
    assert operation_update.callback_options.timeout_seconds == 300
    assert operation_update.callback_options.heartbeat_timeout_seconds == 60


async def test_callback_returns_id_when_checkpoint_returns_started() -> None:
    """Test when checkpoint creation returns
    STARTED (not terminal), the callback operation returns callback_id normally.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    started_op = Operation(
        operation_id="callback-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=CallbackDetails(callback_id="cb-123"),
    )
    mock_new_callback_checkpoint(mock_state, started_op)

    executor = CallbackOperationExecutor(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback-1", OperationSubType.CALLBACK, None, "test_callback"
        ),
    )
    callback_id = await executor.process()

    assert callback_id == "cb-123"
    mock_state.operations.get.assert_called_once_with("callback-1")
    mock_state.create_checkpoint.assert_called_once()  # START checkpoint created


async def test_callback_returns_id_when_checkpoint_returns_started_duplicate() -> None:
    """Test when checkpoint creation returns
    STARTED (not terminal), the callback operation returns callback_id normally.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    started_op = Operation(
        operation_id="callback-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=CallbackDetails(callback_id="cb-123"),
    )
    mock_new_callback_checkpoint(mock_state, started_op)

    executor = CallbackOperationExecutor(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "callback-1", OperationSubType.CALLBACK, None, "test_callback"
        ),
    )
    callback_id = await executor.process()

    assert callback_id == "cb-123"
    mock_state.operations.get.assert_called_once_with("callback-1")
    mock_state.create_checkpoint.assert_called_once()  # START checkpoint created
