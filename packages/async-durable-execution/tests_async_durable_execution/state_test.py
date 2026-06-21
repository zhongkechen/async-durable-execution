"""Unit tests for execution state."""

from __future__ import annotations

import asyncio
import datetime
import json
import time
import unittest.mock
from unittest.mock import Mock, call, create_autospec, patch

import pytest

from async_durable_execution.exceptions import (
    CallableRuntimeError,
    DurableApiErrorCategory,
    GetExecutionStateError,
    OrphanedChildException,
)
from async_durable_execution.models import OperationIdentifier
from async_durable_execution.models import (
    CallbackDetails,
    ChainedInvokeDetails,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ContextDetails,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
    StateOutput,
    StepDetails,
)
from async_durable_execution.client import ThreadedSyncLambdaClient
from async_durable_execution.operation.base import (
    get_checkpoint_result,
    CheckpointedResult,
)
from async_durable_execution.plugin import (
    DurableInstrumentationPlugin,
    PluginExecutor,
)
from async_durable_execution.state import (
    CheckpointBatcherConfig,
    ExecutionState as _ExecutionState,
    QueuedOperation,
    ReplayStatus,
)


async def run_async(awaitable):
    return await awaitable


class _ImmediateAwaitable:
    def __await__(self):
        if False:  # pragma: no cover
            yield
        return None


class _CompatAsyncQueue(asyncio.Queue[QueuedOperation | None]):
    """Compatibility queue so legacy direct test puts still work."""

    def put(self, item: QueuedOperation | None):  # type: ignore[override]
        self.put_nowait(item)
        return _ImmediateAwaitable()


def ExecutionState(
    *,
    durable_execution_arn: str,
    initial_checkpoint_token: str,
    service_client,
    plugin_executor,
    batcher_config: CheckpointBatcherConfig | None = None,
    operations: dict[str, Operation] | None = None,
    replay_status: ReplayStatus | None = None,
):
    state = _ExecutionState(
        durable_execution_arn=durable_execution_arn,
        initial_checkpoint_token=initial_checkpoint_token,
        service_client=service_client,
        plugin_executor=plugin_executor,
        batcher_config=batcher_config,
    )
    state._checkpoint_queue = _CompatAsyncQueue()
    if operations:
        state.operations.update(operations)
    if replay_status is not None:
        state._replay_status = replay_status
    return state


ExecutionState._calculate_operation_size = staticmethod(  # type: ignore[attr-defined]
    _ExecutionState._calculate_operation_size
)


async def stop_checkpointing_task(state: _ExecutionState) -> None:
    state.stop_checkpointing()
    if state._checkpointing_task is not None:
        await asyncio.wait_for(state._checkpointing_task, timeout=1.0)


async def test_checkpointed_result_create_from_operation_step():
    """Test CheckpointedResult.create_from_operation with STEP operation."""
    step_details = StepDetails(result="test_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=step_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.SUCCEEDED
    assert result.result == "test_result"
    assert result.error is None


async def test_checkpointed_result_create_from_operation_callback():
    """Test CheckpointedResult.create_from_operation with CALLBACK operation."""
    callback_details = CallbackDetails(callback_id="cb1", result="callback_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=callback_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.SUCCEEDED
    assert result.result == "callback_result"
    assert result.error is None


async def test_checkpointed_result_create_from_operation_invoke():
    """Test CheckpointedResult.create_from_operation with INVOKE operation."""
    chained_invoke_details = ChainedInvokeDetails(result="invoke_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=chained_invoke_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.SUCCEEDED
    assert result.result == "invoke_result"
    assert result.error is None


async def test_checkpointed_result_create_from_operation_invoke_with_error():
    """Test CheckpointedResult.create_from_operation with INVOKE operation and error."""
    error = ErrorObject(
        message="Invoke error", type="InvokeError", data=None, stack_trace=None
    )
    chained_invoke_details = ChainedInvokeDetails(error=error)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.FAILED,
        chained_invoke_details=chained_invoke_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.FAILED
    assert result.result is None
    assert result.error == error


async def test_checkpointed_result_create_from_operation_invoke_no_details():
    """Test CheckpointedResult.create_from_operation with INVOKE operation but no chained_invoke_details."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.STARTED
    assert result.result is None
    assert result.error is None


async def test_checkpointed_result_create_from_operation_invoke_with_both_result_and_error():
    """Test CheckpointedResult.create_from_operation with INVOKE operation having both result and error."""
    error = ErrorObject(
        message="Invoke error", type="InvokeError", data=None, stack_trace=None
    )
    chained_invoke_details = ChainedInvokeDetails(result="invoke_result", error=error)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.FAILED,
        chained_invoke_details=chained_invoke_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.FAILED
    assert result.result == "invoke_result"
    assert result.error == error


async def test_checkpointed_result_create_from_operation_context():
    """Test CheckpointedResult.create_from_operation with CONTEXT operation."""
    context_details = ContextDetails(result="context_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=context_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.SUCCEEDED
    assert result.result == "context_result"
    assert result.error is None


async def test_checkpointed_result_create_from_operation_context_with_error():
    """Test CheckpointedResult.create_from_operation with CONTEXT operation and error."""
    error = ErrorObject(
        message="Context error", type="ContextError", data=None, stack_trace=None
    )
    context_details = ContextDetails(error=error)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.FAILED,
        context_details=context_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.FAILED
    assert result.result is None
    assert result.error == error


async def test_checkpointed_result_create_from_operation_context_no_details():
    """Test CheckpointedResult.create_from_operation with CONTEXT operation but no context_details."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.STARTED
    assert result.result is None
    assert result.error is None


async def test_checkpointed_result_create_from_operation_context_with_both_result_and_error():
    """Test CheckpointedResult.create_from_operation with CONTEXT operation having both result and error."""
    error = ErrorObject(
        message="Context error", type="ContextError", data=None, stack_trace=None
    )
    context_details = ContextDetails(result="context_result", error=error)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.FAILED,
        context_details=context_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.FAILED
    assert result.result == "context_result"
    assert result.error == error


async def test_checkpointed_result_create_from_operation_unknown_type():
    """Test CheckpointedResult.create_from_operation with unknown operation type."""
    # Create operation with a mock operation type that doesn't match any case
    operation = Operation(
        operation_id="op1",
        operation_type="UNKNOWN_TYPE",  # This will not match any case
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.STARTED
    assert result.result is None
    assert result.error is None


async def test_checkpointed_result_create_from_operation_with_error():
    """Test CheckpointedResult.create_from_operation with error."""
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    step_details = StepDetails(error=error)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=step_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.FAILED
    assert result.result is None
    assert result.error == error


async def test_checkpointed_result_create_from_operation_no_details():
    """Test CheckpointedResult.create_from_operation with no details."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.STARTED
    assert result.result is None
    assert result.error is None


async def test_checkpointed_result_create_not_found():
    """Test CheckpointedResult.create_not_found class method."""
    result = CheckpointedResult.create_not_found()
    assert result.operation is None
    assert result.status is None
    assert result.result is None
    assert result.error is None


async def test_checkpointed_result_is_succeeded():
    """Test CheckpointedResult.is_succeeded method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_succeeded() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_succeeded() is False


async def test_checkpointed_result_is_failed():
    """Test CheckpointedResult.is_failed method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_failed() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_failed() is False


async def test_checkpointed_result_is_cancelled():
    """Test CheckpointedResult.is_cancelled method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.CANCELLED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_cancelled() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_cancelled() is False


async def test_checkpointerd_result_is_pending():
    """Test CheckpointedResult.is_pending method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_pending() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_pending() is False


async def test_checkpointed_result_is_ready():
    """Test CheckpointedResult.is_ready method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.READY,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_ready() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_ready() is False


async def test_checkpointed_result_is_started():
    """Test CheckpointedResult.is_started method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_started() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_started() is False


async def test_checkpointed_result_raise_callable_error():
    """Test CheckpointedResult.raise_callable_error method."""
    error = Mock(spec=ErrorObject)
    error.to_callable_runtime_error.return_value = RuntimeError("Test error")
    result = CheckpointedResult(error=error)

    with pytest.raises(RuntimeError, match="Test error"):
        result.raise_callable_error()

    error.to_callable_runtime_error.assert_called_once()


async def test_checkpointed_result_raise_callable_error_no_error():
    """Test CheckpointedResult.raise_callable_error with no error."""
    result = CheckpointedResult()

    with pytest.raises(CallableRuntimeError, match="Unknown error"):
        result.raise_callable_error()


async def test_checkpointed_result_raise_callable_error_no_error_with_message():
    """Test CheckpointedResult.raise_callable_error with no error and custom message."""
    result = CheckpointedResult()

    with pytest.raises(CallableRuntimeError, match="Custom error message"):
        result.raise_callable_error("Custom error message")


async def test_checkpointed_result_immutable():
    """Test that CheckpointedResult is immutable."""
    result = CheckpointedResult(status=OperationStatus.SUCCEEDED)
    with pytest.raises(AttributeError):
        result.status = OperationStatus.FAILED


async def test_execution_state_creation():
    """Test ExecutionState creation."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="test_token",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )
    assert state.durable_execution_arn == "test_arn"
    assert state.operations == {}


async def test_get_checkpoint_result_success_with_result():
    """Test get_checkpoint_result with successful operation and result."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    step_details = StepDetails(result="test_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=step_details,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={"op1": operation},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    result = get_checkpoint_result(state, "op1")
    assert result.is_succeeded() is True
    assert result.result == "test_result"
    assert result.operation == operation


async def test_get_checkpoint_result_success_without_step_details():
    """Test get_checkpoint_result with successful operation but no step details."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={"op1": operation},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    result = get_checkpoint_result(state, "op1")
    assert result.is_succeeded() is True
    assert result.result is None
    assert result.operation == operation


async def test_get_checkpoint_result_operation_not_succeeded():
    """Test get_checkpoint_result with failed operation."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={"op1": operation},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    result = get_checkpoint_result(state, "op1")
    assert result.is_failed() is True
    assert result.result is None
    assert result.operation == operation


async def test_get_checkpoint_result_operation_not_found():
    """Test get_checkpoint_result with nonexistent operation."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )
    state.start_checkpointing = Mock()
    state.start_checkpointing = Mock()

    result = get_checkpoint_result(state, "nonexistent")
    assert result.is_succeeded() is False
    assert result.result is None
    assert result.operation is None


async def test_create_checkpoint():
    """Test create_checkpoint method enqueues operations asynchronously."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )
    state.start_checkpointing = Mock()

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    # create_checkpoint with is_sync=False just enqueues without blocking
    await state.create_checkpoint(operation_update, is_sync=False)

    # Verify the operation was enqueued (not immediately processed)
    assert not mock_lambda_client.checkpoint.called
    assert state._checkpoint_queue.qsize() == 1

    # Verify we can retrieve the queued operation
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.operation_update == operation_update
    assert (
        queued_op.completion_future is None
    )  # Async operation has no completion future


async def test_create_checkpoint_with_none():
    """Test create_checkpoint method with None operation_update (empty checkpoint)."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )
    state.start_checkpointing = Mock()

    # create_checkpoint with None and is_sync=False enqueues an empty checkpoint
    await state.create_checkpoint(None, is_sync=False)

    # Verify the operation was enqueued (not immediately processed)
    assert not mock_lambda_client.checkpoint.called
    assert state._checkpoint_queue.qsize() == 1

    # Verify we can retrieve the queued operation
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.operation_update is None  # Empty checkpoint
    assert queued_op.completion_future is None  # Async operation


async def test_create_checkpoint_with_no_args():
    """Test create_checkpoint method with no arguments (default None)."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )
    state.start_checkpointing = Mock()

    # create_checkpoint with no args and is_sync=False enqueues an empty checkpoint
    await state.create_checkpoint(is_sync=False)

    # Verify the operation was enqueued (not immediately processed)
    assert not mock_lambda_client.checkpoint.called
    assert state._checkpoint_queue.qsize() == 1

    # Verify we can retrieve the queued operation
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.operation_update is None  # Empty checkpoint (default)
    assert queued_op.completion_future is None  # Async operation


async def test_get_checkpoint_result_started():
    """Test get_checkpoint_result with started operation."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={"op1": operation},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    result = get_checkpoint_result(state, "op1")
    assert result.is_started() is True
    assert result.is_succeeded() is False
    assert result.is_failed() is False
    assert result.operation == operation


async def test_checkpointed_result_is_timed_out():
    """Test CheckpointedResult.is_timed_out method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.TIMED_OUT,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_timed_out() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_timed_out() is False


async def test_checkpointed_result_is_timed_out_false_for_other_statuses():
    """Test CheckpointedResult.is_timed_out returns False for non-timed-out statuses."""
    statuses = [
        OperationStatus.STARTED,
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
        OperationStatus.PENDING,
        OperationStatus.READY,
        OperationStatus.STOPPED,
    ]

    for status in statuses:
        operation = Operation(
            operation_id="op1",
            operation_type=OperationType.STEP,
            status=status,
        )
        result = CheckpointedResult.create_from_operation(operation)
        assert result.is_timed_out() is False, (
            f"is_timed_out should be False for status {status}"
        )


async def test_fetch_paginated_operations_with_marker():
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    def mock_get_execution_state(durable_execution_arn, checkpoint_token, next_marker):
        resp = {
            "marker1": StateOutput(
                operations=[
                    Operation(
                        operation_id="1",
                        operation_type=OperationType.STEP,
                        status=OperationStatus.STARTED,
                    )
                ],
                next_marker="marker2",
            ),
            "marker2": StateOutput(
                operations=[
                    Operation(
                        operation_id="2",
                        operation_type=OperationType.STEP,
                        status=OperationStatus.STARTED,
                    )
                ],
                next_marker="marker3",
            ),
            "marker3": StateOutput(
                operations=[
                    Operation(
                        operation_id="3",
                        operation_type=OperationType.STEP,
                        status=OperationStatus.STARTED,
                    )
                ],
                next_marker=None,
            ),
        }
        return resp.get(next_marker)

    mock_lambda_client.get_execution_state.side_effect = mock_get_execution_state

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    await state.fetch_paginated_operations(
        initial_operations=[
            Operation(
                operation_id="0",
                operation_type=OperationType.STEP,
                status=OperationStatus.STARTED,
            )
        ],
        checkpoint_token="test_token",  # noqa: S106
        next_marker="marker1",
    )

    assert mock_lambda_client.get_execution_state.call_count == 3
    mock_lambda_client.get_execution_state.assert_has_calls(
        [
            call(
                durable_execution_arn="test_arn",
                checkpoint_token="test_token",  # noqa: S106
                next_marker="marker1",
            ),
            call(
                durable_execution_arn="test_arn",
                checkpoint_token="test_token",  # noqa: S106
                next_marker="marker2",
            ),
            call(
                durable_execution_arn="test_arn",
                checkpoint_token="test_token",  # noqa: S106
                next_marker="marker3",
            ),
        ]
    )

    expected_operations = {
        "0": Operation(
            operation_id="0",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        ),
        "1": Operation(
            operation_id="1",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        ),
        "2": Operation(
            operation_id="2",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        ),
        "3": Operation(
            operation_id="3",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        ),
    }

    assert len(state.operations) == len(expected_operations)

    for op_id, operation in state.operations.items():
        assert op_id in expected_operations
        expected_op = expected_operations[op_id]
        assert operation.operation_id == expected_op.operation_id


async def test_fetch_paginated_operations_stores_partial_results_on_error():
    """Test that operations from successful pages are stored even when a later page fails."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    non_retryable_error = GetExecutionStateError(
        message="KMS access denied",
        error_category=DurableApiErrorCategory.EXECUTION,
        error={"Code": "KMSAccessDeniedException", "Message": "KMS access denied"},
        response_metadata={"HTTPStatusCode": 502},
    )

    def mock_get_execution_state(durable_execution_arn, checkpoint_token, next_marker):
        if next_marker == "marker1":
            return StateOutput(
                operations=[
                    Operation(
                        operation_id="1",
                        operation_type=OperationType.STEP,
                        status=OperationStatus.STARTED,
                    )
                ],
                next_marker="marker2",
            )
        raise non_retryable_error

    mock_lambda_client.get_execution_state.side_effect = mock_get_execution_state

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    with pytest.raises(GetExecutionStateError):
        await state.fetch_paginated_operations(
            initial_operations=[
                Operation(
                    operation_id="0",
                    operation_type=OperationType.STEP,
                    status=OperationStatus.STARTED,
                )
            ],
            checkpoint_token="test_token",  # noqa: S106
            next_marker="marker1",
        )

    # Initial operation + page 1 should be stored despite page 2 failing
    assert "0" in state.operations
    assert "1" in state.operations
    assert len(state.operations) == 2


async def test_fetch_paginated_operations_logs_error(caplog):
    """Test that GetExecutionStateError is logged with structured extras."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    error = GetExecutionStateError(
        message="Service error",
        error_category=DurableApiErrorCategory.INVOCATION,
        error={"Code": "ServiceException", "Message": "Service error"},
        response_metadata={"HTTPStatusCode": 500},
    )
    mock_lambda_client.get_execution_state.side_effect = error

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    with pytest.raises(GetExecutionStateError):
        await state.fetch_paginated_operations(
            initial_operations=[],
            checkpoint_token="test_token",  # noqa: S106
            next_marker="marker1",
        )

    assert "Durable API error during state fetch." in caplog.text


# ============================================================================
# Checkpoint Batching Tests
# ============================================================================
# Note: These tests access private members (_checkpoint_queue, _overflow_queue,
# _parent_to_children, etc.) to test internal batching logic. This is justified
# for unit testing the core batching functionality that cannot be tested through
# public APIs alone.
# ruff: noqa: SLF001, BLE001


# Test 8.1: QueuedOperation wrapper and CheckpointBatcherConfig
async def test_queued_operation_creation_with_completion_future():
    """Test QueuedOperation creation with completion future for synchronous operations."""
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    completion_future = asyncio.get_running_loop().create_future()

    queued_op = QueuedOperation(operation_update, completion_future)

    assert queued_op.operation_update == operation_update
    assert queued_op.completion_future == completion_future
    assert not completion_future.done()


async def test_queued_operation_creation_without_completion_future():
    """Test QueuedOperation creation without completion future for async operations."""
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    queued_op = QueuedOperation(operation_update, completion_future=None)

    assert queued_op.operation_update == operation_update
    assert queued_op.completion_future is None


async def test_queued_operation_with_none_operation_update():
    """Test QueuedOperation with None operation_update for empty checkpoints."""
    queued_op = QueuedOperation(operation_update=None, completion_future=None)

    assert queued_op.operation_update is None
    assert queued_op.completion_future is None


async def test_checkpoint_batcher_config_default_values():
    """Test CheckpointBatcherConfig default values."""
    config = CheckpointBatcherConfig()

    assert config.max_batch_size_bytes == 750 * 1024  # 750KB
    assert config.max_batch_time_seconds == 1.0
    assert config.max_batch_operations == 250


async def test_checkpoint_batcher_config_custom_values():
    """Test CheckpointBatcherConfig with custom values."""
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=500 * 1024,
        max_batch_time_seconds=0.5,
        max_batch_operations=10,
    )

    assert config.max_batch_size_bytes == 500 * 1024
    assert config.max_batch_time_seconds == 0.5
    assert config.max_batch_operations == 10


async def test_checkpoint_batcher_config_immutable():
    """Test that CheckpointBatcherConfig is immutable."""
    config = CheckpointBatcherConfig()

    with pytest.raises(AttributeError):
        config.max_batch_size_bytes = 1000


async def test_checkpoint_batch_respects_default_max_items_limit():
    """Test that batch collection respects the default MAX_ITEMS_IN_BATCH (250) limit.

    This ensures consistency across all Durable Execution SDK implementations.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    # Use default config (max_batch_operations=250)
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=10.0,
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Enqueue 300 small operations (exceeds MAX_ITEMS_IN_BATCH of 250)
    for i in range(300):
        operation_update = OperationUpdate(
            operation_id=f"op_{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Collect first batch
    batch1 = await state._collect_checkpoint_batch()

    # First batch should have exactly 250 items
    assert len(batch1) == 250

    # Collect second batch
    batch2 = await state._collect_checkpoint_batch()

    # Second batch should have remaining 50 items
    assert len(batch2) == 50


async def test_calculate_operation_size_with_operation():
    """Test _calculate_operation_size with a real operation."""
    operation_update = OperationUpdate(
        operation_id="test_op_123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    queued_op = QueuedOperation(operation_update, completion_future=None)

    size = ExecutionState._calculate_operation_size(queued_op)

    # Verify size is positive and reasonable
    assert size > 0
    # Verify it matches JSON serialization size
    expected_size = len(json.dumps(operation_update.to_dict()).encode("utf-8"))
    assert size == expected_size


async def test_calculate_operation_size_with_none():
    """Test _calculate_operation_size with None operation_update (empty checkpoint)."""
    queued_op = QueuedOperation(operation_update=None, completion_future=None)

    size = ExecutionState._calculate_operation_size(queued_op)

    assert size == 0


# Test 8.2: Batching logic and size limits
async def test_collect_checkpoint_batch_respects_size_limit():
    """Test that batch collection respects max_batch_size_bytes limit."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    # Create config with small size limit
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=200,  # Small limit to trigger overflow
        max_batch_time_seconds=10.0,  # Long time to avoid time-based flush
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Enqueue multiple operations
    for i in range(5):
        operation_update = OperationUpdate(
            operation_id=f"op_{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Collect batch
    batch = await state._collect_checkpoint_batch()

    # Verify batch size is limited
    assert len(batch) < 5  # Should not include all operations
    assert len(batch) > 0  # Should include at least one

    # Verify total size doesn't exceed limit
    total_size = sum(state._calculate_operation_size(op) for op in batch)
    assert total_size <= config.max_batch_size_bytes


async def test_collect_checkpoint_batch_uses_overflow_queue():
    """Test that overflow queue is processed first to maintain FIFO order."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Put operations in overflow queue
    overflow_op1 = QueuedOperation(
        OperationUpdate(
            operation_id="overflow_1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        None,
    )
    overflow_op2 = QueuedOperation(
        OperationUpdate(
            operation_id="overflow_2",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        None,
    )
    state._overflow_queue.append(overflow_op1)
    state._overflow_queue.append(overflow_op2)

    # Put operation in main queue
    main_op = QueuedOperation(
        OperationUpdate(
            operation_id="main_1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        None,
    )
    state._checkpoint_queue.put(main_op)

    # Collect batch
    batch = await state._collect_checkpoint_batch()

    # Verify overflow operations come first
    assert len(batch) >= 2
    assert batch[0].operation_update.operation_id == "overflow_1"
    assert batch[1].operation_update.operation_id == "overflow_2"


async def test_collect_checkpoint_batch_handles_empty_checkpoint():
    """Test batch collection with empty checkpoints (None operation_update)."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Enqueue empty checkpoint
    state._checkpoint_queue.put(QueuedOperation(None, None))

    # Enqueue regular checkpoint
    state._checkpoint_queue.put(
        QueuedOperation(
            OperationUpdate(
                operation_id="op_1",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
            ),
            None,
        )
    )

    # Collect batch
    batch = await state._collect_checkpoint_batch()

    # Verify both operations are in batch
    assert len(batch) == 2
    assert batch[0].operation_update is None  # Empty checkpoint
    assert batch[1].operation_update is not None


async def test_collect_checkpoint_batch_returns_empty_when_stopped():
    """Test that batch collection returns empty list when checkpointing is stopped."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Signal stop before collecting
    state.stop_checkpointing()

    # Collect batch (should return empty quickly)
    batch = await state._collect_checkpoint_batch()

    assert len(batch) == 0


# Test 8.3: Parallel operation concurrency management
async def test_parent_child_relationship_building():
    """Test that parent-child relationships are built correctly."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Create parent operation
    parent_update = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )
    await state.create_checkpoint(parent_update, is_sync=False)

    # Create child operations
    child1_update = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )
    child2_update = OperationUpdate(
        operation_id="child_2",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )
    await state.create_checkpoint(child1_update, is_sync=False)
    await state.create_checkpoint(child2_update, is_sync=False)

    # Verify parent-child relationships
    assert "parent_1" in state._parent_to_children
    assert "child_1" in state._parent_to_children["parent_1"]
    assert "child_2" in state._parent_to_children["parent_1"]


async def test_descendant_cancellation_when_parent_completes():
    """Test that descendants are marked as orphaned when parent CONTEXT completes."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Build parent-child hierarchy
    parent_update = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )
    await state.create_checkpoint(parent_update, is_sync=False)

    child1_update = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )
    await state.create_checkpoint(child1_update, is_sync=False)

    # Complete parent CONTEXT
    parent_complete = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
    )
    await state.create_checkpoint(parent_complete, is_sync=False)

    # Verify child is marked as orphaned
    assert "child_1" in state._parent_done


async def test_rejection_of_operations_from_completed_parents():
    """Test that operations are rejected if their parent has completed."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Build parent-child hierarchy
    parent_update = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )
    await state.create_checkpoint(parent_update, is_sync=False)

    child1_update = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )
    await state.create_checkpoint(child1_update, is_sync=False)

    # Complete parent CONTEXT
    parent_complete = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
    )
    await state.create_checkpoint(parent_complete, is_sync=False)

    # Try to checkpoint child operation (should raise OrphanedChildException)
    child_checkpoint = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        parent_id="parent_1",
    )
    with pytest.raises(OrphanedChildException) as exc_info:
        await state.create_checkpoint(child_checkpoint, is_sync=False)

    # Verify exception contains operation_id
    assert exc_info.value.operation_id == "child_1"


async def test_nested_parallel_operations_deep_hierarchy():
    """Test that nested parallel operations handle deep hierarchies correctly."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Build deep hierarchy: grandparent -> parent -> child
    grandparent_update = OperationUpdate(
        operation_id="grandparent",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )
    await state.create_checkpoint(grandparent_update, is_sync=False)

    parent_update = OperationUpdate(
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        parent_id="grandparent",
    )
    await state.create_checkpoint(parent_update, is_sync=False)

    child_update = OperationUpdate(
        operation_id="child",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent",
    )
    await state.create_checkpoint(child_update, is_sync=False)

    # Complete grandparent CONTEXT
    grandparent_complete = OperationUpdate(
        operation_id="grandparent",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
    )
    await state.create_checkpoint(grandparent_complete, is_sync=False)

    # Verify all descendants are marked as orphaned
    assert "parent" in state._parent_done
    assert "child" in state._parent_done


# Test 8.4: Thread safety and synchronous operations
async def test_synchronous_checkpoint_blocks_until_complete():
    """Test that create_checkpoint(is_sync=True) returns only after processing."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    await state.create_checkpoint(operation_update, is_sync=True)

    mock_lambda_client.checkpoint.assert_called_once()
    assert state._checkpoint_queue.qsize() == 0


async def test_operations_dictionary_access():
    """Test checkpoint reads reflect direct state updates."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    state.operations["op1"] = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )

    result = get_checkpoint_result(state, "op1")

    assert result.is_succeeded()


async def test_stop_checkpointing_signals_background_thread():
    """Test that stop_checkpointing signals the background thread to stop."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Verify event is not set initially
    assert not state._checkpointing_stopped.is_set()

    # Call stop_checkpointing
    state.stop_checkpointing()

    # Verify event is now set
    assert state._checkpointing_stopped.is_set()


# Additional coverage tests for missing lines
async def test_checkpointed_result_is_replay_children_true():
    """Test CheckpointedResult.is_replay_children when context_details.replay_children is True."""

    context_details = ContextDetails(replay_children=True)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
        context_details=context_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_replay_children() is True


async def test_checkpointed_result_is_replay_children_false():
    """Test CheckpointedResult.is_replay_children when context_details.replay_children is False."""

    context_details = ContextDetails(replay_children=False)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
        context_details=context_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_replay_children() is False


async def test_checkpointed_result_is_replay_children_no_context_details():
    """Test CheckpointedResult.is_replay_children when context_details is None."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_replay_children() is False


async def test_checkpointed_result_is_replay_children_no_operation():
    """Test CheckpointedResult.is_replay_children when operation is None."""
    result = CheckpointedResult.create_not_found()
    assert result.is_replay_children() is False


async def test_checkpointed_result_get_next_attempt_timestamp():
    """Test CheckpointedResult.get_next_attempt_timestamp with timestamp."""

    timestamp = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    step_details = StepDetails(next_attempt_timestamp=timestamp)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=step_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.get_next_attempt_timestamp() == timestamp


async def test_checkpointed_result_get_next_attempt_timestamp_none():
    """Test CheckpointedResult.get_next_attempt_timestamp when no timestamp."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.get_next_attempt_timestamp() is None


async def test_create_checkpoint_with_parent_id():
    """Test create_checkpoint builds parent-child relationships."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Create parent operation
    parent_update = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )

    # Create child operation with parent_id
    child_update = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )

    await state.create_checkpoint(parent_update, is_sync=False)
    await state.create_checkpoint(child_update, is_sync=False)

    # Verify parent-child relationship was built
    assert "parent_1" in state._parent_to_children
    assert "child_1" in state._parent_to_children["parent_1"]


async def test_create_checkpoint_rejects_orphaned_operation():
    """Test create_checkpoint rejects operations whose parent is done."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )
    state.start_checkpointing = Mock()

    # Build parent-child relationship
    parent_update = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )
    child_update = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )

    # Process parent and child
    for update in [parent_update, child_update]:
        await state.create_checkpoint(update, is_sync=False)

    # Complete parent CONTEXT
    parent_complete = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
    )
    await state.create_checkpoint(parent_complete, is_sync=False)

    # Try to checkpoint child (should raise OrphanedChildException)
    child_checkpoint = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        parent_id="parent_1",
    )
    with pytest.raises(OrphanedChildException) as exc_info:
        await state.create_checkpoint(child_checkpoint, is_sync=False)

    # Verify exception contains operation_id
    assert exc_info.value.operation_id == "child_1"


async def test_mark_orphans_handles_cycles():
    """Test _mark_orphans handles potential cycles in parent-child relationships."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Manually create a cycle (shouldn't happen in practice, but test defensive code)
    state._parent_to_children["parent"] = {"child1", "child2"}
    state._parent_to_children["child1"] = {"child2"}
    state._parent_to_children["child2"] = {"child1"}  # Cycle

    # Mark orphans should handle this gracefully
    state._mark_orphans("parent")

    # Verify descendants were marked (cycle detection prevents infinite loop)
    assert "child1" in state._parent_done
    assert "child2" in state._parent_done


async def test_checkpoint_batches_forever_exception_handling():
    """Test checkpoint_batches_forever handles exceptions without signaling completion events.

    This test verifies the bug fix where completion events should NOT be signaled
    when checkpoint fails, preventing callers from continuing with corrupted state.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    mock_lambda_client.checkpoint.side_effect = RuntimeError("API error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Create synchronous operation
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    completion_future = asyncio.get_running_loop().create_future()
    queued_op = QueuedOperation(operation_update, completion_future)
    state._checkpoint_queue.put(queued_op)

    await state.checkpoint_batches_forever()

    assert completion_future.done()
    with pytest.raises(RuntimeError, match="API error"):
        completion_future.result()


async def test_collect_checkpoint_batch_shutdown_path():
    """Test _collect_checkpoint_batch during shutdown with operations in queue.

    With the simplified shutdown logic, once stop_checkpointing() is called,
    _collect_checkpoint_batch() returns empty immediately. Any remaining operations
    in the queue are non-essential async checkpoints that will be abandoned.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Add operation to queue (would be a non-essential async checkpoint in practice)
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Signal shutdown
    state.stop_checkpointing()

    # Collect batch during shutdown - returns empty immediately
    batch = await state._collect_checkpoint_batch()

    # Should return empty batch, abandoning the non-essential async checkpoint
    assert len(batch) == 0


async def test_collect_checkpoint_batch_shutdown_empty_queue():
    """Test _collect_checkpoint_batch during shutdown with empty queue."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Signal shutdown with empty queue
    state.stop_checkpointing()

    # Collect batch during shutdown
    batch = await state._collect_checkpoint_batch()

    # Should return empty batch immediately
    assert len(batch) == 0


async def test_collect_checkpoint_batch_overflow_put_back():
    """Test that operations exceeding size limit are put back in overflow queue."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    # Create config with very small size limit
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=150,  # Small enough to trigger overflow
        max_batch_time_seconds=10.0,  # Long time window
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Enqueue two operations - first will fit, second will overflow
    op1 = OperationUpdate(
        operation_id="op_1" * 10,  # Make ID large
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    op2 = OperationUpdate(
        operation_id="op_2" * 10,  # Make ID large
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(op1, None))
    state._checkpoint_queue.put(QueuedOperation(op2, None))

    # Collect first batch
    batch1 = await state._collect_checkpoint_batch()

    # Should have collected first operation
    assert len(batch1) == 1
    assert batch1[0].operation_update.operation_id == "op_1" * 10

    # Verify second operation was put in overflow queue
    assert len(state._overflow_queue) == 1

    # Collect second batch (should get overflow operation first)
    batch2 = await state._collect_checkpoint_batch()

    # Should have collected the overflow operation
    assert len(batch2) == 1
    assert batch2[0].operation_update.operation_id == "op_2" * 10


# Additional edge case tests for remaining coverage
async def test_create_checkpoint_with_none_operation_update_sync():
    """Test create_checkpoint with None operation_update (empty checkpoint)."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    async def scenario():
        await state.create_checkpoint(None, is_sync=True)
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_checkpoint_batches_forever_exception_with_no_sync_operations():
    """Test checkpoint_batches_forever exception handling when no sync operations exist."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    mock_lambda_client.checkpoint.side_effect = RuntimeError("API error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Create async operation (no completion event)
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    queued_op = QueuedOperation(operation_update, completion_future=None)
    state._checkpoint_queue.put(queued_op)

    await state.checkpoint_batches_forever()
    assert state._checkpointing_failed.is_set()


async def test_collect_checkpoint_batch_size_limit_during_time_window():
    """Test that size limit is enforced during time window collection."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    # Create config with small size limit
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=200,
        max_batch_time_seconds=0.5,  # Short window
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Enqueue first small operation
    small_op = OperationUpdate(
        operation_id="small",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(small_op, None))

    async def enqueue_large_operation() -> None:
        await asyncio.sleep(0.05)  # Let first op be collected
        large_op = OperationUpdate(
            operation_id="large_op" * 20,  # Very large ID
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        state._checkpoint_queue.put(QueuedOperation(large_op, None))

    enqueue_task = asyncio.create_task(enqueue_large_operation())

    # Collect batch
    batch = await state._collect_checkpoint_batch()

    await enqueue_task

    # Should have collected small op, large op should be in overflow
    assert len(batch) >= 1
    # If large op exceeded size limit, it should be in overflow queue
    if len(batch) == 1:
        assert len(state._overflow_queue) == 1


async def test_collect_checkpoint_batch_respects_max_operations_limit():
    """Test that batch collection respects max_batch_operations limit."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    # Create config with max 1 operation per batch
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=1000000,  # Large size limit
        max_batch_time_seconds=10.0,  # Long time window
        max_batch_operations=1,  # Only 1 operation per batch
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Enqueue multiple operations
    for i in range(3):
        operation_update = OperationUpdate(
            operation_id=f"op_{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Collect first batch
    batch1 = await state._collect_checkpoint_batch()

    # Should have collected exactly 1 operation due to max_batch_operations limit
    assert len(batch1) == 1
    assert batch1[0].operation_update.operation_id == "op_0"

    # Collect second batch
    batch2 = await state._collect_checkpoint_batch()

    # Should have collected exactly 1 operation again
    assert len(batch2) == 1
    assert batch2[0].operation_update.operation_id == "op_1"


async def test_collect_checkpoint_batch_time_window_expires():
    """Test that batch collection stops when time window expires (remaining_time <= 0)."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    # Create config with very short time window
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=1000000,  # Large size limit
        max_batch_time_seconds=0.01,  # Very short time window (10ms)
        max_batch_operations=100,  # High operation limit
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Enqueue first operation
    first_op = OperationUpdate(
        operation_id="first_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(first_op, None))

    # Mock time.time() to simulate time window expiring between loop check and remaining_time calculation
    original_time = time.time
    call_count = [0]
    start_time = original_time()

    def mock_time():
        call_count[0] += 1
        # First call: initial time for batch_deadline calculation
        if call_count[0] == 1:
            return start_time
        # Second call: while loop condition check (still within window)
        if call_count[0] == 2:
            return start_time + 0.005  # 5ms elapsed, still within 10ms window
        # Third call: remaining_time calculation (time window expired)
        return start_time + 0.015  # 15ms elapsed, past the 10ms window

    with unittest.mock.patch(
        "async_durable_execution.state.time.time", side_effect=mock_time
    ):
        # Collect batch - should get first operation, then break when remaining_time <= 0
        batch = await state._collect_checkpoint_batch()

    # Should have collected only the first operation (time window expired before second get)
    assert len(batch) == 1
    assert batch[0].operation_update.operation_id == "first_op"


async def test_collect_checkpoint_batch_handles_legacy_asyncio_timeout_error(
    monkeypatch,
):
    """Timeouts from asyncio.wait_for must not strand sync checkpoint waiters."""

    class LegacyAsyncioTimeoutError(Exception):
        pass

    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=1000000,
        max_batch_time_seconds=1.0,
        max_batch_operations=100,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )
    first_op = OperationUpdate(
        operation_id="first_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(first_op, None))

    call_count = 0

    async def fake_wait_for(awaitable, timeout):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return await awaitable

        awaitable.close()
        raise LegacyAsyncioTimeoutError

    monkeypatch.setattr(
        "async_durable_execution.state.asyncio.TimeoutError",
        LegacyAsyncioTimeoutError,
    )
    monkeypatch.setattr(
        "async_durable_execution.state.asyncio.wait_for",
        fake_wait_for,
    )

    batch = await state._collect_checkpoint_batch()

    assert len(batch) == 1
    assert batch[0].operation_update.operation_id == "first_op"


async def test_collect_checkpoint_batch_empty_overflow_queue_path():
    """Test batch collection when overflow queue is empty from the start."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Ensure overflow queue is empty (it should be by default)
    assert len(state._overflow_queue) == 0

    # Enqueue operation in main queue
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Collect batch - should skip overflow queue (empty) and get from main queue
    batch = await state._collect_checkpoint_batch()

    # Should have collected from main queue
    assert len(batch) == 1
    assert batch[0].operation_update.operation_id == "test_op"


async def test_collect_checkpoint_batch_overflow_queue_hits_operation_limit():
    """Test that overflow queue draining stops when max_batch_operations is reached."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    # Create config with max 2 operations per batch
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=1000000,  # Large size limit
        max_batch_time_seconds=10.0,  # Long time window
        max_batch_operations=2,  # Only 2 operations per batch
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Put 3 operations in overflow queue
    for i in range(3):
        operation_update = OperationUpdate(
            operation_id=f"overflow_op_{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        state._overflow_queue.append(QueuedOperation(operation_update, None))

    # Collect batch - should stop after 2 operations due to max_batch_operations
    batch = await state._collect_checkpoint_batch()

    # Should have collected exactly 2 operations from overflow queue
    assert len(batch) == 2
    assert batch[0].operation_update.operation_id == "overflow_op_0"
    assert batch[1].operation_update.operation_id == "overflow_op_1"

    # Third operation should still be in overflow queue
    assert len(state._overflow_queue) == 1


async def test_collect_checkpoint_batch_overflow_queue_size_limit():
    """Test that overflow queue draining respects size limit and puts back oversized operations."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    # Create config with small size limit
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=200,
        max_batch_time_seconds=10.0,
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Put operations in overflow queue - first small, second large
    small_op = OperationUpdate(
        operation_id="small",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    large_op = OperationUpdate(
        operation_id="large_op" * 20,  # Very large ID
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._overflow_queue.append(QueuedOperation(small_op, None))
    state._overflow_queue.append(QueuedOperation(large_op, None))

    # Collect batch - should get small op, large op should be put back
    batch = await state._collect_checkpoint_batch()

    # Should have collected small operation
    assert len(batch) == 1
    assert batch[0].operation_update.operation_id == "small"

    # Large operation should be put back in overflow queue
    assert len(state._overflow_queue) == 1


# ============================================================================
# Error Handling Tests for Task 1.1
# ============================================================================


async def test_checkpoint_error_signals_completion_futures_with_error():
    """Test that completion futures are completed with error when checkpoint fails."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Checkpoint API error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Create synchronous operation with completion event
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    completion_future = asyncio.get_running_loop().create_future()
    queued_op = QueuedOperation(operation_update, completion_future)
    state._checkpoint_queue.put(queued_op)

    await state.checkpoint_batches_forever()

    assert completion_future.done()
    with pytest.raises(RuntimeError, match="Checkpoint API error"):
        completion_future.result()


async def test_synchronous_caller_receives_error_on_background_thread_failure():
    """Test that synchronous callers receive error when background thread fails.

    This verifies that when the background thread encounters an error, synchronous
    callers waiting on checkpoint futures receive the original error,
    allowing them to exit cleanly rather than hanging indefinitely.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Background thread error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        with pytest.raises(RuntimeError, match="Background thread error"):
            await state.create_checkpoint(operation_update, is_sync=True)
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_exception_propagates_through_threadpoolexecutor():
    """Test that checkpoint_batches_forever exits gracefully after signaling errors.

    This verifies that when checkpoint_batches_forever encounters an error,
    it signals the error through completion events and failure state, then
    exits gracefully rather than raising an exception in the background thread.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Checkpoint API failure")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Enqueue an operation
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Run checkpoint_batches_forever and verify it exits gracefully (no exception)
    await state.checkpoint_batches_forever()

    # Verify the failure state was set
    assert state._checkpointing_failed.is_set()


async def test_multiple_sync_operations_all_remain_blocked_on_error():
    """Test that multiple synchronous operations all receive an error when checkpoint fails.

    This verifies that when multiple synchronous operations are waiting and the
    checkpoint processor fails, ALL of them are completed with error rather than
    hanging indefinitely.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Batch processing error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Create multiple synchronous operations
    num_operations = 3
    completion_events = []
    for i in range(num_operations):
        operation_update = OperationUpdate(
            operation_id=f"test_op_{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        completion_future = asyncio.get_running_loop().create_future()
        completion_events.append(completion_future)
        queued_op = QueuedOperation(operation_update, completion_future)
        state._checkpoint_queue.put(queued_op)

    await state.checkpoint_batches_forever()

    # CRITICAL: Verify ALL completion futures are completed with error
    for i, event in enumerate(completion_events):
        assert event.done(), f"Completion future {i} should be completed with error"
        with pytest.raises(RuntimeError, match="Batch processing error"):
            event.result()


async def test_async_operations_not_affected_by_error_handling():
    """Test that async operations (no completion event) are not affected by error handling.

    This verifies that the error handling logic correctly handles batches containing
    only async operations (no completion events to signal).
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("API error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Create async operation (no completion event)
    operation_update = OperationUpdate(
        operation_id="async_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    queued_op = QueuedOperation(operation_update, completion_future=None)
    state._checkpoint_queue.put(queued_op)

    # Run checkpoint_batches_forever and verify it exits gracefully
    await state.checkpoint_batches_forever()

    # Verify the failure state was set
    assert state._checkpointing_failed.is_set()

    # Test passes if no AttributeError or other issues occur
    # (verifying the code handles None completion_future correctly)


async def test_mixed_sync_async_operations_only_sync_blocked_on_error():
    """Test that in mixed batches, only sync operations receive completion errors.

    This verifies that when a batch contains both sync and async operations,
    the error handling correctly processes both types without attempting to
    signal non-existent completion events for async operations.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Mixed batch error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    # Create sync operation with completion future
    sync_op = OperationUpdate(
        operation_id="sync_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    sync_future = asyncio.get_running_loop().create_future()
    state._checkpoint_queue.put(QueuedOperation(sync_op, sync_future))

    # Create async operation without completion future
    async_op = OperationUpdate(
        operation_id="async_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(async_op, None))

    await state.checkpoint_batches_forever()

    assert sync_future.done()
    with pytest.raises(RuntimeError, match="Mixed batch error"):
        sync_future.result()

    # Test passes if no AttributeError occurs when processing async operation
    # (verifying None completion_future is handled correctly)


# ============================================================================
# Task 4.1: Test method signature and defaults
# ============================================================================


async def test_create_checkpoint_accepts_is_sync_parameter():
    """Test that create_checkpoint() accepts is_sync parameter.

    Verifies that the consolidated create_checkpoint method accepts the is_sync
    parameter for controlling synchronous vs asynchronous behavior.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        state.start_checkpointing = Mock()

        await state.create_checkpoint(operation_update, is_sync=False)
        assert state._checkpoint_queue.qsize() == 1

        state._checkpoint_queue.get_nowait()

        await state.create_checkpoint(operation_update, is_sync=False)
        assert state._checkpoint_queue.qsize() == 1

    await run_async(scenario())


async def test_create_checkpoint_default_is_sync_true():
    """Test that create_checkpoint() defaults to is_sync=True (synchronous).

    Verifies that when is_sync parameter is not provided, the method defaults
    to synchronous behavior (is_sync=True), creating a completion event.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        state.start_checkpointing = Mock()

        checkpoint_task = asyncio.create_task(state.create_checkpoint(operation_update))
        await asyncio.sleep(0)

        assert state._checkpoint_queue.qsize() == 1
        queued_op = state._checkpoint_queue.get_nowait()
        assert queued_op.operation_update == operation_update
        assert queued_op.completion_future is not None
        assert isinstance(queued_op.completion_future, asyncio.Future)

        queued_op.completion_future.set_result(None)
        await checkpoint_task

    await run_async(scenario())


async def test_create_checkpoint_explicit_is_sync_true():
    """Test that create_checkpoint(is_sync=True) creates completion event.

    Verifies that explicitly setting is_sync=True results in synchronous behavior
    with a completion event created.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        state.start_checkpointing = Mock()

        checkpoint_task = asyncio.create_task(
            state.create_checkpoint(operation_update, is_sync=True)
        )
        await asyncio.sleep(0)

        assert state._checkpoint_queue.qsize() == 1
        queued_op = state._checkpoint_queue.get_nowait()
        assert queued_op.completion_future is not None
        assert isinstance(queued_op.completion_future, asyncio.Future)

        queued_op.completion_future.set_result(None)
        await checkpoint_task

    await run_async(scenario())


async def test_create_checkpoint_is_sync_false_no_completion_event():
    """Test that create_checkpoint(is_sync=False) does not create completion event.

    Verifies that setting is_sync=False results in asynchronous behavior
    without a completion event.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        state.start_checkpointing = Mock()

        await state.create_checkpoint(operation_update, is_sync=False)

        assert state._checkpoint_queue.qsize() == 1
        queued_op = state._checkpoint_queue.get_nowait()
        assert queued_op.completion_future is None

    await run_async(scenario())


async def test_create_checkpoint_is_sync_false_returns_immediately():
    """Test that create_checkpoint(is_sync=False) returns immediately.

    Verifies that asynchronous checkpoints return immediately without blocking,
    even when the background thread is not processing checkpoints.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        state.start_checkpointing = Mock()

        start_time = time.time()
        await state.create_checkpoint(operation_update, is_sync=False)
        elapsed_time = time.time() - start_time

        assert elapsed_time < 0.05, (
            f"Async checkpoint took {elapsed_time:.3f}s, expected < 0.05s"
        )

        assert state._checkpoint_queue.qsize() == 1
        queued_op = state._checkpoint_queue.get_nowait()
        assert queued_op.operation_update == operation_update
        assert queued_op.completion_future is None

    await run_async(scenario())


async def test_create_checkpoint_with_none_defaults_to_sync():
    """Test that create_checkpoint(None) defaults to synchronous behavior.

    Verifies that empty checkpoints (operation_update=None) also default
    to synchronous behavior when is_sync is not specified.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    async def scenario():
        state.start_checkpointing = Mock()

        checkpoint_task = asyncio.create_task(state.create_checkpoint(None))
        await asyncio.sleep(0)

        assert state._checkpoint_queue.qsize() == 1
        queued_op = state._checkpoint_queue.get_nowait()
        assert queued_op.operation_update is None
        assert queued_op.completion_future is not None

        queued_op.completion_future.set_result(None)
        await checkpoint_task

    await run_async(scenario())


async def test_create_checkpoint_no_args_defaults_to_sync():
    """Test that create_checkpoint() with no arguments defaults to synchronous.

    Verifies that calling create_checkpoint with no arguments results in
    an empty synchronous checkpoint.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    async def scenario():
        state.start_checkpointing = Mock()

        checkpoint_task = asyncio.create_task(state.create_checkpoint())
        await asyncio.sleep(0)

        assert state._checkpoint_queue.qsize() == 1
        queued_op = state._checkpoint_queue.get_nowait()
        assert queued_op.operation_update is None
        assert queued_op.completion_future is not None

        queued_op.completion_future.set_result(None)
        await checkpoint_task

    await run_async(scenario())


async def test_collect_checkpoint_batch_overflow_queue_size_limit_final():
    """Test that overflow queue draining respects size limit and puts back oversized operations."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    # Create config with small size limit
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=200,  # Small size limit
        max_batch_time_seconds=10.0,  # Long time window
        max_batch_operations=10,  # High operation limit
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Put 2 operations in overflow queue - second one will be too large
    small_op = OperationUpdate(
        operation_id="small",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    large_op = OperationUpdate(
        operation_id="large_operation_id" * 20,  # Very large ID
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._overflow_queue.append(QueuedOperation(small_op, None))
    state._overflow_queue.append(QueuedOperation(large_op, None))

    # Collect batch - should get small op, put back large op
    batch = await state._collect_checkpoint_batch()

    # Should have collected only the small operation
    assert len(batch) == 1
    assert batch[0].operation_update.operation_id == "small"

    # Large operation should be back in overflow queue
    assert len(state._overflow_queue) == 1
    remaining_op = state._overflow_queue.popleft()
    assert remaining_op.operation_update.operation_id == "large_operation_id" * 20


# ============================================================================
# Task 4.2: Test synchronous behavior
# ============================================================================


async def test_create_checkpoint_blocks_until_completion_default():
    """Test that create_checkpoint() blocks until completion when is_sync=True (default).

    Verifies that calling create_checkpoint without specifying is_sync results in
    synchronous blocking behavior until the background thread processes the checkpoint.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    def delayed_checkpoint(**_kwargs):
        time.sleep(0.15)
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=[],
                next_marker=None,
            ),
        )

    mock_lambda_client.checkpoint.side_effect = delayed_checkpoint

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        start_time = time.time()
        await state.create_checkpoint(operation_update)
        elapsed = time.time() - start_time

        assert elapsed >= 0.15, f"Expected blocking for at least 0.15s, got {elapsed}s"
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_create_checkpoint_blocks_until_completion_explicit_true():
    """Test that create_checkpoint(is_sync=True) blocks until completion.

    Verifies that explicitly setting is_sync=True results in synchronous blocking
    behavior until the background thread processes the checkpoint.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    def delayed_checkpoint(**_kwargs):
        time.sleep(0.15)
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=[],
                next_marker=None,
            ),
        )

    mock_lambda_client.checkpoint.side_effect = delayed_checkpoint

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        start_time = time.time()
        await state.create_checkpoint(operation_update, is_sync=True)
        elapsed = time.time() - start_time

        assert elapsed >= 0.15, f"Expected blocking for at least 0.15s, got {elapsed}s"
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_create_checkpoint_completion_event_created_and_signaled():
    """Test that a completion future is created and signaled on success.

    Verifies that when is_sync=True, a completion future is created, enqueued,
    and properly resolved after successful checkpoint processing.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        state.start_checkpointing = Mock()

        checkpoint_task = asyncio.create_task(
            state.create_checkpoint(operation_update, is_sync=True)
        )
        await asyncio.sleep(0)

        queued_operation = state._checkpoint_queue.get_nowait()
        assert queued_operation.completion_future is not None
        assert isinstance(queued_operation.completion_future, asyncio.Future)
        assert not queued_operation.completion_future.done()

        queued_operation.completion_future.set_result(None)
        await checkpoint_task
        assert queued_operation.completion_future.done()

    await run_async(scenario())


async def test_create_checkpoint_completion_event_not_signaled_on_failure():
    """Test that synchronous callers receive the original checkpoint failure.

    Verifies that checkpoint failures propagate back through the completion future
    and do not leave the caller blocked.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    # Simulate checkpoint failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Checkpoint failed")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        with pytest.raises(RuntimeError, match="Checkpoint failed"):
            await state.create_checkpoint(operation_update, is_sync=True)
        assert state._checkpointing_failed.is_set()
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_create_checkpoint_caller_remains_blocked_on_background_failure():
    """Test that caller exits promptly with an error when checkpointing fails.

    Verifies that synchronous callers do not hang after a checkpoint failure on
    the single-threaded event loop.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    # Simulate background thread failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Background failure")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    async def scenario():
        start_time = time.time()
        with pytest.raises(RuntimeError, match="Background failure"):
            await state.create_checkpoint(operation_update, is_sync=True)
        elapsed = time.time() - start_time

        assert elapsed < 0.5
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_create_checkpoint_multiple_sync_calls_all_block():
    """Test that multiple synchronous checkpoint calls all block correctly.

    Verifies that when multiple threads call create_checkpoint synchronously,
    they all block until their respective completion events are signaled.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    num_callers = 3

    def delayed_checkpoint(**_kwargs):
        time.sleep(0.15)
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=[],
                next_marker=None,
            ),
        )

    mock_lambda_client.checkpoint.side_effect = delayed_checkpoint

    async def call_checkpoint(index: int) -> float:
        operation_update = OperationUpdate(
            operation_id=f"test_op_{index}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        start_time = time.time()
        await state.create_checkpoint(operation_update, is_sync=True)
        return time.time() - start_time

    async def scenario():
        elapsed_times = await asyncio.gather(
            *(call_checkpoint(i) for i in range(num_callers))
        )

        for i, elapsed in enumerate(elapsed_times):
            assert elapsed >= 0.15, (
                f"Caller {i} expected blocking for at least 0.15s, got {elapsed}s"
            )
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_create_checkpoint_with_empty_checkpoint_sync():
    """Test synchronous behavior with empty checkpoint (None operation_update).

    Verifies that empty checkpoints also block correctly when is_sync=True.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    def delayed_checkpoint(**_kwargs):
        time.sleep(0.15)
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=[],
                next_marker=None,
            ),
        )

    mock_lambda_client.checkpoint.side_effect = delayed_checkpoint

    async def scenario():
        start_time = time.time()
        await state.create_checkpoint(None, is_sync=True)
        elapsed = time.time() - start_time

        assert elapsed >= 0.15, f"Expected blocking for at least 0.15s, got {elapsed}s"
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_create_checkpoint_sync_mode_success():
    """Test create_checkpoint(is_sync=True) works normally when no error occurs."""
    mock_client = Mock(spec=ThreadedSyncLambdaClient)
    mock_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )

    state = ExecutionState(
        durable_execution_arn="test-arn",
        initial_checkpoint_token="initial-token",  # noqa: S106
        operations={},
        service_client=mock_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    async def scenario():
        operation_update = OperationUpdate.create_step_start(
            OperationIdentifier("test-op", OperationSubType.STEP, None, "test-step")
        )

        await state.create_checkpoint(operation_update, is_sync=True)

        assert mock_client.checkpoint.call_count == 1
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_create_checkpoint_sync_mode_raises_original_error():
    """Test create_checkpoint(is_sync=True) raises the original checkpoint error."""
    mock_client = Mock(spec=ThreadedSyncLambdaClient)

    # Make checkpoint fail with a specific error
    original_error = RuntimeError("Original checkpoint error")
    mock_client.checkpoint.side_effect = original_error

    state = ExecutionState(
        durable_execution_arn="test-arn",
        initial_checkpoint_token="initial-token",  # noqa: S106
        operations={},
        service_client=mock_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    async def scenario():
        operation_update = OperationUpdate.create_step_start(
            OperationIdentifier("test-op", OperationSubType.STEP, None, "test-step")
        )

        with pytest.raises(RuntimeError, match="Original checkpoint error"):
            await state.create_checkpoint(operation_update, is_sync=True)

        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_create_checkpoint_sync_mode_always_blocks():
    """Test create_checkpoint(is_sync=True) blocks until completion."""
    mock_client = Mock(spec=ThreadedSyncLambdaClient)

    def delayed_checkpoint(**_kwargs):
        time.sleep(0.15)
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    mock_client.checkpoint.side_effect = delayed_checkpoint

    state = ExecutionState(
        durable_execution_arn="test-arn",
        initial_checkpoint_token="initial-token",  # noqa: S106
        operations={},
        service_client=mock_client,
        plugin_executor=PluginExecutor(plugins=None),
    )

    async def scenario():
        operation_update = OperationUpdate.create_step_start(
            OperationIdentifier("test-op", OperationSubType.STEP, None, "test-step")
        )

        start_time = time.time()
        await state.create_checkpoint(operation_update, is_sync=True)
        elapsed = time.time() - start_time

        assert mock_client.checkpoint.call_count == 1
        assert elapsed >= 0.15
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_state_replay_mode():
    operation1 = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    operation2 = Operation(
        operation_id="op2",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    execution_state = ExecutionState(
        durable_execution_arn="arn:aws:test",
        initial_checkpoint_token="test_token",  # noqa: S106
        operations={"op1": operation1, "op2": operation2},
        service_client=Mock(),
        plugin_executor=PluginExecutor(plugins=None),
        replay_status=ReplayStatus.REPLAY,
    )
    assert execution_state.is_replaying() is True
    execution_state.track_replay(operation_id="op1")
    assert execution_state.is_replaying() is True
    execution_state.track_replay(operation_id="op2")
    assert execution_state.is_replaying() is False


async def test_state_replay_mode_with_timed_out():
    """Test that TIMED_OUT operations are treated as terminal states for replay tracking.

    This test verifies that when an operation has TIMED_OUT status, it is correctly
    recognized as a completed/terminal state, allowing the replay status to transition
    from REPLAY to NEW once all completed operations have been visited.

    Regression test for: https://github.com/zhongkechen/async-durable-execution/issues/262
    """
    operation1 = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.TIMED_OUT,
    )
    operation2 = Operation(
        operation_id="op2",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    execution_state = ExecutionState(
        durable_execution_arn="arn:aws:test",
        initial_checkpoint_token="test_token",  # noqa: S106
        operations={"op1": operation1, "op2": operation2},
        service_client=Mock(),
        plugin_executor=PluginExecutor(plugins=None),
        replay_status=ReplayStatus.REPLAY,
    )
    assert execution_state.is_replaying() is True
    execution_state.track_replay(operation_id="op1")
    assert execution_state.is_replaying() is True
    execution_state.track_replay(operation_id="op2")
    assert execution_state.is_replaying() is False


# Tests for empty checkpoint coalescing (issue #325)


async def test_collect_checkpoint_batch_coalesces_many_empty_checkpoints():
    """Test that many empty checkpoints are collected into a single batch.

    With the coalescing optimization, 999 empty checkpoints should all be collected
    in one batch (effective_operation_count=1), not split across 4 batches of 250.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=10.0,
        max_batch_operations=250,
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Enqueue 999 empty checkpoints (simulates high-concurrency map/parallel resume)
    for _ in range(999):
        state._checkpoint_queue.put(QueuedOperation(None, None))

    # All 999 should be collected in a single batch
    batch = await state._collect_checkpoint_batch()

    assert len(batch) == 999
    assert all(q.operation_update is None for q in batch)
    # Queue should now be empty
    assert state._checkpoint_queue.empty()


async def test_collect_checkpoint_batch_empty_checkpoints_with_real_ops_respects_limit():
    """Test that real operations still respect the max_batch_operations limit
    even when many empty checkpoints are present in the same batch.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=10.0,
        max_batch_operations=5,
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Enqueue 3 empty checkpoints + 10 real operations
    for _ in range(3):
        state._checkpoint_queue.put(QueuedOperation(None, None))
    for i in range(10):
        op = OperationUpdate(
            operation_id=f"op_{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        state._checkpoint_queue.put(QueuedOperation(op, None))

    batch = await state._collect_checkpoint_batch()

    # Empty checkpoints count as 1 effective op, so 4 real ops fit in 5-op limit
    empty_in_batch = sum(1 for q in batch if q.operation_update is None)
    real_in_batch = sum(1 for q in batch if q.operation_update is not None)

    assert empty_in_batch == 3  # All empty checkpoints coalesced
    assert real_in_batch == 4  # 4 real ops (1 slot used by the first empty)


async def test_collect_checkpoint_batch_overflow_coalesces_empty_checkpoints():
    """Test that empty checkpoints in the overflow queue are also coalesced."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=10.0,
        max_batch_operations=250,
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Put 500 empty checkpoints directly into the overflow queue
    for _ in range(500):
        state._overflow_queue.append(QueuedOperation(None, None))

    # All 500 should be collected in a single batch from overflow
    batch = await state._collect_checkpoint_batch()

    assert len(batch) == 500
    assert all(q.operation_update is None for q in batch)
    assert not state._overflow_queue


async def test_checkpoint_batches_forever_single_api_call_for_many_empty_checkpoints():
    """Test that many empty checkpoints result in a single API call, not one per batch.

    This is the core optimization: 999 empty checkpoints should produce exactly 1 API
    call instead of ceil(999/250) = 4 API calls.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )

    # Long time window to ensure all 999 pre-queued items are drained in one batch.
    # The optimization is about the operation COUNT limit, not the time limit.
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=5.0,
        max_batch_operations=250,
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    async def scenario():
        completion_futures = []
        for _ in range(999):
            completion_future = asyncio.get_running_loop().create_future()
            completion_futures.append(completion_future)
            await state._checkpoint_queue.put(QueuedOperation(None, completion_future))

        state.start_checkpointing()
        await asyncio.gather(*completion_futures)

        assert mock_lambda_client.checkpoint.call_count == 1
        call_kwargs = mock_lambda_client.checkpoint.call_args
        assert call_kwargs.kwargs["updates"] == []
        await stop_checkpointing_task(state)

    await run_async(scenario())


async def test_collect_checkpoint_batch_first_empty_counts_toward_limit():
    """Test that only the first empty checkpoint counts toward the batch operation limit.

    With limit=2: an empty op (effective=1) + a real op (effective=2) exactly fills the
    batch. The loop exits after the limit is hit; items after the limit stay in the queue.
    """
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)

    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=10.0,
        max_batch_operations=2,
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    # Queue: 1 empty (effective=1), op_1 (effective=2, hits limit),
    # op_2 moves to overflow and trailing empties remain queued.
    op1 = OperationUpdate(
        operation_id="op_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    op2 = OperationUpdate(
        operation_id="op_2",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(None, None))  # empty — effective=1
    state._checkpoint_queue.put(
        QueuedOperation(op1, None)
    )  # real  — effective=2, limit hit
    state._checkpoint_queue.put(QueuedOperation(op2, None))  # real  - overflows

    for _ in range(50):
        state._checkpoint_queue.put(QueuedOperation(None, None))  # trailing empties

    batch = await state._collect_checkpoint_batch()

    real_in_batch = [q for q in batch if q.operation_update is not None]
    empty_in_batch = [q for q in batch if q.operation_update is None]

    # The batch contains exactly: 1 leading empty + op_1 (limit=2 effective ops)
    assert len(real_in_batch) == 1
    assert real_in_batch[0].operation_update.operation_id == "op_1"
    assert (
        len(empty_in_batch) == 1
    )  # Only the leading empty; trailing deferred to next batch
    # op_2 is preserved for the next batch, ahead of the trailing empties.
    assert len(state._overflow_queue) == 1
    assert state._overflow_queue[0].operation_update.operation_id == "op_2"
    assert state._checkpoint_queue.qsize() == 50


async def test_execution_state_get_execution_operation_no_operations():
    """Test get_execution_operation logs debug and returns None when no operations exist."""
    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=10.0,
        max_batch_operations=2,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    with patch("async_durable_execution.state.logger") as mock_logger:
        result = state.get_execution_operation()

        assert result is None
        mock_logger.debug.assert_called_once_with(
            "No durable operations found in execution state."
        )


async def test_initial_execution_state_get_execution_operation_wrong_type():
    """Test get_execution_operation raises error when first operation is not EXECUTION."""
    operation = Operation(
        operation_id="step1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )

    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=10.0,
        max_batch_operations=2,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn/step1",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={"step1": operation},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    with pytest.raises(
        Exception,
        match="The execution operation in execution state does not have EXECUTION type: OperationType.STEP",
    ):
        state.get_execution_operation()


async def test_initial_execution_state_get_raw_input_payload_none():
    """Test get_raw_input_payload returns None when execution details are missing."""
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=None,
    )

    operation = Operation(
        operation_id="step1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )

    mock_lambda_client = Mock(spec=ThreadedSyncLambdaClient)
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=10.0,
        max_batch_operations=2,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn/exec1",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={"step1": operation},
        service_client=mock_lambda_client,
        plugin_executor=PluginExecutor(plugins=None),
        batcher_config=config,
    )

    result = state.get_raw_input_payload()
    assert result is None


class _RecordingPlugin(DurableInstrumentationPlugin):
    """Plugin that records all hook calls for assertion."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def on_execution_start(self, info):
        self.calls.append("execution_start")

    def on_execution_end(self, info):
        self.calls.append("execution_end")

    async def on_invocation_start(self, info):
        self.calls.append("invocation_start")

    async def on_invocation_end(self, info):
        self.calls.append("invocation_end")

    async def on_operation_start(self, info):
        self.calls.append(f"operation_start:{info.operation_id}")

    async def on_operation_end(self, info):
        self.calls.append(f"operation_end:{info.operation_id}")

    async def on_user_function_start(self, info):
        self.calls.append(f"user_function_start:{info.operation_id}")

    async def on_user_function_end(self, info):
        self.calls.append(f"user_function_end:{info.operation_id}")


async def test_execution_state_accepts_plugin_executor_parameter():
    """Test that ExecutionState can be created with a plugin_executor parameter."""
    mock_client = Mock(spec=ThreadedSyncLambdaClient)
    plugin = _RecordingPlugin()
    plugin_executor = PluginExecutor(plugins=[plugin])

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_client,
        plugin_executor=plugin_executor,
    )

    assert state._plugin_executor is plugin_executor


async def test_plugin_executor_on_operation_action_called_on_checkpoint():
    """Test that plugin_executor.on_operation_action is called for each update after checkpoint."""
    mock_client = create_autospec(ThreadedSyncLambdaClient)

    # Return a succeeded step operation from checkpoint
    step_op = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(attempt=1, result='"done"'),
    )
    mock_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[step_op],
            next_marker=None,
        ),
    )

    plugin = _RecordingPlugin()
    plugin_executor = PluginExecutor(plugins=[plugin])
    with plugin_executor.run():
        state = ExecutionState(
            durable_execution_arn="test_arn",
            initial_checkpoint_token="token123",  # noqa: S106
            operations={},
            service_client=mock_client,
            plugin_executor=plugin_executor,
        )

        async def scenario():
            operation_update = OperationUpdate(
                operation_id="step-1",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
                name="my-step",
            )
            await state.create_checkpoint(operation_update, is_sync=True)
            await stop_checkpointing_task(state)

        await run_async(scenario())

    # on_operation_action is called for START updates
    assert "operation_start:step-1" in plugin.calls


async def test_plugin_executor_on_operation_update_called_for_terminal_operations():
    """Test that plugin_executor.on_operation_update is called for terminal operations."""
    mock_client = create_autospec(ThreadedSyncLambdaClient)

    # Return a succeeded step operation from checkpoint
    step_op = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(attempt=1, result='"done"'),
    )
    mock_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[step_op],
            next_marker=None,
        ),
    )

    plugin = _RecordingPlugin()
    plugin_executor = PluginExecutor(plugins=[plugin])
    with plugin_executor.run():
        state = ExecutionState(
            durable_execution_arn="test_arn",
            initial_checkpoint_token="token123",  # noqa: S106
            operations={},
            service_client=mock_client,
            plugin_executor=plugin_executor,
        )

        async def scenario():
            operation_update = OperationUpdate(
                operation_id="step-1",
                operation_type=OperationType.STEP,
                action=OperationAction.SUCCEED,
                name="my-step",
                payload='"done"',
            )
            await state.create_checkpoint(operation_update, is_sync=True)
            await stop_checkpointing_task(state)

        await run_async(scenario())

    assert "operation_end:step-1" in plugin.calls


async def test_plugin_executor_not_called_for_non_terminal_operations():
    """Test that plugin_executor.on_operation_update does not fire for non-terminal operations."""
    mock_client = create_autospec(spec=ThreadedSyncLambdaClient)

    # Return a STARTED step operation from checkpoint
    step_op = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=None,
    )
    mock_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[step_op],
            next_marker=None,
        ),
    )

    plugin = _RecordingPlugin()
    plugin_executor = PluginExecutor(plugins=[plugin])
    with plugin_executor.run():
        state = ExecutionState(
            durable_execution_arn="test_arn",
            initial_checkpoint_token="token123",  # noqa: S106
            operations={},
            service_client=mock_client,
            plugin_executor=plugin_executor,
        )

        async def scenario():
            operation_update = OperationUpdate(
                operation_id="step-1",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
                name="my-step",
            )
            await state.create_checkpoint(operation_update, is_sync=True)
            await stop_checkpointing_task(state)

        await run_async(scenario())

    # on_operation_action fires for START
    assert "operation_start:step-1" in plugin.calls
    # But on_operation_update should NOT fire operation_end for STARTED status
    operation_end_calls = [c for c in plugin.calls if c.startswith("operation_end")]
    assert len(operation_end_calls) == 0


async def test_plugin_executor_called_for_multiple_updates_in_batch():
    """Test that plugin_executor is called for each update in a batch."""
    mock_client = create_autospec(spec=ThreadedSyncLambdaClient)

    # Return multiple operations from checkpoint
    step_op1 = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(attempt=1, result='"result1"'),
    )
    step_op2 = Operation(
        operation_id="step-2",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(attempt=1, result='"result2"'),
    )
    mock_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[step_op1, step_op2],
            next_marker=None,
        ),
    )

    plugin = _RecordingPlugin()
    plugin_executor = PluginExecutor(plugins=[plugin])
    with plugin_executor.run():
        config = CheckpointBatcherConfig(
            max_batch_time_seconds=0.2,
            max_batch_operations=10,
        )

        state = ExecutionState(
            durable_execution_arn="test_arn",
            initial_checkpoint_token="token123",  # noqa: S106
            operations={},
            service_client=mock_client,
            plugin_executor=plugin_executor,
            batcher_config=config,
        )

        async def scenario():
            op1 = OperationUpdate(
                operation_id="step-1",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
                name="step-1",
            )
            op2 = OperationUpdate(
                operation_id="step-2",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
                name="step-2",
            )
            await state.create_checkpoint(op1, is_sync=False)
            await state.create_checkpoint(op2, is_sync=True)
            await stop_checkpointing_task(state)

        await run_async(scenario())

    # Both operations should have triggered on_operation_action
    assert "operation_start:step-1" in plugin.calls
    assert "operation_start:step-2" in plugin.calls
    # Both terminal operations should have triggered on_operation_update
    assert "operation_end:step-1" in plugin.calls
    assert "operation_end:step-2" in plugin.calls


async def test_plugin_executor_not_called_on_checkpoint_failure():
    """Test that plugin_executor is NOT called when checkpoint API fails."""
    mock_client = create_autospec(spec=ThreadedSyncLambdaClient)
    mock_client.checkpoint.side_effect = RuntimeError("API error")

    plugin = _RecordingPlugin()
    plugin_executor = PluginExecutor(plugins=[plugin])
    with plugin_executor.run():
        state = ExecutionState(
            durable_execution_arn="test_arn",
            initial_checkpoint_token="token123",  # noqa: S106
            operations={},
            service_client=mock_client,
            plugin_executor=plugin_executor,
        )

        async def scenario():
            operation_update = OperationUpdate(
                operation_id="step-1",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
                name="my-step",
            )

            with pytest.raises(RuntimeError, match="API error"):
                await state.create_checkpoint(operation_update, is_sync=True)
            await stop_checkpointing_task(state)

        await run_async(scenario())

    # Plugin should NOT have been called since checkpoint failed
    assert "operation_start:step-1" not in plugin.calls
    assert "operation_end:step-1" not in plugin.calls


async def test_plugin_executor_exception_does_not_break_checkpointing():
    """Test that a plugin exception does not break the checkpoint processing loop."""
    mock_client = create_autospec(spec=ThreadedSyncLambdaClient)

    step_op = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(attempt=1, result='"done"'),
    )
    mock_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[step_op],
            next_marker=None,
        ),
    )

    class _ExplodingPlugin(DurableInstrumentationPlugin):
        async def on_operation_start(self, info):
            raise RuntimeError("plugin exploded")

        async def on_operation_end(self, info):
            raise RuntimeError("plugin exploded")

    exploding_plugin = _ExplodingPlugin()
    plugin_executor = PluginExecutor(plugins=[exploding_plugin])
    with plugin_executor.run():
        state = ExecutionState(
            durable_execution_arn="test_arn",
            initial_checkpoint_token="token123",  # noqa: S106
            operations={},
            service_client=mock_client,
            plugin_executor=plugin_executor,
        )

        async def scenario():
            operation_update = OperationUpdate(
                operation_id="step-1",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
                name="my-step",
            )
            await state.create_checkpoint(operation_update, is_sync=True)

            assert mock_client.checkpoint.call_count == 1
            await stop_checkpointing_task(state)

        await run_async(scenario())


async def test_plugin_executor_not_called_for_pending_operations():
    """Test that plugin_executor.on_operation_update fires on_user_function_end for PENDING operations."""
    mock_client = create_autospec(spec=ThreadedSyncLambdaClient)

    # Return a PENDING step operation from checkpoint (simulates a retry scenario)
    step_op = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(
            attempt=1,
            result=None,
            error=ErrorObject(
                message="transient failure",
                type="RetryableError",
                data=None,
                stack_trace=None,
            ),
        ),
    )
    mock_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[step_op],
            next_marker=None,
        ),
    )

    plugin = _RecordingPlugin()
    plugin_executor = PluginExecutor(plugins=[plugin])
    with plugin_executor.run():
        state = ExecutionState(
            durable_execution_arn="test_arn",
            initial_checkpoint_token="token123",  # noqa: S106
            operations={},
            service_client=mock_client,
            plugin_executor=plugin_executor,
        )

        async def scenario():
            operation_update = OperationUpdate(
                operation_id="step-1",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
                name="my-step",
            )
            await state.create_checkpoint(operation_update, is_sync=True)
            await stop_checkpointing_task(state)

        await run_async(scenario())

    # operation_end should NOT fire for PENDING (only for terminal statuses)
    operation_end_calls = [c for c in plugin.calls if c.startswith("operation_end")]
    assert len(operation_end_calls) == 0
