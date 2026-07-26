"""Unit tests for wait handler."""

import asyncio
import inspect
from unittest.mock import Mock

import pytest

from async_durable_execution.core.exceptions import SuspendExecution
from async_durable_execution.core.models import OperationIdentifier
from async_durable_execution.core.models import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
    WaitOptions,
)
from async_durable_execution.primitive.wait import WaitOperationExecutor, wait
from async_durable_execution.core.state import ExecutionState


async def run_async(awaitable):
    return await awaitable


# Test helper function - maintains old handler signature for backward compatibility
async def wait_handler(seconds: int, state, operation_identifier) -> None:
    """Test helper that wraps WaitOperationExecutor with old handler signature."""
    executor = WaitOperationExecutor(
        seconds=seconds,
        state=state,
        operation_identifier=operation_identifier,
    )
    return await run_async(executor.process())


def test_wait_name_is_keyword_only():
    """wait operation name must be passed as a keyword."""
    parameters = inspect.signature(wait).parameters

    assert parameters["duration"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY


async def test_wait_handler_already_completed():
    """Test wait_handler when operation is already completed."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="wait1",
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    await wait_handler(
        seconds=10,
        state=mock_state,
        operation_identifier=OperationIdentifier("wait1", OperationSubType.WAIT, None),
    )

    mock_state.operations.get.assert_called_once_with("wait1")
    mock_state.create_checkpoint.assert_not_called()


async def test_wait_handler_not_completed():
    """Test wait_handler when operation is not completed."""
    mock_state = Mock(spec=ExecutionState)

    mock_state.operations.get.return_value = None

    with pytest.raises(SuspendExecution, match="Wait for 30 seconds"):
        await wait_handler(
            seconds=30,
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "wait2", OperationSubType.WAIT, None
            ),
        )

    mock_state.operations.get.assert_called_once_with("wait2")

    expected_operation = OperationUpdate(
        operation_id="wait2",
        parent_id=None,
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        sub_type=OperationSubType.WAIT,
        wait_options=WaitOptions(wait_seconds=30),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation, is_sync=True
    )


async def test_wait_handler_with_none_name():
    """Test wait_handler with None name."""
    mock_state = Mock(spec=ExecutionState)

    mock_state.operations.get.return_value = None

    with pytest.raises(SuspendExecution, match="Wait for 5 seconds"):
        await wait_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "wait3", OperationSubType.WAIT, None
            ),
            seconds=5,
        )

    mock_state.operations.get.assert_called_once_with("wait3")

    expected_operation = OperationUpdate(
        operation_id="wait3",
        parent_id=None,
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        sub_type=OperationSubType.WAIT,
        wait_options=WaitOptions(wait_seconds=5),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation, is_sync=True
    )


async def test_wait_handler_with_existent():
    """Test wait_handler with existent operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.operations.get.return_value = Operation(
        operation_id="wait4",
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )

    with pytest.raises(SuspendExecution, match="Wait for 5 seconds"):
        await wait_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "wait4", OperationSubType.WAIT, None
            ),
            seconds=5,
        )

    mock_state.operations.get.assert_called_once_with("wait4")
    mock_state.create_checkpoint.assert_not_called()


# Start/replay handling tests


async def test_wait_starts_without_second_status_evaluation():
    """Test that start creates the checkpoint and suspends without re-reading state."""
    # Arrange
    mock_state = Mock(spec=ExecutionState)

    mock_state.operations.get.return_value = None

    executor = WaitOperationExecutor(
        seconds=30,
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wait_eval", OperationSubType.WAIT, None, "test_wait"
        ),
    )

    # Act
    with pytest.raises(SuspendExecution):
        await run_async(executor.process())

    mock_state.operations.get.assert_called_once_with("wait_eval")

    # Verify checkpoint created with is_sync=True
    expected_operation = OperationUpdate(
        operation_id="wait_eval",
        parent_id=None,
        name="test_wait",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        sub_type=OperationSubType.WAIT,
        wait_options=WaitOptions(wait_seconds=30),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation, is_sync=True
    )


async def test_wait_new_operation_suspends_after_checkpoint_creation():
    """Test that a new wait suspends immediately after checkpoint creation."""
    # Arrange
    mock_state = Mock(spec=ExecutionState)

    # First call: checkpoint doesn't exist
    not_found_result = None

    mock_state.operations.get.return_value = not_found_result

    executor = WaitOperationExecutor(
        seconds=5,
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wait_immediate", OperationSubType.WAIT, None, "immediate_wait"
        ),
    )

    with pytest.raises(SuspendExecution, match="Wait for 5 seconds"):
        await run_async(executor.process())

    # Verify checkpoint was created
    assert mock_state.create_checkpoint.call_count == 1

    mock_state.operations.get.assert_called_once_with("wait_immediate")


async def test_wait_no_immediate_response_suspends():
    """Test that wait suspends after creating a checkpoint."""
    # Arrange
    mock_state = Mock(spec=ExecutionState)

    mock_state.operations.get.return_value = None

    executor = WaitOperationExecutor(
        seconds=60,
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wait_suspend", OperationSubType.WAIT, None
        ),
    )

    # Act & Assert - verify suspend occurs
    with pytest.raises(SuspendExecution) as exc_info:
        await run_async(executor.process())

    # Verify suspend message
    assert "Wait for 60 seconds" in str(exc_info.value)

    # Verify checkpoint was created
    assert mock_state.create_checkpoint.call_count == 1

    mock_state.operations.get.assert_called_once_with("wait_suspend")


async def test_wait_already_completed_no_checkpoint():
    """Test that already completed wait doesn't create checkpoint.

    When replaying and the wait is already completed, it should return
    immediately without creating a new checkpoint.
    """
    # Arrange
    mock_state = Mock(spec=ExecutionState)

    # Checkpoint already exists and succeeded
    succeeded_operation = Operation(
        operation_id="wait_replay",
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
    )
    succeeded_result = succeeded_operation

    mock_state.operations.get.return_value = succeeded_result

    executor = WaitOperationExecutor(
        seconds=10,
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wait_replay", OperationSubType.WAIT, None, "completed_wait"
        ),
    )

    # Act
    result = await run_async(executor.process())

    # Assert - verify immediate return without checkpoint
    assert result is None

    # Verify no checkpoint created
    mock_state.create_checkpoint.assert_not_called()

    # Verify status checked only once
    mock_state.operations.get.assert_called_once_with("wait_replay")


async def test_wait_with_various_durations():
    """Test wait operations with different durations suspend with the right delay."""
    for seconds in [1, 30, 300, 3600]:
        # Arrange
        mock_state = Mock(spec=ExecutionState)

        # First call: checkpoint doesn't exist
        not_found_result = None

        mock_state.operations.get.return_value = not_found_result

        executor = WaitOperationExecutor(
            seconds=seconds,
            state=mock_state,
            operation_identifier=OperationIdentifier(
                f"wait_duration_{seconds}", OperationSubType.WAIT, None
            ),
        )

        # Act
        with pytest.raises(SuspendExecution, match=f"Wait for {seconds} seconds"):
            await run_async(executor.process())

        # Assert
        mock_state.operations.get.assert_called_once_with(f"wait_duration_{seconds}")

        # Verify correct wait duration in checkpoint
        call_args = mock_state.create_checkpoint.call_args
        assert call_args[1]["operation_update"].wait_options.wait_seconds == seconds


async def test_wait_suspends_without_second_check():
    """Test that a new wait suspends without checking the created operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_state.operations.get.return_value = None

    executor = WaitOperationExecutor(
        seconds=5,
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wait-1", OperationSubType.WAIT, None, "test_wait"
        ),
    )

    with pytest.raises(SuspendExecution):
        await run_async(executor.process())

    mock_state.operations.get.assert_called_once_with("wait-1")
    mock_state.create_checkpoint.assert_called_once()


async def test_wait_suspends_without_second_check_duplicate():
    """Test that a new wait suspends without checking the created operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_state.operations.get.return_value = None

    executor = WaitOperationExecutor(
        seconds=5,
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wait-1", OperationSubType.WAIT, None, "test_wait"
        ),
    )

    with pytest.raises(SuspendExecution):
        await run_async(executor.process())

    mock_state.operations.get.assert_called_once_with("wait-1")
    mock_state.create_checkpoint.assert_called_once()
