"""Unit tests for InMemoryServiceClient."""

from unittest.mock import Mock, patch

from async_durable_execution.models import (
    CheckpointOutput,
    OperationAction,
    OperationType,
    OperationUpdate,
    StateOutput,
)
from async_durable_execution.runner.local import InMemoryServiceClient
from async_durable_execution.runner.local.model import CheckpointToken


async def test_checkpoint():
    """Test checkpoint method delegates to the sync checkpoint handler."""
    scheduler = Mock()
    expected_output = CheckpointOutput(
        checkpoint_token="new-token",  # noqa: S106
        new_execution_state=Mock(),
    )

    client = InMemoryServiceClient(scheduler)
    client.process_checkpoint = Mock(return_value=expected_output)

    updates = [
        OperationUpdate(
            operation_id="test-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
    ]

    result = await client.checkpoint(
        "arn:aws:lambda:us-east-1:123456789012:function:test",
        "token",
        updates,
        "client-token",
    )

    assert result == expected_output
    client.process_checkpoint.assert_called_once_with("token", updates, "client-token")


async def test_get_execution_state():
    """Test get_execution_state returns navigable operations."""
    scheduler = Mock()
    execution = Mock()
    execution.get_navigable_operations.return_value = []
    executor = Mock()
    executor.get_execution.return_value = execution
    client = InMemoryServiceClient(scheduler)
    client.bind_executor(executor)

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_from_str.return_value = mock_token

        result = await client.get_execution_state(
            "arn:aws:lambda:us-east-1:123456789012:function:test",
            "token",
            "marker",
            500,
        )

    assert result == StateOutput(operations=[], next_marker=None)
    executor.get_execution.assert_called_once_with("arn:test")


async def test_get_execution_state_default_max_items():
    """Test get_execution_state with default max_items."""
    scheduler = Mock()
    execution = Mock()
    execution.get_navigable_operations.return_value = []
    executor = Mock()
    executor.get_execution.return_value = execution

    client = InMemoryServiceClient(scheduler)
    client.bind_executor(executor)

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_from_str.return_value = mock_token

        result = await client.get_execution_state(
            "arn:aws:lambda:us-east-1:123456789012:function:test",
            "token",
            "marker",
        )

    assert result == StateOutput(operations=[], next_marker=None)
