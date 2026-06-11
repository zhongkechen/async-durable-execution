"""Unit tests for CheckpointProcessor."""

from unittest.mock import Mock, patch

import pytest
from async_durable_execution.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    OperationAction,
    OperationType,
    OperationUpdate,
    StateOutput,
)

from async_durable_execution_runner.checkpoint.processor import (
    CheckpointProcessor,
)
from async_durable_execution_runner.exceptions import (
    InvalidParameterValueException,
)
from async_durable_execution_runner.execution import Execution
from async_durable_execution_runner.scheduler import Scheduler
from async_durable_execution_runner.stores.base import ExecutionStore
from async_durable_execution_runner.token import CheckpointToken


def test_init():
    """Test CheckpointProcessor initialization."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)

    processor = CheckpointProcessor(store, scheduler)

    # Test that processor was created successfully by calling a public method
    # This indirectly verifies that internal components were initialized
    assert processor is not None

    # Test that we can add observers (verifies notifier is initialized)
    observer = Mock()
    processor.add_execution_observer(observer)  # Should not raise an exception


@patch("async_durable_execution_runner.checkpoint.processor.ExecutionNotifier")
def test_add_execution_observer(mock_notifier_class):
    """Test adding execution observer."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    mock_notifier_instance = Mock()
    mock_notifier_class.return_value = mock_notifier_instance

    processor = CheckpointProcessor(store, scheduler)
    observer = Mock()

    processor.add_execution_observer(observer)

    # Verify observer was added through the notifier's public method
    mock_notifier_instance.add_observer.assert_called_once_with(observer)


@patch("async_durable_execution_runner.checkpoint.processor.CheckpointValidator")
@patch("async_durable_execution_runner.checkpoint.processor.OperationTransformer")
def test_process_checkpoint_success(mock_transformer_class, mock_validator):
    """Test successful checkpoint processing."""
    # Setup mocks
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    mock_transformer_instance = Mock()
    mock_transformer_class.return_value = mock_transformer_instance

    processor = CheckpointProcessor(store, scheduler)

    # Mock execution
    execution = Mock(spec=Execution)
    execution.is_complete = False
    execution.token_sequence = 1
    execution.operations = []
    execution.updates = []
    execution.get_new_checkpoint_token.return_value = "new-token"
    execution.get_navigable_operations.return_value = []

    store.load.return_value = execution

    # Mock transformer
    mock_transformer_instance.process_updates.return_value = ([], [])

    # Test data
    checkpoint_token = "test-token"  # noqa: S105
    updates = [
        OperationUpdate(
            operation_id="test-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
    ]

    # Mock token parsing
    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_token.token_sequence = 1
        mock_from_str.return_value = mock_token

        result = processor.process_checkpoint(checkpoint_token, updates, "client-token")

    # Verify calls
    store.load.assert_called_once_with("arn:test")
    mock_validator.validate_input.assert_called_once_with(updates, execution)
    mock_transformer_instance.process_updates.assert_called_once()
    store.update.assert_called_once_with(execution)

    # Verify result
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new-token"  # noqa: S105
    assert isinstance(result.new_execution_state, CheckpointUpdatedExecutionState)


@patch("async_durable_execution_runner.checkpoint.processor.CheckpointValidator")
def test_process_checkpoint_invalid_token_complete_execution(mock_validator):
    """Test checkpoint processing with complete execution."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution as complete
    execution = Mock(spec=Execution)
    execution.is_complete = True
    execution.token_sequence = 1

    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105
    updates = []

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_token.token_sequence = 1
        mock_from_str.return_value = mock_token

        with pytest.raises(
            InvalidParameterValueException, match="Invalid checkpoint token"
        ):
            processor.process_checkpoint(checkpoint_token, updates, "client-token")


@patch("async_durable_execution_runner.checkpoint.processor.CheckpointValidator")
def test_process_checkpoint_invalid_token_sequence(mock_validator):
    """Test checkpoint processing with invalid token sequence."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution with different token sequence
    execution = Mock(spec=Execution)
    execution.is_complete = False
    execution.token_sequence = 2

    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105
    updates = []

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_token.token_sequence = 1  # Different from execution
        mock_from_str.return_value = mock_token

        with pytest.raises(
            InvalidParameterValueException, match="Invalid checkpoint token"
        ):
            processor.process_checkpoint(checkpoint_token, updates, "client-token")


@patch("async_durable_execution_runner.checkpoint.processor.CheckpointValidator")
@patch("async_durable_execution_runner.checkpoint.processor.OperationTransformer")
def test_process_checkpoint_updates_execution_state(
    mock_transformer_class, mock_validator
):
    """Test that checkpoint processing updates execution state correctly."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    mock_transformer_instance = Mock()
    mock_transformer_class.return_value = mock_transformer_instance

    processor = CheckpointProcessor(store, scheduler)

    # Mock execution
    execution = Mock(spec=Execution)
    execution.is_complete = False
    execution.token_sequence = 1
    execution.operations = []
    execution.updates = []
    execution.get_new_checkpoint_token.return_value = "new-token"
    execution.get_navigable_operations.return_value = []

    store.load.return_value = execution

    # Mock transformer to return updated operations and updates
    updated_operations = [Mock()]
    all_updates = [Mock()]
    mock_transformer_instance.process_updates.return_value = (
        updated_operations,
        all_updates,
    )

    checkpoint_token = "test-token"  # noqa: S105
    updates = [
        OperationUpdate(
            operation_id="test-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
    ]

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_token.token_sequence = 1
        mock_from_str.return_value = mock_token

        processor.process_checkpoint(checkpoint_token, updates, "client-token")

    # Verify execution state was updated
    assert execution.operations == updated_operations
    # Check that updates were extended (execution.updates is a real list)
    assert len(execution.updates) == len(all_updates)


def test_get_execution_state():
    """Test getting execution state."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution
    execution = Mock(spec=Execution)
    navigable_ops = [Mock()]
    execution.get_navigable_operations.return_value = navigable_ops

    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_from_str.return_value = mock_token

        result = processor.get_execution_state(checkpoint_token, "next-marker", 500)

    # Verify calls
    store.load.assert_called_once_with("arn:test")
    execution.get_navigable_operations.assert_called_once()

    # Verify result
    assert isinstance(result, StateOutput)
    assert result.operations == navigable_ops
    assert result.next_marker is None


def test_get_execution_state_default_max_items():
    """Test getting execution state with default max_items."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    execution = Mock(spec=Execution)
    execution.get_navigable_operations.return_value = []
    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_from_str.return_value = mock_token

        result = processor.get_execution_state(checkpoint_token, "next-marker")

    assert isinstance(result, StateOutput)
