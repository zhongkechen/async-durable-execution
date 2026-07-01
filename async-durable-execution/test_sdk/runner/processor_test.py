"""Unit tests for checkpoint processing helpers."""

from unittest.mock import Mock, patch

import pytest

from async_durable_execution.models import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    OperationAction,
    OperationType,
    OperationUpdate,
    StateOutput,
)
from async_durable_execution.runner.local import (
    InMemoryExecutionStore,
    InMemoryServiceClient,
)
from async_durable_execution.runner.exceptions import (
    InvalidParameterValueException,
)
from async_durable_execution.runner.execution import Execution
from async_durable_execution.runner.scheduler import Scheduler


def test_init():
    """Test InMemoryServiceClient checkpoint processing initialization."""
    store = Mock(spec=InMemoryExecutionStore)
    scheduler = Mock(spec=Scheduler)

    client = InMemoryServiceClient(store, scheduler)

    # Test that client was created successfully by calling a public method
    # This indirectly verifies that internal components were initialized
    assert client is not None

    # Test that we can add observers (verifies notifier is initialized)
    observer = Mock()
    client.add_execution_observer(observer)  # Should not raise an exception


@patch("async_durable_execution.runner.local.ExecutionNotifier")
def test_add_execution_observer(mock_notifier_class):
    """Test adding execution observer."""
    store = Mock(spec=InMemoryExecutionStore)
    scheduler = Mock(spec=Scheduler)
    mock_notifier_instance = Mock()
    mock_notifier_class.return_value = mock_notifier_instance

    client = InMemoryServiceClient(store, scheduler)
    observer = Mock()

    client.add_execution_observer(observer)

    # Verify observer was added through the notifier's public method
    mock_notifier_instance.add_observer.assert_called_once_with(observer)


@patch("async_durable_execution.runner.local.CheckpointValidator")
@patch("async_durable_execution.runner.local.OperationTransformer")
def test_process_checkpoint_success(mock_transformer_class, mock_validator):
    """Test successful checkpoint processing."""
    # Setup mocks
    store = Mock(spec=InMemoryExecutionStore)
    scheduler = Mock(spec=Scheduler)
    mock_transformer_instance = Mock()
    mock_transformer_class.return_value = mock_transformer_instance

    client = InMemoryServiceClient(store, scheduler)

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

        result = client.process_checkpoint(checkpoint_token, updates, "client-token")

    # Verify calls
    store.load.assert_called_once_with("arn:test")
    mock_validator.validate_input.assert_called_once_with(
        updates,
        execution,
        processors=mock_transformer_instance.processors,
    )
    mock_transformer_instance.process_updates.assert_called_once()
    store.update.assert_called_once_with(execution)

    # Verify result
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new-token"  # noqa: S105
    assert isinstance(result.new_execution_state, CheckpointUpdatedExecutionState)


@patch("async_durable_execution.runner.local.CheckpointValidator")
def test_process_checkpoint_invalid_token_complete_execution(mock_validator):
    """Test checkpoint processing with complete execution."""
    store = Mock(spec=InMemoryExecutionStore)
    scheduler = Mock(spec=Scheduler)
    client = InMemoryServiceClient(store, scheduler)

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
            client.process_checkpoint(checkpoint_token, updates, "client-token")


@patch("async_durable_execution.runner.local.CheckpointValidator")
def test_process_checkpoint_invalid_token_sequence(mock_validator):
    """Test checkpoint processing with invalid token sequence."""
    store = Mock(spec=InMemoryExecutionStore)
    scheduler = Mock(spec=Scheduler)
    client = InMemoryServiceClient(store, scheduler)

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
            client.process_checkpoint(checkpoint_token, updates, "client-token")


@patch("async_durable_execution.runner.local.CheckpointValidator")
@patch("async_durable_execution.runner.local.OperationTransformer")
def test_process_checkpoint_updates_execution_state(
    mock_transformer_class, mock_validator
):
    """Test that checkpoint processing updates execution state correctly."""
    store = Mock(spec=InMemoryExecutionStore)
    scheduler = Mock(spec=Scheduler)
    mock_transformer_instance = Mock()
    mock_transformer_class.return_value = mock_transformer_instance

    client = InMemoryServiceClient(store, scheduler)

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

        client.process_checkpoint(checkpoint_token, updates, "client-token")

    # Verify execution state was updated
    assert execution.operations == updated_operations
    # Check that updates were extended (execution.updates is a real list)
    assert len(execution.updates) == len(all_updates)


async def test_get_execution_state():
    """Test getting execution state."""
    store = Mock(spec=InMemoryExecutionStore)
    scheduler = Mock(spec=Scheduler)
    client = InMemoryServiceClient(store, scheduler)

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

        result = await client.get_execution_state(
            "arn:ignored", checkpoint_token, "next-marker", 500
        )

    # Verify calls
    store.load.assert_called_once_with("arn:test")
    execution.get_navigable_operations.assert_called_once()

    # Verify result
    assert isinstance(result, StateOutput)
    assert result.operations == navigable_ops
    assert result.next_marker is None


async def test_get_execution_state_default_max_items():
    """Test getting execution state with default max_items."""
    store = Mock(spec=InMemoryExecutionStore)
    scheduler = Mock(spec=Scheduler)
    client = InMemoryServiceClient(store, scheduler)

    execution = Mock(spec=Execution)
    execution.get_navigable_operations.return_value = []
    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_from_str.return_value = mock_token

        result = await client.get_execution_state(
            "arn:ignored", checkpoint_token, "next-marker"
        )

    assert isinstance(result, StateOutput)


# CheckpointValidator tests

"""Unit tests for checkpoint validator."""

import json

import pytest

from async_durable_execution.models import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.runner.processor import (
    MAX_ERROR_PAYLOAD_SIZE_BYTES,
    CheckpointValidator,
)
from async_durable_execution.runner.exceptions import (
    InvalidParameterValueException,
)
from async_durable_execution.runner.execution import Execution
from async_durable_execution.runner.model import (
    StartDurableExecutionInput,
    CheckpointToken,
)


def _create_test_execution() -> Execution:
    """Create a test execution with basic setup."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=900,
        execution_retention_period_days=7,
        input=json.dumps({"test": "data"}),
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(start_input)
    execution.start()
    return execution


def test_validate_input_empty_updates():
    """Test validation with empty updates list."""
    execution = _create_test_execution()
    CheckpointValidator.validate_input([], execution)


def test_validate_input_single_valid_update():
    """Test validation with single valid update."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="test-step-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_conflicting_execution_update_multiple():
    """Test validation fails with multiple execution updates."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="exec-1",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
        ),
        OperationUpdate(
            operation_id="exec-2",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.FAIL,
        ),
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot checkpoint multiple EXECUTION updates",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_conflicting_execution_update_not_last():
    """Test validation fails when execution update is not last."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="exec-1",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
        ),
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match="EXECUTION checkpoint must be the last update",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_execution_update_as_last():
    """Test validation passes when execution update is last."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="exec-1",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
        ),
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_payload_sizes_error_too_large():
    """Test validation fails when error payload is too large."""
    execution = _create_test_execution()

    large_message = "x" * (MAX_ERROR_PAYLOAD_SIZE_BYTES + 1)
    large_error = ErrorObject(
        message=large_message, type="TestError", data=None, stack_trace=None
    )

    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.FAIL,
            error=large_error,
        )
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match=f"Error object size must be less than {MAX_ERROR_PAYLOAD_SIZE_BYTES} bytes",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_payload_sizes_error_within_limit():
    """Test validation passes when error payload is within limit."""
    execution = _create_test_execution()

    small_error = ErrorObject(
        message="Small error", type="TestError", data=None, stack_trace=None
    )
    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.FAIL,
            error=small_error,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_duplicate_operation_ids():
    """Test validation allows duplicate operation IDs in same batch.

    With background batching, the SDK can send multiple updates for the same
    operation in a single batch (e.g., START followed by SUCCEED). This is
    valid behavior and should be allowed.
    """
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="duplicate-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="duplicate-id",
            operation_type=OperationType.STEP,
            action=OperationAction.SUCCEED,
        ),
    ]

    # Should not raise - duplicate operation IDs are allowed in batches
    CheckpointValidator.validate_input(updates, execution)


def test_validate_valid_parent_id_in_execution():
    """Test validation passes with valid parent ID from execution."""
    execution = _create_test_execution()

    context_op = Operation(
        operation_id="context-1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(context_op)

    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            parent_id="context-1",
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_valid_parent_id_in_updates():
    """Test validation passes with valid parent ID from updates."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="context-1",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            parent_id="context-1",
        ),
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_invalid_parent_id_wrong_type():
    """Test validation fails with parent ID of wrong operation type."""
    execution = _create_test_execution()

    step_op = Operation(
        operation_id="step-parent",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(step_op)

    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            parent_id="step-parent",
        )
    ]

    with pytest.raises(
        InvalidParameterValueException, match="Invalid parent operation id"
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_invalid_parent_id_not_found():
    """Test validation fails with parent ID that doesn't exist."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            parent_id="non-existent-parent",
        )
    ]

    with pytest.raises(
        InvalidParameterValueException, match="Invalid parent operation id"
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_no_parent_id():
    """Test validation passes with no parent ID."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            parent_id=None,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_step():
    """Test validation calls step validator for STEP operations."""
    execution = _create_test_execution()

    step_op = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.READY,
    )
    execution.operations.append(step_op)

    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_context():
    """Test validation calls context validator for CONTEXT operations."""
    execution = _create_test_execution()

    context_op = Operation(
        operation_id="context-1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(context_op)

    updates = [
        OperationUpdate(
            operation_id="context-1",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.SUCCEED,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_wait():
    """Test validation calls wait validator for WAIT operations."""
    execution = _create_test_execution()

    wait_op = Operation(
        operation_id="wait-1",
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(wait_op)

    updates = [
        OperationUpdate(
            operation_id="wait-1",
            operation_type=OperationType.WAIT,
            action=OperationAction.CANCEL,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_invoke():
    """Test validation calls invoke validator for INVOKE operations."""
    execution = _create_test_execution()

    invoke_op = Operation(
        operation_id="invoke-1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(invoke_op)

    updates = [
        OperationUpdate(
            operation_id="invoke-1",
            operation_type=OperationType.CHAINED_INVOKE,
            action=OperationAction.CANCEL,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_execution():
    """Test validation calls execution validator for EXECUTION operations."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="exec-1",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_inconsistent_operation_type():
    """Test validation fails when operation type is inconsistent."""
    execution = _create_test_execution()

    # Add existing operation
    step_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(step_op)

    # Try to update with different type
    updates = [
        OperationUpdate(
            operation_id="op-1",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.SUCCEED,
        )
    ]

    with pytest.raises(
        InvalidParameterValueException, match="Inconsistent operation type"
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_inconsistent_operation_subtype():
    """Test validation fails when operation subtype is inconsistent."""
    execution = _create_test_execution()

    # Add existing operation with subtype
    from async_durable_execution.models import OperationSubType

    context_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
        sub_type=OperationSubType.PARALLEL,
    )
    execution.operations.append(context_op)

    # Try to update with different subtype
    updates = [
        OperationUpdate(
            operation_id="op-1",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.SUCCEED,
            sub_type=OperationSubType.MAP,
        )
    ]

    with pytest.raises(
        InvalidParameterValueException, match="Inconsistent operation subtype"
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_inconsistent_operation_name():
    """Test validation fails when operation name is inconsistent."""
    execution = _create_test_execution()

    # Add existing operation with name
    step_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        name="original_name",
    )
    execution.operations.append(step_op)

    # Try to update with different name
    updates = [
        OperationUpdate(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            action=OperationAction.SUCCEED,
            name="different_name",
        )
    ]

    with pytest.raises(
        InvalidParameterValueException, match="Inconsistent operation name"
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_inconsistent_parent_operation_id():
    """Test validation fails when parent operation ID is inconsistent."""
    execution = _create_test_execution()

    # Add TWO context operations
    context_op1 = Operation(
        operation_id="context-1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(context_op1)

    context_op2 = Operation(
        operation_id="context-2",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(context_op2)

    # Add existing step with parent context-1
    step_op = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        parent_id="context-1",
    )
    execution.operations.append(step_op)

    # Try to update with different parent context-2 (which exists, so passes parent validation)
    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.SUCCEED,
            parent_id="context-2",
        )
    ]

    with pytest.raises(
        InvalidParameterValueException, match="Inconsistent parent operation id"
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_invalid_duplicate_wait_operations():
    """Test validation fails with duplicate WAIT operations."""
    execution = _create_test_execution()

    # WAIT operations cannot have duplicate updates in same batch
    updates = [
        OperationUpdate(
            operation_id="wait-1",
            operation_type=OperationType.WAIT,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="wait-1",
            operation_type=OperationType.WAIT,
            action=OperationAction.CANCEL,
        ),
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot checkpoint multiple operations with the same ID",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_invalid_duplicate_callback_operations():
    """Test validation fails with duplicate CALLBACK operations."""
    execution = _create_test_execution()

    # CALLBACK operations cannot have duplicate updates in same batch
    updates = [
        OperationUpdate(
            operation_id="callback-1",
            operation_type=OperationType.CALLBACK,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="callback-1",
            operation_type=OperationType.CALLBACK,
            action=OperationAction.SUCCEED,
        ),
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot checkpoint multiple operations with the same ID",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_invalid_duplicate_invoke_operations():
    """Test validation fails with duplicate CHAINED_INVOKE operations."""
    execution = _create_test_execution()

    # CHAINED_INVOKE operations cannot have duplicate updates in same batch
    updates = [
        OperationUpdate(
            operation_id="invoke-1",
            operation_type=OperationType.CHAINED_INVOKE,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="invoke-1",
            operation_type=OperationType.CHAINED_INVOKE,
            action=OperationAction.SUCCEED,
        ),
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot checkpoint multiple operations with the same ID",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_invalid_duplicate_execution_operations():
    """Test validation fails with duplicate EXECUTION operations."""
    execution = _create_test_execution()

    # EXECUTION operations cannot have duplicate updates in same batch
    # (though this is also caught by _validate_conflicting_execution_update)
    updates = [
        OperationUpdate(
            operation_id="exec-1",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
        ),
        OperationUpdate(
            operation_id="exec-1",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
        ),
    ]

    with pytest.raises(InvalidParameterValueException):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_duplicate_context_start_then_succeed():
    """Test validation allows CONTEXT START followed by SUCCEED."""
    execution = _create_test_execution()

    # CONTEXT operations can have START + non-START in same batch
    updates = [
        OperationUpdate(
            operation_id="context-1",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="context-1",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.SUCCEED,
        ),
    ]

    # Should not raise
    CheckpointValidator.validate_input(updates, execution)


def test_validate_invalid_duplicate_context_non_start():
    """Test validation fails with duplicate CONTEXT non-START operations."""
    execution = _create_test_execution()

    # CONTEXT operations cannot have duplicate non-START updates
    updates = [
        OperationUpdate(
            operation_id="context-1",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.SUCCEED,
        ),
        OperationUpdate(
            operation_id="context-1",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.SUCCEED,
        ),
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot checkpoint multiple operations with the same ID",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_invalid_duplicate_step_non_start():
    """Test validation fails with duplicate STEP non-START operations."""
    execution = _create_test_execution()

    # STEP operations cannot have duplicate non-START updates
    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.SUCCEED,
        ),
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.SUCCEED,
        ),
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot checkpoint multiple operations with the same ID",
    ):
        CheckpointValidator.validate_input(updates, execution)


# OperationTransformer tests

"""Unit tests for OperationTransformer."""

from unittest.mock import Mock

import pytest

from async_durable_execution.models import (
    OperationAction,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.runner.processors.base import (
    OperationProcessor,
)
from async_durable_execution.runner.processor import (
    OperationTransformer,
)
from async_durable_execution.runner.exceptions import (
    InvalidParameterValueException,
)


class MockProcessor(OperationProcessor):
    """Mock processor for testing."""

    def __init__(self, return_value=None):
        self.return_value = return_value
        self.process_calls = []

    def process(self, update, current_op, notifier, execution_arn):
        self.process_calls.append((update, current_op, notifier, execution_arn))
        return self.return_value


def test_init_with_default_processors():
    """Test initialization with default processors."""
    transformer = OperationTransformer()

    assert OperationType.STEP in transformer.processors
    assert OperationType.WAIT in transformer.processors
    assert OperationType.CONTEXT in transformer.processors
    assert OperationType.CALLBACK in transformer.processors
    assert OperationType.EXECUTION in transformer.processors
    assert OperationType.CHAINED_INVOKE in transformer.processors


def test_init_with_custom_processors():
    """Test initialization with custom processors."""
    custom_processors = {OperationType.STEP: MockProcessor()}
    transformer = OperationTransformer(processors=custom_processors)

    assert transformer.processors == custom_processors


def test_process_updates_empty_lists():
    """Test processing with empty updates and operations."""
    transformer = OperationTransformer()
    notifier = Mock()

    operations, updates = transformer.process_updates([], [], notifier, "arn:test")

    assert operations == []
    assert updates == []


def test_process_updates_processor_not_found_raises_error():
    """Test that missing processor raises InvalidParameterValueException."""
    transformer = OperationTransformer(processors={OperationType.STEP: MockProcessor()})
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
    )
    notifier = Mock()

    with pytest.raises(
        InvalidParameterValueException,
        match="Checkpoint for OperationType.WAIT is not implemented yet.",
    ):
        transformer.process_updates([update], [], notifier, "arn:test")


def test_process_updates_processor_returns_none():
    """Test processing when processor returns None."""
    mock_processor = MockProcessor(return_value=None)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    notifier = Mock()

    operations, updates = transformer.process_updates(
        [update], [], notifier, "arn:test"
    )

    assert operations == []
    assert updates == [update]
    assert len(mock_processor.process_calls) == 1


def test_process_updates_new_operation():
    """Test processing creates new operation."""
    new_operation = Mock()
    new_operation.operation_id = "new-id"
    mock_processor = MockProcessor(return_value=new_operation)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="new-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    notifier = Mock()

    operations, updates = transformer.process_updates(
        [update], [], notifier, "arn:test"
    )

    assert len(operations) == 1
    assert operations[0] == new_operation
    assert updates == [update]


def test_process_updates_existing_operation():
    """Test processing updates existing operation."""
    existing_operation = Mock()
    existing_operation.operation_id = "existing-id"
    updated_operation = Mock()
    updated_operation.operation_id = "existing-id"

    mock_processor = MockProcessor(return_value=updated_operation)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="existing-id",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
    )
    notifier = Mock()

    operations, updates = transformer.process_updates(
        [update], [existing_operation], notifier, "arn:test"
    )

    assert len(operations) == 1
    assert operations[0] == updated_operation
    assert updates == [update]


def test_process_updates_multiple_operations_preserve_order():
    """Test processing multiple operations preserves order."""
    op1 = Mock()
    op1.operation_id = "op1"
    op2 = Mock()
    op2.operation_id = "op2"
    op3 = Mock()
    op3.operation_id = "op3"

    updated_op2 = Mock()
    updated_op2.operation_id = "op2"
    new_op4 = Mock()
    new_op4.operation_id = "op4"

    mock_processor = MockProcessor()
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    mock_processor.return_value = updated_op2

    updates = [
        OperationUpdate(
            operation_id="op2",
            operation_type=OperationType.STEP,
            action=OperationAction.SUCCEED,
        ),
    ]
    notifier = Mock()

    operations, result_updates = transformer.process_updates(
        updates, [op1, op2, op3], notifier, "arn:test"
    )

    assert len(operations) == 3
    assert operations[0] == op1
    assert operations[1] == updated_op2
    assert operations[2] == op3
    assert result_updates == updates

    mock_processor.return_value = new_op4
    updates2 = [
        OperationUpdate(
            operation_id="op4",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
    ]

    operations2, result_updates2 = transformer.process_updates(
        updates2, [op1, updated_op2, op3], notifier, "arn:test"
    )

    assert len(operations2) == 4
    assert operations2[0] == op1
    assert operations2[1] == updated_op2
    assert operations2[2] == op3
    assert operations2[3] == new_op4


def test_process_updates_multiple_processors():
    """Test processing with multiple processor types."""
    step_op = Mock()
    step_op.operation_id = "step-id"
    wait_op = Mock()
    wait_op.operation_id = "wait-id"

    step_processor = MockProcessor(return_value=step_op)
    wait_processor = MockProcessor(return_value=wait_op)

    transformer = OperationTransformer(
        processors={
            OperationType.STEP: step_processor,
            OperationType.WAIT: wait_processor,
        }
    )

    updates = [
        OperationUpdate(
            operation_id="step-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="wait-id",
            operation_type=OperationType.WAIT,
            action=OperationAction.START,
        ),
    ]
    notifier = Mock()

    operations, result_updates = transformer.process_updates(
        updates, [], notifier, "arn:test"
    )

    assert len(operations) == 2
    assert operations[0] == step_op
    assert operations[1] == wait_op
    assert len(step_processor.process_calls) == 1
    assert len(wait_processor.process_calls) == 1


def test_process_updates_passes_correct_parameters():
    """Test that correct parameters are passed to processor."""
    existing_op = Mock()
    existing_op.operation_id = "test-id"
    mock_processor = MockProcessor(return_value=existing_op)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    notifier = Mock()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    transformer.process_updates([update], [existing_op], notifier, execution_arn)

    call_args = mock_processor.process_calls[0]
    assert call_args[0] == update
    assert call_args[1] == existing_op
    assert call_args[2] == notifier
    assert call_args[3] == execution_arn


def test_process_updates_new_operation_not_in_map():
    """Test processing creates new operation when operation_id not in current operations."""
    new_operation = Mock()
    new_operation.operation_id = "new-id"
    mock_processor = MockProcessor(return_value=new_operation)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    # Existing operations with different IDs
    existing_op = Mock()
    existing_op.operation_id = "existing-id"

    update = OperationUpdate(
        operation_id="new-id",  # Different from existing operation
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    notifier = Mock()

    operations, updates = transformer.process_updates(
        [update], [existing_op], notifier, "arn:test"
    )

    # Should have both existing and new operation
    assert len(operations) == 2
    assert operations[0] == existing_op  # Original operation preserved
    assert operations[1] == new_operation  # New operation appended
    assert updates == [update]


def test_process_updates_in_place_update_with_multiple_operations():
    """Test in-place update when operation exists in middle of operations list."""
    # Create three operations
    op1 = Mock()
    op1.operation_id = "op1"
    op2 = Mock()
    op2.operation_id = "op2"
    op3 = Mock()
    op3.operation_id = "op3"

    # Updated version of op2
    updated_op2 = Mock()
    updated_op2.operation_id = "op2"

    mock_processor = MockProcessor(return_value=updated_op2)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    # Update for op2 (middle operation)
    update = OperationUpdate(
        operation_id="op2",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
    )
    notifier = Mock()

    # Process update with op2 in the middle of the list
    operations, updates = transformer.process_updates(
        [update], [op1, op2, op3], notifier, "arn:test"
    )

    # Verify in-place update occurred
    assert len(operations) == 3
    assert operations[0] == op1  # First operation unchanged
    assert operations[1] == updated_op2  # Middle operation updated in-place
    assert operations[2] == op3  # Last operation unchanged
    assert updates == [update]


def test_process_updates_in_place_update_break_coverage():
    """Test to ensure break statement in in-place update loop is covered."""
    # Create operations where target is first in list to ensure break is hit
    target_op = Mock()
    target_op.operation_id = "target"
    other_op = Mock()
    other_op.operation_id = "other"

    updated_target = Mock()
    updated_target.operation_id = "target"

    mock_processor = MockProcessor(return_value=updated_target)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="target",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
    )
    notifier = Mock()

    # Target operation is first - should hit break immediately
    operations, updates = transformer.process_updates(
        [update], [target_op, other_op], notifier, "arn:test"
    )

    assert len(operations) == 2
    assert operations[0] == updated_target


def test_process_updates_empty_operations_list():
    """Test for loop exit when result_operations is empty."""
    updated_op = Mock()
    updated_op.operation_id = "test-id"

    mock_processor = MockProcessor(return_value=updated_op)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
    )
    notifier = Mock()

    # Empty current_operations list - for loop should exit immediately
    operations, updates = transformer.process_updates(
        [update], [], notifier, "arn:test"
    )

    assert len(operations) == 1
    assert operations[0] == updated_op
