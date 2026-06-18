"""Unit tests for OperationExecutor base helpers."""

from __future__ import annotations

import pytest
from abc import ABC
from unittest.mock import AsyncMock, Mock

from async_durable_execution.models import (
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationIdentifier,
)
from async_durable_execution.operation.base import (
    OperationExecutor,
)
from async_durable_execution.serdes import DEFAULT_JSON_SERDES
from async_durable_execution.state import CheckpointedResult


# Test fixtures and helpers


class ConcreteOperationExecutor(OperationExecutor[str]):
    """Concrete implementation for testing the abstract base class."""

    def __init__(self, state=None, operation_identifier=None):
        if state is None:
            state = Mock()
            state.durable_execution_arn = "test-arn"
        if operation_identifier is None:
            operation_identifier = OperationIdentifier(
                "test_op", OperationSubType.STEP, None, "test-name"
            )
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.execute_called = 0
        self.execute_result_to_return = "executed_result"

    async def execute(self, checkpointed_result: CheckpointedResult) -> str:
        """Mock implementation that returns configured result."""
        self.execute_called += 1
        return self.execute_result_to_return

    async def process(self) -> str:
        """Mock implementation that delegates to execute()."""
        return await self.execute(create_mock_checkpoint(OperationStatus.STARTED))


def create_mock_checkpoint(status: OperationStatus) -> CheckpointedResult:
    """Create a mock CheckpointedResult with the given status."""
    operation = Operation(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        status=status,
    )
    return CheckpointedResult.create_from_operation(operation)


async def test_operation_executor_common_properties_and_helpers():
    """Test OperationExecutor exposes shared fields and helpers."""
    state = Mock()
    state.durable_execution_arn = "arn:aws:lambda:us-west-2:123:function:test"
    checkpoint = create_mock_checkpoint(OperationStatus.STARTED)
    state.get_checkpoint_result.return_value = checkpoint
    operation_identifier = OperationIdentifier(
        "shared-op", OperationSubType.STEP, "parent-1", "shared-name"
    )
    executor = ConcreteOperationExecutor(
        state=state, operation_identifier=operation_identifier
    )

    assert executor.operation_id == "shared-op"
    assert executor.operation_name == "shared-name"
    assert executor.durable_execution_arn == state.durable_execution_arn
    assert executor.get_checkpointed_result() is checkpoint
    state.get_checkpoint_result.assert_called_once_with("shared-op")


async def test_operation_executor_common_serialization_helpers():
    """Test OperationExecutor serializes and deserializes with shared metadata."""
    executor = ConcreteOperationExecutor()

    serialized = executor.serialize_value({"hello": "world"}, DEFAULT_JSON_SERDES)
    deserialized = executor.deserialize_value(serialized, DEFAULT_JSON_SERDES)

    assert serialized == '{"hello": "world"}'
    assert deserialized == {"hello": "world"}


async def test_operation_executor_create_checkpoint_uses_default_signature():
    """Test create_checkpoint omits is_sync when using the default behavior."""
    state = Mock()
    state.durable_execution_arn = "test-arn"
    state._create_checkpoint_async = AsyncMock()
    executor = ConcreteOperationExecutor(state=state)
    operation_update = Mock()

    await executor.create_checkpoint(operation_update)

    state._create_checkpoint_async.assert_called_once_with(
        operation_update=operation_update
    )


async def test_operation_executor_create_checkpoint_passes_is_sync_override():
    """Test create_checkpoint forwards explicit is_sync overrides."""
    state = Mock()
    state.durable_execution_arn = "test-arn"
    state._create_checkpoint_async = AsyncMock()
    executor = ConcreteOperationExecutor(state=state)
    operation_update = Mock()

    await executor.create_checkpoint(operation_update, is_sync=False)

    state._create_checkpoint_async.assert_called_once_with(
        operation_update=operation_update,
        is_sync=False,
    )


def test_operation_executor_requires_subclass_process_and_execute():
    """Test OperationExecutor remains abstract for process and execute."""

    class IncompleteOperationExecutor(OperationExecutor[str], ABC):
        pass

    state = Mock()
    state.durable_execution_arn = "test-arn"

    with pytest.raises(TypeError):
        IncompleteOperationExecutor(
            state=state,
            operation_identifier=OperationIdentifier(
                "test_op", OperationSubType.STEP, None, "test-name"
            ),
        )
