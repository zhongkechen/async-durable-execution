"""Unit tests for OperationExecutor base helpers."""

from __future__ import annotations

import pytest
from abc import ABC
from unittest.mock import AsyncMock, Mock

from async_durable_execution.core.models import (
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationIdentifier,
)
from async_durable_execution.primitive.base import OperationExecutor
from async_durable_execution.core.serdes import DEFAULT_JSON_SERDES


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
        self.start_called = 0
        self.replay_called = 0

    async def execute(self, operation: Operation | None) -> str:
        """Mock implementation that returns configured result."""
        self.execute_called += 1
        return self.execute_result_to_return

    async def start(self) -> str:
        """Mock implementation for a new operation."""
        self.start_called += 1
        return "started_result"

    async def replay(self, operation: Operation) -> str:
        """Mock implementation for a replayed operation."""
        self.replay_called += 1
        self.execute_called += 1
        return self.execute_result_to_return


def create_mock_operation(status: OperationStatus) -> Operation:
    """Create a mock operation with the given status."""
    return Operation(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        status=status,
    )


async def test_operation_executor_common_properties():
    """Test OperationExecutor exposes shared fields."""
    state = Mock()
    state.durable_execution_arn = "arn:aws:lambda:us-west-2:123:function:test"
    operation_identifier = OperationIdentifier(
        "shared-op", OperationSubType.STEP, "parent-1", "shared-name"
    )
    executor = ConcreteOperationExecutor(
        state=state, operation_identifier=operation_identifier
    )

    assert executor.operation_id == "shared-op"
    assert executor.operation_name == "shared-name"
    assert executor.durable_execution_arn == state.durable_execution_arn


async def test_operation_executor_common_serialization_helpers():
    """Test OperationExecutor serializes and deserializes with shared metadata."""
    executor = ConcreteOperationExecutor()

    serialized = await executor.serialize_value({"hello": "world"}, DEFAULT_JSON_SERDES)
    deserialized = await executor.deserialize_value(serialized, DEFAULT_JSON_SERDES)

    assert serialized == '{"hello": "world"}'
    assert deserialized == {"hello": "world"}


async def test_operation_executor_create_checkpoint_uses_default_signature():
    """Test create_checkpoint omits is_sync when using the default behavior."""
    state = Mock()
    state.durable_execution_arn = "test-arn"
    state.create_checkpoint = AsyncMock()
    executor = ConcreteOperationExecutor(state=state)
    operation_update = Mock()

    await executor.create_checkpoint(operation_update)

    state.create_checkpoint.assert_called_once_with(operation_update=operation_update)


async def test_operation_executor_create_checkpoint_passes_is_sync_override():
    """Test create_checkpoint forwards explicit is_sync overrides."""
    state = Mock()
    state.durable_execution_arn = "test-arn"
    state.create_checkpoint = AsyncMock()
    executor = ConcreteOperationExecutor(state=state)
    operation_update = Mock()

    await executor.create_checkpoint(operation_update, is_sync=False)

    state.create_checkpoint.assert_called_once_with(
        operation_update=operation_update,
        is_sync=False,
    )


async def test_operation_executor_process_dispatches_to_start_for_new_operations():
    """Test base process dispatches to start when no checkpoint exists."""
    state = Mock()
    state.durable_execution_arn = "test-arn"
    state.operations.get.return_value = None
    executor = ConcreteOperationExecutor(state=state)

    result = await executor.process()

    assert result == "started_result"
    assert executor.start_called == 1
    assert executor.replay_called == 0
    assert executor.execute_called == 0


async def test_operation_executor_process_dispatches_to_replay_for_existing_operations():
    """Test base process dispatches to replay when a checkpoint exists."""
    state = Mock()
    state.durable_execution_arn = "test-arn"
    state.operations.get.return_value = create_mock_operation(OperationStatus.STARTED)
    executor = ConcreteOperationExecutor(state=state)

    result = await executor.process()

    assert result == "executed_result"
    assert executor.start_called == 0
    assert executor.replay_called == 1
    assert executor.execute_called == 1


def test_operation_executor_requires_subclass_start_and_replay():
    """Test OperationExecutor remains abstract for start and replay."""

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


async def test_operation_executor_execute_is_not_abstract():
    """Test execute is not required for subclasses that implement start and replay."""

    class MinimalOperationExecutor(OperationExecutor[str]):
        async def start(self) -> str:
            return "started"

        async def replay(self, operation: Operation) -> str:
            return "replayed"

    state = Mock()
    state.durable_execution_arn = "test-arn"

    executor = MinimalOperationExecutor(
        state=state,
        operation_identifier=OperationIdentifier(
            "test_op", OperationSubType.STEP, None, "test-name"
        ),
    )

    assert not hasattr(executor, "execute")
    state.operations.get.return_value = None
    assert await executor.process() == "started"

    state.operations.get.return_value = create_mock_operation(OperationStatus.SUCCEEDED)
    assert await executor.process() == "replayed"
