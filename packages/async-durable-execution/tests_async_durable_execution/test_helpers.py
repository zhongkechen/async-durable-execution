"""Test helpers for generating expected step IDs."""

from unittest.mock import Mock

from async_durable_execution.context import DurableContext, ExecutionContext
from async_durable_execution.execution import ExecutionState


def operation_id_sequence(parent_id: str | None = None):
    """Generator that yields step IDs in sequence using DurableContext."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test-arn"

    execution_context = ExecutionContext(durable_execution_arn="test-arn")
    context = DurableContext(
        state=mock_state, execution_context=execution_context, parent_id=parent_id
    )

    while True:
        yield context._create_step_id()  # noqa: SLF001
