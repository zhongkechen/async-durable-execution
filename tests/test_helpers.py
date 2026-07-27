"""Test helpers for generating expected step IDs."""

from unittest.mock import Mock

from async_durable_execution import DurableContext
from async_durable_execution._core.execution import ExecutionState
from async_durable_execution._core.models import OperationIdentifier, OperationSubType


def operation_id_sequence(parent_id: str | None = None):
    """Generator that yields step IDs in sequence using DurableContext."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test-arn"

    context = DurableContext(
        execution_state=mock_state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id=parent_id,
        ),
    )

    while True:
        yield context.step_counter.create_step_id()  # noqa: SLF001
