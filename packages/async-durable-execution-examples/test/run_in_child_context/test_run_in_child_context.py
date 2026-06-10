"""Tests for run_in_child_context example."""

import pytest
from async_durable_execution.execution import InvocationStatus

from async_durable_execution_examples.run_in_child_context import run_in_child_context
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=run_in_child_context.handler,
)
def test_run_in_child_context(durable_runner):
    """Test run_in_child_context example."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "Child context result: 10"

    # Verify child context operation exists
    context_ops = [
        op for op in result.operations if op.operation_type.value == "CONTEXT"
    ]
    assert len(context_ops) >= 1
