"""Tests for wait operation permutations."""

import pytest
from async_durable_execution.execution import InvocationStatus

from async_durable_execution_examples.wait import wait_with_name
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_with_name.handler,
)
def test_wait_with_name(durable_runner):
    """Test wait with explicit name."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "Wait with name completed"

    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 1
    assert wait_ops[0].name == "custom_wait"
