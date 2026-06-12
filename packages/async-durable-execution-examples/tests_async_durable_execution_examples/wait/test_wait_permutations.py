"""Tests for wait operation permutations."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution_examples.wait import wait_with_name


def test_wait_with_name(durable_runner):
    """Test wait with explicit name."""
    with durable_runner(
        handler=wait_with_name.handler, input="test", timeout=10
    ) as runner:
        result = runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "Wait with name completed"

    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 1
    assert wait_ops[0].name == "custom_wait"
