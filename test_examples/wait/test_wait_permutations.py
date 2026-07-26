"""Tests for wait operation permutations."""

from async_durable_execution import InvocationStatus
from examples.wait import wait_with_name


async def test_wait_with_name(durable_runner):
    """Test wait with explicit name."""
    async with durable_runner(
        handler=wait_with_name.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "Wait with name completed"

    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 1
    assert wait_ops[0].name == "custom_wait"
