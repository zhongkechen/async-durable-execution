"""Tests for wait example."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution_examples.wait import wait


async def test_wait(durable_runner):
    """Test wait example."""
    with durable_runner(handler=wait.handler, input="test", timeout=10) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "Wait completed"

    # Find the wait operation (it should be the only non-execution operation)
    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 1
    wait_op = wait_ops[0]
    assert wait_op.scheduled_end_timestamp is not None
