"""Tests for step_semantics_at_most_once example."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationType
from async_durable_execution_examples.step import step_semantics_at_most_once


async def test_step_semantics_at_most_once(durable_runner):
    """Test step with at-most-once semantics."""
    async with durable_runner(
        handler=step_semantics_at_most_once.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert (
        result.get_deserialized_result() == "Result: AT_MOST_ONCE_PER_RETRY semantics"
    )

    # Verify step operation exists with correct name
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 1
    assert step_ops[0].name == "at_most_once_step"
