"""Tests for run_in_child_context example."""

from async_durable_execution import InvocationStatus
from examples.run_in_child_context import run_in_child_context


async def test_run_in_child_context(durable_runner) -> None:
    """Test run_in_child_context example."""
    async with durable_runner(
        handler=run_in_child_context.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "Child context result: 10"

    # Verify child context operation exists
    context_ops = [
        op for op in result.operations if op.operation_type.value == "CONTEXT"
    ]
    assert len(context_ops) >= 1
