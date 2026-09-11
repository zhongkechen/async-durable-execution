"""Tests for step example."""

from async_durable_execution import InvocationStatus
from examples.step import step


async def test_step(durable_runner) -> None:
    """Test basic step example."""
    async with durable_runner(handler=step.handler, input="test", timeout=10) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == 8

    step_result = result.get_step("add_numbers")
    assert result.get_operation_deserialized_result(step_result) == 8
