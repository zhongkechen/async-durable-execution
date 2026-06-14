"""Tests for step example."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution_examples.plugin import execution_with_plugin


async def test_plugin(durable_runner):
    """Test basic step example."""
    with durable_runner(
        handler=execution_with_plugin.handler, input="{}", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == 12

    step_result = result.get_step("add-result-to-2")
    assert step_result.get_deserialized_result() == 12
