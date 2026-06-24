"""Tests for run_in_child_context_failing_step."""

from async_durable_execution import InvocationStatus
from async_durable_execution_examples.run_in_child_context import (
    run_in_child_context_step_failure,
)


async def test_succeed_despite_failing_step_in_child_context(durable_runner):
    """Test that execution succeeds despite failing step in child context."""
    with durable_runner(
        handler=run_in_child_context_step_failure.handler, input=None, timeout=30
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()
    assert result_data == {"success": True, "error": "Step failed in child context"}
