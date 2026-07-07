"""Tests for step_with_retry example."""

from async_durable_execution import InvocationStatus
from async_durable_execution_examples.step import step_with_retry


async def test_step_with_retry(durable_runner):
    """Test step with retry configuration.

    With counter-based deterministic behavior:
    - Attempt 1: counter = 1 < 2 → raises RuntimeError ❌
    - Attempt 2: counter = 2 >= 2 → succeeds ✓

    The function deterministically fails once then succeeds on the second attempt.
    """
    async with durable_runner(
        handler=step_with_retry.handler, input="test", timeout=30
    ) as runner:
        result = await runner.run()

    # With counter-based deterministic behavior, succeeds on attempt 2
    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "Operation succeeded"

    # The step should have succeeded on attempt 2 (after 1 failure)
    # Attempt numbering: 1 (initial attempt), 2 (first retry)
    step_op = result.get_step("unreliable_operation")
    assert (
        step_op.step_details.attempt == 2
    )  # Succeeded on first retry (1-indexed: 2=first retry)
