"""Tests for steps_with_retry."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationType
from examples.step import steps_with_retry


async def test_steps_with_retry(durable_runner) -> None:
    """Test steps_with_retry pattern.

    With counter-based deterministic behavior:
    - Poll 1, Attempt 1: counter = 1 → raises RuntimeError ❌
    - Poll 1, Attempt 2: counter = 2 → returns None
    - Poll 2, Attempt 1: counter = 3 → returns item ✓

    The function finds the item on poll 2 after 1 retry on poll 1.
    """
    async with durable_runner(
        handler=steps_with_retry.handler, input={"name": "test-item"}, timeout=30
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    # With counter-based deterministic behavior, finds item on poll 2
    result_data = result.get_deserialized_result()
    assert isinstance(result_data, dict)
    assert result_data.get("success") is True
    assert result_data.get("pollsRequired") == 2
    assert "item" in result_data
    assert result_data["item"]["id"] == "test-item"

    # Verify step operations exist
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    # Should have exactly 2 step operations (poll 1 and poll 2)
    assert len(step_ops) == 2

    # Poll 1: succeeded after 1 retry (returned None)
    poll_1 = result.get_step("get_item_poll_1")
    assert poll_1.step_details.result == "null"
    assert (
        poll_1.step_details.attempt == 2
    )  # 1 retry occurred (1-indexed: 2=first retry)

    # Poll 2: succeeded immediately (returned item)
    poll_2 = result.get_step("get_item_poll_2")
    assert poll_2.step_details.attempt == 1  # No retries needed (1-indexed: 1=initial)
