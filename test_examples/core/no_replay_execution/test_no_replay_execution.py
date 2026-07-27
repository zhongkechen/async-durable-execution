"""Tests for no_replay_execution."""

from async_durable_execution import InvocationStatus
from examples.core.no_replay_execution import no_replay_execution


async def test_handle_step_operations_when_no_replay_occurs(durable_runner):
    """Test step operations when no replay occurs."""
    async with durable_runner(
        handler=no_replay_execution.handler, input=None, timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    # Verify final result
    assert result.get_deserialized_result() == {"completed": True}

    # Get step operations
    user1_step = result.get_step("fetch-user-1")
    user2_step = result.get_step("fetch-user-2")

    # Verify first-time execution tracking (no replay)
    assert user1_step.operation_type.value == "STEP"
    assert user1_step.status.value == "SUCCEEDED"
    assert result.get_operation_deserialized_result(user1_step) == "user-1"

    assert user2_step.operation_type.value == "STEP"
    assert user2_step.status.value == "SUCCEEDED"
    assert result.get_operation_deserialized_result(user2_step) == "user-2"

    # Verify both operations tracked
    assert len(result.operations) == 2
